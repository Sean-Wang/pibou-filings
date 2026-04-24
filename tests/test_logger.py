"""Tests for ``FilingLogger`` — CSV shape and concurrent-write safety."""

from __future__ import annotations

import csv
import threading

import pandas as pd

from piboufilings.core.logger import FilingLogger


def test_logger_creates_file_with_expected_headers(tmp_path):
    logger = FilingLogger(log_dir=tmp_path)
    assert logger.log_file.exists()
    with open(logger.log_file) as f:
        header = next(csv.reader(f))
    assert header == [
        "timestamp",
        "level",
        "operation_type",
        "cik",
        "form_type_processed",
        "accession_number",
        "download_success",
        "download_error_message",
        "parse_success",
        "error_code",
        "custom_identifier",
    ]


def test_logger_infers_level_from_success_flags(tmp_path):
    logger = FilingLogger(log_dir=tmp_path)

    logger.log_operation(operation_type="OK", download_success=True)
    logger.log_operation(operation_type="OK_PARSE", download_success=True, parse_success=True)
    logger.log_operation(operation_type="WARN_PARSE", download_success=True, parse_success=False)
    logger.log_operation(operation_type="FAIL_DOWNLOAD", download_success=False)
    logger.log_operation(operation_type="EXPLICIT_DEBUG", level="DEBUG")

    df = pd.read_csv(logger.log_file)
    levels = dict(zip(df["operation_type"], df["level"], strict=False))
    assert levels == {
        "OK": "INFO",
        "OK_PARSE": "INFO",
        "WARN_PARSE": "WARN",
        "FAIL_DOWNLOAD": "ERROR",
        "EXPLICIT_DEBUG": "DEBUG",
    }


def test_logger_creates_nested_dirs(tmp_path):
    """Regression: log_dir with non-existent parents should not fail."""
    deep = tmp_path / "a" / "b" / "c"
    logger = FilingLogger(log_dir=deep)
    assert logger.log_file.exists()


def test_logger_concurrent_writes_produce_valid_csv(tmp_path):
    """100 threads each emit 10 rows. Every row must round-trip through the
    CSV reader with the correct header width and no truncated/interleaved lines.
    """
    logger = FilingLogger(log_dir=tmp_path)

    def writer(i: int):
        for j in range(10):
            logger.log_operation(
                operation_type="BENCH",
                cik=f"{i:010d}",
                accession_number=f"acc-{i}-{j}",
                download_success=True,
                download_error_message=f"thread {i} msg {j}",
            )

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    df = pd.read_csv(logger.log_file)
    # 1 header + 100 threads × 10 rows = 1000 data rows.
    assert len(df) == 1000
    assert set(df["operation_type"].unique()) == {"BENCH"}
    # All CIKs accounted for; each CIK should appear exactly 10 times.
    counts = df["cik"].value_counts()
    assert (counts == 10).all()


def test_logger_parse_success_none_is_serialized_as_empty(tmp_path):
    """When ``parse_success`` is explicitly None (info-style events), the CSV
    cell should be empty — not the string 'False'."""
    logger = FilingLogger(log_dir=tmp_path)
    logger.log_operation(
        operation_type="INFO",
        cik="0000000001",
        download_success=True,
        parse_success=None,
    )
    df = pd.read_csv(logger.log_file, keep_default_na=False, dtype=str)
    row = df.iloc[0]
    assert row["parse_success"] == ""
