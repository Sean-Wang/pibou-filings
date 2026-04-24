"""Time-boundary tests: partition bucketing + default-year derivation must be
deterministic regardless of when/where the test runs."""

from __future__ import annotations

import pandas as pd
import pytest
from freezegun import freeze_time

from piboufilings.core.downloader import SECDownloader


@pytest.fixture
def downloader(tmp_path, monkeypatch):
    dl = SECDownloader(
        user_name="Test",
        user_agent_email="test@example.com",
        log_dir=tmp_path / "logs",
        data_dir=tmp_path / "raw",
        max_workers=1,
    )
    monkeypatch.setattr(dl, "_respect_rate_limit", lambda: None)
    return dl


@freeze_time("2024-12-31 23:59:59")
def test_partitioning_is_stable_at_year_end(downloader):
    filings = pd.DataFrame(
        [
            {"Date Filed": "20241230", "Filename": "edgar/data/1/a-1.txt"},
            {"Date Filed": "20241231", "Filename": "edgar/data/1/a-2.txt"},
            {"Date Filed": "20250101", "Filename": "edgar/data/1/a-3.txt"},
        ]
    )
    buckets = downloader._partition_filings_by_period(filings, "13F-HR", "0000000001")
    assert [k for k, _ in buckets] == ["2024-Q4", "2025-Q1"]


@freeze_time("2025-01-01 00:00:01")
def test_partitioning_is_stable_just_after_year_end(downloader):
    filings = pd.DataFrame(
        [
            {"Date Filed": "20241231", "Filename": "edgar/data/1/a-1.txt"},
            {"Date Filed": "20250101", "Filename": "edgar/data/1/a-2.txt"},
        ]
    )
    buckets = downloader._partition_filings_by_period(filings, "13F-HR", "0000000001")
    # Identical result to the pre-midnight call: the bucketing depends only on
    # the filing's own `Date Filed`, not on wall-clock time.
    assert [k for k, _ in buckets] == ["2024-Q4", "2025-Q1"]


@freeze_time("2024-06-15 12:00:00")
def test_default_start_year_uses_wall_clock_year(monkeypatch, tmp_path):
    """If a user calls get_filings with no start_year, we default to the
    current calendar year. Freeze time so the test never breaks on New Year's
    Eve."""
    import piboufilings
    import piboufilings.core.downloader as downloader_module

    captured = {}

    def fake_get_index(self, start_year, end_year, **kw):
        captured["start_year"] = start_year
        captured["end_year"] = end_year
        return pd.DataFrame(
            columns=["CIK", "Name", "Date Filed", "Form Type", "accession_number", "Filename"]
        )

    monkeypatch.setattr(downloader_module.SECDownloader, "get_sec_index_data", fake_get_index)

    piboufilings.get_filings(
        user_name="X",
        user_agent_email="x@example.com",
        cik=None,
        form_type="13F-HR",
        base_dir=str(tmp_path / "parsed"),
        log_dir=str(tmp_path / "logs"),
        raw_data_dir=str(tmp_path / "raw_root"),
        show_progress=False,
        export_format="csv",
    )
    assert captured == {"start_year": 2024, "end_year": 2024}
