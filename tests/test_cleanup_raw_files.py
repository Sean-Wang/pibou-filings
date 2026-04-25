"""Unit test for the extracted ``_cleanup_raw_files_for_cik`` helper."""

from __future__ import annotations

import pandas as pd

from piboufilings import _cleanup_raw_files_for_cik
from piboufilings.core.logger import FilingLogger


def test_cleanup_removes_raw_files_and_empty_parents(tmp_path):
    """Non-13F-file-number layout: ``<data>/<cik>/<form>/filename``.

    This is the layout ``_save_raw_filing`` produces when no ``form 13F file
    number`` is available (i.e. Section 16 / NPORT, or 13F with the sentinel
    ``unknown_13F_file_number``). The cleanup helper is built for this shape.
    """
    cik_dir = tmp_path / "data" / "0000000001"
    form_dir = cik_dir / "13F-HR"
    form_dir.mkdir(parents=True)
    raw_file = form_dir / "filing.txt"
    raw_file.write_text("body")

    df = pd.DataFrame([{"cik": "0000000001", "raw_path": str(raw_file), "accession_number": "x"}])
    logger = FilingLogger(log_dir=tmp_path / "logs")

    _cleanup_raw_files_for_cik(df, cik="0000000001", form_type="13F-HR", logger=logger)

    assert not raw_file.exists(), "raw file should be deleted"
    assert not form_dir.exists(), "empty form_dir should be removed"
    assert not cik_dir.exists(), "empty cik_dir should be removed"


def test_cleanup_13f_nested_accession_layout_leaves_identifier_dir(tmp_path):
    """When ``_save_raw_filing`` uses the ``FORM_13F_FILE_NUMBER`` path
    (accession subdir), cleanup removes the raw file and all now-empty parent
    directories up to the raw root.
    """
    identifier_dir = tmp_path / "data" / "028_12345"
    form_dir = identifier_dir / "13F-HR"
    accession_dir = form_dir / "0000001-01-000001"
    accession_dir.mkdir(parents=True)
    raw_file = accession_dir / "filing.txt"
    raw_file.write_text("body")

    df = pd.DataFrame([{"cik": "0000000001", "raw_path": str(raw_file), "accession_number": "x"}])
    logger = FilingLogger(log_dir=tmp_path / "logs")

    _cleanup_raw_files_for_cik(df, cik="0000000001", form_type="13F-HR", logger=logger)

    assert not raw_file.exists()
    # The accession-specific subdir is removed (it was empty).
    assert not accession_dir.exists()
    # form_dir is also now empty → removed.
    assert not form_dir.exists()
    # identifier_dir is also now empty → removed.
    assert not identifier_dir.exists()


def test_cleanup_does_nothing_for_empty_df(tmp_path):
    logger = FilingLogger(log_dir=tmp_path / "logs")
    _cleanup_raw_files_for_cik(pd.DataFrame(), cik="x", form_type="13F-HR", logger=logger)
    _cleanup_raw_files_for_cik(None, cik="x", form_type="13F-HR", logger=logger)
    # No exception; nothing to check.


def test_cleanup_preserves_non_empty_parent(tmp_path):
    """If a sibling file remains in the parent, we do not remove the parent."""
    identifier_dir = tmp_path / "data" / "028_12345"
    form_dir = identifier_dir / "13F-HR"
    form_dir.mkdir(parents=True)
    raw_file = form_dir / "a.txt"
    raw_file.write_text("x")
    sibling = form_dir / "b.txt"
    sibling.write_text("y")

    df = pd.DataFrame([{"cik": "1", "raw_path": str(raw_file), "accession_number": "a"}])
    logger = FilingLogger(log_dir=tmp_path / "logs")

    _cleanup_raw_files_for_cik(df, cik="1", form_type="13F-HR", logger=logger)

    assert not raw_file.exists()
    assert sibling.exists()
    assert form_dir.exists()
    assert identifier_dir.exists()
