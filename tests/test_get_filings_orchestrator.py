"""End-to-end orchestrator test for ``get_filings`` — mocked downloader, real
parsers, real storage backends.

Goal: exercise the full glue code in ``piboufilings.__init__`` without any
network I/O. We feed a canned index DataFrame and pre-staged raw filing files
on disk; the orchestrator should pick them up, parse them, and land rows in
both CSV and DuckDB backends.
"""

from __future__ import annotations

import textwrap

import pandas as pd
import pytest

import piboufilings
import piboufilings.core.downloader as downloader_module

_SAMPLE_13F = textwrap.dedent(
    """
    SEC-HEADER
    ACCESSION NUMBER: 0001067983-24-000001
    CONFORMED SUBMISSION TYPE: 13F-HR
    CENTRAL INDEX KEY: 0001067983
    IRS NUMBER: 12-3456789
    CONFORMED PERIOD OF REPORT: 20231231
    FILED AS OF DATE: 20240131
    form13FFileNumber>028-12345</form13FFileNumber>
    tableEntryTotal>1</tableEntryTotal>
    tableValueTotal>100</tableValueTotal>
    <PAGE>
    <XML><placeholder>header</placeholder></XML>
    <XML>
    <?xml version="1.0" encoding="UTF-8"?>
    <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
      <infoTable>
        <nameOfIssuer>BERKSHIRE TEST HOLDING</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>000000000</cusip>
        <value>12345</value>
        <shrsOrPrnAmt>
          <sshPrnamt>100</sshPrnamt>
          <sshPrnamtType>SH</sshPrnamtType>
        </shrsOrPrnAmt>
        <votingAuthority><Sole>100</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
    </informationTable>
    </XML>
    """
).strip()


@pytest.fixture
def stubbed_download(monkeypatch, tmp_path):
    """Replace ``SECDownloader.get_sec_index_data`` and ``download_filings`` so
    the orchestrator runs purely on local fixtures."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    raw_file = raw_dir / "filing.txt"
    raw_file.write_text(_SAMPLE_13F)

    canned_index = pd.DataFrame(
        [
            {
                "CIK": "0001067983",
                "Name": "BERKSHIRE TEST",
                "Date Filed": "2024-01-31",
                "Form Type": "13F-HR",
                "accession_number": "0001067983-24-000001",
                "Filename": "edgar/data/1067983/0001067983-24-000001.txt",
            }
        ]
    )
    download_result = pd.DataFrame(
        [
            {
                "cik": "0001067983",
                "accession_number": "0001067983-24-000001",
                "form_type": "13F-HR",
                "download_date": "2024-01-31",
                "raw_path": str(raw_file),
                "url": "https://example.test/filing.txt",
            }
        ]
    )

    def fake_get_index(self, *a, **kw):
        return canned_index

    def fake_download(self, *a, **kw):
        return download_result

    monkeypatch.setattr(downloader_module.SECDownloader, "get_sec_index_data", fake_get_index)
    monkeypatch.setattr(downloader_module.SECDownloader, "download_filings", fake_download)
    return raw_file


def test_get_filings_writes_csv_output(stubbed_download, tmp_path):
    base = tmp_path / "parsed"
    piboufilings.get_filings(
        user_name="Reviewer",
        user_agent_email="reviewer@example.com",
        cik="0001067983",
        form_type="13F-HR",
        start_year=2023,
        end_year=2023,
        base_dir=str(base),
        log_dir=str(tmp_path / "logs"),
        raw_data_dir=str(tmp_path / "raw_root"),
        show_progress=False,
        max_workers=1,
        keep_raw_files=True,
        export_format="csv",
    )

    info_path = base / "13f_info_2023_Q4.csv"
    holdings_path = base / "13f_holdings_2023_Q4.csv"
    assert info_path.exists(), "CSV info file missing"
    assert holdings_path.exists(), "CSV holdings file missing"
    holdings_df = pd.read_csv(holdings_path)
    assert len(holdings_df) == 1
    assert holdings_df.iloc[0]["NAME_OF_ISSUER"] == "BERKSHIRE TEST HOLDING"
    assert holdings_df.iloc[0]["CUSIP"] == 0  # parsed as int from "000000000"


def test_get_filings_writes_duckdb_output(stubbed_download, tmp_path):
    duckdb = pytest.importorskip("duckdb")
    base = tmp_path / "parsed"
    piboufilings.get_filings(
        user_name="Reviewer",
        user_agent_email="reviewer@example.com",
        cik="0001067983",
        form_type="13F-HR",
        start_year=2023,
        end_year=2023,
        base_dir=str(base),
        log_dir=str(tmp_path / "logs"),
        raw_data_dir=str(tmp_path / "raw_root"),
        show_progress=False,
        max_workers=1,
        keep_raw_files=True,
        export_format="duckdb",
    )

    db_path = base / "piboufilings.duckdb"
    assert db_path.exists(), "DuckDB output file missing"
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            row[0] for row in con.execute("SELECT table_name FROM information_schema.tables").fetchall()
        }
        assert "filing_info_13f" in tables
        assert "holdings_13f" in tables
        assert con.execute("SELECT COUNT(*) FROM holdings_13f").fetchone()[0] == 1
        name = con.execute("SELECT NAME_OF_ISSUER FROM holdings_13f").fetchone()[0]
        assert name == "BERKSHIRE TEST HOLDING"
    finally:
        con.close()


def test_get_filings_unknown_export_format_raises(tmp_path):
    with pytest.raises(ValueError, match="Unknown export_format"):
        piboufilings.get_filings(
            user_name="Reviewer",
            user_agent_email="reviewer@example.com",
            cik="0001067983",
            form_type="13F-HR",
            start_year=2023,
            end_year=2023,
            base_dir=str(tmp_path / "parsed"),
            log_dir=str(tmp_path / "logs"),
            raw_data_dir=str(tmp_path / "raw_root"),
            show_progress=False,
            export_format="parquet",
        )
