"""Tests for the resume / recovery feature.

Covers six scenarios:
1. Fresh run, resume=True: everything downloaded.
2. Level A (backend has accession): download + parse both skipped.
3. Level B (raw on disk, backend empty): download skipped, parse runs.
4. resume=False: opt-out; all accessions re-downloaded.
5. CSV backend variant of (2).
6. Legacy DuckDB without ACCESSION_NUMBER column: no crash, runs fresh.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pandas as pd
import pytest

import piboufilings
import piboufilings.core.downloader as downloader_module

duckdb = pytest.importorskip("duckdb")


def _sample_13f(accession: str, cusip: str = "000000000", conformed: str = "20231231") -> str:
    return textwrap.dedent(
        f"""
        SEC-HEADER
        ACCESSION NUMBER: {accession}
        CONFORMED SUBMISSION TYPE: 13F-HR
        CENTRAL INDEX KEY: 0001067983
        IRS NUMBER: 12-3456789
        CONFORMED PERIOD OF REPORT: {conformed}
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
            <nameOfIssuer>TEST HOLDING {accession}</nameOfIssuer>
            <titleOfClass>COM</titleOfClass>
            <cusip>{cusip}</cusip>
            <value>12345</value>
            <shrsOrPrnAmt><sshPrnamt>100</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
            <votingAuthority><Sole>100</Sole><Shared>0</Shared><None>0</None></votingAuthority>
          </infoTable>
        </informationTable>
        </XML>
        """
    ).strip()


@pytest.fixture
def three_filings(tmp_path):
    """Stage three raw 13F filings on disk under a predictable layout and
    stub the downloader so the orchestrator uses them. Returns a dict with:

    - ``raw_dir``: where the raw files live
    - ``accessions``: list of the three accession strings
    - ``raw_paths``: {accession -> path}
    - ``http_called``: list (populated by the fake download call)
    """
    raw_dir = tmp_path / "raw"
    accessions = [
        "0001067983-24-000001",
        "0001067983-24-000002",
        "0001067983-24-000003",
    ]
    cusips = ["000000001", "000000002", "000000003"]
    # Distinct CONFORMED_DATE per filing so the 13F PK
    # (SEC_FILE_NUMBER, CONFORMED_DATE) doesn't collapse all three into one row.
    conformed_dates = ["20230331", "20230630", "20230930"]
    raw_paths: dict[str, str] = {}
    for accn, cusip, conformed in zip(accessions, cusips, conformed_dates, strict=False):
        # Layout mirrors what _save_raw_filing would write for 13F-with-file-number:
        # <raw>/<file_number>/13F-HR/<accession>/<file_number>_<accession>.txt
        acc_dir = raw_dir / "028_12345" / "13F-HR" / accn
        acc_dir.mkdir(parents=True)
        p = acc_dir / f"028-12345_{accn}.txt"
        p.write_text(_sample_13f(accn, cusip, conformed))
        raw_paths[accn] = str(p)

    canned_index = pd.DataFrame(
        [
            {
                "CIK": "0001067983",
                "Name": "TEST",
                "Date Filed": "2024-01-31",
                "Form Type": "13F-HR",
                "accession_number": accn,
                "Filename": f"edgar/data/1067983/{accn}.txt",
            }
            for accn in accessions
        ]
    )
    return {
        "raw_dir": raw_dir,
        "accessions": accessions,
        "raw_paths": raw_paths,
        "canned_index": canned_index,
        "tmp_path": tmp_path,
    }


@pytest.fixture
def stub_network(monkeypatch, three_filings):
    """Replace ``get_sec_index_data`` and ``_download_single_filing`` so the
    orchestrator is driven by local fixtures. Tracks every call to the fake
    ``_download_single_filing`` in ``three_filings['http_called']``."""
    http_called: list[str] = []
    three_filings["http_called"] = http_called

    def fake_get_index(self, *a, **kw):
        return three_filings["canned_index"]

    def fake_download_single(self, cik, accession_number, form_type, save_raw=True):
        http_called.append(accession_number)
        # Return the metadata the real downloader would emit, pointing at the
        # pre-staged raw file (so the parse stage works too).
        return {
            "cik": cik,
            "accession_number": accession_number,
            "form_type": form_type,
            "download_date": "2024-01-31",
            "raw_path": three_filings["raw_paths"][accession_number],
            "url": f"https://example.test/{accession_number}",
        }

    monkeypatch.setattr(downloader_module.SECDownloader, "get_sec_index_data", fake_get_index)
    monkeypatch.setattr(downloader_module.SECDownloader, "_download_single_filing", fake_download_single)
    return three_filings


def _run_get_filings(
    tmp_path,
    *,
    export_format="duckdb",
    resume=True,
    raw_data_dir=None,
):
    piboufilings.get_filings(
        user_name="Reviewer",
        user_agent_email="reviewer@example.com",
        cik="0001067983",
        form_type="13F-HR",
        start_year=2023,
        end_year=2023,
        base_dir=str(tmp_path / "parsed"),
        log_dir=str(tmp_path / "logs"),
        raw_data_dir=str(raw_data_dir if raw_data_dir else tmp_path / "raw_root"),
        show_progress=False,
        max_workers=1,
        keep_raw_files=True,
        export_format=export_format,
        resume=resume,
    )


def _duckdb_filing_info_accessions(base: Path) -> set[str]:
    con = duckdb.connect(str(base / "piboufilings.duckdb"))
    try:
        rows = con.execute("SELECT ACCESSION_NUMBER FROM filing_info_13f").fetchall()
    finally:
        con.close()
    return {r[0] for r in rows if r[0] is not None}


# ---------------------------------------------------------------------------
# Scenario 1 — Fresh run, resume=True: everything is downloaded.
# ---------------------------------------------------------------------------


def test_resume_fresh_run_downloads_everything(stub_network):
    tmp_path = stub_network["tmp_path"]
    _run_get_filings(tmp_path, raw_data_dir=tmp_path / "empty_raw_root")
    # All three filings were "downloaded" via the fake.
    assert stub_network["http_called"] == stub_network["accessions"]
    saved = _duckdb_filing_info_accessions(tmp_path / "parsed")
    assert saved == set(stub_network["accessions"])


# ---------------------------------------------------------------------------
# Scenario 2 — Level A: two of three accessions already in the backend.
# ---------------------------------------------------------------------------


def test_resume_level_a_backend_has_accession(stub_network):
    tmp_path = stub_network["tmp_path"]
    accessions = stub_network["accessions"]
    # Pre-seed the DuckDB with 2/3 accessions.
    from piboufilings.storage import DuckDBBackend

    base = tmp_path / "parsed"
    base.mkdir(parents=True, exist_ok=True)
    seed = DuckDBBackend(base / "piboufilings.duckdb")
    seed.upsert(
        "filing_info_13f",
        "2023_Q4",
        pd.DataFrame(
            [
                {
                    "ACCESSION_NUMBER": accessions[0],
                    "SEC_FILE_NUMBER": "028-12345",
                    "CONFORMED_DATE": "2023-12-31",
                },
                {
                    "ACCESSION_NUMBER": accessions[1],
                    "SEC_FILE_NUMBER": "028-12345",
                    "CONFORMED_DATE": "2023-12-30",
                },
            ]
        ),
        key_cols=("SEC_FILE_NUMBER", "CONFORMED_DATE"),
    )
    seed.close()

    # Run. Only the third accession should hit HTTP.
    _run_get_filings(tmp_path, raw_data_dir=tmp_path / "empty_raw_root")
    assert stub_network["http_called"] == [accessions[2]]
    saved = _duckdb_filing_info_accessions(base)
    assert saved == set(accessions)


# ---------------------------------------------------------------------------
# Scenario 3 — Level B: raw files on disk, backend empty.
# ---------------------------------------------------------------------------


def test_resume_level_b_recovery_from_disk(stub_network):
    tmp_path = stub_network["tmp_path"]
    accessions = stub_network["accessions"]
    # Raw dir is pre-staged in the fixture; point get_filings at it.
    _run_get_filings(tmp_path, raw_data_dir=stub_network["raw_dir"])
    # Zero HTTP calls: all three filings were found on disk and re-parsed.
    assert stub_network["http_called"] == []
    saved = _duckdb_filing_info_accessions(tmp_path / "parsed")
    assert saved == set(accessions)


# ---------------------------------------------------------------------------
# Scenario 4 — resume=False: same pre-seeded DB as (2), but everything refetches.
# ---------------------------------------------------------------------------


def test_resume_false_forces_full_refetch(stub_network):
    tmp_path = stub_network["tmp_path"]
    accessions = stub_network["accessions"]
    from piboufilings.storage import DuckDBBackend

    base = tmp_path / "parsed"
    base.mkdir(parents=True, exist_ok=True)
    seed = DuckDBBackend(base / "piboufilings.duckdb")
    seed.upsert(
        "filing_info_13f",
        "2023_Q4",
        pd.DataFrame(
            [
                {
                    "ACCESSION_NUMBER": accessions[0],
                    "SEC_FILE_NUMBER": "028-12345",
                    "CONFORMED_DATE": "2023-12-31",
                }
            ]
        ),
        key_cols=("SEC_FILE_NUMBER", "CONFORMED_DATE"),
    )
    seed.close()

    _run_get_filings(
        tmp_path,
        raw_data_dir=tmp_path / "empty_raw_root",
        resume=False,
    )
    # resume=False: all three accessions hit HTTP, even the one already in DB.
    assert stub_network["http_called"] == accessions
    # ON CONFLICT DO NOTHING keeps row counts sane.
    saved = _duckdb_filing_info_accessions(base)
    assert saved == set(accessions)


# ---------------------------------------------------------------------------
# Scenario 5 — CSV backend variant of (2).
# ---------------------------------------------------------------------------


def test_resume_level_a_csv_backend(stub_network):
    tmp_path = stub_network["tmp_path"]
    accessions = stub_network["accessions"]
    base = tmp_path / "parsed"
    base.mkdir(parents=True, exist_ok=True)
    # Pre-populate the CSV file so known_accessions reads two accessions.
    existing = pd.DataFrame(
        [
            {
                "ACCESSION_NUMBER": accessions[0],
                "SEC_FILE_NUMBER": "028-12345",
                "CONFORMED_DATE": "2023-12-31",
            },
            {
                "ACCESSION_NUMBER": accessions[1],
                "SEC_FILE_NUMBER": "028-12345",
                "CONFORMED_DATE": "2023-12-30",
            },
        ]
    )
    existing.to_csv(base / "13f_info_2023_Q4.csv", index=False)

    _run_get_filings(tmp_path, export_format="csv", raw_data_dir=tmp_path / "empty_raw_root")
    assert stub_network["http_called"] == [accessions[2]]
    # CSV filing_info is period-partitioned — the pre-seed lives in
    # 13f_info_2023_Q4.csv and the new filing lands in 13f_info_2023_Q3.csv.
    # known_accessions() globs all period files, so it's the right assertion.
    from piboufilings.storage import CSVBackend

    saved = CSVBackend(base).known_accessions("13F-HR")
    assert saved == set(accessions)


# ---------------------------------------------------------------------------
# Scenario 6 — Legacy DuckDB without ACCESSION_NUMBER column: no crash.
# ---------------------------------------------------------------------------


def test_resume_legacy_duckdb_without_accession_column(stub_network):
    tmp_path = stub_network["tmp_path"]
    accessions = stub_network["accessions"]
    base = tmp_path / "parsed"
    base.mkdir(parents=True, exist_ok=True)

    # Hand-craft a legacy-looking DB: filing_info_13f with NO ACCESSION_NUMBER.
    con = duckdb.connect(str(base / "piboufilings.duckdb"))
    try:
        con.execute(
            "CREATE TABLE filing_info_13f ("
            '"SEC_FILE_NUMBER" VARCHAR, "CONFORMED_DATE" VARCHAR, '
            'PRIMARY KEY ("SEC_FILE_NUMBER", "CONFORMED_DATE")'
            ")"
        )
        con.execute("INSERT INTO filing_info_13f VALUES ('028-12345', '2023-12-31')")
    finally:
        con.close()

    _run_get_filings(tmp_path, raw_data_dir=tmp_path / "empty_raw_root")
    # known_accessions() saw no column → returned set() → no Level-A skip.
    # All three accessions were "downloaded".
    assert stub_network["http_called"] == accessions
    # After a successful parse the schema-evolution path added ACCESSION_NUMBER.
    saved = _duckdb_filing_info_accessions(base)
    assert saved == set(accessions)
