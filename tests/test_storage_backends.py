"""Unit tests for the CSV and DuckDB storage backends."""

import sys
import threading

import pandas as pd
import pytest

from piboufilings.storage import CSVBackend, DuckDBBackend, get_backend

duckdb = pytest.importorskip("duckdb")


def test_duckdb_backend_missing_install_raises_with_hint(monkeypatch, tmp_path):
    """If the user installs piboufilings without the [duckdb] extra and then
    asks for the DuckDB backend, they should get a helpful ImportError that
    mentions the install command."""
    import piboufilings.storage.duckdb_backend as dbmod

    # Simulate "duckdb is not importable" even though we have it installed.
    original = sys.modules.pop("duckdb", None)
    monkeypatch.setitem(sys.modules, "duckdb", None)
    try:
        with pytest.raises(ImportError, match=r"pip install piboufilings\[duckdb\]"):
            dbmod.DuckDBBackend(tmp_path / "whatever.duckdb")
    finally:
        if original is not None:
            sys.modules["duckdb"] = original
        else:
            sys.modules.pop("duckdb", None)


def test_get_backend_missing_duckdb_raises(monkeypatch, tmp_path):
    """get_backend('duckdb', ...) should surface the same ImportError."""
    original = sys.modules.pop("duckdb", None)
    monkeypatch.setitem(sys.modules, "duckdb", None)
    try:
        with pytest.raises(ImportError, match=r"piboufilings\[duckdb\]"):
            get_backend("duckdb", tmp_path)
    finally:
        if original is not None:
            sys.modules["duckdb"] = original
        else:
            sys.modules.pop("duckdb", None)


# ---------------------------------------------------------------------------
# CSVBackend
# ---------------------------------------------------------------------------


def test_csv_backend_writes_expected_filename(tmp_path):
    backend = CSVBackend(tmp_path)
    df = pd.DataFrame([{"A": 1}])
    backend.upsert("holdings_13f", "2024_Q1", df)
    assert (tmp_path / "13f_holdings_2024_Q1.csv").exists()


def test_csv_backend_dedup_by_key_across_saves(tmp_path):
    backend = CSVBackend(tmp_path)
    df = pd.DataFrame([{"SEC_FILE_NUMBER": "028-1", "CONFORMED_DATE": "20240101", "X": 1}])
    backend.upsert("filing_info_13f", "2024_Q1", df, key_cols=("SEC_FILE_NUMBER", "CONFORMED_DATE"))
    backend.upsert("filing_info_13f", "2024_Q1", df, key_cols=("SEC_FILE_NUMBER", "CONFORMED_DATE"))
    saved = pd.read_csv(tmp_path / "13f_info_2024_Q1.csv")
    assert len(saved) == 1


def test_csv_backend_prefer_non_null_keeps_row_with_value(tmp_path):
    backend = CSVBackend(tmp_path)
    row_missing = pd.DataFrame([{"ACCESSION_NUMBER": "a", "CUSIP": pd.NA, "X": 1}])
    row_present = pd.DataFrame([{"ACCESSION_NUMBER": "a", "CUSIP": "111", "X": 1}])

    backend.upsert(
        "holdings_nport",
        "2024_03",
        row_missing,
        key_cols=("ACCESSION_NUMBER",),
        prefer_non_null="CUSIP",
    )
    backend.upsert(
        "holdings_nport",
        "2024_03",
        row_present,
        key_cols=("ACCESSION_NUMBER",),
        prefer_non_null="CUSIP",
    )

    saved = pd.read_csv(tmp_path / "nport_holdings_2024_03.csv")
    assert len(saved) == 1
    assert str(saved.iloc[0]["CUSIP"]) == "111"


def test_csv_backend_full_row_dedup_when_no_keys(tmp_path):
    backend = CSVBackend(tmp_path)
    df = pd.DataFrame([{"A": 1, "B": "x"}, {"A": 1, "B": "x"}])
    backend.upsert("holdings_sec16", "2024_01", df)
    backend.upsert("holdings_sec16", "2024_01", df)
    saved = pd.read_csv(tmp_path / "sec16_holdings_2024_01.csv")
    assert len(saved) == 1


# ---------------------------------------------------------------------------
# DuckDBBackend
# ---------------------------------------------------------------------------


def test_duckdb_backend_creates_table_with_primary_key(tmp_path):
    backend = DuckDBBackend(tmp_path / "t.duckdb")
    df = pd.DataFrame([{"ACCESSION_NUMBER": "a1", "X": 1}])
    backend.upsert("holdings_sec16", "2024_01", df, key_cols=("ACCESSION_NUMBER",))
    backend.close()

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    try:
        assert con.execute("SELECT COUNT(*) FROM holdings_sec16").fetchone()[0] == 1
    finally:
        con.close()


def test_duckdb_backend_idempotent_reinsert(tmp_path):
    backend = DuckDBBackend(tmp_path / "t.duckdb")
    df = pd.DataFrame(
        [
            {"ACCESSION_NUMBER": "a1", "X": 1},
            {"ACCESSION_NUMBER": "a2", "X": 2},
        ]
    )
    backend.upsert("holdings_sec16", "2024_01", df, key_cols=("ACCESSION_NUMBER",))
    backend.upsert("holdings_sec16", "2024_01", df, key_cols=("ACCESSION_NUMBER",))
    backend.close()

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    try:
        assert con.execute("SELECT COUNT(*) FROM holdings_sec16").fetchone()[0] == 2
    finally:
        con.close()


def test_duckdb_backend_schema_evolution_adds_columns(tmp_path):
    backend = DuckDBBackend(tmp_path / "t.duckdb")
    df1 = pd.DataFrame([{"ACCESSION_NUMBER": "a1", "X": 1}])
    df2 = pd.DataFrame([{"ACCESSION_NUMBER": "a2", "X": 2, "NEW_COL": "hi"}])
    backend.upsert("holdings_sec16", "2024_01", df1, key_cols=("ACCESSION_NUMBER",))
    backend.upsert("holdings_sec16", "2024_01", df2, key_cols=("ACCESSION_NUMBER",))
    backend.close()

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    try:
        rows = con.execute(
            "SELECT ACCESSION_NUMBER, NEW_COL FROM holdings_sec16 ORDER BY ACCESSION_NUMBER"
        ).fetchall()
        assert rows == [("a1", None), ("a2", "hi")]
    finally:
        con.close()


def test_duckdb_backend_close_allows_readonly_reopen(tmp_path):
    """After close(), the same file must be re-openable in a different config.

    Regression: DuckDB keeps a process-wide instance cache. If ``close()`` does
    not drop our reference, a subsequent ``duckdb.connect(..., read_only=True)``
    raises ``ConnectionException: Can't open a connection to same database
    file with a different configuration than existing connections``.
    """
    backend = DuckDBBackend(tmp_path / "t.duckdb")
    backend.upsert(
        "holdings_sec16",
        "2024_01",
        pd.DataFrame([{"ACCESSION_NUMBER": "a1", "X": 1}]),
        key_cols=("ACCESSION_NUMBER",),
    )
    backend.close()
    # This is exactly what the demo notebooks used to do. Must not raise.
    con = duckdb.connect(str(tmp_path / "t.duckdb"), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM holdings_sec16").fetchone()[0] == 1
    finally:
        con.close()


def test_duckdb_backend_upsert_after_close_raises(tmp_path):
    backend = DuckDBBackend(tmp_path / "t.duckdb")
    backend.close()
    with pytest.raises(RuntimeError, match="after close"):
        backend.upsert(
            "holdings_sec16",
            "2024_01",
            pd.DataFrame([{"ACCESSION_NUMBER": "a1"}]),
            key_cols=("ACCESSION_NUMBER",),
        )


def test_duckdb_backend_concurrent_writes_are_serialized(tmp_path):
    backend = DuckDBBackend(tmp_path / "t.duckdb")

    def writer(i: int):
        df = pd.DataFrame([{"ACCESSION_NUMBER": f"a{i}", "X": i}])
        backend.upsert("holdings_sec16", "2024_01", df, key_cols=("ACCESSION_NUMBER",))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    backend.close()

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    try:
        assert con.execute("SELECT COUNT(*) FROM holdings_sec16").fetchone()[0] == 20
    finally:
        con.close()


# ---------------------------------------------------------------------------
# get_backend
# ---------------------------------------------------------------------------


def test_get_backend_factory_returns_expected_types(tmp_path):
    assert isinstance(get_backend("csv", tmp_path), CSVBackend)
    db = get_backend("duckdb", tmp_path)
    assert isinstance(db, DuckDBBackend)
    db.close()


def test_get_backend_unknown_format_raises(tmp_path):
    with pytest.raises(ValueError):
        get_backend("parquet", tmp_path)


def test_13f_parser_writes_to_duckdb_backend(tmp_path):
    """End-to-end: 13F parser → DuckDBBackend produces expected tables and dedup."""
    from piboufilings.parsers.form_13f_parser import Form13FParser

    backend = DuckDBBackend(tmp_path / "p.duckdb")
    parser = Form13FParser(output_dir=tmp_path, backend=backend)
    parsed_data = {
        "filing_info": pd.DataFrame(
            [{"FORM_13F_FILE_NUMBER": "028-1", "IRS_NUMBER": "12-3", "CONFORMED_DATE": "20231231"}]
        ),
        "holdings": pd.DataFrame(
            [
                {
                    "FORM_13F_FILE_NUMBER": "028-1",
                    "CUSIP": "000",
                    "PUT_CALL": None,
                    "CONFORMED_DATE": "20231231",
                    "SHARE_VALUE": 100,
                }
            ]
        ),
        "other_managers_reporting": pd.DataFrame(),
        "other_included_managers": pd.DataFrame(),
    }
    parser.save_parsed_data(parsed_data, "028-1", "0000000001")
    parser.save_parsed_data(parsed_data, "028-1", "0000000001")  # idempotent
    backend.close()

    con = duckdb.connect(str(tmp_path / "p.duckdb"))
    try:
        assert con.execute("SELECT COUNT(*) FROM filing_info_13f").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM holdings_13f").fetchone()[0] == 1
        # Column rename from FORM_13F_FILE_NUMBER → SEC_FILE_NUMBER should hold.
        cols = {row[1] for row in con.execute("PRAGMA table_info(filing_info_13f)").fetchall()}
        assert "SEC_FILE_NUMBER" in cols
        assert "FORM_13F_FILE_NUMBER" not in cols
    finally:
        con.close()
