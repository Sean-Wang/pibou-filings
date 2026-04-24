"""DuckDB storage backend.

One database file holds all parsed datasets as tables. Dedup is enforced via
PRIMARY KEY + `INSERT ... ON CONFLICT DO NOTHING`, so the DB stays consistent
even across interrupted runs.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Optional

import pandas as pd

from .base import resolve_filing_info_dataset

logger = logging.getLogger(__name__)

_DUCKDB_INSTALL_HINT = (
    "DuckDB is not installed. Install it with:\n"
    "    pip install piboufilings[duckdb]\n"
    "Or pass export_format='csv' to fall back to CSV output."
)


def _import_duckdb():
    try:
        import duckdb  # type: ignore
    except ImportError as e:
        raise ImportError(_DUCKDB_INSTALL_HINT) from e
    return duckdb


class DuckDBBackend:
    """Appends parsed filings to a single DuckDB file with PK-based dedup."""

    def __init__(self, db_path: Path):
        self._duckdb = _import_duckdb()
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._con = self._duckdb.connect(str(self.db_path))
        self._lock = threading.Lock()
        # Tables whose schema has been ensured in this session.
        self._known_tables: set[str] = set()

    # ------------------------------------------------------------------

    def upsert(
        self,
        dataset: str,
        period_suffix: str,  # noqa: ARG002 — ignored; tables are canonical.
        df: pd.DataFrame,
        *,
        key_cols: Sequence[str] = (),
        prefer_non_null: Optional[str] = None,  # noqa: ARG002 — DO NOTHING semantics.
    ) -> None:
        if df is None or df.empty:
            return
        if self._con is None:
            raise RuntimeError("DuckDBBackend.upsert() called after close(); construct a new backend.")

        # Sanitize the table name defensively.
        table = "".join(c if c.isalnum() or c == "_" else "_" for c in dataset)
        prepared = self._prepare_pk_columns(df, list(key_cols))
        with self._lock:
            self._ensure_table(table, prepared, list(key_cols))
            self._align_columns(table, prepared)
            self._insert(table, prepared)

    @staticmethod
    def _prepare_pk_columns(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
        """Fill NA in PK columns with an empty-string sentinel.

        DuckDB PRIMARY KEY implies NOT NULL; many of our natural keys include
        optional fields (e.g. PUT_CALL). Coalescing to "" keeps the PK intact
        and treats "missing" as a single bucket.
        """
        if not key_cols:
            return df
        prepared = df.copy()
        for col in key_cols:
            if col in prepared.columns:
                prepared[col] = prepared[col].where(prepared[col].notna(), "")
                # Cast to string so numeric "0" and NA-coalesced "" coexist cleanly.
                prepared[col] = prepared[col].astype(str)
        return prepared

    def known_accessions(self, form_type: str) -> set[str]:
        """Return accession numbers already persisted for this form.

        Returns an empty set when the backend hasn't seen this form type yet
        (no table, or the table exists but has no ``ACCESSION_NUMBER``
        column — e.g. legacy 13F DBs from before the resume feature).
        """
        dataset = resolve_filing_info_dataset(form_type)
        if not dataset or self._con is None:
            return set()

        # Sanitize for identifier quoting (same rule as upsert).
        table = "".join(c if c.isalnum() or c == "_" else "_" for c in dataset)

        with self._lock:
            try:
                exists = self._con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                    [table],
                ).fetchone()
            except Exception as e:  # noqa: BLE001 — defensive; log and bail safely.
                logger.warning("known_accessions: table check failed for %s: %s", table, e)
                return set()

            if exists is None:
                return set()

            try:
                rows = self._con.execute(
                    f'SELECT DISTINCT "ACCESSION_NUMBER" FROM "{table}" WHERE "ACCESSION_NUMBER" IS NOT NULL'
                ).fetchall()
            except Exception as e:  # noqa: BLE001 — column may be missing in legacy DBs.
                logger.info(
                    "known_accessions: %s has no ACCESSION_NUMBER column yet (%s); "
                    "resume will treat this form as fresh.",
                    table,
                    e,
                )
                return set()

        return {str(r[0]) for r in rows if r and r[0] is not None}

    def close(self) -> None:
        """Close the connection and drop our reference to it.

        DuckDB keeps a process-wide instance cache keyed by the DB file path.
        Even after ``close()``, the underlying database can linger in that
        cache until Python garbage-collects the last reference, at which point
        reopening the same file with a *different* configuration (e.g.
        ``read_only=True``) raises::

            ConnectionException: Can't open a connection to same database
            file with a different configuration than existing connections

        Explicitly nulling ``self._con`` lets the GC reclaim the connection
        promptly; the ``duckdb`` module reference is cleared for good measure.
        """
        with self._lock:
            with contextlib.suppress(Exception):
                if self._con is not None:
                    self._con.close()
            self._con = None
            self._duckdb = None

    # ------------------------------------------------------------------

    def _ensure_table(self, table: str, df: pd.DataFrame, key_cols: list[str]) -> None:
        if table in self._known_tables:
            return
        existing = self._con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()
        if existing is None:
            cols_sql = ", ".join(f'"{col}" {_duckdb_type_for(df[col])}' for col in df.columns)
            pk_sql = ""
            if key_cols:
                pk_cols = [c for c in key_cols if c in df.columns]
                if pk_cols:
                    quoted = ", ".join('"' + c + '"' for c in pk_cols)
                    pk_sql = f", PRIMARY KEY ({quoted})"
            self._con.execute(f'CREATE TABLE "{table}" ({cols_sql}{pk_sql})')
        self._known_tables.add(table)

    def _align_columns(self, table: str, df: pd.DataFrame) -> None:
        """Add any new DataFrame columns to the table (schema evolution)."""
        current_cols = {
            row[1]  # PRAGMA table_info yields (cid, name, type, ...)
            for row in self._con.execute(f'PRAGMA table_info("{table}")').fetchall()
        }
        for col in df.columns:
            if col not in current_cols:
                self._con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {_duckdb_type_for(df[col])}')

    def _insert(self, table: str, df: pd.DataFrame) -> None:
        # Register the DataFrame as a virtual table so DuckDB reads it directly.
        incoming = df.copy()
        self._con.register("_piboufilings_incoming", incoming)
        try:
            col_list = ", ".join(f'"{c}"' for c in incoming.columns)
            self._con.execute(
                f'INSERT INTO "{table}" ({col_list}) '
                f"SELECT {col_list} FROM _piboufilings_incoming "
                f"ON CONFLICT DO NOTHING"
            )
        finally:
            self._con.unregister("_piboufilings_incoming")


def _duckdb_type_for(series: pd.Series) -> str:
    """Best-effort pandas dtype → DuckDB type mapping."""
    dtype = series.dtype
    kind = getattr(dtype, "kind", None)
    name = str(dtype).lower()

    if "datetime" in name:
        return "TIMESTAMP"
    if "bool" in name:
        return "BOOLEAN"
    if kind == "i" or "int" in name:
        return "BIGINT"
    if kind == "f" or "float" in name:
        return "DOUBLE"
    if "date" in name:
        return "DATE"
    # Strings, pd.NA-heavy object columns, mixed types → VARCHAR is safest.
    return "VARCHAR"
