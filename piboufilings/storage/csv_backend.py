"""CSV fallback backend. Period-partitioned files with append + dedup."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .base import DATASET_CSV_STEMS, resolve_filing_info_dataset

logger = logging.getLogger(__name__)


def _is_missing(val: Any) -> bool:
    """Treat pandas NA, NaN, None, empty string, and '<NA>'/'nan' as missing."""
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except (TypeError, ValueError):
        # pd.isna raises on unhashable/array-like values we don't care about
        # (e.g. lists, dicts) — they're clearly not missing, fall through.
        pass
    if isinstance(val, str):
        stripped = val.strip().upper()
        if stripped in ("", "<NA>", "NAN"):
            return True
    return False


def _norm_value(val: Any) -> Optional[str]:
    """Stringify a value for key comparison. Missing → None.

    CSV round-trip coerces types (e.g. "20231231" → int 20231231), so compared
    keys must be normalized or the same logical row reads as two different keys.
    """
    if _is_missing(val):
        return None
    # Integers round-trip through CSV as int; normalize "123" and 123 equal.
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    return str(val).strip()


class CSVBackend:
    """Writes parsed filings to period-partitioned CSVs with dedup on append."""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def upsert(
        self,
        dataset: str,
        period_suffix: str,
        df: pd.DataFrame,
        *,
        key_cols: Sequence[str] = (),
        prefer_non_null: Optional[str] = None,
    ) -> None:
        if df is None or df.empty:
            return

        stem = DATASET_CSV_STEMS.get(dataset, dataset)
        path = self.output_dir / f"{stem}_{period_suffix}.csv"

        if prefer_non_null and key_cols:
            self._upsert_prefer_non_null(path, df, list(key_cols), prefer_non_null)
        elif key_cols:
            self._upsert_key_dedup(path, df, list(key_cols))
        else:
            self._upsert_full_row_dedup(path, df)

    def known_accessions(self, form_type: str) -> set[str]:
        """Return accession numbers already persisted across all period CSVs
        for this form's filing_info dataset.

        Empty set when no matching files exist or the CSV lacks an
        ``ACCESSION_NUMBER`` column (e.g. legacy 13F files written before the
        resume feature added the column).
        """
        dataset = resolve_filing_info_dataset(form_type)
        if not dataset:
            return set()
        stem = DATASET_CSV_STEMS.get(dataset, dataset)
        matches = sorted(self.output_dir.glob(f"{stem}_*.csv"))
        if not matches:
            return set()

        seen: set[str] = set()
        for path in matches:
            try:
                # usecols + chunksize keeps memory flat for large period files.
                for chunk in pd.read_csv(path, usecols=["ACCESSION_NUMBER"], chunksize=50_000, dtype=str):
                    col = chunk["ACCESSION_NUMBER"].dropna()
                    seen.update(str(v).strip() for v in col if str(v).strip())
            except (ValueError, KeyError) as e:
                # ValueError: usecols didn't find ACCESSION_NUMBER (legacy file).
                logger.info(
                    "CSVBackend.known_accessions: %s has no ACCESSION_NUMBER column (%s); "
                    "treating as no prior state for this file.",
                    path.name,
                    e,
                )
                continue
            except Exception as e:  # noqa: BLE001 — never let resume read crash the run.
                logger.warning("CSVBackend.known_accessions: failed to read %s: %s", path, e)
                continue
        return seen

    def close(self) -> None:
        return

    # ------------------------------------------------------------------
    # Internal write strategies
    # ------------------------------------------------------------------

    def _upsert_full_row_dedup(self, path: Path, df: pd.DataFrame) -> None:
        """Append, then dedup on every column (string-normalized for robustness
        against CSV round-trip dtype drift)."""
        columns = list(df.columns)
        frames = [df.copy()]
        if path.exists():
            existing = pd.read_csv(path)
            combined_cols = list(dict.fromkeys(list(existing.columns) + columns))
            frames = [existing.reindex(columns=combined_cols), df.reindex(columns=combined_cols)]
            columns = combined_cols

        combined = pd.concat(frames, ignore_index=True).reindex(columns=columns)

        # Normalize values before dedupe so int/str CSV coercions don't create
        # false duplicates (the 13F regression this pattern was originally fixing).
        dedupe_view = combined.astype(str).apply(lambda col: col.str.strip())
        combined = combined.loc[~dedupe_view.duplicated()].reset_index(drop=True)
        combined.to_csv(path, index=False)

    def _upsert_key_dedup(self, path: Path, df: pd.DataFrame, key_cols: list[str]) -> None:
        """Dedup by natural key (keep-first across existing + incoming)."""
        existing_cols: list[str] = []
        records: Dict[tuple, Dict[str, Any]] = {}

        def _norm_key(row: Dict[str, Any]) -> tuple:
            return tuple(_norm_value(row.get(k)) for k in key_cols)

        if path.exists():
            for chunk in pd.read_csv(path, chunksize=50000):
                if not existing_cols:
                    existing_cols = list(chunk.columns)
                for _, row in chunk.iterrows():
                    rec = row.to_dict()
                    records.setdefault(_norm_key(rec), rec)

        for _, row in df.iterrows():
            rec = row.to_dict()
            records.setdefault(_norm_key(rec), rec)

        if not records:
            return

        columns = list(dict.fromkeys([*existing_cols, *df.columns]))
        pd.DataFrame(records.values()).reindex(columns=columns).to_csv(path, index=False)

    def _upsert_prefer_non_null(
        self,
        path: Path,
        df: pd.DataFrame,
        key_cols: list[str],
        prefer_col: str,
    ) -> None:
        """Dedup by key, preferring rows where `prefer_col` is non-null.

        Preserves the NPORT "prefer the row with a CUSIP" semantics.
        """
        existing_cols: list[str] = []
        records: Dict[tuple, Dict[str, Any]] = {}

        def _norm_key(row: Dict[str, Any]) -> tuple:
            return tuple(_norm_value(row.get(k)) for k in key_cols)

        def _add(row: Dict[str, Any]) -> None:
            key = _norm_key(row)
            existing = records.get(key)
            if existing is None:
                records[key] = row
                return
            cur_missing = _is_missing(existing.get(prefer_col))
            new_present = not _is_missing(row.get(prefer_col))
            if cur_missing and new_present:
                records[key] = row

        if path.exists():
            for chunk in pd.read_csv(path, chunksize=50000):
                if not existing_cols:
                    existing_cols = list(chunk.columns)
                for _, row in chunk.iterrows():
                    _add(row.to_dict())

        for _, row in df.iterrows():
            _add(row.to_dict())

        if not records:
            return

        columns = list(dict.fromkeys([*existing_cols, *df.columns]))
        pd.DataFrame(records.values()).reindex(columns=columns).to_csv(path, index=False)
