"""Storage backend protocol and shared dataset metadata."""

from collections.abc import Sequence
from typing import Optional, Protocol

import pandas as pd

_SECTION16_BASE_FORMS = ("3", "4", "5")


class StorageBackend(Protocol):
    """Persist parsed filings. Implementations dedupe however they see fit."""

    def upsert(
        self,
        dataset: str,
        period_suffix: str,
        df: pd.DataFrame,
        *,
        key_cols: Sequence[str] = (),
        prefer_non_null: Optional[str] = None,
    ) -> None:
        """Insert rows into the dataset, deduplicating on key_cols when given.

        Args:
            dataset: Logical dataset name (e.g. "holdings_13f").
            period_suffix: Period tag like "2023_Q4" or "2024_03". CSV uses it
                in the filename; DuckDB ignores it.
            df: Rows to insert.
            key_cols: Columns forming the natural key for dedup.
            prefer_non_null: If set, on key collision prefer the row whose
                value in this column is non-null (used for NPORT CUSIP preference).
        """
        ...

    def known_accessions(self, form_type: str) -> set[str]:
        """Return accession numbers already persisted for this form's filing_info.

        Used by the resume/recovery path in ``get_filings`` to decide which
        filings can be skipped on a re-run. Implementations must return an
        empty set (not raise) when the backing table/file/column is missing:
        the caller then behaves as if no prior state exists.

        Accession numbers are returned in SEC's hyphenated form
        (``NNNNNNNNNN-YY-NNNNNN``), matching what the downloader passes in.
        """
        ...

    def close(self) -> None:
        """Flush and release resources."""
        ...


# Maps the logical dataset name used by parsers/backends to the CSV filename
# stem. The period suffix is appended by the CSV backend.
DATASET_CSV_STEMS = {
    "filing_info_13f": "13f_info",
    "holdings_13f": "13f_holdings",
    "other_managers_reporting_13f": "13f_other_managers_reporting",
    "other_included_managers_13f": "13f_other_included_managers",
    "filing_info_nport": "nport_filing_info",
    "holdings_nport": "nport_holdings",
    "filing_info_sec16": "sec16_info",
    "transactions_sec16": "sec16_transactions",
    "holdings_sec16": "sec16_holdings",
}


def resolve_filing_info_dataset(form_type: str) -> Optional[str]:
    """Map a user-facing form type (``"13F-HR"``, ``"NPORT-P"``, ``"4"``,
    ``"SECTION-6"``, etc.) to the backend's filing_info dataset name.

    Returns ``None`` for unsupported forms, so callers can treat it as "no
    known-accessions pre-filter available" and process everything.
    """
    if not form_type:
        return None
    upper = str(form_type).upper()
    if "EX" in upper:
        # Exhibit filings are never parsed; no filing_info table for them.
        return None
    if "13F" in upper:
        return "filing_info_13f"
    if "NPORT" in upper:
        return "filing_info_nport"
    if upper == "SECTION-6" or upper.startswith(_SECTION16_BASE_FORMS):
        return "filing_info_sec16"
    return None
