"""Storage backends for parsed filings (DuckDB default, CSV fallback)."""

from pathlib import Path
from typing import Union

from .base import DATASET_CSV_STEMS, StorageBackend, resolve_filing_info_dataset
from .csv_backend import CSVBackend
from .duckdb_backend import DuckDBBackend


def get_backend(export_format: str, base_dir: Union[str, Path]) -> StorageBackend:
    """Construct a storage backend. Raises ValueError for unknown formats."""
    fmt = (export_format or "").lower().strip()
    base_path = Path(base_dir).expanduser().resolve()
    base_path.mkdir(parents=True, exist_ok=True)

    if fmt == "duckdb":
        return DuckDBBackend(base_path / "piboufilings.duckdb")
    if fmt == "csv":
        return CSVBackend(base_path)
    raise ValueError(f"Unknown export_format {export_format!r}; expected 'duckdb' or 'csv'.")


__all__ = [
    "StorageBackend",
    "CSVBackend",
    "DuckDBBackend",
    "DATASET_CSV_STEMS",
    "get_backend",
    "resolve_filing_info_dataset",
]
