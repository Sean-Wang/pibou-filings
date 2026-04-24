"""
Logging functionality for SEC EDGAR filings operations.
"""

from __future__ import annotations

import csv
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Canonical header order for the operations CSV. ``level`` is second so log
# analysts can filter by severity cheaply.
_LOG_HEADER = [
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

_VALID_LEVELS = ("INFO", "WARN", "ERROR", "DEBUG")


class FilingLogger:
    """A class to handle logging of filing operations to CSV."""

    def __init__(self, log_dir: str | Path = "./logs"):
        """
        Initialize the FilingLogger.

        Args:
            log_dir: Directory to store log files. Parents are created if missing.
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        # Serialize concurrent appends so thread-spawned writes never interleave
        # partial rows into the CSV.
        self._write_lock = threading.Lock()
        self.log_file = self.log_dir / f"filing_operations_{datetime.now().strftime('%Y%m%d')}.csv"

        # Create log file with headers if it doesn't exist
        if not self.log_file.exists():
            with open(self.log_file, "w", newline="") as f:
                csv.writer(f).writerow(_LOG_HEADER)

    def log_operation(
        self,
        operation_type: Optional[str] = "",
        cik: Optional[str] = None,
        form_type_processed: Optional[str] = None,
        accession_number: Optional[str] = None,
        download_success: bool = False,
        download_error_message: Optional[str] = None,
        parse_success: Optional[bool] = None,
        error_code: Optional[Any] = None,
        custom_identifier: Optional[str] = None,
        level: Optional[str] = None,
    ) -> None:
        """
        Log a filing operation to the CSV file.

        Args:
            operation_type: Short event name (e.g. ``DOWNLOAD_SINGLE_FILING_SUCCESS``).
            cik: Company CIK number (optional, for system-wide events).
            form_type_processed: Form type this event relates to (optional).
            accession_number: Filing accession number (optional).
            download_success: Whether the download step succeeded.
            download_error_message: Free-text message (used for both info and errors).
            parse_success: Whether parsing succeeded (``None`` = not applicable).
            error_code: Optional HTTP status code or similar numeric code.
            custom_identifier: Any secondary identifier (IRS number, bucket key, …).
            level: Severity. If omitted, derived from the success flags:
                - ``"ERROR"`` when ``download_success`` is False
                - ``"WARN"`` when ``parse_success`` is explicitly False
                - ``"INFO"`` otherwise.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if level is None:
            if not download_success:
                level = "ERROR"
            elif parse_success is False:
                level = "WARN"
            else:
                level = "INFO"
        elif level.upper() not in _VALID_LEVELS:
            # Don't crash on unexpected values, just normalize and record.
            level = str(level).upper()

        if parse_success is None:
            parse_cell = ""
        else:
            parse_cell = "True" if parse_success else "False"

        row = [
            timestamp,
            level,
            operation_type,
            cik or "SYSTEM",
            form_type_processed or "",
            accession_number or "",
            "True" if download_success else "False",
            download_error_message or "",
            parse_cell,
            str(error_code) if error_code is not None else "",
            custom_identifier or "",
        ]

        with self._write_lock, open(self.log_file, "a", newline="") as f:
            csv.writer(f).writerow(row)
