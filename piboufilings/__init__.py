"""
piboufilings - A Python library for downloading and parsing SEC EDGAR filings.
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd
import requests
from tqdm import tqdm

from ._version import __version__
from .core.downloader import SECDownloader, normalize_filters, resolve_io_paths
from .core.logger import FilingLogger
from .parsers.form_13f_parser import Form13FParser
from .parsers.form_nport_parser import FormNPORTParser
from .parsers.form_sec16_parser import FormSection16Parser
from .parsers.parser_utils import validate_filing_content
from .storage import StorageBackend, get_backend

package_version = __version__


SECTION16_ALIAS = "SECTION-6"
SECTION16_BASE_FORMS = ("3", "4", "5")
ALL_PARSEABLE_FORMS: List[str] = ["13F-HR", "NPORT-P", SECTION16_ALIAS]


def _normalize_form_type(form_type: str) -> str:
    """Translate friendly aliases to SEC form identifiers used in filtering/downloading."""
    if not form_type:
        return form_type
    # Keep the Section 16 alias intact so downstream logic can handle all 3/4/5 variants.
    return SECTION16_ALIAS if form_type.upper() == SECTION16_ALIAS else form_type


### IF YOU'RE BUILDING A NEW PARSER, YOU'LL NEED TO UPDATE THIS FUNCTION ###
def get_parser_for_form_type_internal(
    form_type: str,
    base_dir: str,
    backend: Optional[StorageBackend] = None,
):
    """Get the appropriate parser for a form type using the new restructured parsers.

    If ``backend`` is omitted, each parser constructs a default CSVBackend at
    ``base_dir`` (preserving direct-parser-usage behavior).
    """
    if "EX" in form_type:
        # Exhibit filings are parsed
        return None
    elif "13F" in form_type:
        return Form13FParser(output_dir=f"{base_dir}", backend=backend)
    elif "NPORT" in form_type:
        return FormNPORTParser(output_dir=f"{base_dir}", backend=backend)
    elif form_type.upper() == SECTION16_ALIAS or form_type.upper().startswith(SECTION16_BASE_FORMS):
        return FormSection16Parser(output_dir=f"{base_dir}", backend=backend)
    else:
        return None


def _cleanup_raw_files_for_cik(
    downloaded_df: pd.DataFrame,
    cik: str,
    form_type: str,
    logger: FilingLogger,
    raw_root: Optional[Path] = None,
) -> None:
    """Delete the raw-filing files we just parsed, plus any now-empty parent
    directories. Called when ``keep_raw_files=False``.

    Logs successes and failures via the provided logger so the caller has an
    auditable record of what was removed.
    """
    if downloaded_df is None or downloaded_df.empty or "raw_path" not in downloaded_df.columns:
        return

    valid_raw_paths = downloaded_df["raw_path"].dropna()
    logger.log_operation(
        cik=cik,
        form_type_processed=form_type,
        operation_type="RAW_FILE_DELETION_START",
        download_success=True,
        parse_success=None,
        download_error_message=(
            f"Attempting to delete {len(valid_raw_paths)} raw files for CIK {cik}, Form {form_type}."
        ),
    )

    deleted_count = 0
    failed_count = 0
    for raw_file_path in valid_raw_paths:
        try:
            if os.path.exists(raw_file_path):
                os.remove(raw_file_path)
                deleted_count += 1
        except OSError as e_del:
            failed_count += 1
            logger.log_operation(
                cik=cik,
                form_type_processed=form_type,
                operation_type="RAW_FILE_DELETION_ERROR",
                download_success=True,
                parse_success=False,
                download_error_message=f"Failed to delete raw file {raw_file_path}: {e_del}",
            )

    logger.log_operation(
        cik=cik,
        form_type_processed=form_type,
        operation_type="RAW_FILE_DELETION_COMPLETE",
        download_success=True,
        parse_success=failed_count == 0,
        download_error_message=(
            f"Deleted {deleted_count} raw files. Failed to delete {failed_count} "
            f"files for CIK {cik}, Form {form_type}."
        ),
    )

    # Walk up from each deleted file's parent and remove empty dirs until we
    # hit a non-empty dir or the raw root. This handles all on-disk layouts
    # (3-, 4-, and 5-level) without duplicating layout knowledge from
    # ``_save_raw_filing``.
    if valid_raw_paths.empty:
        return

    root_resolved = Path(raw_root).resolve() if raw_root is not None else None
    seen: set = set()
    for raw_path_str in valid_raw_paths:
        cur = Path(raw_path_str).parent.resolve()
        while cur not in seen:
            seen.add(cur)
            if root_resolved is not None and (cur == root_resolved or root_resolved not in cur.parents):
                break
            if not cur.exists():
                cur = cur.parent
                continue
            try:
                if os.listdir(cur):
                    break
                os.rmdir(cur)
                logger.log_operation(
                    cik=cik,
                    form_type_processed=form_type,
                    operation_type="DIR_DELETION_SUCCESS",
                    download_success=True,
                    download_error_message=f"Successfully deleted empty directory: {cur}",
                )
            except OSError as e_rm_dir:
                logger.log_operation(
                    cik=cik,
                    form_type_processed=form_type,
                    operation_type="DIR_DELETION_ERROR",
                    download_error_message=(
                        f"Error deleting directory {cur} (not empty or other issue): {e_rm_dir}"
                    ),
                )
                break
            cur = cur.parent


def _build_raw_index(raw_root: Path) -> dict:
    """One-shot scan: map accession number → first raw-filing path on disk.

    `_save_raw_filing` writes every filing at ``<root>/.../<accession>/<name>.txt``
    (the parent directory name is always the accession). We walk ``raw_root``
    once with ``rglob("*.txt")`` and group by parent name. The cache
    subdirectory (``<root>/cache/``) is excluded.

    Returns ``{}`` when ``raw_root`` doesn't exist or is empty.
    """
    raw_root = Path(raw_root)
    if not raw_root.exists():
        return {}
    index: dict = {}
    cache_dir = raw_root / "cache"
    for path in raw_root.rglob("*.txt"):
        # Skip anything under the form.idx cache dir.
        try:
            path.relative_to(cache_dir)
            continue
        except ValueError:
            # Not under cache_dir — this is a real filing path, keep processing.
            pass
        accession = path.parent.name
        # Sanity: accession numbers look like ``NNNNNNNNNN-YY-NNNNNN``.
        # Don't let top-level directories pollute the index.
        if not re.match(r"^\d{10}-\d{2}-\d{6}$", accession):
            continue
        # First path wins; duplicate-accession writes are rare and not harmful.
        index.setdefault(accession, str(path))
    return index


def process_filings_for_cik(
    current_cik,
    downloaded,
    form_type,
    base_dir,
    logger,
    show_progress=True,
    backend: Optional[StorageBackend] = None,
    known_accessions: Optional[set] = None,
):
    """
    Process filings for a specific CIK with the restructured parsers.

    ``known_accessions`` (optional) is the set of accession numbers already
    present in the storage backend. Filings whose accession is in this set
    are skipped — no re-parse, no re-upsert. Supplied by ``get_filings``
    when ``resume=True``.
    """
    known_accessions = known_accessions or set()
    # Determine the identifier to use in log messages (IRS_NUMBER or SEC_FILE_NUMBER if available)
    identifier_for_log = current_cik  # Default to CIK
    if downloaded is not None and not downloaded.empty:
        # Attempt to get IRS_NUMBER or SEC_FILE_NUMBER from the first downloaded filing
        # This assumes these might be present after downloader enrichment or initial parsing
        # For simplicity, we check the first entry. A more robust way might involve looking across all entries.
        first_filing_data = downloaded.iloc[0]
        if pd.notna(first_filing_data.get("IRS_NUMBER")):
            identifier_for_log = first_filing_data.get("IRS_NUMBER")
        elif pd.notna(first_filing_data.get("SEC_FILE_NUMBER")):
            identifier_for_log = first_filing_data.get("SEC_FILE_NUMBER")

    logger.log_operation(
        operation_type="PROCESS_FILINGS_FOR_IDENTIFIER_START",  # Changed from CIK
        cik=current_cik,  # Keep original CIK for backend logging if needed
        custom_identifier=identifier_for_log,  # Add the new identifier
        form_type_processed=form_type,
        download_success=True,
        download_error_message=f"Starting processing for {identifier_for_log}, Form {form_type}. Downloaded count: {len(downloaded) if downloaded is not None else 0}",
    )

    # Get parser for the form type
    parser = get_parser_for_form_type_internal(form_type, str(base_dir), backend=backend)
    if parser is None:
        logger.log_operation(
            cik=current_cik,
            operation_type="PARSER_LOOKUP",
            download_success=True,
            parse_success=False,
            download_error_message=f"No parser available or needed for form type {form_type}. Skipping parsing.",
        )
        return downloaded["raw_path"].tolist(), {}, downloaded

    # Determine parser_operation_type for logging, MUST be after parser is confirmed not None
    parser_operation_type = f"PARSER-{form_type.upper().replace('-', '_')}"

    # Filter valid filings (remove NaN paths)
    total_filings = len(downloaded)
    valid_filings = downloaded.dropna(subset=["raw_path"])
    remaining_filings = len(valid_filings)
    skipped_filings = total_filings - remaining_filings

    if skipped_filings > 0:
        logger.log_operation(
            cik=current_cik,
            accession_number=None,  # This log is not per-accession
            operation_type="PARSER_INPUT_FILTER",  # More generic type for pre-parsing step
            download_success=True,  # This indicates the inputs to parsing might be problematic, not download itself
            download_error_message=f"Skipped {skipped_filings} filings (missing file paths). Proceeding with {remaining_filings} valid filings.",
        )

    # Process filings with progress bar
    parsed_files = {}
    filing_iterator = (
        tqdm(
            valid_filings.iterrows(),
            desc=f"Parsing {form_type} filings for {identifier_for_log}",  # Changed from CIK
            total=remaining_filings,
            disable=not show_progress,
        )
        if show_progress
        else valid_filings.iterrows()
    )

    successful_parses = 0
    total_holdings_extracted = 0

    for _, filing in filing_iterator:
        try:
            cik = filing["cik"]
            raw_path = filing["raw_path"]
            accession_number = filing["accession_number"]

            # Resume short-circuit: already in the storage backend → skip
            # re-parse entirely. The downloader already logged the Level-A
            # skip; no need to double-log here.
            if accession_number in known_accessions:
                continue

            if not raw_path or not os.path.exists(raw_path):
                logger.log_operation(
                    cik=current_cik,
                    accession_number=accession_number,
                    operation_type=parser_operation_type,
                    download_success=True,
                    parse_success=False,
                    download_error_message=f"Raw file not found at {raw_path}",
                )
                continue

            # Read and validate filing content
            with open(raw_path, encoding="utf-8", errors="ignore") as f:
                content = f.read()

            # Quick validation
            validation = validate_filing_content(content)
            if not validation["is_valid_sec_filing"]:
                logger.log_operation(
                    cik=current_cik,
                    accession_number=accession_number,
                    operation_type=parser_operation_type,
                    download_success=True,
                    parse_success=False,
                    download_error_message="Invalid SEC filing format",
                )
                continue

            # Parse the filing using the new parser structure
            parsed_data = parser.parse_filing(content)
            filing_url = filing.get("url")
            if "filing_info" in parsed_data and not parsed_data["filing_info"].empty:
                filing_info_df = parsed_data["filing_info"].copy()
                if "SEC_FILING_URL" not in filing_info_df.columns:
                    filing_info_df["SEC_FILING_URL"] = pd.NA
                filing_info_df["SEC_FILING_URL"] = filing_info_df["SEC_FILING_URL"].astype("object")
                filing_info_df.loc[:, "SEC_FILING_URL"] = filing_url if filing_url else pd.NA
                parsed_data["filing_info"] = filing_info_df

            # Save parsed data according to parser type
            if isinstance(parser, Form13FParser):
                form_13f_file_number_for_saving = "unknown_file_number"
                if "filing_info" in parsed_data and not parsed_data["filing_info"].empty:
                    filing_info_df = parsed_data["filing_info"]
                    if "FORM_13F_FILE_NUMBER" in filing_info_df.columns:
                        val = filing_info_df["FORM_13F_FILE_NUMBER"].iloc[0]
                        if pd.notna(val):
                            form_13f_file_number_for_saving = str(val)
                parser.save_parsed_data(parsed_data, form_13f_file_number_for_saving, cik)
            elif isinstance(parser, (FormNPORTParser, FormSection16Parser)):
                parser.save_parsed_data(parsed_data)
            else:
                # Fallback or error for unknown parser types, though get_parser_for_form_type_internal should prevent this.
                logger.log_operation(
                    cik=current_cik,
                    accession_number=accession_number,  # Accession still available here from the loop
                    operation_type="SAVE_PARSED_DATA_ERROR",
                    download_success=True,
                    parse_success=True,  # Assuming parse was ok, but save failed due to parser type
                    download_error_message=f"Cannot save data: Unknown parser type {type(parser).__name__}",
                )

            # Track parsing results
            holdings_count = len(parsed_data["holdings"])

            company_data_found = False
            # Check the type of parser to determine how to find company data
            if isinstance(parser, FormNPORTParser):
                if "filing_info" in parsed_data and not parsed_data["filing_info"].empty:
                    # For NPORT, company info is part of filing_info.
                    # Check if 'COMPANY_NAME' column exists and has non-null values.
                    company_data_found = (
                        "COMPANY_NAME" in parsed_data["filing_info"].columns
                        and not parsed_data["filing_info"]["COMPANY_NAME"].dropna().empty
                    )
            elif "company" in parsed_data:  # Retain existing logic for other parsers (e.g., Form13FParser)
                company_data_found = not parsed_data["company"].empty

            parsed_files[accession_number] = {
                "company_data_found": company_data_found,
                "filing_info_found": "filing_info" in parsed_data and not parsed_data["filing_info"].empty,
                "holdings_count": holdings_count,
                "file_size_kb": len(content) // 1024,
                "form_type_detected": validation.get("form_type", form_type),
            }

            successful_parses += 1
            total_holdings_extracted += holdings_count

            # Log successful parse
            logger.log_operation(
                cik=current_cik,
                accession_number=accession_number,
                operation_type=parser_operation_type,
                download_success=True,
                parse_success=True,
                download_error_message=f"Successfully parsed {holdings_count:,} holdings",
            )

        except Exception as e:
            status_code = None
            if isinstance(e, requests.RequestException):
                response = getattr(e, "response", None)
                status_code = getattr(response, "status_code", None)
            logger.log_operation(
                cik=current_cik,
                accession_number=filing.get("accession_number", "unknown"),
                operation_type=parser_operation_type,
                download_success=True,
                parse_success=False,
                download_error_message=f"Parse error: {str(e)}",
                error_code=status_code,
            )

    logger.log_operation(
        operation_type="PROCESS_FILINGS_FOR_IDENTIFIER_END",  # Changed from CIK
        cik=current_cik,  # Keep original CIK
        custom_identifier=identifier_for_log,
        form_type_processed=form_type,
        download_success=True,
        download_error_message=f"Finished processing for {identifier_for_log}, Form {form_type}. Successful parses: {successful_parses}, Holdings: {total_holdings_extracted}",
    )
    return downloaded["raw_path"].tolist(), parsed_files, downloaded


def get_filings(
    user_name: str,
    user_agent_email: str,
    cik: Union[str, List[str], None] = None,
    form_type: Union[str, List[str], None] = "13F-HR",
    start_year: int = None,
    end_year: Optional[int] = None,
    base_dir: Optional[str] = None,
    log_dir: Optional[str] = None,
    raw_data_dir: Optional[str] = None,
    show_progress: bool = True,
    max_workers: int = 5,
    keep_raw_files: bool = True,
    export_format: str = "duckdb",
    resume: bool = True,
) -> None:
    """
    Download and parse SEC filings for one or more companies and form types.

    Args:
        user_name: Name of the user or organization (required for User-Agent)
        user_agent_email: Email address for SEC's fair access rules (required for User-Agent)
        cik: Company CIK number(s) - can be a single CIK string, a list of CIKs, or None to get all CIKs
        form_type: Type of form(s) to download (e.g., '13F-HR', ['13F-HR', 'NPORT-P']). Defaults to '13F-HR'.
            Pass ``None`` to expand to all parseable forms (13F-HR, NPORT-P, SECTION-6).
        start_year: Starting year (defaults to current year)
        end_year: Ending year (defaults to current year)
        base_dir: Base directory for parsed data (defaults to './data_parsed')
        log_dir: Directory to store log files (defaults to './logs')
        raw_data_dir: Base directory for raw filings (defaults to config DATA_DIR)
        show_progress: Whether to show progress bars (defaults to True)
        max_workers: Maximum number of parallel download workers (defaults to 5)
        keep_raw_files: If False, raw filing files will be deleted after processing for each CIK. Defaults to True (files are kept).
        export_format: Output backend for parsed data. "duckdb" (default) writes to a single
            ``piboufilings.duckdb`` file under ``base_dir`` with one table per dataset and
            PK-based dedup. "csv" keeps the legacy period-partitioned CSVs. DuckDB requires
            ``pip install piboufilings[duckdb]``.
        resume: If True (default), skip any filing whose accession number is already in the
            storage backend (Level A: no download + no parse) or already on disk via
            ``raw_data_dir`` (Level B: no download, re-parse). Pass False to force a full
            refetch and re-parse, e.g. after a schema change. Note: switching
            ``export_format`` between runs makes the new backend see no prior state —
            Level B on-disk reuse still applies if ``keep_raw_files=True``.
    """

    ### VALIDATE INPUTS ###
    if start_year is None:
        start_year = datetime.today().year

    if end_year is None:
        end_year = start_year

    # Resolve directories with env-aware defaults
    base_dir, log_dir_path, raw_data_dir_path = resolve_io_paths(
        base_dir=base_dir, log_dir=log_dir, raw_data_dir=raw_data_dir, default_base=Path.cwd() / "data_parsed"
    )

    # Initialize downloader and logger
    downloader = SECDownloader(
        user_name=user_name,
        user_agent_email=user_agent_email,
        package_version=package_version,
        log_dir=log_dir_path,
        max_workers=max_workers,
        data_dir=raw_data_dir_path,
    )
    logger = FilingLogger(log_dir=log_dir_path)

    # Single shared storage backend for all parsers in this run.
    try:
        backend = get_backend(export_format, base_dir)
    except ImportError as e:
        logger.log_operation(
            operation_type="BACKEND_INIT_FAIL",
            download_success=False,
            download_error_message=f"Storage backend init failed: {e}",
        )
        raise

    logger.log_operation(
        operation_type="GET_FILINGS_START",
        download_success=True,
        download_error_message=f"Starting get_filings. CIKs: {cik}, Forms: {form_type}, Years: {start_year}-{end_year}, Export: {export_format}",
    )

    # Determine the list of form types to process
    form_type_list: List[str]
    if form_type is None:
        form_type_list = list(ALL_PARSEABLE_FORMS)
        logger.log_operation(
            operation_type="FORM_TYPE_DEFAULTED_ALL",
            download_success=True,
            level="INFO",
            download_error_message=(
                f"form_type=None; expanding to all parseable forms: {form_type_list}"
            ),
        )
    elif isinstance(form_type, str):
        form_type_list = [form_type]
    elif isinstance(form_type, list):
        form_type_list = form_type
    else:
        logger.log_operation(
            operation_type="INPUT_VALIDATION_ERROR",
            cik=None,
            download_success=False,
            parse_success=False,
            download_error_message=(
                f"Invalid form_type type {type(form_type).__name__}; defaulting to '13F-HR'."
            ),
        )
        form_type_list = ["13F-HR"]

    ### GET ALL FILING FOR SPECIC DATE RANGE ###
    # Get index data once for all specified years
    form_filters_for_index, cik_filters_for_index = normalize_filters(form_type_list, cik)
    if form_filters_for_index:
        form_filters_for_index = [_normalize_form_type(ft) for ft in form_filters_for_index]

    full_index_data_for_years = downloader.get_sec_index_data(
        start_year,
        end_year,
        form_filters=form_filters_for_index,
        cik_filters=cik_filters_for_index,
        show_progress=show_progress,
    )

    if full_index_data_for_years.empty:
        logger.log_operation(
            operation_type="INDEX_FETCH_FAIL",
            cik=None,
            download_success=False,
            parse_success=False,
            download_error_message=f"No index data found for years {start_year}-{end_year}. Cannot proceed.",
        )
        return

    # Build the raw-on-disk index once per call when resuming; reused across forms.
    # Skipped when resume=False to preserve the pre-0.6 "always refetch" semantics.
    raw_index = _build_raw_index(raw_data_dir_path) if resume else {}

    # Process each form type from the list
    for current_form_str in form_type_list:
        normalized_form_for_download = _normalize_form_type(current_form_str)
        is_section16_alias = current_form_str.upper() == SECTION16_ALIAS

        # Per-form known-accessions snapshot. Built once per form (not per CIK)
        # to minimize backend queries.
        known_accessions: set = backend.known_accessions(current_form_str) if resume else set()
        logger.log_operation(
            operation_type="RESUME_KNOWN_ACCESSIONS",
            form_type_processed=current_form_str,
            download_success=True,
            level="INFO",
            download_error_message=(
                f"resume={resume}; known accessions in backend: {len(known_accessions)}; "
                f"raw files on disk index: {len(raw_index)}"
            ),
        )

        logger.log_operation(
            operation_type="FORM_TYPE_PROCESSING_START",
            cik=None,
            download_success=True,
            parse_success=None,
            download_error_message=f"Processing form type: {current_form_str}",
        )

        # Filter index data for the current form type
        form_type_series = full_index_data_for_years["Form Type"].astype(str).str.strip()
        if is_section16_alias:
            # Section 16 filings sometimes carry leading spaces; strip before matching 3/4/5 prefixes
            index_data_for_current_form = full_index_data_for_years[
                form_type_series.str.startswith(SECTION16_BASE_FORMS, na=False)
            ]
        else:
            index_data_for_current_form = full_index_data_for_years[
                form_type_series.str.contains(normalized_form_for_download, na=False)
            ]

        if index_data_for_current_form.empty:
            logger.log_operation(
                operation_type="INDEX_FILTER_NO_RESULTS",
                cik=None,
                form_type_processed=current_form_str,
                download_success=False,
                parse_success=False,
                download_error_message=f"No index entries found for form type {current_form_str} from years {start_year}-{end_year}",
            )
            continue  # Move to the next form type in the list

        # Normalize CIKs for the current form's filtered index data
        index_data_for_current_form = index_data_for_current_form.copy()  # Avoid SettingWithCopyWarning
        index_data_for_current_form.loc[:, "CIK"] = index_data_for_current_form["CIK"].astype(str)

        ### EXTRACT UNIQUE CIKS FROM FILTERED INDEX DATA FOR THE CURRENT FORM TYPE ###
        available_ciks_for_form = index_data_for_current_form["CIK"].unique().tolist()

        logger.log_operation(
            operation_type="CIK_IDENTIFICATION",
            download_success=True,
            parse_success=None,
            download_error_message=f"Found {len(available_ciks_for_form)} CIKs for form type {current_form_str} from years {start_year}-{end_year}",
        )

        ciks_to_process_for_current_form: List[str]
        if cik is not None:  # User has specified CIK(s)
            user_ciks_input_list: List[str]
            if isinstance(cik, str):
                user_ciks_input_list = [str(cik).zfill(10)]
            elif isinstance(cik, list):
                user_ciks_input_list = [str(c).zfill(10) for c in cik]
            else:
                logger.log_operation(
                    operation_type="INPUT_VALIDATION_ERROR",
                    cik=None,
                    download_success=False,
                    parse_success=False,
                    download_error_message="Invalid CIK input type.",
                )
                continue

            ciks_to_process_for_current_form = [
                c_val for c_val in user_ciks_input_list if c_val in available_ciks_for_form
            ]

            if not ciks_to_process_for_current_form:
                logger.log_operation(
                    operation_type="CIK_FILTER_NO_MATCH",
                    cik=", ".join(user_ciks_input_list),
                    form_type_processed=current_form_str,
                    download_success=False,
                    parse_success=False,
                    download_error_message=f"None of the provided CIK(s) filed form type {current_form_str} in the specified date range.",
                )
                continue
        else:
            ciks_to_process_for_current_form = available_ciks_for_form

        if not ciks_to_process_for_current_form:
            logger.log_operation(
                operation_type="CIK_PROCESSING_SKIP",
                form_type_processed=current_form_str,
                download_success=False,
                parse_success=False,
                download_error_message=f"No CIKs to process for form type {current_form_str}.",
            )
            continue

        cik_iterator = (
            tqdm(
                ciks_to_process_for_current_form,
                desc=f"Processing firms with {current_form_str} filings",
                disable=not show_progress,
            )
            if show_progress
            else ciks_to_process_for_current_form
        )

        for current_cik_str in cik_iterator:
            try:
                # Further filter the index_data_for_current_form for the specific CIK.
                # This will be the subset passed to download_filings.
                company_filings_to_download = index_data_for_current_form[
                    index_data_for_current_form["CIK"] == current_cik_str
                ]

                if company_filings_to_download.empty:
                    # This case should ideally be caught by the CIK processing logic above,
                    # but as a safeguard if a CIK was in ciks_to_process_for_current_form
                    # but somehow has no entries in index_data_for_current_form.
                    logger.log_operation(
                        cik=current_cik_str,
                        operation_type="DOWNLOAD_PRECHECK_FAIL",
                        form_type_processed=current_form_str,
                        download_success=False,
                        parse_success=False,
                        download_error_message=f"No specific index entries found for CIK {current_cik_str} and form {current_form_str} before download call.",
                    )
                    continue

                downloaded_df = downloader.download_filings(
                    cik=current_cik_str,
                    form_type=normalized_form_for_download,
                    start_year=start_year,  # Still needed for fallback if index_data_subset is empty
                    end_year=end_year,  # Still needed for fallback
                    show_progress=False,
                    index_data_subset=company_filings_to_download,  # Pass the pre-filtered subset
                    known_accessions=known_accessions,
                    raw_index=raw_index,
                )

                if downloaded_df.empty:
                    logger.log_operation(
                        cik=current_cik_str,
                        operation_type="DOWNLOAD_NO_FILES_FOR_CIK",
                        form_type_processed=current_form_str,
                        download_success=False,
                        parse_success=False,
                        download_error_message=f"No filings found for CIK {current_cik_str}, form {current_form_str}",
                    )
                    continue  # Next CIK

                # Dispatch supported forms to their parsers; log + skip others.
                is_supported_form = (
                    any(sub in current_form_str.upper() for sub in ["13F", "NPORT"])
                    or current_form_str.upper() == SECTION16_ALIAS
                    or str(current_form_str).startswith(SECTION16_BASE_FORMS)
                )
                if is_supported_form:
                    process_filings_for_cik(
                        current_cik=current_cik_str,
                        downloaded=downloaded_df,
                        form_type=current_form_str,
                        base_dir=base_dir,
                        logger=logger,
                        show_progress=False,
                        backend=backend,
                        known_accessions=known_accessions,
                    )
                else:
                    logger.log_operation(
                        cik=current_cik_str,
                        operation_type="PARSING_SKIPPED_UNSUPPORTED_FORM",
                        form_type_processed=current_form_str,
                        download_success=True,
                        parse_success=False,
                        download_error_message=f"Form type '{current_form_str}' not specifically supported for parsing; storing raw files.",
                    )

                # After processing (parsing or storing raw) for the current CIK and form type
                if not keep_raw_files:
                    _cleanup_raw_files_for_cik(
                        downloaded_df=downloaded_df,
                        cik=current_cik_str,
                        form_type=current_form_str,
                        logger=logger,
                        raw_root=raw_data_dir_path,
                    )
                    # If dir_path doesn't exist (e.g., already removed in a previous step), do nothing

            except Exception as e:
                logger.log_operation(
                    cik=current_cik_str,
                    operation_type="CIK_PROCESSING_ERROR",
                    form_type_processed=current_form_str,
                    download_success=False,
                    parse_success=False,
                    download_error_message=f"Processing error for CIK {current_cik_str}, Form {current_form_str}: {str(e)}",
                )

    try:
        backend.close()
    except Exception as e_close:
        logger.log_operation(
            operation_type="BACKEND_CLOSE_ERROR",
            download_error_message=f"Error closing storage backend: {e_close}",
        )

    logger.log_operation(
        operation_type="GET_FILINGS_END",
        download_success=True,
        download_error_message=f"Finished get_filings. CIKs: {cik}, Forms: {form_type}, Years: {start_year}-{end_year}",
    )


__all__ = [
    "__version__",
    "get_filings",
    "get_parser_for_form_type_internal",
    "SECDownloader",
    "FilingLogger",
    "Form13FParser",
    "FormNPORTParser",
    "FormSection16Parser",
    "StorageBackend",
    "get_backend",
]
