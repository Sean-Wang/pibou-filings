# Changelog

All notable changes to `piboufilings` are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.1] — 2026-04-25

### Added
- **Resume / crash recovery** for `get_filings`. New `resume: bool = True`
  parameter. On a re-run, the orchestrator:
  - **Level A** — queries the storage backend for accession numbers already
    persisted in the matching `filing_info_*` table. Matching filings are
    skipped entirely (no HTTP call, no parse).
  - **Level B** — scans `raw_data_dir` once per call and maps accession
    numbers to any raw `.txt` already on disk. Matches that aren't in the
    backend are re-parsed from disk (no HTTP call).
  - Pass `resume=False` to force a full re-fetch (e.g. after a parser
    schema change you want to re-ingest).
- `StorageBackend.known_accessions(form_type: str) -> set[str]` on the
  Protocol. Both DuckDB and CSV backends implement it; missing
  table/column/file return an empty set (safe fallback).
- `piboufilings.storage.resolve_filing_info_dataset(form_type)` helper —
  maps `"13F-HR"` → `"filing_info_13f"`, `"NPORT-P"` → `"filing_info_nport"`,
  Section 16 alias / `"3"` / `"4"` / `"5"` → `"filing_info_sec16"`.
- 13F `filing_info` rows now include `ACCESSION_NUMBER` (extracted via the
  same header regex used by NPORT / Sec16). PK unchanged
  (`SEC_FILE_NUMBER, CONFORMED_DATE`). Legacy DuckDB files gain the
  column automatically via the backend's schema-evolution path.
- Two new log event types: `DOWNLOAD_SKIPPED_KNOWN` (Level A) and
  `DOWNLOAD_SKIPPED_RAW_EXISTS` (Level B), plus a per-form
  `RESUME_KNOWN_ACCESSIONS` summary.
- `tests/test_resume.py` — six scenarios (fresh run, Level A, Level B,
  opt-out, CSV backend, legacy DB without ACCESSION_NUMBER column).

### Changed
- `get_filings(..., resume=True)` is the new default. A second invocation
  with the same arguments will now skip already-processed filings. Pass
  `resume=False` to restore the pre-0.5.1 "always refetch" behavior.

### Migration notes
- **If you rely on 'always refetch' semantics**: pass `resume=False`.
- **If you upgrade an existing DuckDB file**: the first run after upgrade
  will re-fetch all 13F filings once (their rows pre-date the
  `ACCESSION_NUMBER` column). Subsequent runs resume normally.
- **Switching `export_format` between runs**: each backend only sees its
  own files. The new backend's `known_accessions` is empty — Level A
  won't skip anything, but Level B still reuses raw files on disk if
  `keep_raw_files=True`.

## [0.5.0] — 2026-04-24

### Added

- **DuckDB storage backend** as the default output format. A single
  `piboufilings.duckdb` file is written at `base_dir`, with one SQL table per
  dataset (`holdings_13f`, `filing_info_nport`, `transactions_sec16`, …) and
  primary-key-based deduplication. Crash-safe across interrupted runs.
- `export_format` parameter on `get_filings` — `"duckdb"` (default) or
  `"csv"`. The CSV path preserves the legacy period-partitioned filenames
  exactly.
- `piboufilings.storage` package exposing `StorageBackend` Protocol,
  `CSVBackend`, `DuckDBBackend`, and `get_backend(...)` factory.
- `piboufilings[duckdb]` install extra. `pip install 'piboufilings[duckdb]'`
  pulls in `duckdb>=0.9`; without the extra, only `export_format="csv"` works.
- Defense against XML external-entity (XXE) attacks: `defusedxml` for 13F and
  Section 16 parsing; a hardened `lxml` parser (`resolve_entities=False`,
  `no_network=True`) for NPORT.
- **Operation log `level` column** (`INFO`/`WARN`/`ERROR`/`DEBUG`), inferred
  from success flags when not set explicitly. Makes log analysis tractable.
- Test suite expanded from 22 → 64 tests covering: mocked-HTTP download,
  `get_filings` orchestrator, missing-duckdb `ImportError`, concurrent
  `FilingLogger` writes, timezone boundary behavior, XXE resistance, rate
  limiter semantics, raw-file cleanup helper.
- CI matrix expanded to Python 3.9–3.13. Ruff lint + format checks gate PRs;
  mypy runs in report-only mode; coverage gate at 65 %.
- `GlobalRateLimiter.reset()` for test use, plus a warning when a second
  init is called with different parameters.
- `py.typed` marker — downstream type checkers now pick up library hints.
- `CHANGELOG.md` (this file) and `CONTRIBUTING.md`.

### Changed

- **Breaking**: `get_filings` default output is now **DuckDB**, not CSV.
  Users relying on `13f_holdings_YYYY_Qn.csv` etc. must pass
  `export_format="csv"` explicitly. See the migration guide below.
- Package metadata moved from `setup.py` + `pytest.ini` into
  `pyproject.toml` (PEP 621). `setup.py` has been removed.
- `python_requires` raised to `>=3.9` (3.8 is EOL since Oct 2024, and the
  code was already using PEP 585 generics that fail at runtime on 3.8).
- Publish workflow migrated to **PyPI Trusted Publishing** (OIDC). No more
  `TWINE_PASSWORD` secrets. The workflow now requires a configured
  `pypi` / `testpypi` environment on the repo.
- Integration tests moved off the PR/push path. They now run only on a
  nightly schedule (05:00 UTC) or on manual `workflow_dispatch` with
  `run_integration=true`.
- Pandas minimum raised to `>=2.0`. Requests, tqdm, urllib3, lxml bumped to
  more realistic lower bounds.
- `_save_raw_filing` now returns `Optional[str]` (returning `None` for
  exhibit filings) instead of `numpy.nan` as a path sentinel.
- `get_filings` logic reduced in size: raw-file cleanup extracted into
  `_cleanup_raw_files_for_cik`.

### Fixed

- **Silent data loss in 13F holdings**: `astype(float, errors='ignore')` was
  deprecated-but-silently-accepted in pandas 2.x and would cascade into
  `ValueError` in the chained `.astype('Int64')`, caught by a broad
  `except Exception` that returned an empty DataFrame. Replaced with
  `pd.to_numeric(..., errors='coerce').astype('Int64')`; coercion losses are
  now logged rather than discarded.
- `AttributeError` inside the `process_filings_for_cik` exception handler
  when a `requests.RequestException` carried a `Response` object
  (`getattr(e, 'response', {}).get('status_code')` treated a Response as a
  dict). Replaced with `getattr(response, 'status_code', None)`.
- `FilingLogger` now creates nested parent directories and serializes writes
  from multiple threads with an explicit lock. Previously, a deep `log_dir`
  would raise `FileNotFoundError` and concurrent writes could interleave
  lines.
- Removed stray `print()` in the NPORT parser. Library code now routes
  through the standard `logging` module.
- Stale `LOG_FILE_PATH` / `LOG_HEADERS` constants, unused `os` import, dead
  orchestration dicts, and the `last_request_time` phantom field in
  `SECDownloader` have all been removed.

### Removed

- `setup.py` (replaced by `pyproject.toml [project]`).
- `pytest.ini` (configuration moved to `[tool.pytest.ini_options]`).
- Unused module-level constants and dead code identified in the pre-release
  audit.

---

## Migration guide: 0.4.x → 0.5.1

### I want my CSV output to keep working

Pass `export_format="csv"` to `get_filings`:

```python
get_filings(
    user_name=...,
    user_agent_email=...,
    cik=...,
    form_type=[...],
    export_format="csv",   # ← add this line
)
```

All legacy filenames (`13f_holdings_YYYY_Qn.csv`, `nport_holdings_YYYY_MM.csv`,
`sec16_info_YYYY_MM.csv`, etc.) and column schemas are preserved bit-for-bit.

### I want to adopt DuckDB

1. `pip install 'piboufilings[duckdb]'`
2. Re-run your pipeline; output will land at
   `<base_dir>/piboufilings.duckdb`.
3. Query it:

```python
import duckdb
con = duckdb.connect("./my_sec_data/piboufilings.duckdb", read_only=True)
con.sql("SELECT NAME_OF_ISSUER, SHARE_VALUE FROM holdings_13f "
        "ORDER BY SHARE_VALUE DESC LIMIT 10").df()
```

See `README.md` for the dataset → table mapping.

### I'm on Python 3.8

Upgrade to 3.9+. Python 3.8 reached end-of-life in October 2024; the 0.5.0
changes use PEP 585 runtime generics that fail on 3.8 anyway.

### I had custom code relying on `SECDownloader(package_version="0.4.0")`

The default is now read from `piboufilings._version.__version__`, so you can
drop the explicit argument entirely.

### I parsed the operations log CSV

The CSV now has a new `level` column (values: `INFO`/`WARN`/`ERROR`/`DEBUG`)
as the second column. If you pinned column positions, move off that or use
`pandas.read_csv` which indexes by name.
