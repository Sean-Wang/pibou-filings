# PibouFilings

<h3 style="text-align: center;">A Python library to download, parse, and analyze SEC EDGAR filings at scale. </h3>

[![PyPI](https://img.shields.io/pypi/v/piboufilings?color=blue)](https://pypi.org/project/piboufilings/)
[![Python](https://img.shields.io/pypi/pyversions/piboufilings?color=blue)](https://pypi.org/project/piboufilings/)
[![License](https://img.shields.io/badge/License-Non_Commercial-blue)](./LICENCE)
[![Downloads](https://img.shields.io/pepy/dt/piboufilings?color=blue)](https://pepy.tech/projects/piboufilings)
[![Open In Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/14CGkio1NVXI6pkuPliAmdBL4sT8u6H-t#scrollTo=wk3GmLlhbidZ)

---

> **What's new in 0.5.1** — Parsed data now lands in a **DuckDB** file by
> default (one table per dataset, PK-based dedup, crash-safe), and interrupted
> runs now resume safely by default. Pass
> `export_format="csv"` to keep the legacy period-partitioned CSV files.
> See [CHANGELOG.md](./CHANGELOG.md) for the full list and migration guide.
>
> `0.5.1` is still pre-1.0 (Alpha). The data schemas are stable; the public
> API may evolve.

## Filing Contents at a Glance

Unlock structured, analysis-ready data from the SEC’s filings:

- **Filer Metadata:** Clean, machine-ready identifiers and attributes for every SEC registrant.
- **13F Holdings:** Quarter-by-quarter institutional portfolios — securities, CUSIPs, share counts, values, voting authority, and manager relationships.
- **N-PORT Fund Disclosures:** Monthly holdings for mutual funds and ETFs, enriched with fund/series metadata, balance-sheet items, and returns.
- **Section 16 (Forms 3/4/5):** Fully normalized insider trading data — filing metadata, issuer/owner links, transaction tables (non-derivative & derivative), prices, share amounts, and end-of-period holdings.


## Installation

```bash
# Default: DuckDB output (recommended for large runs)
pip install 'piboufilings[duckdb]'

# Or the minimal install if you only need the CSV fallback
pip install piboufilings
```

`piboufilings` writes parsed filings through a pluggable storage layer. The default backend is **DuckDB** (single `piboufilings.duckdb` file at `base_dir`), which scales to tens of millions of holdings rows. The legacy **CSV** backend is still available via `export_format="csv"`.

## Quick Start

The primary entry point is `get_filings()`:

```python
from piboufilings import get_filings

# Remember to replace with your actual email for the User-Agent
USER_AGENT_EMAIL = "yourname@example.com"
USER_NAME = "Your Name or Company"  # Add your name or company

get_filings(
    user_name=USER_NAME,
    user_agent_email=USER_AGENT_EMAIL,
    cik="0001067983",               # Berkshire Hathaway CIK; None: download all available data
    form_type=["13F-HR", "NPORT-P", "SECTION-6"],# String or list of strings
    start_year=2020,
    end_year=2025,
    base_dir="./my_sec_data",       # Where parsed data is written
    log_dir="./my_sec_logs",        # Where operation logs go
    raw_data_dir="./my_sec_raw_data",# Where raw .txt filings are cached
    keep_raw_files=True,            # Set to False to delete raw .txt files after parsing
    max_workers=5,                  # Parallel workers (auto bucketed: quarterly for 13F, monthly for NPORT/Section16)
    export_format="duckdb",         # "duckdb" (default) or "csv"
)
```

After running with `export_format="duckdb"` (the default), parsed data lands in a single DuckDB database at `./my_sec_data/piboufilings.duckdb`. Each filing dataset becomes a SQL table; dedup is enforced by primary key so reruns are idempotent. If you prefer period-partitioned CSV files instead, pass `export_format="csv"` and the legacy filenames (`13f_holdings_YYYY_Qn.csv`, `nport_holdings_YYYY_MM.csv`, etc.) are written under `base_dir`.

Logs go to `./my_sec_logs` (or `./logs` by default). Raw filings default to `./data_raw/<identifier>/<form>/<accession>/`; set `raw_data_dir` to place them elsewhere.

### Querying the DuckDB output

```python
import duckdb

con = duckdb.connect("./my_sec_data/piboufilings.duckdb")
con.execute("SELECT COUNT(*) FROM holdings_13f").fetchone()
con.sql("""
    SELECT NAME_OF_ISSUER, SUM(SHARE_VALUE) AS total_value
    FROM holdings_13f
    WHERE CONFORMED_DATE >= '2024-01-01'
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10
""").df()
```

CIK number can be obtained from [SEC EDGAR Search Filings](https://www.sec.gov/search-filings).

### Recovery & resume

If `get_filings` is interrupted (Ctrl-C, OOM, network failure, SEC 5xx),
re-running the same command is safe and efficient: by default (`resume=True`)
the orchestrator skips anything already fully processed.

- **Level A** — if the accession is already in the storage backend's
  `filing_info_*` table, both the download and the parse are skipped.
- **Level B** — if the raw `.txt` is still on disk at `raw_data_dir` but
  missing from the backend (e.g. parse crashed after download), the HTTP
  call is skipped and the filing is re-parsed from disk.
- Pass `resume=False` to force a full re-fetch (e.g. after a parser
  schema change).

Skips are audited in the operations log (`DOWNLOAD_SKIPPED_KNOWN`,
`DOWNLOAD_SKIPPED_RAW_EXISTS`) and counted in a per-form
`RESUME_KNOWN_ACCESSIONS` summary.

---

## Filing Structure & Identifiers
PibouFilings organizes EDGAR data around two key public identifiers:

*   **IRS_NUMBER:** The Employer Identification Number (EIN) issued by the U.S. Internal Revenue Service. It uniquely identifies the legal entity submitting the filing.
*   **SEC_FILE_NUMBER:** The registration number assigned by the SEC. It distinguishes different types of filers and registrations (e.g., 028-xxxxx for 13F filers, 811-xxxxx for investment companies).

These two identifiers are public information, managed by U.S. federal agencies (IRS and SEC respectively), and act as the primary keys for organizing and indexing filings within PibouFilings.

### Filing Index
All parsed filings are first grouped by `IRS_NUMBER` and `SEC_FILE_NUMBER` into a structured index of registrants.

This allows you to track a fund or manager across multiple filings and time periods with full auditability.

### Holdings Reports
Each individual security holding (from 13F or N-PORT forms) is reported with a corresponding `SEC_FILE_NUMBER`.

This structure lets you link back any security-level data to the registered filing entity while keeping sensitive personal identifiers (e.g., signatory names) optional or excluded. When a filing discloses a CUSIP or similar identifier, it is preserved in the resulting CSVs so you can reconcile holdings downstream (see the legal notice below for usage requirements).

## Key Features

-   **Automated Downloads:** Fetch 13F, N-PORT, and Section 16 filings (via alias `SECTION-6`) by CIK, date range, or retrieve all available.
-   **Smart Parsing:**
    -   `Form13FParser`: Extracts detailed holdings and cover page data (including `IRS_NUMBER` and `SEC_FILE_NUMBER`) from 13F-HR filings.
    -   `FormNPORTParser`: Parses comprehensive fund/filer information (including `IRS_NUMBER` and `SEC_FILE_NUMBER`) and security holdings from N-PORT-P filings.
    -   `FormSection16Parser`: Normalizes Section 16 ownership XML (Forms 3/4/5) into filing-, transaction-, and holdings-level DataFrames (written to `filing_info_sec16` / `transactions_sec16` / `holdings_sec16` tables, or the equivalent `sec16_*.csv` files under the CSV backend).
-   **Two storage backends (same schemas):**
    -   **DuckDB** (`export_format="duckdb"`, default) — one `piboufilings.duckdb` file at `base_dir`, with one SQL table per dataset and primary-key-based deduplication. Crash-safe across interrupted runs; queryable from DuckDB, Python, pandas, SQL, or any tool that speaks `.duckdb`.
    -   **CSV** (`export_format="csv"`) — legacy period-partitioned files under `base_dir`, identical column schema to the DuckDB tables.

    **Dataset → DuckDB table / CSV filename mapping**

    | Dataset                   | DuckDB table                     | CSV filename (period-partitioned)               |
    |---------------------------|----------------------------------|-------------------------------------------------|
    | 13F filing info           | `filing_info_13f`                | `13f_info_YYYY_Qn.csv`                          |
    | 13F holdings              | `holdings_13f`                   | `13f_holdings_YYYY_Qn.csv`                      |
    | 13F other managers reporting | `other_managers_reporting_13f` | `13f_other_managers_reporting_YYYY_Qn.csv`     |
    | 13F other included managers  | `other_included_managers_13f`  | `13f_other_included_managers_YYYY_Qn.csv`      |
    | N-PORT filing info        | `filing_info_nport`              | `nport_filing_info_YYYY_MM.csv`                 |
    | N-PORT holdings           | `holdings_nport`                 | `nport_holdings_YYYY_MM.csv`                    |
    | Section 16 filing info    | `filing_info_sec16`              | `sec16_info_YYYY_MM.csv`                        |
    | Section 16 transactions   | `transactions_sec16`             | `sec16_transactions_YYYY_MM.csv`                |
    | Section 16 holdings       | `holdings_sec16`                 | `sec16_holdings_YYYY_MM.csv`                    |

    Column names and meanings are the same across both backends — see the **Field Reference** below.
-   **Robust EDGAR Interaction:**
    -   Adheres to SEC rate limits (10 req/sec) via a configurable global token bucket rate limiter.
    -   Comprehensive retry mechanism for network requests (handles connection errors, read errors, and specific HTTP status codes like 429, 5xx).
-   **Efficient & Configurable:**
    -   Parallelized downloads using `ThreadPoolExecutor` for faster processing of CIKs with multiple filings.
    -   **Mandatory auto-partitioned worker distribution (no legacy mixed-file scheduling):**
        - 13F workers process **independent quarter buckets** (`YYYY-Qn`).
        - Section 16 workers process **independent month buckets** (`YYYY-MM`).
        - N-PORT workers process **independent month buckets** (`YYYY-MM`).
        - This design prevents workers from contending over the same time slice and reduces file-level conflicts.
    -   **Multi-progress display for downloads:**
        - One global progress bar tracks all filings for the CIK.
        - One worker/bucket progress bar is shown per period (quarter/month), so each worker reports independently.
    -   Option to `keep_raw_files` (default True) or delete them after processing.
    -   Customizable directories for data and logs.
-   **Detailed Logging:**
    -   Records operations to a daily CSV log file (e.g., `logs/filing_operations_YYYYMMDD.csv`).
    -   Logs include timestamps, descriptive `operation_type` (e.g., `DOWNLOAD_SINGLE_FILING_SUCCESS`), CIK, accession number, success/failure status, error messages, and specific `error_code` (like HTTP status codes) where applicable.
-   **Data Analytics Ready:** Pandas DataFrames internally; DuckDB and CSV outputs share identical column schemas.
-   **Handles Amendments:** Automatically processes and correctly identifies amended filings (e.g., `13F-HR/A`, `NPORT-P/A`).


## Supported Form Types

| Category       | Supported Forms                               | Notes                                                                 |
|----------------|-----------------------------------------------|-----------------------------------------------------------------------|
| 13F Filings    | `13F-HR`, `13F-HR/A`                          | Institutional Investment Manager holdings reports.                    |
| N-PORT Filings | `NPORT-P`, `NPORT-P/A`                        | Monthly portfolio holdings for registered investment companies (funds). |
| Section 16     | `3`, `3/A`, `4`, `4/A`, `5`, `5/A` (or alias `SECTION-6`) | Insider ownership and trade reports filed by officers/directors/10% holders. |
| Ignored        | `NPORT-EX`, `NPORT-EX/A`, `NT NPORT-P`, `NT NPORT-EX` | Exhibit-only or notice filings, typically not parsed for holdings.    |

## Field Reference

Column names below apply to **both** the DuckDB tables and the CSV files — they share the same schema (see the mapping table above). The headings use the CSV filename for readability.

<details>
<summary> 13f_info_YYYY_Qn.csv </summary>

### `13f_info_YYYY_Qn.csv`
| Column | Description |
| --- | --- |
| `CIK` | Central Index Key for the registrant (10-digit, zero padded). |
| `REPORT_TYPE` | Verbatim `reportType` value (e.g., “13F COMBINATION REPORT”). |
| `IRS_NUMBER` | Employer Identification Number (EIN) extracted from the header. |
| `SEC_FILE_NUMBER` | The filer’s Form 13F file number (028-xxxxx). |
| `DOC_TYPE` | Conformed submission type (e.g., `13F-HR`, `13F-HR/A`). |
| `CONFORMED_DATE` | Period of report in `YYYY-MM-DD`. |
| `FILED_DATE` | Filing date in `YYYY-MM-DD`. |
| `ACCEPTANCE_DATETIME` | EDGAR acceptance timestamp (`YYYY-MM-DD HH:MM:SS`). |
| `PUBLIC_DOCUMENT_COUNT` | Number of public documents attached to the submission. |
| `SEC_ACT` | Applicable Securities Act reference (e.g., `1934 Act`). |
| `FILM_NUMBER` | SEC film number assigned to the filing. |
| `NUMBER_TRADES` | `tableEntryTotal` (count of holdings rows). |
| `TOTAL_VALUE` | `tableValueTotal` (total holdings value reported, thousands USD). |
| `OTHER_INCLUDED_MANAGERS_COUNT` | `otherIncludedManagersCount` element value. |
| `IS_CONFIDENTIAL_OMITTED` | `true/false` flag from `isConfidentialOmitted`. |
| `SIGNATURE_NAME`/`TITLE`/`CITY`/`STATE` | Signatory block metadata. |
| `AMENDMENT_FLAG` | `Y` or `N` depending on whether the filing is an amendment. |
| `MAIL_*`, `BUSINESS_*` fields | Mailing and business address lines captured from the header. |
| `COMPANY_NAME` | “COMPANY CONFORMED NAME” value. |
| `BUSINESS_PHONE` | Phone number provided in the header. |
| `STATE_INC` | State of incorporation. |
| `FORMER_COMPANY_NAME` | Most recent former name, if supplied. |
| `FISCAL_YEAR_END` | Fiscal year end in `MMDD`. |
| `STANDARD_INDUSTRIAL_CLASSIFICATION` | SIC description reported by the filer. |
| `SEC_FILING_URL` | Direct HTTPS link to the raw EDGAR text file that was parsed. |
| `CREATED_AT` / `UPDATED_AT` | Timestamps for when the row was generated by PibouFilings. |
</details>

<details>
<summary> 13f_holdings_YYYY_Qn.csv </summary>

### `13f_holdings_YYYY_Qn.csv`
| Column | Description |
| --- | --- |
| `SEC_FILE_NUMBER` | File number of the reporting manager for the holding. |
| `CONFORMED_DATE` | Reporting period (`YYYY-MM-DD`). |
| `NAME_OF_ISSUER` | `nameOfIssuer` element from the XML table. |
| `TITLE_OF_CLASS` | Security class (`titleOfClass`). |
| `CUSIP` | Reported CUSIP identifier (exactly as filed). |
| `SHARE_VALUE` | Market value reported (`value`, in thousands USD). |
| `SHARE_AMOUNT` | Number of shares or principal amount (`sshPrnamt`). |
| `SH_PRN` | Share/Principal type (`sshPrnamtType`, e.g., `SH`, `PRN`). |
| `PUT_CALL` | `putCall` tag for option positions (often blank). |
| `DISCRETION` | `investmentDiscretion` field (`SOLE`, `SHARED`, `DEFINED`). |
| `SOLE_VOTING_AUTHORITY` | Shares with sole voting authority. |
| `SHARED_VOTING_AUTHORITY` | Shares with shared voting authority. |
| `NONE_VOTING_AUTHORITY` | Shares with no voting authority. |
| `CREATED_AT` / `UPDATED_AT` | Generation timestamps for each holding row. |

</details>

<details>
<summary> nport_filing_info_YYYY_MM.csv </summary>

### `nport_filing_info_YYYY_MM.csv`
| Column | Description |
| --- | --- |
| `ACCESSION_NUMBER` | EDGAR accession number for the N-PORT filing. |
| `CIK` | Central Index Key of the registrant (fund complex). |
| `FORM_TYPE` | Conformed submission type from the header (e.g., `NPORT-P`, `NPORT-P/A`). |
| `PERIOD_OF_REPORT` | Header `CONFORMED PERIOD OF REPORT` date for the filing. |
| `FILED_DATE` | Header `FILED AS OF DATE`; official filing date. |
| `SEC_FILE_NUMBER` | Header `SEC FILE NUMBER`; registrant’s file number (e.g., `811-xxxxx`). |
| `FILM_NUMBER` | SEC film number assigned to the submission. |
| `ACCEPTANCE_DATETIME` | Header `ACCEPTANCE-DATETIME`; EDGAR acceptance timestamp. |
| `PUBLIC_DOCUMENT_COUNT` | Header `PUBLIC DOCUMENT COUNT`; number of public documents attached. |
| `COMPANY_NAME` | Header `COMPANY CONFORMED NAME`; registrant name. |
| `IRS_NUMBER` | EIN/IRS number of the registrant. |
| `STATE_INC` | State of incorporation from the header. |
| `FISCAL_YEAR_END` | Fiscal year end from the header (`MMDD`). |
| `BUSINESS_STREET_1` | Business address street line 1 from the header. |
| `BUSINESS_STREET_2` | Business address street line 2 from the header. |
| `BUSINESS_CITY` | Business address city. |
| `BUSINESS_STATE` | Business address state. |
| `BUSINESS_ZIP` | Business address ZIP code. |
| `BUSINESS_PHONE` | Business phone number from the header. |
| `MAIL_STREET_1` | Mailing address street line 1. |
| `MAIL_STREET_2` | Mailing address street line 2. |
| `MAIL_CITY` | Mailing address city. |
| `MAIL_STATE` | Mailing address state. |
| `MAIL_ZIP` | Mailing address ZIP code. |
| `FORMER_COMPANY_NAMES` | Semicolon-separated former names with change dates, as reported in the header. |
| `REPORT_DATE` | Reporting period end date from N-PORT XML (`genInfo/repPdEnd`). |
| `FUND_REG_NAME` | Fund registrant name from N-PORT XML (`regName`). |
| `FUND_FILE_NUMBER` | Fund file number from N-PORT XML (`regFileNumber`). |
| `FUND_LEI` | Fund registrant LEI from N-PORT XML (`regLei`). |
| `SERIES_NAME` | Series name reported in N-PORT (`seriesName`). |
| `SERIES_LEI` | Series LEI (`seriesLei`). |
| `FUND_TOTAL_ASSETS` | Total assets from `fundInfo/totAssets`. |
| `FUND_TOTAL_LIABS` | Total liabilities from `fundInfo/totLiabs`. |
| `FUND_NET_ASSETS` | Net assets from `fundInfo/netAssets`. |
| `ASSETS_ATTR_MISC_SEC` | Assets attributable to miscellaneous securities (`assetsAttrMiscSec`). |
| `ASSETS_INVESTED` | Net assets invested in securities (`assetsInvested`). |
| `AMT_PAY_ONE_YR_BANKS_BORR` | Amount payable within one year to banks for borrowings. |
| `AMT_PAY_ONE_YR_CTRLD_COMP` | Amount payable within one year to controlled companies. |
| `AMT_PAY_ONE_YR_OTH_AFFIL` | Amount payable within one year to other affiliates. |
| `AMT_PAY_ONE_YR_OTHER` | Amount payable within one year to non-affiliates/other parties. |
| `AMT_PAY_AFT_ONE_YR_BANKS_BORR` | Amount payable after one year to banks for borrowings. |
| `AMT_PAY_AFT_ONE_YR_CTRLD_COMP` | Amount payable after one year to controlled companies. |
| `AMT_PAY_AFT_ONE_YR_OTH_AFFIL` | Amount payable after one year to other affiliates. |
| `AMT_PAY_AFT_ONE_YR_OTHER` | Amount payable after one year to non-affiliates/other parties. |
| `DELAY_DELIVERY` | Delayed delivery and when-issued commitments (`delayDeliv`). |
| `STANDBY_COMMIT` | Standby commitment agreements (`standByCommit`). |
| `LIQUID_PREF` | Amount of liquid preferred stock and similar instruments (`liquidPref`). |
| `CASH_NOT_RPTD_IN_COR_D` | Cash not reported in the core data section (`cshNotRptdInCorD`). |
| `IS_NON_CASH_COLLATERAL` | Flag indicating presence of non-cash collateral at the fund level. |
| `MONTH_1_RETURN` | Total return for the most recent month (`monthlyTotReturn@rtn1`). |
| `MONTH_2_RETURN` | Total return for the second month preceding the report date (`rtn2`). |
| `MONTH_3_RETURN` | Total return for the third month preceding the report date (`rtn3`). |
| `MONTH_1_NET_REALIZED_GAIN` | Net realized gain/loss for the most recent month. |
| `MONTH_2_NET_REALIZED_GAIN` | Net realized gain/loss for the second month preceding the report date. |
| `MONTH_3_NET_REALIZED_GAIN` | Net realized gain/loss for the third month preceding the report date. |
| `MONTH_1_NET_UNREALIZED_APPR` | Net unrealized appreciation/depreciation for the most recent month. |
| `MONTH_2_NET_UNREALIZED_APPR` | Net unrealized appreciation/depreciation for the second month. |
| `MONTH_3_NET_UNREALIZED_APPR` | Net unrealized appreciation/depreciation for the third month. |
| `MONTH_1_REDEMPTION` | Redemptions during the most recent month (`mon1Flow@redemption`). |
| `MONTH_2_REDEMPTION` | Redemptions during the second month. |
| `MONTH_3_REDEMPTION` | Redemptions during the third month. |
| `MONTH_1_REINVESTMENT` | Reinvestments during the most recent month (`mon1Flow@reinvestment`). |
| `MONTH_2_REINVESTMENT` | Reinvestments during the second month. |
| `MONTH_3_REINVESTMENT` | Reinvestments during the third month. |
| `MONTH_1_SALES` | Sales during the most recent month (`mon1Flow@sales`). |
| `MONTH_2_SALES` | Sales during the second month. |
| `MONTH_3_SALES` | Sales during the third month. |
| `CREATED_AT` / `UPDATED_AT` | Timestamps for when the row was generated/updated by PibouFilings. |

</details>

<details>
<summary> nport_holdings_YYYY_MM.csv </summary>

### `nport_holdings_YYYY_MM.csv`
| Column | Description |
| --- | --- |
| `ACCESSION_NUMBER` | Accession number of the N-PORT filing this holding comes from. |
| `CIK` | Registrant CIK (fund complex) copied from filing info. |
| `PERIOD_OF_REPORT` | Reporting period end date used for the holding (from `REPORT_DATE`). |
| `FILED_DATE` | Filing date (`FILED AS OF DATE`). |
| `SEC_FILE_NUMBER` | Registrant’s SEC file number associated with the holding. |
| `SECURITY_NAME` | `invstOrSec/name`; name of the security or issuer. |
| `TITLE` | `invstOrSec/title`; security title or description. |
| `CUSIP` | Security CUSIP from `cusip` (or `idenOther` when typed as CUSIP). |
| `LEI` | Security LEI from `lei` (or `idenOther` when typed as LEI). |
| `BALANCE` | `invstOrSec/balance`; quantity or notional amount of the position. |
| `UNITS` | `invstOrSec/units`; unit type or share class for the balance. |
| `CURRENCY` | `invstOrSec/curCd`; currency code of the holding. |
| `VALUE_USD` | `invstOrSec/valUSD`; fair value of the position in U.S. dollars. |
| `PCT_VALUE` | `invstOrSec/pctVal`; position’s percentage of fund net assets. |
| `PAYOFF_PROFILE` | `invstOrSec/payoffProfile`; payoff profile classification (e.g., debt, equity, derivative). |
| `ASSET_CATEGORY` | `invstOrSec/assetCat`; asset category for the holding. |
| `ISSUER_CATEGORY` | `invstOrSec/issuerCat`; issuer category classification. |
| `COUNTRY` | `invstOrSec/invCountry`; country of investment or issuer. |
| `IS_RESTRICTED` | `invstOrSec/isRestrictedSec`; flag if the security is restricted. |
| `FAIR_VALUE_LEVEL` | `invstOrSec/fairValLevel`; fair value hierarchy level (e.g., 1, 2, 3). |
| `IS_CASH_COLLATERAL` | `securityLending/isCashCollateral`; flag if position is posted as cash collateral. |
| `IS_NON_CASH_COLLATERAL` | `securityLending/isNonCashCollateral`; flag if position is posted as non-cash collateral. |
| `IS_LOAN_BY_FUND` | `securityLending/isLoanByFund`; flag if this is a security loaned by the fund. |
| `MATURITY_DATE` | `debtSec@maturityDt`; maturity date for debt securities. |
| `COUPON_KIND` | `debtSec@couponKind`; coupon type (e.g., fixed, floating). |
| `ANNUAL_RATE` | `debtSec@annualizedRt`; annualized interest rate for debt securities. |
| `IS_DEFAULT` | `debtSec@isDefault`; flag if the issuer is in default. |
| `NUM_PAYMENTS_ARREARS` | `debtSec@numPaymentsInArrears`; number of payments in arrears. |
| `DERIVATIVE_CAT` | `derivativeInfo/derivCat`; derivative category for the position. |
| `COUNTERPARTY_NAME` | `derivativeInfo/counterpartyName`; name of the derivative counterparty. |
| `ABS_CAT` | `assetBackedSec/absCat`; asset-backed security category. |
| `ABS_SUB_CAT` | `assetBackedSec/absSubCat`; asset-backed security subcategory. |
| `CREATED_AT` / `UPDATED_AT` | Timestamps for when the row was generated/updated by PibouFilings. |

</details>

<details>
<summary> sec16_info_YYYY_MM.csv </summary>

### `sec16_info_YYYY_MM.csv`
| Column | Description |
| --- | --- |
| `ACCESSION_NUMBER` | EDGAR accession number for the Section 16 filing. |
| `DOCUMENT_TYPE` | XML `documentType` (e.g., `3`, `4`, `5`, `3/A`, `4/A`, `5/A`). |
| `PERIOD_OF_REPORT` | XML `periodOfReport`; the reporting period date for the form. |
| `DATE_FILED` | Header `FILED AS OF DATE`; official filing date. |
| `ACCEPTANCE_DATETIME` | Header `ACCEPTANCE-DATETIME`; EDGAR acceptance timestamp. |
| `SCHEMA_VERSION` | Section 16 XML `schemaVersion` used by the filing. |
| `ISSUER_CIK` | XML `issuerCik`; CIK of the issuer whose securities are reported. |
| `ISSUER_NAME` | XML `issuerName`; legal name of the issuer. |
| `ISSUER_TRADING_SYMBOL` | XML `issuerTradingSymbol`; issuer’s ticker symbol. |
| `RPT_OWNER_CIK` | XML `rptOwnerCik`; CIK of the reporting owner (insider). |
| `RPT_OWNER_NAME` | XML `rptOwnerName`; name of the reporting owner. |
| `RPT_OWNER_STREET1` | XML `rptOwnerStreet1`; first address line for the reporting owner. |
| `RPT_OWNER_STREET2` | XML `rptOwnerStreet2`; second address line, if present. |
| `RPT_OWNER_CITY` | XML `rptOwnerCity`; city of the reporting owner. |
| `RPT_OWNER_STATE` | XML `rptOwnerState`; state or province of the reporting owner. |
| `RPT_OWNER_ZIP` | XML `rptOwnerZipCode`; postal/ZIP code of the reporting owner. |
| `IS_DIRECTOR` | XML `isDirector`; boolean flag if the owner is a director of the issuer. |
| `IS_OFFICER` | XML `isOfficer`; boolean flag if the owner is an officer of the issuer. |
| `OFFICER_TITLE` | XML `officerTitle`; officer role/title (e.g., “Chief Executive Officer”). |
| `IS_TEN_PCT_OWNER` | XML `isTenPercentOwner`; boolean flag for ≥10% beneficial ownership. |
| `IS_OTHER` | XML `isOther`; boolean flag indicating any “other” relationship to the issuer. |
| `OTHER_TEXT` | XML `otherText`; description of the “other” relationship when `IS_OTHER` is true. |
| `REMARKS` | XML `remarks`; free-form remarks section from the filing. |
| `SEC_FILING_URL` | Direct HTTPS link to the raw EDGAR text file that was parsed. |
| `CREATED_AT` / `UPDATED_AT` | Timestamps for when the row was generated/updated by PibouFilings. |

</details>

<details>
<summary> sec16_transactions_YYYY_MM.csv </summary>

### `sec16_transactions_YYYY_MM.csv`
| Column | Description |
| --- | --- |
| `ACCESSION_NUMBER` | Accession number of the filing this transaction comes from. |
| `DOCUMENT_TYPE` | Filing-level `documentType` (e.g., `3`, `4`, `5`). |
| `PERIOD_OF_REPORT` | Filing-level `periodOfReport`; reporting period of the form. |
| `ISSUER_CIK` | Issuer CIK from `issuerCik`. |
| `ISSUER_NAME` | Issuer name from `issuerName`. |
| `ISSUER_TRADING_SYMBOL` | Issuer ticker symbol from `issuerTradingSymbol`. |
| `RPT_OWNER_CIK` | Reporting owner CIK from `rptOwnerCik`. |
| `RPT_OWNER_NAME` | Reporting owner name from `rptOwnerName`. |
| `TABLE_TYPE` | Source table: `NON_DERIVATIVE` or `DERIVATIVE`. |
| `SECURITY_TITLE` | XML `securityTitle/value`; title of the security transacted. |
| `TRANSACTION_FORM_TYPE` | XML `transactionCoding/transactionFormType`; form subtype for the line. |
| `TRANSACTION_CODE` | XML `transactionCoding/transactionCode`; Form 4 transaction code (e.g., `P`, `S`, `M`). |
| `EQUITY_SWAP_INVOLVED` | XML `transactionCoding/equitySwapInvolved`; flag if an equity swap is involved. |
| `TRANSACTION_DATE` | XML `transactionDate/value`; date on which the transaction occurred. |
| `DEEMED_EXECUTION_DATE` | XML `deemedExecutionDate/value`; deemed execution date, if reported. |
| `TRANSACTION_SHARES` | XML `transactionAmounts/transactionShares/value`; number of shares/units transacted. |
| `TRANSACTION_PRICE_PER_SHARE` | XML `transactionAmounts/transactionPricePerShare/value`; price per share or unit. |
| `SHARES_OWNED_FOLLOWING_TRANSACTION` | XML `postTransactionAmounts/sharesOwnedFollowingTransaction/value`; shares beneficially owned after the transaction. |
| `DIRECT_OR_INDIRECT_OWNERSHIP` | XML `ownershipNature/directOrIndirectOwnership/value`; typically `D` (direct) or `I` (indirect). |
| `NATURE_OF_OWNERSHIP` | XML `ownershipNature/natureOfOwnership/value`; text describing the nature of ownership. |
| `CONVERSION_OR_EXERCISE_PRICE` | For derivative transactions: XML `conversionOrExercisePrice/value`; exercise/conversion price. |
| `EXERCISE_DATE` | XML `exerciseDate/value`; date when the derivative becomes exercisable. |
| `EXPIRATION_DATE` | XML `expirationDate/value`; expiration date of the derivative instrument. |
| `UNDERLYING_SECURITY_TITLE` | XML `underlyingSecurity/underlyingSecurityTitle/value`; title of the underlying security. |
| `UNDERLYING_SECURITY_SHARES` | XML `underlyingSecurity/underlyingSecurityShares/value`; number of underlying shares represented. |
| `FOOTNOTE_IDS` | Comma-separated list of `footnoteId` values referenced by this transaction row. |
| `CREATED_AT` / `UPDATED_AT` | Timestamps for when the row was generated/updated by PibouFilings. |

</details>

<details>
<summary> sec16_holdings_YYYY_MM.csv </summary>

### `sec16_holdings_YYYY_MM.csv`
| Column | Description |
| --- | --- |
| `ACCESSION_NUMBER` | Accession number of the filing this holding comes from. |
| `DOCUMENT_TYPE` | Filing-level `documentType` (e.g., `3`, `4`, `5`). |
| `PERIOD_OF_REPORT` | Filing-level `periodOfReport`; date of the ownership snapshot. |
| `ISSUER_CIK` | Issuer CIK from `issuerCik`. |
| `ISSUER_NAME` | Issuer name from `issuerName`. |
| `ISSUER_TRADING_SYMBOL` | Issuer ticker symbol from `issuerTradingSymbol`. |
| `RPT_OWNER_CIK` | Reporting owner CIK from `rptOwnerCik`. |
| `RPT_OWNER_NAME` | Reporting owner name from `rptOwnerName`. |
| `TABLE_TYPE` | Source table: `NON_DERIVATIVE_HOLDING` or `DERIVATIVE_HOLDING`. |
| `SECURITY_TITLE` | XML `securityTitle/value`; title of the held security or derivative. |
| `SHARES_OWNED` | XML `postTransactionAmounts/sharesOwnedFollowingTransaction/value`; shares/units beneficially owned. |
| `DIRECT_OR_INDIRECT_OWNERSHIP` | XML `ownershipNature/directOrIndirectOwnership/value`; `D` (direct) or `I` (indirect). |
| `NATURE_OF_OWNERSHIP` | XML `ownershipNature/natureOfOwnership/value`; text describing the nature of ownership. |
| `CONVERSION_OR_EXERCISE_PRICE` | For derivative holdings: XML `conversionOrExercisePrice/value`; exercise/conversion price. |
| `EXERCISE_DATE` | XML `exerciseDate/value`; date when the derivative becomes exercisable. |
| `EXPIRATION_DATE` | XML `expirationDate/value`; expiration date of the derivative holding. |
| `UNDERLYING_SECURITY_TITLE` | XML `underlyingSecurity/underlyingSecurityTitle/value`; title of the underlying security. |
| `UNDERLYING_SECURITY_SHARES` | XML `underlyingSecurity/underlyingSecurityShares/value`; number of underlying shares represented. |
| `FOOTNOTE_IDS` | Comma-separated `footnoteId` values referenced by this holding row. |
| `CREATED_AT` / `UPDATED_AT` | Timestamps for when the row was generated/updated by PibouFilings. |

</details>
<br>
The helper CSVs `13f_other_managers_reporting_YYYY_Qn.csv` and `13f_other_included_managers_YYYY_Qn.csv` mirror the tables from each cover page and contain the SEC file numbers and names necessary to interpret the numbered manager references that appear in the holdings.

---

## Troubleshooting

**`ImportError: DuckDB is not installed…`**
You installed `piboufilings` without the `[duckdb]` extra, but called
`get_filings(...)` with the default `export_format="duckdb"`. Either install
the extra (`pip install 'piboufilings[duckdb]'`) or pass
`export_format="csv"`.

**`ValueError: Unknown export_format 'parquet'`**
Only `"duckdb"` and `"csv"` are supported in 0.5.1. File an issue if you'd
like another backend.

**SEC returns `HTTP 429` under high parallelism**
The built-in rate limiter targets ~7 req/s (10 req/s SEC cap × 0.7 safety).
If you see 429s, lower `max_workers` or run the workload over fewer CIKs.
Retries with exponential backoff are handled automatically.

**Download stalls or my disk fills up**
The legacy CSV append path was quadratic in existing row count. Upgrade to
0.5.1 and use the DuckDB backend (the default). See the
[CHANGELOG](./CHANGELOG.md).

**`FileNotFoundError` on the log directory**
Fixed in 0.5.1 — `FilingLogger` now creates missing parent directories.
Older versions required the parent to exist.

**`ConnectionException: Can't open a connection to same database file with a different configuration than existing connections`**
DuckDB keeps a process-wide instance cache — if you opened the DB in
read-write mode (for example by calling `get_filings(...)`), you can't
reopen the same file with a *different* config in the same Python process
until the first connection is dropped.

- In notebooks, open the DB with the default read-write config (omit
  `read_only=True`) and call `con.close()` when done with the peek cell.
- Or restart the kernel to reset all connections.
- In library code, call `backend.close()` (0.5.1 drops its reference to the
  connection so the next `duckdb.connect(...)` can use any config).

**My operations-log CSV has an unexpected `level` column**
New in 0.5.1 — second column is now `level` (`INFO`/`WARN`/`ERROR`/`DEBUG`).
Read the CSV with `pandas.read_csv` and address columns by name rather than
position.

**I restarted after a crash and most filings are being skipped — is that right?**
Yes. As of 0.5.1, `get_filings` defaults to `resume=True`. On a re-run, any
filing whose accession is already in your storage backend is skipped (no
HTTP call, no parse). Check the operations log for
`DOWNLOAD_SKIPPED_KNOWN` entries. If you deliberately want a full
re-fetch — for example, after upgrading and needing to pick up new
parser columns — pass `resume=False`.

**My filings all have empty `NAME_OF_ISSUER` / zero rows**
Before 0.5.1, a single non-numeric cell in a 13F `<value>` or `<sshPrnamt>`
element would silently wipe the entire holdings DataFrame for that filing.
Fixed — bad cells become `<NA>` and a warning is logged; the rest of the
filing is preserved.

**Integration tests are failing in CI**
They're opt-in as of 0.5.1 — triggered by the nightly schedule or manual
`workflow_dispatch`. PRs don't hit live SEC.

---

## Disclaimer


### Identifier & Data Usage Notice

PibouFilings emits the identifiers that appear in the original documents (e.g., CUSIP, CINS, ISIN, LEI). Those identifiers remain the property of their respective issuers (for example, CUSIP Global Services and the American Bankers Association for CUSIPs). By using this software you agree to:

1. Use such identifiers only in accordance with the licensing terms imposed by their owners.
2. Obtain any required licenses for commercial redistribution or downstream products that include those identifiers.
3. Remove or redact identifiers if your use case is not covered by those licenses.

The project itself does not grant any rights to proprietary identifier datasets.

### General Disclaimer

PibouFilings is an independent, open-source research tool and is not affiliated with, endorsed by, or in any way connected to the U.S. Securities and Exchange Commission (SEC), the EDGAR system, CUSIP Global Services, or any other proprietary data provider.

PibouFilings processes only publicly accessible EDGAR filings and does not incorporate external or third-party datasets. All information is extracted directly from the original SEC submissions. Some filings may contain licensed proprietary identifiers (including, but not limited to, CUSIP, ISIN, and CINS codes). These identifiers are retained solely as they appear in the source filings to support accurate record reconciliation. PibouFilings does not grant, sublicense, or convey any rights to such identifiers. Users are solely responsible for securing any necessary licenses and ensuring compliance with all applicable intellectual property or data-usage restrictions.

This project is distributed under a Non-Commercial License and is intended solely for research and educational purposes. This license governs the PibouFilings source code and the processed formats generated by the library; it does not supersede or modify any third-party rights associated with identifiers contained in the filings. Any commercial use or redistribution of the software or its outputs requires prior written permission from the author.

Users must comply with the SEC’s [Fair Access guidelines](https://www.sec.gov/edgar/sec-api-documentation), including the use of a valid User-Agent and adherence to rate-limit requirements. By using PibouFilings, you acknowledge these obligations.

The author makes no warranty regarding the accuracy, completeness, or suitability of any information produced by this tool and disclaims all liability for any losses or damages arising from its use. Users assume full responsibility for how they apply the software and for any data generated through it.

For questions about usage or compliance, please contact the author directly.
