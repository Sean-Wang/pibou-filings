# Contributing to piboufilings

Thanks for your interest! This doc covers the mechanics of contributing
code. For questions and design discussion, please open an issue first so we
can agree on approach before you invest time in a PR.

## Dev setup

```bash
git clone https://github.com/Pierre-Bouquet/pibou-filings.git
cd pibou-filings

python -m venv .venv
source .venv/bin/activate         # Windows: .venv\Scripts\activate

# Install everything needed to run the full CI locally
pip install -e ".[duckdb,test,lint]"
```

Minimum Python: **3.10**. CI runs 3.10, 3.11, 3.12, 3.13.

## Running the local CI

The three gating checks PRs must pass are:

```bash
# 1) Lint (gates every PR)
ruff check piboufilings tests
ruff format --check piboufilings tests

# 2) Unit tests + coverage (≥ 65% required)
pytest -m "not integration" --cov=piboufilings --cov-fail-under=65

# 3) Type-check (informational only, not gating today)
mypy piboufilings
```

All three are orchestrated by `.github/workflows/tests.yml`.

## Running the full test suite, including live SEC

Integration tests hit **live SEC EDGAR** and are therefore off the PR path.
Locally you can run them with:

```bash
pytest -m integration
```

In CI, they run only on the nightly schedule and on manual
`workflow_dispatch` with `run_integration=true`.

## Fixing lint drift

Ruff can auto-fix the majority of issues:

```bash
ruff check piboufilings tests --fix
ruff format piboufilings tests
```

If `ruff check --fix` applies changes that alter behavior (unlikely but
possible with `--unsafe-fixes`), review the diff carefully before committing.

## Writing tests

- **Prefer offline tests** (`@pytest.mark.unit` or unmarked) over integration
  tests. HTTP interactions are mocked with `requests-mock`; storage tests use
  `tmp_path`. No test should write outside `tmp_path`.
- **Time-sensitive tests** should use `freezegun` (already a test dep).
- **Concurrency-sensitive code** should be exercised with threads; see
  `tests/test_logger.py::test_logger_concurrent_writes_produce_valid_csv` as
  a template.
- **Security regressions** (XXE, etc.) live in `tests/test_xml_security.py`.
  Any new XML-parsing surface should get a regression test there.

## Pull request expectations

- A PR should touch one concern. Split storage changes, parser changes, and
  documentation updates into separate PRs when possible.
- Update `CHANGELOG.md` under `## [Unreleased]` in the same PR as the code
  change.
- CI must be green before requesting review.
- Avoid unrelated formatting/import reordering in code files — let the
  pre-commit ruff pass handle it.

## Project layout

```
piboufilings/
  __init__.py          # Top-level get_filings orchestrator
  _version.py          # Single source of truth for the version string
  config/              # Runtime-resolved settings, no import-time side effects
  core/
    downloader.py      # SEC HTTP, index caching, download partitioning
    logger.py          # Operation log CSV writer
    rate_limiter.py    # Token bucket + process-wide singleton
  parsers/
    form_13f_parser.py
    form_nport_parser.py
    form_sec16_parser.py
    parser_utils.py
  storage/
    base.py            # StorageBackend Protocol
    csv_backend.py     # Legacy period-partitioned CSVs
    duckdb_backend.py  # DuckDB with PK-based dedup
tests/
.github/workflows/     # CI + publish workflows
pyproject.toml         # PEP 621 project metadata
README.md
CHANGELOG.md
CONTRIBUTING.md
```

## Releasing (maintainers only)

### One-time setup — PyPI Trusted Publishers

Before the first release, configure [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
on both PyPI and TestPyPI so the workflow can upload via OIDC without any
long-lived tokens.

On https://pypi.org → *Your projects* → `piboufilings` → *Publishing*:

| Field | Value |
|---|---|
| PyPI Project Name | `piboufilings` |
| Owner | `Pierre-Bouquet` |
| Repository name | `pibou-filings` |
| Workflow name | `publish-to-pypi.yml` |
| Environment name | `pypi` |

Repeat on https://test.pypi.org with Environment name `testpypi`.

Then in GitHub → *Settings* → *Environments* create two environments named
`pypi` and `testpypi` (no reviewers needed; the OIDC claim is what PyPI
validates).

### Cutting a release

1. Bump `piboufilings/_version.py` (`__version__ = "x.y.z"`).
2. Update `CHANGELOG.md`: move `## [Unreleased]` entries under a new
   `## [x.y.z] — YYYY-MM-DD` heading.
3. Open a PR, get it merged, wait for CI (lint + unit matrix) to go green.
4. **Optional dry-run**: trigger `publish-to-pypi.yml` via GitHub Actions
   → *Run workflow* with `environment=testpypi`. Install from TestPyPI in a
   throwaway venv and re-run the README quick-start.
5. Tag and push:
   ```bash
   git tag -a vx.y.z -m "Release x.y.z"
   git push origin vx.y.z
   ```
6. Create a GitHub Release for the tag (copy the relevant `CHANGELOG.md`
   section into the body). The `Publish Python Package` workflow fires on
   release and pushes to PyPI via Trusted Publishing.

## Code of conduct

Be kind. Review the code, not the person. If something's frustrating, say
so — but stay specific and actionable.

## License

By contributing, you agree that your contributions will be licensed under
the project's [Non-Commercial License](./LICENCE). If you're submitting on
behalf of an employer, please make sure you have authority to do so.
