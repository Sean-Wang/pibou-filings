---
name: Bug report
about: Something in the library doesn't work as documented
title: "[BUG] "
labels: ["bug"]
assignees: []
---

## What you did

A short description of what you were trying to accomplish.
If possible, paste the exact `get_filings(...)` call (with your real
user_agent_email replaced by `you@example.com`).

```python
# your call here
```

## What you expected

What you thought would happen.

## What actually happened

What happened instead. Include the full traceback if there was one.

```
traceback here
```

## Environment

- `piboufilings` version: (e.g. `0.5.0`; run `python -c "import piboufilings; print(piboufilings.__version__)"`)
- Python version:
- OS: (macOS / Linux / Windows) + version
- Installed with `[duckdb]` extra? (yes / no)
- `export_format`: (`duckdb` / `csv`)

## Log excerpts (optional)

Grab the relevant rows from `<log_dir>/filing_operations_YYYYMMDD.csv`.
Redact any internal CIKs if needed.

## Minimal reproducer (optional but very helpful)

The smallest CIK/form_type/year combination that reproduces the issue.
