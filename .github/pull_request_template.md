<!--
Thanks for the contribution! Please fill in the sections below so the
review can focus on substance rather than context.
-->

## Summary

<!-- One or two sentences. What changed and why. -->

## What this touches

- [ ] Parsers (13F / NPORT / Section 16)
- [ ] Storage backends (DuckDB / CSV)
- [ ] Downloader / rate limiter
- [ ] Orchestrator (`get_filings`)
- [ ] Tests only
- [ ] Docs / CI only
- [ ] Other: <!-- … -->

## Test plan

<!--
What did you run locally to convince yourself this works? Minimum:

  ruff check piboufilings tests
  ruff format --check piboufilings tests
  pytest -m "not integration"

For network-facing changes, also list the CIK(s) / form(s) you exercised.
-->

## CHANGELOG

- [ ] Updated `CHANGELOG.md` under `## [Unreleased]`, OR this PR is
      purely internal and doesn't warrant a changelog entry.

## Backwards compatibility

<!-- Flag any API / schema / file-format change that a downstream user
     might trip over. If none, say "none". -->
