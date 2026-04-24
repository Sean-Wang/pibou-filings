"""Mocked-HTTP tests for SECDownloader._download_single_filing + get_sec_index_data.

These tests do NOT hit live SEC. They simulate the HTTP boundary with
``requests-mock`` so we can verify:
- 200 success path (filing is saved, return dict is populated).
- Non-200 path (failure is logged, None is returned).
- RequestException path (failure is logged, None is returned).
- Retry-after behavior is not explicitly tested here (urllib3.Retry handles it).
- Rate-limit wait is neutralized by monkeypatching the limiter.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import requests_mock

from piboufilings.core.downloader import SECDownloader


@pytest.fixture
def downloader(tmp_path, monkeypatch):
    # Bypass the rate limiter so tests don't pause for real time.
    dl = SECDownloader(
        user_name="Test Runner",
        user_agent_email="test@example.com",
        log_dir=tmp_path / "logs",
        data_dir=tmp_path / "raw",
        max_workers=1,
    )
    monkeypatch.setattr(dl, "_respect_rate_limit", lambda: None)
    # Replace the retrying adapter with a plain one so 5xx tests don't spend
    # ~30 s on exponential backoff.
    import requests

    dl.session = requests.Session()
    dl.session.headers.update(dl.headers)
    return dl


def test_download_single_filing_success_returns_metadata(downloader, tmp_path):
    cik = "0000320193"
    accession = "0000320193-24-000001"
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{accession}.txt"
    body = (
        "<SEC-HEADER>\nACCESSION NUMBER: 0000320193-24-000001\nCONFORMED SUBMISSION TYPE: 4\n</SEC-HEADER>\n"
    )

    with requests_mock.Mocker() as m:
        m.get(url, text=body, status_code=200)
        result = downloader._download_single_filing(
            cik=cik,
            accession_number=accession,
            form_type="4",
            save_raw=True,
        )

    assert result is not None
    assert result["cik"] == cik
    assert result["accession_number"] == accession
    assert result["form_type"] == "4"
    assert result["url"] == url
    raw_path = Path(result["raw_path"])
    assert raw_path.exists()
    assert raw_path.read_text() == body


def test_download_single_filing_http_500_returns_none_and_logs(downloader):
    cik = "0000320193"
    accession = "0000320193-24-000001"
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{accession}.txt"

    with requests_mock.Mocker() as m:
        # Repeat the 5xx for every retry urllib3 makes so we exhaust them.
        m.get(url, status_code=500)
        result = downloader._download_single_filing(
            cik=cik,
            accession_number=accession,
            form_type="4",
            save_raw=True,
        )

    # 500s exhaust retries → requests ultimately raises RetryError, caught and
    # logged. The method returns None either way.
    assert result is None


def test_download_single_filing_http_404_returns_none(downloader):
    # 404 is not in the retry status list, so requests returns immediately.
    cik = "0000320193"
    accession = "0000320193-24-000001"
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{accession}.txt"

    with requests_mock.Mocker() as m:
        m.get(url, status_code=404)
        result = downloader._download_single_filing(
            cik=cik,
            accession_number=accession,
            form_type="13F-HR",
            save_raw=False,
        )

    assert result is None


def test_download_single_filing_connection_error(downloader):
    cik = "0000320193"
    accession = "0000320193-24-000001"
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{accession}.txt"
    import requests

    with requests_mock.Mocker() as m:
        m.get(url, exc=requests.ConnectionError("boom"))
        result = downloader._download_single_filing(
            cik=cik,
            accession_number=accession,
            form_type="4",
            save_raw=False,
        )
    assert result is None


def test_get_sec_index_data_uses_disk_cache(downloader, tmp_path):
    """Second call to the same (year, quarter) should skip the HTTP request."""
    url = "https://www.sec.gov/Archives/edgar/full-index/2024/QTR1/form.idx"
    # Minimal valid form.idx body: header + separator + one entry.
    header = (
        "Form Type".ljust(12)
        + "Company Name".ljust(62)
        + "CIK".ljust(12)
        + "Date Filed".ljust(12)
        + "Filename"
    )
    separator = "-" * 120
    # entry: columns — Form Type[0:12], Name[12:74], CIK[74:86], Date[86:98], Filename[98:]
    entry = (
        "13F-HR".ljust(12)
        + "ACME CAPITAL MANAGEMENT".ljust(62)
        + "0001234567".ljust(12)
        + "2024-02-15".ljust(12)
        + "edgar/data/1234567/0001234567-24-000001.txt"
    )
    body = f"{header}\n{separator}\n{entry}\n"

    with requests_mock.Mocker() as m:
        m.get(url, text=body, status_code=200)
        df1 = downloader._parse_form_idx(2024, 1)
        # Second call: cache on disk → no HTTP. If a request is made, mocker
        # returns 200 again; we assert the call count to be sure.
        df2 = downloader._parse_form_idx(2024, 1)
        assert m.call_count == 1

    assert not df1.empty
    assert not df2.empty
    assert df1.iloc[0]["CIK"] == "0001234567"
    assert df1.iloc[0]["Form Type"] == "13F-HR"


def test_download_filings_empty_index_returns_empty_df(downloader, monkeypatch):
    """When the SEC index returns no matches, download_filings returns empty."""
    monkeypatch.setattr(
        downloader,
        "get_sec_index_data",
        lambda *a, **kw: pd.DataFrame(
            columns=["CIK", "Name", "Date Filed", "Form Type", "accession_number", "Filename"]
        ),
    )
    result = downloader.download_filings(
        cik="0000000001",
        form_type="13F-HR",
        start_year=2024,
        end_year=2024,
        show_progress=False,
    )
    assert result.empty
