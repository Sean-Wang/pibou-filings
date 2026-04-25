"""
Configuration settings for the piboufilings package.

Defaults are intentionally user-writable and have no import-time side effects.
"""

import os
from pathlib import Path

from .._version import __version__

# Base paths (user-overridable via env). Resolution happens at runtime in callers.
DEFAULT_BASE_DIR = Path(os.getenv("PIBOUFILINGS_BASE_DIR", Path.cwd()))
DATA_DIR = Path(os.getenv("PIBOUFILINGS_DATA_DIR", DEFAULT_BASE_DIR / "data_raw")).expanduser().resolve()
LOGS_DIR = Path(os.getenv("PIBOUFILINGS_LOG_DIR", DEFAULT_BASE_DIR / "logs")).expanduser().resolve()

# SEC API settings
SEC_MAX_REQ_PER_SEC = 10
SAFETY_FACTOR = 0.7
SAFE_REQ_PER_SEC = SEC_MAX_REQ_PER_SEC * SAFETY_FACTOR
REQUEST_DELAY = 1 / SAFE_REQ_PER_SEC

# HTTP settings (User-Agent is set at runtime with user-provided name/email)
DEFAULT_HEADERS = {
    "User-Agent": f"piboufilings/{__version__} (set-user-name; contact: set-email@example.com)"
}

# Retry settings
MAX_RETRIES = 5
BACKOFF_FACTOR = 1
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

# Timeout / diagnostics settings
# ``requests`` accepts a ``(connect_timeout, read_timeout)`` tuple. Keeping the
# read timeout lower than the previous scalar 30s prevents one pathological SEC
# response from tying up a worker for a long time, especially when urllib3
# retries are also enabled.
SEC_CONNECT_TIMEOUT = float(os.getenv("PIBOUFILINGS_SEC_CONNECT_TIMEOUT", "5"))
SEC_READ_TIMEOUT = float(os.getenv("PIBOUFILINGS_SEC_READ_TIMEOUT", "15"))

# Only log rate-limiter waits that are operationally meaningful to avoid noisy
# per-request logs during normal throttling.
RATE_LIMIT_LOG_THRESHOLD_SECONDS = float(
    os.getenv("PIBOUFILINGS_RATE_LIMIT_LOG_THRESHOLD_SECONDS", "2")
)
