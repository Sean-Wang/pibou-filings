"""
Configuration package for piboufilings.
"""

from .settings import (
    BACKOFF_FACTOR,
    DATA_DIR,
    DEFAULT_BASE_DIR,
    DEFAULT_HEADERS,
    LOGS_DIR,
    MAX_RETRIES,
    REQUEST_DELAY,
    RETRY_STATUS_CODES,
    SAFE_REQ_PER_SEC,
    SAFETY_FACTOR,
    SEC_MAX_REQ_PER_SEC,
)

__all__ = [
    "BACKOFF_FACTOR",
    "DATA_DIR",
    "DEFAULT_BASE_DIR",
    "DEFAULT_HEADERS",
    "LOGS_DIR",
    "MAX_RETRIES",
    "REQUEST_DELAY",
    "RETRY_STATUS_CODES",
    "SAFE_REQ_PER_SEC",
    "SAFETY_FACTOR",
    "SEC_MAX_REQ_PER_SEC",
]
