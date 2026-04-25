"""Single source of truth for the package version.

Read at runtime by the package, the downloader's User-Agent, and by ``setup.py``
via regex (so installing does not import the package)."""

__version__ = "0.5.1"
