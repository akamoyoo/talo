"""talo's directory: ``TALO_HOME`` (default ``~/.talo``)."""

from __future__ import annotations

import os
from pathlib import Path


def home() -> Path:
    return Path(os.environ.get("TALO_HOME") or Path.home() / ".talo")


def socket_path() -> Path:
    return home() / "supervisor.sock"
