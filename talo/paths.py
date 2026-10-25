"""talo's directory: ``TALO_HOME`` (default ``~/.talo``)."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

SOCKET_PATH_LIMIT = 100  # AF_UNIX paths are limited to 108 bytes on Linux


def home() -> Path:
    return Path(os.environ.get("TALO_HOME") or Path.home() / ".talo")


def socket_path() -> Path:
    """``$TALO_HOME/supervisor.sock``; when that path is too long for a Unix socket, a short path under
    ``$XDG_RUNTIME_DIR`` (or ``/tmp``) derived from ``TALO_HOME``."""
    path = home() / "supervisor.sock"
    if len(str(path).encode()) <= SOCKET_PATH_LIMIT:
        return path
    digest = hashlib.sha256(str(home().resolve()).encode()).hexdigest()[:16]
    base = Path(os.environ.get("XDG_RUNTIME_DIR") or "/tmp")
    return base / f"talo-{os.getuid()}-{digest}.sock"
