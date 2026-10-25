"""talo's directory: ``TALO_HOME`` (default ``~/.talo``)."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

SOCKET_PATH_LIMIT = 100  # AF_UNIX paths are limited to 108 bytes on Linux


def home() -> Path:
    return Path(os.environ.get("TALO_HOME") or Path.home() / ".talo")


def socket_for(path: Path) -> Path:
    """``path`` if it is short enough for a Unix socket, else a short path under ``$XDG_RUNTIME_DIR`` (or ``/tmp``)
    derived from it."""
    if len(str(path).encode()) <= SOCKET_PATH_LIMIT:
        return path
    digest = hashlib.sha256(str(path.parent.resolve() / path.name).encode()).hexdigest()[:16]
    base = Path(os.environ.get("XDG_RUNTIME_DIR") or "/tmp")
    return base / f"talo-{os.getuid()}-{digest}.sock"


def socket_path() -> Path:
    """The supervisor's socket: ``$TALO_HOME/supervisor.sock`` (shortened when necessary)."""
    return socket_for(home() / "supervisor.sock")
