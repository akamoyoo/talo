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


def shared_socket_path() -> Path:
    """The socket other users connect to when sessions are shared with them: in a directory they can traverse
    (``/tmp/talo-shared-UID``, mode 0711), one per ``TALO_HOME``. Access is checked per connection (``SO_PEERCRED``)."""
    digest = hashlib.sha256(str(home().resolve()).encode()).hexdigest()[:12]
    return Path("/tmp") / f"talo-shared-{os.getuid()}" / f"{digest}.sock"


def entry(module: str) -> list[str]:
    """``[python, "-c", ...]`` running ``module.main()`` with the current directory left out of ``sys.path``: run as
    ``python -m``, a directory named ``talo`` in the current directory (a checkout, say) would shadow the installed
    package. (``-P`` does the same from Python 3.11.)"""
    import sys
    code = ("import sys; sys.path[:] = [p for p in sys.path if p not in ('', '.')]; "
            f"from {module} import main; sys.exit(main())")
    return [sys.executable, "-c", code]


def datasets_root() -> Path:
    """Where shared datasets live: memory (``/dev/shm``) when there is one, one directory per ``TALO_HOME``."""
    digest = hashlib.sha256(str(home().resolve()).encode()).hexdigest()[:12]
    shm = Path("/dev/shm")
    if shm.is_dir() and os.access(shm, os.W_OK):
        return shm / f"talo-{os.getuid()}-{digest}"
    return home() / "datasets"
