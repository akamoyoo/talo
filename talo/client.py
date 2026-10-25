"""Requests from the CLI to the supervisor: one JSON request line, one JSON reply line; starts the supervisor when
it is not running."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time

from talo.paths import home, socket_path


class TaloError(RuntimeError):
    """An error reported by the supervisor: ``type`` is the error class, the message is for humans."""

    def __init__(self, type_: str, message: str):
        super().__init__(message)
        self.type = type_


def _connect() -> socket.socket:
    """The supervisor's socket, or a shared one named by ``$TALO_CONNECT`` (``talo --connect PATH``)."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(os.environ.get("TALO_CONNECT") or str(socket_path()))
    return s


def start_supervisor(timeout: float = 15.0) -> None:
    """Start the supervisor in the background (detached from the terminal) and wait until its socket accepts."""
    home().mkdir(parents=True, exist_ok=True)
    with open(home() / "supervisor.log", "ab") as out:
        subprocess.Popen([sys.executable, "-m", "talo.supervisor"], stdin=subprocess.DEVNULL, stdout=out, stderr=out,
                         start_new_session=True, env={**os.environ})
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _connect().close()
            return
        except OSError:
            time.sleep(0.05)
    raise TaloError("SupervisorStart", f"the supervisor did not start within {timeout:.0f}s; see {home() / 'supervisor.log'}")


def call(op: str, *, autostart: bool = True, **params) -> dict:
    """Send one request and wait for the reply; raises TaloError on failure."""
    try:
        s = _connect()
    except OSError:
        if os.environ.get("TALO_CONNECT"):
            raise TaloError("NotRunning", f"cannot connect to {os.environ['TALO_CONNECT']}") from None
        if not autostart:
            raise TaloError("NotRunning", "the supervisor is not running") from None
        start_supervisor()
        s = _connect()
    with s:
        request = {"op": op, "owner": os.environ.get("TALO_AGENT") or "user", **params}
        s.sendall(json.dumps(request, ensure_ascii=False).encode() + b"\n")
        data = s.makefile("rb").readline()
    if not data:
        raise TaloError("Disconnected", "the supervisor closed the connection (see supervisor.log)")
    reply = json.loads(data)
    if not reply.pop("ok"):
        raise TaloError(reply["error"]["type"], reply["error"]["message"])
    return reply
