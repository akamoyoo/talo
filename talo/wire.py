"""Message format and transport.

Messages follow the structure of the Jupyter messaging protocol (5.3): ``header`` / ``parent_header`` / ``metadata`` /
``content``. Execution-related ``msg_type`` values and fields follow the spec (``execute_request``, ``execute_reply``,
``execute_result``, ``stream``, ``error``, ``status``, ...); talo's own features use extension types prefixed with
``talo_``. The transport is newline-delimited JSON over a Unix socket (one message per line).

Standard library only: the worker loads this file by path inside the user's interpreter, so the target environment
does not need talo installed.
"""

from __future__ import annotations

import datetime as _dt
import json
import uuid

VERSION = "0.1.0a1"
PROTOCOL_VERSION = "5.3"
MAX_LINE = 64 << 20  # upper bound for one message in bytes; results and tracebacks are truncated well below this


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def message(msg_type: str, content: dict | None = None, *, parent: dict | None = None, session: str = "",
            metadata: dict | None = None) -> dict:
    """A Jupyter-shaped message; ``parent`` is the message being answered (its header becomes ``parent_header``)."""
    return {
        "header": {"msg_id": new_id(), "msg_type": msg_type, "session": session, "username": "talo",
                   "date": now(), "version": PROTOCOL_VERSION},
        "parent_header": dict(parent["header"]) if parent else {},
        "metadata": metadata or {},
        "content": content or {},
    }


def msg_type(msg: dict) -> str:
    return msg["header"]["msg_type"]


def encode(msg: dict) -> bytes:
    return json.dumps(msg, ensure_ascii=False, separators=(",", ":"), default=str).encode() + b"\n"


def decode(line: bytes) -> dict:
    return json.loads(line)


def send(stream, msg: dict) -> None:
    """Write to a binary file object (``socket.makefile("wb")``) and flush."""
    stream.write(encode(msg))
    stream.flush()


def recv(stream) -> dict | None:
    """Read one message from a binary file object; None when the peer has closed."""
    line = stream.readline(MAX_LINE)
    if not line:
        return None
    return decode(line)
