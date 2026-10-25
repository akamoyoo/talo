"""Turn job records (from the ``export`` request) into a replayable ``.py`` script or an ``.ipynb`` notebook.

Runs in the CLI process: it reads the jobs' output files and saved images directly. See "talo export" in
docs/design.md.
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path

STREAM_LIMIT = 100 << 10  # bytes of a job's output kept in a notebook (the last ones)


def _failure(rec: dict) -> str:
    error = rec.get("error") or {}
    detail = f": {error.get('ename')}: {error.get('evalue')}" if error.get("ename") else ""
    return f"job {rec['id']} {'failed' if rec['status'] == 'error' else rec['status']}{detail}"


def _snap_title(rec: dict) -> str:
    snap = rec["result"]
    return f"snapshot {snap['name']} ({snap['ref']})" + (f": {snap['message']}" if snap.get("message") else "")


def _checkout_title(rec: dict) -> str:
    target = (rec.get("result") or {}).get("base") or "a snapshot"
    return f"checkout {target}" + ("" if rec["status"] == "ok" else f" ({_failure(rec)})") + \
        " (the state below continues from there)"


def _reload_title(rec: dict) -> str:
    modules = (rec.get("result") or {}).get("modules") or []
    parts = [f"{m['module']} {m['status']}" + (f" ({m.get('old_hash')} -> {m.get('new_hash')})" if m.get("new_hash") else "")
             for m in modules]
    return ("reload " + ", ".join(parts) + "; a replay imports the source files as they are now, which may differ")


def to_py(jobs: list, header: str) -> str:
    parts = [f"# {line}" if line else "#" for line in header.splitlines()]
    for rec in jobs:
        if rec["kind"] == "snap":
            parts.append(f"\n# --- {_snap_title(rec)}")
        elif rec["kind"] == "checkout":
            parts.append(f"\n# --- {_checkout_title(rec)}")
        elif rec["kind"] == "reload":
            parts.append(f"\n# --- {_reload_title(rec)}")
        elif rec["kind"] == "execute":
            code = rec["code"].rstrip("\n")
            if rec["status"] == "ok":
                parts.append(f"\n# [{rec.get('execution_count') or rec['id']}]\n{code}")
            else:
                commented = "\n".join(f"# {line}" if line else "#" for line in code.splitlines())
                parts.append(f"\n# [{_failure(rec)}]\n{commented}")
    return "\n".join(parts) + "\n"


def _stream(path: str | None) -> list:
    if not path or not Path(path).exists():
        return []
    data = Path(path).read_bytes()
    note = ""
    if len(data) > STREAM_LIMIT:
        note = f"[talo: {len(data) - STREAM_LIMIT} earlier bytes cut]\n"
        data = data[-STREAM_LIMIT:]
    text = note + data.decode(errors="replace")
    return [{"output_type": "stream", "name": "stdout", "text": text.splitlines(keepends=True)}] if text else []


def _image(output: dict) -> dict | None:
    path = Path(output["path"])
    if not path.exists():
        return None
    data = path.read_bytes()
    value = data.decode(errors="replace") if output["mime"] == "image/svg+xml" else base64.b64encode(data).decode()
    return {"output_type": "display_data", "data": {output["mime"]: value, "text/plain": [f"<{path.name}>"]},
            "metadata": {}}


def _cell_id(n: int) -> str:
    return f"talo-{n}"


def to_ipynb(jobs: list, header: str) -> str:
    cells = [{"cell_type": "markdown", "id": _cell_id(0), "metadata": {}, "source": header.splitlines(keepends=True)}]
    for rec in jobs:
        if rec["kind"] in ("snap", "checkout", "reload"):
            title = {"snap": _snap_title, "checkout": _checkout_title, "reload": _reload_title}[rec["kind"]](rec)
            cells.append({"cell_type": "markdown", "id": _cell_id(len(cells)), "metadata": {},
                          "source": [f"**{title}**"]})
            continue
        if rec["kind"] != "execute":
            continue
        outputs = _stream(rec.get("output")) + [o for o in map(_image, rec.get("outputs") or []) if o]
        if rec["status"] == "ok" and rec.get("result_text"):
            outputs.append({"output_type": "execute_result", "execution_count": rec.get("execution_count"),
                            "data": {"text/plain": rec["result_text"].splitlines(keepends=True)}, "metadata": {}})
        elif rec["status"] != "ok":
            error = rec.get("error") or {}
            outputs.append({"output_type": "error", "ename": error.get("ename", rec["status"]),
                            "evalue": error.get("evalue") or "", "traceback": error.get("traceback") or []})
        cells.append({"cell_type": "code", "id": _cell_id(len(cells)), "execution_count": rec.get("execution_count"),
                      "metadata": {} if rec["status"] == "ok" else {"talo_status": rec["status"]},
                      "source": rec["code"].rstrip("\n").splitlines(keepends=True), "outputs": outputs})
    notebook = {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
                "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
                             "language_info": {"name": "python"}}}
    return json.dumps(notebook, ensure_ascii=False, indent=1) + "\n"


def header(session: str, what: str) -> str:
    return (f"Exported by Talo from session {session} ({what}) on {time.strftime('%Y-%m-%d %H:%M:%S')}.\n"
            "Failed and interrupted jobs are kept as comments; side effects outside the process are not replayed.")
