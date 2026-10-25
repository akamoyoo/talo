"""``talo mcp``: a Model Context Protocol server over stdio (see "MCP" in docs/design.md).

Newline-delimited JSON-RPC 2.0 on stdin / stdout, standard library only. It implements the lifecycle of MCP
revisions 2024-11-05 to 2025-11-25 (``initialize``, ``notifications/initialized``, ``tools/list``, ``tools/call``,
``ping``). Each request is handled in its own thread, so ``interrupt`` can be called while a ``wait`` is pending.
Tool results carry the same data as the CLI's ``--json`` in ``structuredContent``, bounded, with a JSON text copy.
Nothing but protocol messages is written to stdout.
"""

from __future__ import annotations

import json
import os
import sys
import threading

from talo import __version__
from talo.client import TaloError, call

SUPPORTED = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")
TEXT_LIMIT = 32_000   # characters of the JSON text copy of a result
OUTPUT_LINES = 40


def _obj(props: dict, required: list[str] = ()) -> dict:
    return {"type": "object", "properties": props, "required": list(required), "additionalProperties": False}


S = {"type": "string"}
N = {"type": "number"}
I = {"type": "integer"}
B = {"type": "boolean"}
SESSION = {"type": "string", "description": "session name (default: the server's session)"}

TOOLS = {
    "run": ("Run Python code in the resident session. Returns the job: its result, error and the last lines of its "
            "output; if it takes longer than `timeout` seconds it keeps running and you get its id for `wait`.",
            _obj({"code": S, "session": SESSION, "timeout": N, "atomic": B, "key": S, "expect_rev": I}, ["code"])),
    "wait": ("Wait for a job, map or call by id; returns it (with the last output lines for a job).",
             _obj({"id": S, "timeout": N}, ["id"])),
    "interrupt": ("Interrupt a running job, map or call.", _obj({"id": S}, ["id"])),
    "brief": ("What you need to resume work on a session: state, revision, notes, largest variables, recent and "
              "running jobs.", _obj({"session": SESSION})),
    "vars": ("Variables of the session: type, shape, dtype, size.", _obj({"session": SESSION, "pattern": S})),
    "show": ("Summarize a variable or a name / attribute / constant subscript (df, df.x, d['k']).",
             _obj({"name": S, "session": SESSION, "rows": I}, ["name"])),
    "snap": ("Snapshot the session's in-memory state (milliseconds, copy-on-write).",
             _obj({"name": S, "message": S, "session": SESSION, "key": S})),
    "checkout": ("Roll the session back to a snapshot. `discard` drops the state of jobs since the current base.",
                 _obj({"name": S, "discard": B, "snap": S, "session": SESSION, "expect_rev": I, "key": S}, ["name"])),
    "undo": ("Go back to the state before the last execution (needs autosnap or a snapshot).",
             _obj({"session": SESSION})),
    "diff": ("Variables added, removed or changed between two states: snapshots, or '@' for the live worker.",
             _obj({"a": S, "b": S, "session": SESSION, "max_bytes": I}, ["a"])),
    "map": ("Run code once per case, each in a fork of a snapshot, in parallel; `case` holds the case. Returns the "
            "map (results per case) or its id while it runs.",
            _obj({"code": S, "cases": {"type": "array"}, "range": I, "from": S, "parallel": I, "timeout": N,
                  "session": SESSION, "key": S}, ["code"])),
    "call": ("Call a service exposed by a session (SESSION.NAME) with JSON arguments.",
             _obj({"service": S, "args": {"type": "object"}, "version": I, "timeout": N, "key": S}, ["service"])),
    "record": ("Add a record (numbers or short strings) to the experiment ledger.",
               _obj({"fields": {"type": "object"}, "session": SESSION, "key": S}, ["fields"])),
    "ledger": ("Records of the experiment ledger, with where each came from and its outcome.",
               _obj({"session": S, "has": {"type": "array", "items": S}, "where": {"type": "array", "items": S},
                     "sort": S, "last": I})),
    "events": ("Events after a cursor (jobs, maps, calls, snapshots, notes ...), waiting up to `wait` seconds.",
               _obj({"after": S, "wait": N, "session": S, "type": S, "limit": I})),
    "note": ("Attach a note (goal, finding, next step) to the session's current state.",
             _obj({"text": S, "session": SESSION, "job": S}, ["text"])),
    "output": ("Read a job's output from a byte offset (pass back next_offset to continue).",
               _obj({"id": S, "offset": I, "max_bytes": I}, ["id"])),
}


class Server:
    def __init__(self, session: str):
        self.session = session
        self.lock = threading.Lock()
        self.initialized = False

    def write(self, msg: dict) -> None:
        line = json.dumps(msg, ensure_ascii=False, default=str)
        with self.lock:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()

    # ---------------------------------------------------------------- tools

    def _job_with_output(self, job: dict) -> dict:
        from talo.cli import MAX_RESULT, bound_result, condense, tail_lines
        if "map" in job or "call" in job:
            return job
        out = call("output", id=job["id"], max_bytes=256 << 10)
        text, cut = tail_lines(condense(out["text"]), OUTPUT_LINES)
        return {"job": bound_result(job, MAX_RESULT), "output": {"text": text, "lines_cut": cut,
                                                                  "bytes_cut": out["offset"], "size": out["size"]}}

    def tool(self, name: str, a: dict) -> tuple[dict, bool]:
        """(structured result, is_error)."""
        s = a.get("session") or self.session
        if name == "run":
            job = call("run", session=s, code=a["code"], timeout=a.get("timeout", 30), atomic=a.get("atomic"),
                       key=a.get("key"), expect_rev=a.get("expect_rev"))
            r = self._job_with_output(job)
            return r, job["status"] in ("error", "interrupted", "lost", "cancelled")
        if name == "wait":
            j = call("job", id=a["id"], wait=a.get("timeout", 30))
            r = self._job_with_output(j)
            status = (j.get("map") or j.get("call") or j).get("status")
            return r, status in ("error", "interrupted", "lost", "cancelled")
        if name == "interrupt":
            return call("interrupt", id=a["id"]), False
        if name in ("brief", "vars", "undo"):
            return call(name, session=s, **{k: v for k, v in a.items() if k != "session"}), False
        if name == "show":
            r = call("show", session=s, name=a["name"], rows=a.get("rows", 10))
            return r, r.get("status") == "error"
        if name == "snap":
            return call("snap", session=s, name=a.get("name"), message=a.get("message", ""), key=a.get("key")), False
        if name == "checkout":
            return call("checkout", session=s, name=a["name"], discard=a.get("discard"), snap=a.get("snap"),
                        expect_rev=a.get("expect_rev"), key=a.get("key")), False
        if name == "diff":
            return call("diff", session=s, a=a["a"], b=a.get("b") or "@", max_bytes=a.get("max_bytes", 256 << 20)), False
        if name == "map":
            r = call("map", session=s, code=a["code"], cases=a.get("cases"), range=a.get("range"),
                     parallel=a.get("parallel", 4), timeout=a.get("timeout", 30), key=a.get("key"),
                     **{"from": a.get("from")})
            return r, r["map"]["status"] == "error"
        if name == "call":
            r = call("call", service=a["service"], args=a.get("args") or {}, version=a.get("version"),
                     timeout=a.get("timeout", 30), key=a.get("key"))
            return r, r["call"]["status"] in ("error", "lost")
        if name == "record":
            return call("record", session=s, fields=a["fields"], key=a.get("key")), False
        if name == "ledger":
            return call("ledger", **a), False
        if name == "events":
            return call("events", **a), False
        if name == "note":
            return call("note", session=s, text=a["text"], job=a.get("job")), False
        if name == "output":
            return call("output", id=a["id"], offset=a.get("offset"), max_bytes=min(int(a.get("max_bytes") or 65536),
                                                                                       1 << 20)), False
        raise KeyError(name)

    # ---------------------------------------------------------------- protocol

    def handle(self, msg: dict) -> None:
        method, mid, params = msg.get("method"), msg.get("id"), msg.get("params") or {}
        if mid is None:  # a notification: never answered
            if method == "notifications/initialized":
                self.initialized = True
            return
        try:
            if method == "initialize":
                asked = params.get("protocolVersion")
                version = asked if asked in SUPPORTED else SUPPORTED[0]
                result = {"protocolVersion": version, "capabilities": {"tools": {"listChanged": False}},
                          "serverInfo": {"name": "talo", "version": __version__},
                          "instructions": (f"Tools act on resident Talo sessions (default: {self.session!r}). Data "
                                           "stays in memory between calls; snapshot before risky changes.")}
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = {"tools": [{"name": n, "description": d, "inputSchema": schema}
                                    for n, (d, schema) in TOOLS.items()]}
            elif method == "tools/call":
                name, args = params.get("name"), params.get("arguments") or {}
                if name not in TOOLS:
                    return self.write({"jsonrpc": "2.0", "id": mid,
                                       "error": {"code": -32602, "message": f"unknown tool {name!r}"}})
                try:
                    data, is_error = self.tool(name, args)
                except TaloError as error:
                    data, is_error = {"error": {"type": error.type, "message": str(error)}}, True
                except (KeyError, TypeError, ValueError) as error:
                    data, is_error = {"error": {"type": "BadArgument", "message": str(error)}}, True
                text = json.dumps(data, ensure_ascii=False, default=str)
                if len(text) > TEXT_LIMIT:
                    text = text[:TEXT_LIMIT] + f" ... [{len(text) - TEXT_LIMIT} characters cut]"
                result = {"content": [{"type": "text", "text": text}],
                          "structuredContent": json.loads(json.dumps(data, default=str)), "isError": is_error}
            else:
                return self.write({"jsonrpc": "2.0", "id": mid,
                                   "error": {"code": -32601, "message": f"method {method!r} not supported"}})
            self.write({"jsonrpc": "2.0", "id": mid, "result": result})
        except Exception as error:  # noqa: BLE001 - one bad request must not end the server
            self.write({"jsonrpc": "2.0", "id": mid, "error": {"code": -32603, "message": str(error)}})

    def serve(self) -> int:
        for line in sys.stdin:
            if not line.strip():
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                self.write({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}})
                continue
            threading.Thread(target=self.handle, args=(msg,), daemon=True).start()
        return 0  # end of input: detach; sessions and jobs keep running


def main(session: str) -> int:
    os.environ.setdefault("TALO_AGENT", "mcp")
    return Server(session).serve()
