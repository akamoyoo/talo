"""``talo kernel``: a Jupyter kernel that is a bridge to a resident Talo session (see "The kernel bridge" in
docs/design.md).

Jupyter starts this process with a connection file. It speaks the Jupyter messaging protocol over ZMQ to the front
end and Talo's control requests to the supervisor; the session, with its data, keeps running when the front end
restarts or shuts the kernel down. Needs ``pyzmq`` (``pip install talo[jupyter]``).
"""

from __future__ import annotations

import asyncio
import base64
import codeop
import contextlib
import hashlib
import hmac
import io
import json
import os
import re
import shlex
import sys
import threading
import time
import uuid
from pathlib import Path

from talo import wire
from talo.client import TaloError, call

DELIM = b"<IDS|MSG>"
POLL_S = 0.1
IMAGE_MIMES = ("image/png", "image/jpeg", "image/svg+xml")
MAGIC_HELP = """Talo commands (a cell of lines that all start with %):
%snap [NAME] [-m MSG]   %checkout NAME   %undo   %autosnap on|off [--keep N]
%tree   %snaps   %diff A [B]   %vars   %brief   %history   %jobs   %help
%export --to ipynb -o FILE.ipynb   (the code behind the current state, to reopen with the text matching memory)
The worker is plain Python: IPython magics such as %timeit are not available."""


def _now() -> str:
    return wire.now()


class Bridge:
    def __init__(self, connection: dict, session: str, python: str | None, cwd: str):
        import zmq
        import zmq.asyncio
        self.zmq = zmq
        self.conn = connection
        self.session = session
        self.python, self.cwd = python, cwd
        self.key = connection.get("key", "").encode()
        self.ctx = zmq.asyncio.Context()
        self.id = uuid.uuid4().hex
        self.mine: set[str] = set()
        self.seen = 0
        self.notice: str | None = None
        self.running: str | None = None  # the job id of the execution in progress

    # ---------------------------------------------------------------- wire

    def _url(self, port_name: str) -> str:
        c = self.conn
        sep = ":" if c.get("transport", "tcp") == "tcp" else "-"
        return f"{c.get('transport', 'tcp')}://{c['ip']}{sep}{c[port_name]}"

    def _sign(self, parts: list[bytes]) -> bytes:
        if not self.key:
            return b""
        h = hmac.new(self.key, digestmod=hashlib.sha256)
        for p in parts:
            h.update(p)
        return h.hexdigest().encode()

    def _msg(self, msg_type: str, content: dict, parent: dict | None, metadata: dict | None = None) -> dict:
        return {"header": {"msg_id": uuid.uuid4().hex, "msg_type": msg_type, "session": self.id, "username": "talo",
                           "date": _now(), "version": wire.PROTOCOL_VERSION},
                "parent_header": parent["header"] if parent else {}, "metadata": metadata or {}, "content": content}

    async def send(self, sock, msg: dict, idents: list[bytes] | None = None) -> None:
        parts = [json.dumps(msg[k], default=str).encode() for k in ("header", "parent_header", "metadata", "content")]
        await sock.send_multipart([*(idents or []), DELIM, self._sign(parts), *parts])

    def recv_parse(self, frames: list[bytes]) -> tuple[list[bytes], dict | None]:
        i = frames.index(DELIM)
        idents, sig, parts = frames[:i], frames[i + 1], frames[i + 2:i + 6]
        if self.key and not hmac.compare_digest(sig, self._sign(parts)):
            return idents, None  # bad signature: ignored, as the spec requires
        header, parent, metadata, content = (json.loads(p) for p in parts)
        return idents, {"header": header, "parent_header": parent, "metadata": metadata, "content": content}

    async def iopub(self, msg_type: str, content: dict, parent: dict | None) -> None:
        msg = self._msg(msg_type, content, parent)
        parts = [json.dumps(msg[k], default=str).encode() for k in ("header", "parent_header", "metadata", "content")]
        topic = f"kernel.{self.id}.{msg_type}".encode()
        await self.iopub_sock.send_multipart([topic, DELIM, self._sign(parts), *parts])

    async def status(self, state: str, parent: dict | None) -> None:
        await self.iopub("status", {"execution_state": state}, parent)

    async def stream(self, text: str, parent: dict, name: str = "stdout") -> None:
        if text:
            await self.iopub("stream", {"name": name, "text": text}, parent)

    # ---------------------------------------------------------------- talo

    async def talo(self, op: str, **params) -> dict:
        """A control request, off the event loop (the client is blocking)."""
        return await asyncio.to_thread(call, op, **params)

    async def ensure_session(self) -> None:
        sessions = {s["name"]: s for s in (await self.talo("ls"))["sessions"]}
        if self.session in sessions:
            s = sessions[self.session]
            rss = f"{s['rss'] / (1 << 30):.1f} GB" if s.get("rss") else "?"
            self.notice = (f"[Talo kept session {self.session} ({rss}, rev {s.get('rev', '?')}); its variables are as "
                           f"you left them. %checkout or `talo kill {self.session}` resets it.]\n")
        else:
            await self.talo("new", name=self.session, python=self.python or sys.executable, cwd=self.cwd)
        jobs = (await self.talo("jobs", session=self.session, last=1))["jobs"]
        self.seen = max([0, *(int(j["id"]) for j in jobs if str(j["id"]).isdigit())])

    async def other_clients(self) -> str:
        jobs = (await self.talo("jobs", session=self.session, last=50))["jobs"]
        lines = []
        for j in jobs:
            if not str(j["id"]).isdigit() or int(j["id"]) <= self.seen or j["id"] in self.mine:
                continue
            if j["status"] in ("queued", "running"):
                continue
            preview = (j.get("code_preview", "").strip().splitlines() or [""])[0][:60]
            lines.append(f"[{j['owner']} ran job {j['id']} ({j['kind']} {j['status']}, rev {j.get('rev')}): {preview}]")
        self.seen = max([self.seen, *(int(j["id"]) for j in jobs if str(j["id"]).isdigit())])
        return "\n".join(lines) + ("\n" if lines else "")

    # ---------------------------------------------------------------- handlers

    async def execute(self, msg: dict) -> tuple[dict, dict]:
        content = msg["content"]
        code = content.get("code", "")
        silent = content.get("silent", False)
        prefix = (self.notice or "") + await self.other_clients()
        self.notice = None
        if prefix and not silent:
            await self.stream(prefix, msg)
        lines = [ln for ln in code.splitlines() if ln.strip()]
        if lines and all(ln.lstrip().startswith("%") for ln in lines):
            return await self.magics(lines, msg)
        job = await self.talo("run", session=self.session, code=code, timeout=0)
        self.mine.add(job["id"])
        self.running = job["id"]
        offset, count_sent = 0, False
        try:
            while True:
                out = await self.talo("output", id=job["id"], offset=offset, max_bytes=1 << 20)
                if out["text"] and not silent:
                    await self.stream(out["text"], msg)
                offset = out["next_offset"]
                if not count_sent:
                    j = await self.talo("job", id=job["id"])
                    if j.get("execution_count") and not silent:
                        await self.iopub("execute_input", {"code": code, "execution_count": j["execution_count"]}, msg)
                        count_sent = True
                if out["status"] not in ("running", "queued") and offset >= out["size"]:
                    break
                await asyncio.sleep(POLL_S)
        finally:
            self.running = None
        job = await self.talo("job", id=job["id"])
        n = job.get("execution_count") or 0
        if not count_sent and not silent:
            await self.iopub("execute_input", {"code": code, "execution_count": n}, msg)
        if not silent:
            for o in job.get("outputs") or []:
                if o["mime"] in IMAGE_MIMES and Path(o["path"]).exists():
                    raw = Path(o["path"]).read_bytes()
                    value = raw.decode(errors="replace") if o["mime"] == "image/svg+xml" else base64.b64encode(raw).decode()
                    await self.iopub("display_data", {"data": {o["mime"]: value, "text/plain": f"<{Path(o['path']).name}>"},
                                                      "metadata": {}, "transient": {}}, msg)
            result = job.get("result")
            if job["status"] == "ok" and result and "data" in result:
                data = {k: v for k, v in result["data"].items()}
                await self.iopub("execute_result", {"execution_count": n, "data": data,
                                                    "metadata": {}}, msg)
        if job["status"] == "ok":
            return {"status": "ok", "execution_count": n, "user_expressions": {}, "payload": []}, {}
        error = job.get("error") or {"ename": job["status"], "evalue": "", "traceback": []}
        err = {"ename": error.get("ename") or job["status"], "evalue": str(error.get("evalue") or ""),
               "traceback": error.get("traceback") or [f"{error.get('ename')}: {error.get('evalue')}"]}
        if not silent:
            await self.iopub("error", err, msg)
        return {"status": "error", "execution_count": n, **err}, {}

    async def magics(self, lines: list[str], msg: dict) -> tuple[dict, dict]:
        from talo.cli import main
        status = "ok"
        for line in lines:
            words = shlex.split(line.lstrip()[1:])
            if not words or words[0] == "help":
                await self.stream(MAGIC_HELP + "\n", msg)
                continue
            argv = [words[0], *words[1:]]
            if words[0] not in ("tree", "ls"):
                argv += ["-s", self.session]
            buf = io.StringIO()

            def run_cli() -> int:
                with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                    try:
                        return main(argv) or 0
                    except SystemExit as e:  # argparse
                        return int(e.code or 0)
            code = await asyncio.to_thread(run_cli)
            text = buf.getvalue()
            if words[0] in ("checkout", "undo") and code == 0:
                s = next((x for x in (await self.talo("ls"))["sessions"] if x["name"] == self.session), {})
                text += (f"[state restored to {s.get('base')}; cells run after it are no longer in memory, "
                         "although the notebook still shows them]\n")
            await self.stream(text, msg, "stdout" if code == 0 else "stderr")
            if code not in (0, 3):
                status = "error"
        jobs = (await self.talo("jobs", session=self.session, last=50))["jobs"]
        self.seen = max([self.seen, *(int(j["id"]) for j in jobs if str(j["id"]).isdigit())])
        if status == "error":
            err = {"ename": "TaloCommandFailed", "evalue": "a Talo command failed (see its output)", "traceback": []}
            await self.iopub("error", err, msg)
            return {"status": "error", "execution_count": 0, **err}, {}
        return {"status": "ok", "execution_count": 0, "user_expressions": {}, "payload": []}, {}

    async def complete(self, msg: dict) -> dict:
        c = msg["content"]
        code, pos = c.get("code", ""), c.get("cursor_pos", len(c.get("code", "")))
        try:
            reply = await self.talo("complete", session=self.session, code=code, cursor_pos=pos)
            return {"status": "ok", "matches": reply["matches"], "cursor_start": reply["cursor_start"],
                    "cursor_end": reply["cursor_end"], "metadata": {}}
        except TaloError:  # busy: no completion rather than a wait
            return {"status": "ok", "matches": [], "cursor_start": pos, "cursor_end": pos, "metadata": {}}

    async def inspect(self, msg: dict) -> dict:
        c = msg["content"]
        code, pos = c.get("code", ""), c.get("cursor_pos", len(c.get("code", "")))
        start = pos
        while start > 0 and re.match(r"[\w.\[\]'\"]", code[start - 1]):
            start -= 1
        name = code[start:pos] or code.strip()
        try:
            d = await self.talo("show", session=self.session, name=name, rows=5)
        except TaloError:
            d = {"status": "error"}
        if d.get("status") != "ok":
            return {"status": "ok", "found": False, "data": {}, "metadata": {}}
        return {"status": "ok", "found": True, "data": {"text/plain": d["text"]}, "metadata": {}}

    def is_complete(self, msg: dict) -> dict:
        code = msg["content"].get("code", "")
        try:
            if codeop.compile_command(code, "<input>", "exec") is None:
                return {"status": "incomplete", "indent": "    "}
            return {"status": "complete"}
        except (SyntaxError, ValueError, OverflowError):
            return {"status": "invalid"}

    async def history(self, msg: dict) -> dict:
        n = int(msg["content"].get("n") or 100)
        rows = (await self.talo("history", session=self.session, last=n))["history"]
        return {"status": "ok", "history": [[0, r.get("execution_count") or 0, r["code"]] for r in rows
                                            if r.get("kind") == "execute"]}

    def kernel_info(self) -> dict:
        return {"status": "ok", "protocol_version": wire.PROTOCOL_VERSION, "implementation": "talo",
                "implementation_version": wire.VERSION,
                "language_info": {"name": "python", "version": sys.version.split()[0], "mimetype": "text/x-python",
                                  "file_extension": ".py", "pygments_lexer": "ipython3",
                                  "codemirror_mode": {"name": "ipython", "version": 3}, "nbconvert_exporter": "python"},
                "banner": f"Talo session {self.session}", "help_links": []}

    # ---------------------------------------------------------------- loops

    async def handle(self, sock, frames: list[bytes], control: bool = False) -> None:
        idents, msg = self.recv_parse(frames)
        if msg is None:
            return
        kind = msg["header"]["msg_type"]
        await self.status("busy", msg)
        try:
            if kind == "execute_request":
                content, meta = await self.execute(msg)
            elif kind == "kernel_info_request":
                content, meta = self.kernel_info(), {}
            elif kind == "complete_request":
                content, meta = await self.complete(msg), {}
            elif kind == "inspect_request":
                content, meta = await self.inspect(msg), {}
            elif kind == "is_complete_request":
                content, meta = self.is_complete(msg), {}
            elif kind == "history_request":
                content, meta = await self.history(msg), {}
            elif kind == "comm_info_request":
                content, meta = {"status": "ok", "comms": {}}, {}
            elif kind == "interrupt_request":
                if self.running:
                    await self.talo("interrupt", id=self.running)
                content, meta = {"status": "ok"}, {}
            elif kind == "shutdown_request":
                content, meta = {"status": "ok", "restart": msg["content"].get("restart", False)}, {}
                await self.send(sock, self._msg("shutdown_reply", content, msg), idents)
                await self.status("idle", msg)
                self.stop.set()  # the session keeps running: only the bridge exits
                return
            else:
                return
        except TaloError as error:
            content, meta = {"status": "error", "ename": "TaloError", "evalue": str(error), "traceback": [str(error)]}, {}
        reply_type = kind.replace("_request", "_reply")
        await self.send(sock, self._msg(reply_type, content, msg, meta), idents)
        await self.status("idle", msg)

    async def serve_shell(self) -> None:
        while not self.stop.is_set():
            frames = await self.shell.recv_multipart()
            await self.handle(self.shell, frames)  # one request at a time, as a kernel's shell channel does

    async def serve_control(self) -> None:
        while not self.stop.is_set():
            frames = await self.control.recv_multipart()
            asyncio.create_task(self.handle(self.control, frames, control=True))

    async def serve_stdin(self) -> None:
        while not self.stop.is_set():
            await self.stdin.recv_multipart()  # input() is not supported: requests are never sent

    def heartbeat(self) -> None:
        import zmq
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REP)
        sock.bind(self._url("hb_port"))
        while True:
            try:
                sock.send(sock.recv())
            except zmq.ZMQError:
                return

    async def run(self) -> None:
        zmq = self.zmq
        self.stop = asyncio.Event()
        self.shell = self.ctx.socket(zmq.ROUTER)
        self.control = self.ctx.socket(zmq.ROUTER)
        self.stdin = self.ctx.socket(zmq.ROUTER)
        self.iopub_sock = self.ctx.socket(zmq.PUB)
        for sock, port in ((self.shell, "shell_port"), (self.control, "control_port"), (self.stdin, "stdin_port"),
                           (self.iopub_sock, "iopub_port")):
            sock.bind(self._url(port))
        threading.Thread(target=self.heartbeat, daemon=True).start()
        await self.ensure_session()
        await self.status("starting", None)
        tasks = [asyncio.create_task(f()) for f in (self.serve_shell, self.serve_control, self.serve_stdin)]
        await self.stop.wait()
        await asyncio.sleep(0.1)  # let the last replies leave
        for task in tasks:
            task.cancel()


def main(connection_file: str, session: str, python: str | None = None) -> int:
    try:
        import zmq  # noqa: F401
    except ImportError:
        print("talo kernel needs pyzmq: pip install 'talo[jupyter]'", file=sys.stderr)
        return 2
    connection = json.loads(Path(connection_file).read_text())
    asyncio.run(Bridge(connection, session, python, os.getcwd()).run())
    return 0


def install_kernelspec(session: str, name: str | None = None, python: str | None = None, user: bool = True,
                       prefix: str | None = None) -> Path:
    """Write a kernelspec that starts ``talo kernel`` for ``session``."""
    name = name or f"talo-{session}"
    if prefix:
        base = Path(prefix) / "share" / "jupyter" / "kernels"
    else:
        data = os.environ.get("JUPYTER_DATA_DIR") or str(Path.home() / ".local" / "share" / "jupyter")
        base = Path(data) / "kernels"
    path = base / name
    path.mkdir(parents=True, exist_ok=True)
    argv = [sys.executable, "-m", "talo.cli", "kernel", "--session", session, "-f", "{connection_file}"]
    if python:
        argv[-2:-2] = ["--python", python]
    spec = {"argv": argv, "display_name": f"Talo: {session}", "language": "python",
            "metadata": {"talo": {"session": session}}, "interrupt_mode": "message"}
    if "TALO_HOME" in os.environ:
        spec["env"] = {"TALO_HOME": os.environ["TALO_HOME"]}
    (path / "kernel.json").write_text(json.dumps(spec, indent=1) + "\n")
    return path
