"""``talo kernel``: a Jupyter kernel that is a bridge to a resident Talo session (see "The kernel bridge" in
docs/design.md).

Jupyter starts this process with a connection file. It speaks the Jupyter messaging protocol over ZMQ to the front
end and Talo's control requests to the supervisor; the session, with its data, keeps running when the front end
restarts or shuts the kernel down. Needs ``pyzmq`` (``pip install talo[jupyter]``).
"""

from __future__ import annotations

import asyncio
import base64
import codecs
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
MARK = re.compile("\x1b\\]5379;talo-display=(\\d+)\x07")  # display markers in a job's output (worker.DISPLAY_MARK)
CONTROL_TYPES = {"interrupt_request", "shutdown_request", "kernel_info_request"}
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
        scheme = connection.get("signature_scheme", "hmac-sha256")
        if not scheme.startswith("hmac-") or scheme[5:] not in hashlib.algorithms_available:
            raise SystemExit(f"talo kernel: unsupported signature scheme {scheme!r}")
        self.digest = scheme[5:]
        self.seen_ids: dict[str, None] = {}  # msg_ids of recent messages, to refuse replays
        self.pending_interrupt = False
        self.ctx = zmq.asyncio.Context()
        self.id = uuid.uuid4().hex
        self.mine: set[str] = set()
        self.seen = 0
        self.notice: str | None = None
        self.running: str | None = None  # the job id of the execution in progress
        self.submitting = False          # between receiving an execute_request and knowing its job id
        self.abort_queued = False

    # ---------------------------------------------------------------- wire

    def _url(self, port_name: str) -> str:
        c = self.conn
        sep = ":" if c.get("transport", "tcp") == "tcp" else "-"
        return f"{c.get('transport', 'tcp')}://{c['ip']}{sep}{c[port_name]}"

    def _sign(self, parts: list[bytes]) -> bytes:
        if not self.key:
            return b""
        h = hmac.new(self.key, digestmod=self.digest)
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
        """(routing identities, message), or (…, None) for a frame set that is malformed, wrongly signed or a
        replay: those are dropped, as the specification requires."""
        try:
            i = frames.index(DELIM)
        except ValueError:
            return [], None
        idents, sig, parts = frames[:i], frames[i + 1:i + 2], frames[i + 2:i + 6]
        if len(parts) < 4 or not sig:
            return idents, None
        if self.key and not hmac.compare_digest(sig[0], self._sign(parts)):
            return idents, None
        try:
            header, parent, metadata, content = (json.loads(p) for p in parts)
        except ValueError:
            return idents, None
        if not all(isinstance(x, dict) for x in (header, parent, metadata, content)) or "msg_type" not in header:
            return idents, None
        msg_id = str(header.get("msg_id"))
        if msg_id in self.seen_ids:  # an authenticated message seen before: a replay
            return idents, None
        self.seen_ids[msg_id] = None
        if len(self.seen_ids) > 10_000:
            self.seen_ids.pop(next(iter(self.seen_ids)))
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
                           f"you left them. `talo kill {self.session}` starts it empty; %checkout SNAPSHOT returns to a snapshot.]\n")
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
        lines = [ln for ln in code.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
        if lines and all(ln.lstrip().startswith("%") for ln in lines):
            reply = await self.magics(lines, msg, silent)
        else:
            reply = await self.execute_code(msg, code, silent, content)
        if reply[0]["status"] == "error" and content.get("stop_on_error", True):
            self.abort_queued = True
        return reply

    async def execute_code(self, msg: dict, code: str, silent: bool, content: dict) -> tuple[dict, dict]:
        self.submitting = True
        try:
            job = await self.talo("run", session=self.session, code=code, timeout=0, silent=silent,
                                  store_history=content.get("store_history", True),
                                  user_expressions=content.get("user_expressions") or None)
        finally:
            self.submitting = False
        self.mine.add(job["id"])
        self.running = job["id"]
        if self.pending_interrupt:  # an interrupt arrived while the job was being submitted
            self.pending_interrupt = False
            await self.talo("interrupt", id=job["id"])
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        offset, count_sent, buffered = 0, False, ""
        try:
            while True:
                j = await self.talo("job", id=job["id"])
                if not count_sent and j.get("execution_count") and not silent:
                    await self.iopub("execute_input", {"code": code, "execution_count": j["execution_count"]}, msg)
                    count_sent = True
                done = j["status"] not in ("running", "queued")
                out = await self.talo("output", id=job["id"], offset=offset, max_bytes=1 << 20, raw=True)
                offset = out["next_offset"]
                buffered += decoder.decode(base64.b64decode(out["data"]), final=done and offset >= out["size"])
                buffered = await self.flush_until_mark(buffered, job["id"], msg, silent)
                if done and offset >= out["size"]:
                    break
                if out["next_offset"] >= out["size"]:
                    await asyncio.sleep(POLL_S)
        finally:
            self.running = None
        job = await self.talo("job", id=job["id"])
        n = job.get("execution_count") or 0
        if not count_sent and not silent:
            await self.iopub("execute_input", {"code": code, "execution_count": n}, msg)
        if not silent:
            for d in job.get("displays") or []:  # displays without a marker (no output file), if any
                if d.get("offset") is None:
                    await self.send_display(d, msg)
            result = job.get("result")
            if job["status"] == "ok" and result and "data" in result:
                data = dict(result["data"])
                for mime, path in (result.get("metadata") or {}).get("talo_files", {}).items():
                    if (value := self.file_value(mime, path)) is not None:
                        data[mime] = value
                await self.iopub("execute_result", {"execution_count": n, "data": data, "metadata": {}}, msg)
        if job["status"] == "ok":
            return {"status": "ok", "execution_count": n, "user_expressions": job.get("user_expressions") or {},
                    "payload": []}, {}
        error = job.get("error") or {"ename": job["status"], "evalue": "", "traceback": []}
        err = {"ename": error.get("ename") or job["status"], "evalue": str(error.get("evalue") or ""),
               "traceback": error.get("traceback") or [f"{error.get('ename')}: {error.get('evalue')}"]}
        if not silent:
            await self.iopub("error", err, msg)
        return {"status": "error", "execution_count": n, **err}, {}

    async def flush_until_mark(self, text: str, job_id: str, msg: dict, silent: bool) -> str:
        """Stream ``text`` up to each display marker, send that display (waiting for its message if needed) and
        skip the text the worker printed for it; return what is left (a possibly incomplete marker)."""
        while True:
            m = MARK.search(text)
            if m is None:
                cut = text.rfind("\x1b")  # keep a marker that may be split across reads
                head, rest = (text[:cut], text[cut:]) if cut >= 0 and "\x07" not in text[cut:] else (text, "")
                if head and not silent:
                    await self.stream(head, msg)
                return rest
            if m.start() and not silent:
                await self.stream(text[:m.start()], msg)
            number = int(m.group(1))
            display = None
            for _ in range(200):  # the display message follows its marker closely
                j = await self.talo("job", id=job_id)
                display = next((d for d in j.get("displays") or [] if d.get("n") == number), None)
                if display is not None or j["status"] not in ("running", "queued"):
                    break
                await asyncio.sleep(0.02)
            rest = text[m.end():]
            if display is not None:
                if not silent:
                    await self.send_display(display, msg)
                printed = (display.get("end") or 0) - (display.get("offset") or 0) - len(m.group(0).encode())
                if printed > 0:  # the text the worker printed for the CLI: skip it (bytes → characters)
                    skip = len(rest.encode()[:printed].decode(errors="ignore"))
                    rest = rest[skip:]
            text = rest

    @staticmethod
    def file_value(mime: str, path: str) -> str | None:
        """An image file as a Jupyter mime value: base64 for PNG / JPEG, text for SVG."""
        if not Path(path).exists():
            return None
        raw = Path(path).read_bytes()
        return raw.decode(errors="replace") if mime == "image/svg+xml" else base64.b64encode(raw).decode()

    async def send_display(self, d: dict, msg: dict) -> None:
        data = dict(d.get("data") or {})
        for mime, path in (d.get("files") or {}).items():
            if (value := self.file_value(mime, path)) is not None:
                data[mime] = value
        await self.iopub("display_data", {"data": data, "metadata": {}, "transient": {}}, msg)

    async def magics(self, lines: list[str], msg: dict, silent: bool = False) -> tuple[dict, dict]:
        from talo.cli import main
        status = "ok"
        for line in lines:
            try:
                words = shlex.split(line.lstrip()[1:])
            except ValueError as error:  # an unmatched quote
                if not silent:
                    await self.stream(f"{line.strip()}: {error}\n", msg, "stderr")
                status = "error"
                continue
            if not words or words[0] == "help":
                if not silent:
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
            if not silent:
                await self.stream(text, msg, "stdout" if code == 0 else "stderr")
            if code not in (0, 3):
                status = "error"
        jobs = (await self.talo("jobs", session=self.session, last=50))["jobs"]
        self.seen = max([self.seen, *(int(j["id"]) for j in jobs if str(j["id"]).isdigit())])
        if status == "error":
            err = {"ename": "TaloCommandFailed", "evalue": "a Talo command failed (see its output)", "traceback": []}
            if not silent:
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
        if control and kind not in CONTROL_TYPES:
            return  # executions and inspections come on the shell channel only
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
                else:
                    self.pending_interrupt = self.submitting  # applied as soon as the job id is known
                content, meta = {"status": "ok"}, {}
            elif kind == "shutdown_request":
                content, meta = {"status": "ok", "restart": msg["content"].get("restart", False)}, {}
                await self.send(sock, self._msg("shutdown_reply", content, msg), idents)
                await self.status("idle", msg)
                self.stop.set()  # the session keeps running: only the bridge exits
                return
            else:
                await self.status("idle", msg)  # unknown requests get no reply, but busy is always closed
                return
        except Exception as error:  # noqa: BLE001 - one bad request must not stop the channel
            content, meta = {"status": "error", "ename": type(error).__name__, "evalue": str(error),
                             "traceback": [f"{type(error).__name__}: {error}"]}, {}
        reply_type = kind.replace("_request", "_reply")
        await self.send(sock, self._msg(reply_type, content, msg, meta), idents)
        await self.status("idle", msg)

    async def serve_shell(self) -> None:
        while not self.stop.is_set():
            frames = await self.shell.recv_multipart()
            try:
                await self.handle(self.shell, frames)  # one request at a time, as a kernel's shell channel does
            except Exception as error:  # noqa: BLE001 - never let the channel die while heartbeat says we live
                print(f"talo kernel: shell request failed: {error!r}", file=sys.stderr)
            if self.abort_queued:
                await self.abort_queue()

    async def abort_queue(self) -> None:
        """stop_on_error: execute requests already waiting when a cell failed are answered `aborted`."""
        self.abort_queued = False
        while await self.shell.poll(0):
            frames = await self.shell.recv_multipart()
            idents, msg = self.recv_parse(frames)
            if msg is None:
                continue
            if msg["header"]["msg_type"] != "execute_request":
                await self.handle(self.shell, frames)
                continue
            await self.status("busy", msg)
            await self.send(self.shell, self._msg("execute_reply", {"status": "aborted", "execution_count": 0}, msg),
                            idents)
            await self.status("idle", msg)

    async def serve_control(self) -> None:
        while not self.stop.is_set():
            frames = await self.control.recv_multipart()
            task = asyncio.create_task(self.handle(self.control, frames, control=True))
            task.add_done_callback(lambda t: t.cancelled() or t.exception() and print(
                f"talo kernel: control request failed: {t.exception()!r}", file=sys.stderr))

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
    spec = {"argv": argv, "display_name": f"Talo: {session} (persistent)", "language": "python",
            "metadata": {"talo": {"session": session}}, "interrupt_mode": "message"}
    if "TALO_HOME" in os.environ:
        spec["env"] = {"TALO_HOME": os.environ["TALO_HOME"]}
    (path / "kernel.json").write_text(json.dumps(spec, indent=1) + "\n")
    return path
