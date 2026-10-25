"""talo supervisor: one resident process per machine (per ``TALO_HOME``) that manages sessions, jobs and history.

- The CLI talks to it over the Unix socket ``$TALO_HOME/supervisor.sock``: one JSON request line, one JSON reply line.
- Each session has one worker process (``worker.py``), started with the session's interpreter; supervisor and worker
  exchange Jupyter-shaped messages (``wire.py``).
- The supervisor holds no user data: when a worker dies, the supervisor is still there to report what happened.
- Execution requests to one session are queued and run one at a time. Each execution is a job whose output goes to its
  own file, readable while the job runs.

Layout of ``TALO_HOME`` (default ``~/.talo``)::

    supervisor.sock  supervisor.log  state.json
    sessions/<session>/history.jsonl    one line per finished job (code, status, duration, memory)
    sessions/<session>/jobs/<job>.out   the job's stdout + stderr
    sessions/<session>/worker.log       output of the worker process itself (normally empty)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from talo import wire
from talo.paths import home, socket_path

log = logging.getLogger("talo")
WORKER = Path(__file__).with_name("worker.py")
FINISHED = ("ok", "error", "interrupted", "lost", "cancelled")


@dataclass
class Job:
    id: str
    session: str
    code: str
    output: str
    status: str = "queued"  # queued -> running -> ok / error / interrupted / lost / cancelled
    submitted: float = field(default_factory=time.time)
    started: float | None = None
    finished: float | None = None
    execution_count: int | None = None
    result: dict | None = None  # data of execute_result (a mime bundle)
    error: dict | None = None   # ename / evalue / traceback
    metrics: dict = field(default_factory=dict)
    interrupt_requested: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def to_dict(self) -> dict:
        out = {k: getattr(self, k) for k in ("id", "session", "status", "submitted", "started", "finished",
                                            "execution_count", "result", "error", "metrics", "output")}
        out["elapsed_s"] = round((self.finished or time.time()) - (self.started or time.time()), 3) if self.started else None
        return out


class SessionDead(RuntimeError):
    pass


class Session:
    def __init__(self, name: str, python: str, cwd: str, env: dict | None = None):
        self.name, self.python, self.cwd = name, python, cwd
        self.env = env or {}
        self.dir = home() / "sessions" / name
        (self.dir / "jobs").mkdir(parents=True, exist_ok=True)
        self.created = time.time()
        self.proc: asyncio.subprocess.Process | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.waiters: dict[str, asyncio.Queue] = {}
        self.queue: asyncio.Queue[Job] = asyncio.Queue()
        self.current: Job | None = None
        self.state = "starting"  # starting -> idle / busy -> dead
        self.death: str | None = None
        self.worker_pid: int | None = None
        self.tasks: list[asyncio.Task] = []

    # ---------------------------------------------------------------- process and messages

    async def start(self) -> None:
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        env = {**os.environ, **self.env, "PYTHONUNBUFFERED": "1"}
        with open(self.dir / "worker.log", "ab") as errlog:
            self.proc = await asyncio.create_subprocess_exec(
                self.python, str(WORKER), "--fd", str(child.fileno()), "--session", self.name,
                cwd=self.cwd, env=env, pass_fds=(child.fileno(),), stdin=asyncio.subprocess.DEVNULL,
                stdout=errlog, stderr=errlog, start_new_session=True)
        child.close()
        reader, self.writer = await asyncio.open_unix_connection(sock=parent, limit=wire.MAX_LINE)
        self.tasks = [asyncio.create_task(self._read(reader)), asyncio.create_task(self._execute_loop())]
        info = await self.request("kernel_info_request", {}, timeout=60)
        self.worker_pid = info["content"].get("talo_pid")
        self.state = "idle"

    async def _read(self, reader: asyncio.StreamReader) -> None:
        while True:
            line = await reader.readline()
            if not line:
                break
            msg = wire.decode(line)
            parent = msg.get("parent_header", {}).get("msg_id")
            if parent in self.waiters:
                await self.waiters[parent].put(msg)
        await self._died()

    async def _died(self) -> None:
        code = await self.proc.wait() if self.proc else None
        if code is not None and code < 0:
            how = f"was killed by {signal.Signals(-code).name}" + (
                " (possibly by the OOM killer)" if -code == signal.SIGKILL else "")
        else:
            how = f"exited with code {code}"
        self.death = f"the worker process {how}"
        self.state = "dead"
        log.warning("session %s: %s", self.name, self.death)
        for q in list(self.waiters.values()):
            await q.put(None)

    def _send(self, msg: dict) -> asyncio.Queue:
        if self.state == "dead" or self.writer is None:
            raise SessionDead(self.death or "the worker is gone")
        q: asyncio.Queue = asyncio.Queue()
        self.waiters[msg["header"]["msg_id"]] = q
        self.writer.write(wire.encode(msg))
        return q

    async def request(self, msg_type: str, content: dict, *, timeout: float | None = None) -> dict:
        """Send a non-execution request and return the matching ``*_reply``."""
        msg = wire.message(msg_type, content, session=self.name)
        q = self._send(msg)
        try:
            reply_type = msg_type.replace("_request", "_reply")
            while True:
                got = await asyncio.wait_for(q.get(), timeout)
                if got is None:
                    raise SessionDead(self.death or "the worker is gone")
                if wire.msg_type(got) in (reply_type, "error"):
                    return got
        finally:
            self.waiters.pop(msg["header"]["msg_id"], None)

    # ---------------------------------------------------------------- execution

    async def _execute_loop(self) -> None:
        while True:
            job = await self.queue.get()
            if job.status == "cancelled":
                continue
            if self.state == "dead":
                self._finish(job, "lost", error={"ename": "SessionDead", "evalue": self.death, "traceback": []})
                continue
            await self._run(job)

    async def _run(self, job: Job) -> None:
        self.current, self.state = job, "busy"
        job.status, job.started = "running", time.time()
        msg = wire.message("execute_request", {"code": job.code, "silent": False, "store_history": True,
                                               "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
                           session=self.name, metadata={"talo_output": job.output, "talo_job": job.id})
        try:
            q = self._send(msg)
        except SessionDead as error:
            self._finish(job, "lost", error={"ename": "SessionDead", "evalue": str(error), "traceback": []})
            return
        try:
            while True:
                got = await q.get()
                if got is None:  # the worker died mid-job
                    self._finish(job, "lost", error={"ename": "WorkerDied", "evalue": self.death, "traceback": []})
                    return
                kind = wire.msg_type(got)
                if kind == "execute_input":
                    job.execution_count = got["content"].get("execution_count")
                elif kind == "execute_result":
                    job.result = {"data": got["content"]["data"], "metadata": got["content"].get("metadata", {})}
                elif kind == "error":
                    job.error = dict(got["content"])
                elif kind == "execute_reply":
                    status = got["content"]["status"]
                    job.metrics = dict(got.get("metadata", {}))
                    if status == "error" and job.error and job.error.get("ename") == "KeyboardInterrupt" \
                            and job.interrupt_requested:
                        status = "interrupted"
                    self._finish(job, status)
                    return
        finally:
            self.waiters.pop(msg["header"]["msg_id"], None)
            self.current = None
            if self.state != "dead":
                self.state = "idle"

    def _finish(self, job: Job, status: str, *, error: dict | None = None) -> None:
        job.status, job.finished = status, time.time()
        if error is not None:
            job.error = error
        job.done.set()
        record = {k: v for k, v in job.to_dict().items() if k != "result"}
        record["code"] = job.code
        record["result_text"] = (job.result or {}).get("data", {}).get("text/plain")
        with open(self.dir / "history.jsonl", "a") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def interrupt(self, job: Job) -> str:
        if job.status == "queued":
            job.status, job.finished = "cancelled", time.time()
            job.done.set()
            return "cancelled"
        if job.status == "running" and self.worker_pid:
            job.interrupt_requested = True
            os.kill(self.worker_pid, signal.SIGINT)
            return "interrupting"
        return job.status

    async def stop(self) -> None:
        if self.state != "dead":
            try:
                await self.request("shutdown_request", {"restart": False}, timeout=5)
            except (SessionDead, asyncio.TimeoutError):
                pass
        if self.proc and self.proc.returncode is None:
            try:
                await asyncio.wait_for(self.proc.wait(), 5)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
        for t in self.tasks:
            t.cancel()

    def to_dict(self) -> dict:
        rss = None
        if self.worker_pid and self.state != "dead":
            try:
                with open(f"/proc/{self.worker_pid}/statm") as f:
                    rss = int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
            except OSError:
                pass
        return {"name": self.name, "state": self.state, "python": self.python, "cwd": self.cwd,
                "pid": self.worker_pid, "rss": rss, "created": self.created, "queued": self.queue.qsize(),
                "current_job": self.current.id if self.current else None, "death": self.death}


class Supervisor:
    def __init__(self):
        self.sessions: dict[str, Session] = {}
        self.jobs: dict[str, Job] = {}
        self.state_file = home() / "state.json"
        state = json.loads(self.state_file.read_text()) if self.state_file.exists() else {}
        self.next_job = int(state.get("next_job", 1))
        self.stopping = asyncio.Event()

    def _new_job_id(self) -> str:
        n = self.next_job
        self.next_job += 1
        self.state_file.write_text(json.dumps({"next_job": self.next_job}))
        return str(n)

    def _session(self, name: str) -> Session:
        if name not in self.sessions:
            raise KeyError(f"no session {name!r} (create it with `talo new {name}`; `talo ls` lists sessions)")
        return self.sessions[name]

    def _job(self, job_id: str) -> Job:
        if str(job_id) not in self.jobs:
            raise KeyError(f"no job {job_id!r} (`talo jobs` lists jobs)")
        return self.jobs[str(job_id)]

    async def _wait(self, job: Job, timeout: float | None) -> dict:
        if timeout is None or timeout > 0:
            try:
                await asyncio.wait_for(job.done.wait(), timeout)
            except asyncio.TimeoutError:
                pass
        return job.to_dict()

    # ---------------------------------------------------------------- requests

    async def op_ping(self, req):
        return {"pid": os.getpid(), "home": str(home()), "sessions": len(self.sessions)}

    async def op_new(self, req):
        name = req["name"]
        if name in self.sessions and self.sessions[name].state != "dead":
            raise ValueError(f"session {name!r} already exists")
        session = Session(name, req.get("python") or sys.executable, req.get("cwd") or os.getcwd(), req.get("env"))
        self.sessions[name] = session
        try:
            await session.start()
        except Exception:
            self.sessions.pop(name, None)
            await session.stop()
            raise
        return session.to_dict()

    async def op_ls(self, req):
        return {"sessions": [s.to_dict() for s in self.sessions.values()]}

    async def op_kill(self, req):
        session = self._session(req["name"])
        await session.stop()
        del self.sessions[req["name"]]
        return {"killed": req["name"]}

    async def op_run(self, req):
        session = self._session(req["session"])
        if session.state == "dead":
            raise SessionDead(f"session {session.name} is dead: {session.death} "
                              f"(`talo kill {session.name}`, then create it again)")
        job = Job(self._new_job_id(), session.name, req["code"], "")
        job.output = str(session.dir / "jobs" / f"{job.id}.out")
        Path(job.output).touch()
        self.jobs[job.id] = job
        await session.queue.put(job)
        return await self._wait(job, req.get("timeout", 30))

    async def op_job(self, req):
        return await self._wait(self._job(req["id"]), req.get("wait", 0))

    async def op_jobs(self, req):
        name = req.get("session")
        return {"jobs": [j.to_dict() for j in self.jobs.values() if not name or j.session == name]}

    async def op_output(self, req):
        job = self._job(req["id"])
        path = Path(job.output)
        size = path.stat().st_size
        offset = req.get("offset")
        limit = int(req.get("max_bytes", 64 << 10))
        if offset is None:  # default: the end of the output
            offset = max(0, size - limit)
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read(limit)
        return {"id": job.id, "offset": offset, "next_offset": offset + len(data), "size": size,
                "text": data.decode(errors="replace"), "status": job.status}

    async def op_interrupt(self, req):
        job = self._job(req["id"])
        return {"id": job.id, "result": self._session(job.session).interrupt(job)}

    async def _ask_idle(self, session: Session, msg_type: str, content: dict) -> dict:
        if session.state == "busy":
            raise RuntimeError(f"session {session.name} is busy with job {session.current.id}; variables can be "
                               f"inspected once it finishes (`talo wait {session.current.id}`)")
        reply = await session.request(msg_type, content, timeout=60)
        return reply["content"]

    async def op_vars(self, req):
        return await self._ask_idle(self._session(req["session"]), "talo_vars_request", {"pattern": req.get("pattern")})

    async def op_show(self, req):
        return await self._ask_idle(self._session(req["session"]), "talo_show_request",
                                    {"name": req["name"], "rows": req.get("rows", 10)})

    async def op_history(self, req):
        path = self._session(req["session"]).dir / "history.jsonl"
        lines = path.read_text().splitlines() if path.exists() else []
        return {"history": [json.loads(x) for x in lines[-int(req.get("last", 20)):]]}

    async def op_shutdown(self, req):
        self.stopping.set()
        return {"stopping": True}

    # ---------------------------------------------------------------- server

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            req = json.loads(line)
            fn = getattr(self, f"op_{req.get('op')}", None)
            if fn is None:
                reply = {"ok": False, "error": {"type": "UnknownOp", "message": f"unknown operation {req.get('op')!r}"}}
            else:
                try:
                    reply = {"ok": True, **await fn(req)}
                except Exception as error:  # noqa: BLE001 - report to the client; the supervisor keeps running
                    msg = error.args[0] if isinstance(error, KeyError) and error.args else str(error)
                    reply = {"ok": False, "error": {"type": type(error).__name__, "message": msg}}
            writer.write(json.dumps(reply, ensure_ascii=False, default=str).encode() + b"\n")
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def serve(self) -> None:
        path = socket_path()
        if path.exists():
            path.unlink()
        server = await asyncio.start_unix_server(self.handle, path=str(path), limit=wire.MAX_LINE)
        os.chmod(path, 0o600)
        log.info("supervisor started: pid %s, %s", os.getpid(), path)
        async with server:
            await self.stopping.wait()
        for s in list(self.sessions.values()):
            await s.stop()
        path.unlink(missing_ok=True)
        log.info("supervisor stopped")


def main() -> None:
    home().mkdir(parents=True, exist_ok=True)
    os.chmod(home(), 0o700)
    logging.basicConfig(filename=home() / "supervisor.log", level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    sup = Supervisor()
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, sup.stopping.set)
    loop.run_until_complete(sup.serve())


if __name__ == "__main__":
    main()
