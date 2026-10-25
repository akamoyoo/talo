"""talo supervisor: one resident process per machine (per ``TALO_HOME``) that manages sessions, jobs, snapshots and
history.

- The CLI talks to it over the Unix socket ``$TALO_HOME/supervisor.sock``: one JSON request line, one JSON reply line.
- Each session has one worker process (``worker.py``), started with the session's interpreter or forked from a
  snapshot; supervisor and worker exchange Jupyter-shaped messages (``wire.py``).
- The supervisor holds no user data: when a worker dies, the supervisor is still there to report what happened, and
  the session's snapshots are still there to roll back to.
- Requests that change a session's state (executions, snapshots, checkouts) are queued and handled one at a time.
  Each execution is a job whose output goes to its own file, readable while the job runs.

Layout of ``TALO_HOME`` (default ``~/.talo``)::

    supervisor.sock  supervisor.log  state.json
    sessions/<session>/history.jsonl          one line per finished job (code, status, duration, memory)
    sessions/<session>/jobs/<job>.out         the job's stdout + stderr
    sessions/<session>/snapshots/<id>.sock    a snapshot's socket (<id>.children: exit status of workers it spawned)
    sessions/<session>/worker.log             output of the worker process itself (normally empty)
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
from talo.paths import home, socket_for, socket_path

log = logging.getLogger("talo")
WORKER = Path(__file__).with_name("worker.py")
DIFF_MAX_BYTES = 256 << 20  # variables larger than this are compared by summary only, unless the request says otherwise


def private_dirty(pid: int | None) -> int | None:
    """Bytes of memory only this process holds (Private_Dirty in /proc/PID/smaps_rollup)."""
    if not pid:
        return None
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                if line.startswith("Private_Dirty:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def resident(pid: int | None) -> int | None:
    if not pid:
        return None
    try:
        with open(f"/proc/{pid}/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except OSError:
        return None


def describe_exit(code: int | None) -> str:
    if code is None:
        return "exited (status unknown)"
    if code < 0:
        how = f"was killed by {signal.Signals(-code).name}"
        return how + (" (possibly by the OOM killer)" if -code == signal.SIGKILL else "")
    return f"exited with code {code}"


@dataclass
class Job:
    id: str
    session: str
    code: str
    output: str
    kind: str = "execute"   # execute | snap | checkout: everything that changes a session's state is queued
    params: dict = field(default_factory=dict)
    status: str = "queued"  # queued -> running -> ok / error / interrupted / lost / cancelled
    submitted: float = field(default_factory=time.time)
    started: float | None = None
    finished: float | None = None
    execution_count: int | None = None
    result: dict | None = None  # data of execute_result (a mime bundle); for control jobs, their result
    error: dict | None = None   # ename / evalue / traceback
    metrics: dict = field(default_factory=dict)
    interrupt_requested: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def to_dict(self) -> dict:
        out = {k: getattr(self, k) for k in ("id", "session", "kind", "status", "submitted", "started", "finished",
                                            "execution_count", "result", "error", "metrics", "output")}
        out["elapsed_s"] = round((self.finished or time.time()) - (self.started or time.time()), 3) if self.started else None
        return out


@dataclass
class Snapshot:
    id: str
    name: str
    session: str
    socket: str
    pid: int
    parent: str | None          # the snapshot the worker descended from when this one was taken ("session:id")
    jobs: list                  # job ids run between the parent and this snapshot
    execution_count: int
    threads: list
    message: str = ""
    created: float = field(default_factory=time.time)
    hold: socket.socket | None = field(default=None, repr=False)

    @property
    def ref(self) -> str:
        return f"{self.session}:{self.id}"

    def to_dict(self) -> dict:
        return {"id": self.id, "name": self.name, "session": self.session, "ref": self.ref, "parent": self.parent,
                "pid": self.pid, "created": self.created, "message": self.message,
                "execution_count": self.execution_count, "jobs": self.jobs, "threads": self.threads,
                "private_bytes": private_dirty(self.pid), "alive": private_dirty(self.pid) is not None}


class SessionDead(RuntimeError):
    pass


class Session:
    def __init__(self, name: str, python: str, cwd: str, env: dict | None = None, memory_budget: int | None = None):
        self.name, self.python, self.cwd = name, python, cwd
        self.env = env or {}
        self.memory_budget = memory_budget
        self.dir = home() / "sessions" / name
        for sub in ("jobs", "snapshots"):
            (self.dir / sub).mkdir(parents=True, exist_ok=True)
        self.created = time.time()
        self.proc: asyncio.subprocess.Process | None = None  # only for a worker the supervisor started itself
        self.children_file: Path | None = None              # for a worker forked from a snapshot
        self.writer: asyncio.StreamWriter | None = None
        self.generation = 0                                  # increases each time the worker is replaced
        self.waiters: dict[str, asyncio.Queue] = {}
        self.queue: asyncio.Queue[Job] = asyncio.Queue()
        self.current: Job | None = None
        self.state = "starting"  # starting -> idle / busy -> dead
        self.death: str | None = None
        self.worker_pid: int | None = None
        self.snapshots: dict[str, Snapshot] = {}
        self.next_snapshot = 1
        self.base: str | None = None        # ref of the snapshot the current worker descends from
        self.jobs_since_base: list[str] = []
        self.tasks: list[asyncio.Task] = []

    # ---------------------------------------------------------------- workers

    async def start(self) -> None:
        """Start a fresh worker process."""
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        env = {**os.environ, **self.env, "PYTHONUNBUFFERED": "1"}
        with open(self.dir / "worker.log", "ab") as errlog:
            proc = await asyncio.create_subprocess_exec(
                self.python, str(WORKER), "--fd", str(child.fileno()), "--session", self.name,
                cwd=self.cwd, env=env, pass_fds=(child.fileno(),), stdin=asyncio.subprocess.DEVNULL,
                stdout=errlog, stderr=errlog, start_new_session=True)
        child.close()
        reader, writer = await asyncio.open_unix_connection(sock=parent, limit=wire.MAX_LINE)
        await self._attach(reader, writer, proc=proc)
        self.tasks.append(asyncio.create_task(self._execute_loop()))

    async def start_from(self, snap: Snapshot) -> None:
        """Start this session with a worker forked from a snapshot (possibly another session's)."""
        await self._spawn_from(snap)
        self.tasks.append(asyncio.create_task(self._execute_loop()))

    async def _spawn_from(self, snap: Snapshot) -> None:
        reader, writer = await asyncio.open_unix_connection(snap.socket, limit=wire.MAX_LINE)
        writer.write(b'{"op": "spawn"}\n')
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), 30)
        if not line or not json.loads(line).get("ok"):
            writer.close()
            raise RuntimeError(f"snapshot {snap.name} did not spawn a worker")
        await self._attach(reader, writer, children_file=Path(snap.socket).with_suffix(".children"))
        self.base, self.jobs_since_base = snap.ref, []

    async def _attach(self, reader, writer, *, proc=None, children_file=None) -> None:
        """Make this connection the session's worker channel (replacing the previous worker, if any)."""
        self.generation += 1
        self.proc, self.children_file, self.writer = proc, children_file, writer
        self.state, self.death = "starting", None
        self.tasks.append(asyncio.create_task(self._read(reader, self.generation)))
        info = await self.request("kernel_info_request", {}, timeout=60)
        self.worker_pid = info["content"].get("talo_pid")
        self.state = "idle"

    async def _retire(self) -> None:
        """Ask the current worker to exit (used when a checkout replaces it); its death is not reported."""
        old_writer, old_proc, pid = self.writer, self.proc, self.worker_pid
        self.generation += 1  # the old worker's reader now belongs to a past generation
        if old_writer is not None:
            try:
                old_writer.write(wire.encode(wire.message("shutdown_request", {"restart": False}, session=self.name)))
                await old_writer.drain()
            except (ConnectionError, RuntimeError):
                pass
            old_writer.close()
        if old_proc is not None and old_proc.returncode is None:
            try:
                await asyncio.wait_for(old_proc.wait(), 5)
            except asyncio.TimeoutError:
                old_proc.kill()
        elif pid and old_proc is None:
            await asyncio.sleep(0)  # a forked worker is reaped by its snapshot

    async def _read(self, reader: asyncio.StreamReader, generation: int) -> None:
        while True:
            line = await reader.readline()
            if not line:
                break
            msg = wire.decode(line)
            parent = msg.get("parent_header", {}).get("msg_id")
            if parent in self.waiters:
                await self.waiters[parent].put(msg)
        if generation == self.generation:
            await self._died()

    async def _exit_code(self) -> int | None:
        if self.proc is not None:
            return await self.proc.wait()
        if self.children_file is not None and self.worker_pid:
            for _ in range(50):  # the snapshot records the status from its SIGCHLD handler
                if self.children_file.exists():
                    for line in self.children_file.read_text().splitlines():
                        pid, code = line.split()
                        if int(pid) == self.worker_pid:
                            return int(code)
                await asyncio.sleep(0.02)
        return None

    async def _died(self) -> None:
        self.death = f"the worker process {describe_exit(await self._exit_code())}"
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

    # ---------------------------------------------------------------- the queue

    async def _execute_loop(self) -> None:
        while True:
            job = await self.queue.get()
            if job.status == "cancelled":
                continue
            self.current, job.status, job.started = job, "running", time.time()
            if self.state != "dead":
                self.state = "busy"
            try:
                if job.kind == "execute":
                    await self._execute(job)
                elif job.kind == "snap":
                    self._finish(job, "ok", result=(await self._snap(**job.params)).to_dict())
                elif job.kind == "checkout":
                    self._finish(job, "ok", result=await self._checkout(**job.params))
            except Exception as error:  # noqa: BLE001 - a failed control job is reported, the session goes on
                if not job.done.is_set():
                    self._finish(job, "error", error={"ename": type(error).__name__, "evalue": str(error),
                                                      "traceback": []})
            finally:
                self.current = None
                if self.state != "dead":
                    self.state = "idle"

    async def _execute(self, job: Job) -> None:
        if self.state == "dead":
            self._finish(job, "lost", error={"ename": "SessionDead", "evalue": self.death, "traceback": []})
            return
        msg = wire.message("execute_request", {"code": job.code, "silent": False, "store_history": True,
                                               "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
                           session=self.name, metadata={"talo_output": job.output, "talo_job": job.id})
        q = self._send(msg)
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
                    if status == "error" and job.interrupt_requested and (job.error or {}).get("ename") == "KeyboardInterrupt":
                        status = "interrupted"
                    self.jobs_since_base.append(job.id)
                    self._finish(job, status)
                    return
        finally:
            self.waiters.pop(msg["header"]["msg_id"], None)

    def _finish(self, job: Job, status: str, *, error: dict | None = None, result=None) -> None:
        job.status, job.finished = status, time.time()
        if error is not None:
            job.error = error
        if result is not None:
            job.result = result
        job.done.set()
        record = {k: v for k, v in job.to_dict().items() if k != "result"}
        record["code"] = job.code
        if job.kind == "execute":
            record["result_text"] = (job.result or {}).get("data", {}).get("text/plain")
        else:
            record["result"] = job.result
        with open(self.dir / "history.jsonl", "a") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    # ---------------------------------------------------------------- snapshots

    def memory_in_use(self) -> int:
        return (resident(self.worker_pid) or 0) + sum(private_dirty(s.pid) or 0 for s in self.snapshots.values())

    async def _snap(self, name: str | None = None, message: str = "", strict: bool = False) -> Snapshot:
        if self.state == "dead":
            raise SessionDead(f"session {self.name} is dead ({self.death}); check out a snapshot first")
        sid = f"s{self.next_snapshot}"
        name = name or sid
        if self.find(name, missing_ok=True):
            raise ValueError(f"session {self.name} already has a snapshot named {name!r}")
        if self.memory_budget and self.memory_in_use() > self.memory_budget:
            biggest = sorted(self.snapshots.values(), key=lambda s: -(private_dirty(s.pid) or 0))[:3]
            hint = ", ".join(f"{s.name} ({(private_dirty(s.pid) or 0) >> 20} MB)" for s in biggest) or "none"
            raise MemoryError(f"session {self.name} uses {self.memory_in_use() >> 20} MB, over its budget of "
                              f"{self.memory_budget >> 20} MB; snapshots that could be dropped: {hint}")
        path = str(socket_for(self.dir / "snapshots" / f"{sid}.sock"))
        reply = await self.request("talo_snapshot_request", {"id": sid, "socket": path, "strict": strict}, timeout=120)
        content = reply["content"]
        if content.get("status") != "ok":
            raise RuntimeError(f"{content.get('ename')}: {content.get('evalue')}")
        self.next_snapshot += 1
        snap = Snapshot(sid, name, self.name, path, content["pid"], self.base, list(self.jobs_since_base),
                        content.get("execution_count", 0), content.get("threads", []), message)
        snap.hold = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        snap.hold.connect(path)
        snap.hold.sendall(b'{"op": "hold"}\n')
        self.snapshots[sid] = snap
        self.base, self.jobs_since_base = snap.ref, []
        return snap

    def find(self, name: str, missing_ok: bool = False) -> Snapshot | None:
        for snap in self.snapshots.values():
            if name in (snap.id, snap.name):
                return snap
        if missing_ok:
            return None
        raise KeyError(f"session {self.name} has no snapshot {name!r} (`talo snaps -s {self.name}` lists them)")

    async def _checkout(self, snap: Snapshot, discard: bool = False, save_as: str | None = None) -> dict:
        if self.jobs_since_base and self.state != "dead" and not discard and not save_as:
            raise RuntimeError(f"jobs {', '.join(self.jobs_since_base)} ran since this worker's base "
                               f"({self.base or 'session start'}); use --discard to drop that state or "
                               f"--snap NAME to save it first")
        if save_as and self.state != "dead":
            await self._snap(save_as, message=f"saved before checking out {snap.name}")
        if private_dirty(snap.pid) is None:
            raise RuntimeError(f"snapshot {snap.name} (pid {snap.pid}) is gone")
        await self._retire()
        await self._spawn_from(snap)
        return self.to_dict()

    def drop(self, snap: Snapshot) -> None:
        if snap.hold is not None:
            snap.hold.close()  # the snapshot exits when its hold connection closes
        self.snapshots.pop(snap.id, None)
        for path in (Path(snap.socket), Path(snap.socket).with_suffix(".children")):
            path.unlink(missing_ok=True)

    # ---------------------------------------------------------------- lifecycle

    def interrupt(self, job: Job) -> str:
        if job.status == "queued":
            job.status, job.finished = "cancelled", time.time()
            job.done.set()
            return "cancelled"
        if job.status == "running" and job.kind == "execute" and self.worker_pid:
            job.interrupt_requested = True
            os.kill(self.worker_pid, signal.SIGINT)
            return "interrupting"
        return job.status

    async def stop(self) -> None:
        for snap in list(self.snapshots.values()):
            self.drop(snap)
        if self.state != "dead":
            try:
                await self.request("shutdown_request", {"restart": False}, timeout=5)
            except (SessionDead, asyncio.TimeoutError, ConnectionError):
                pass
        if self.proc and self.proc.returncode is None:
            try:
                await asyncio.wait_for(self.proc.wait(), 5)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
        elif self.worker_pid and self.state != "dead":
            try:
                os.kill(self.worker_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for t in self.tasks:
            t.cancel()

    def to_dict(self) -> dict:
        return {"name": self.name, "state": self.state, "python": self.python, "cwd": self.cwd,
                "pid": self.worker_pid, "rss": resident(self.worker_pid) if self.state != "dead" else None,
                "created": self.created, "queued": self.queue.qsize(),
                "current_job": self.current.id if self.current else None, "death": self.death, "base": self.base,
                "jobs_since_base": list(self.jobs_since_base), "snapshots": len(self.snapshots),
                "memory_budget": self.memory_budget}


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

    def _snapshot(self, ref: str, default_session: str | None) -> Snapshot:
        """``[session:]name`` -> the snapshot. Without a session, look in ``default_session`` first, then in every
        session, requiring the name to be unique (so a branch can name the snapshot it came from)."""
        session, _, name = ref.rpartition(":")
        if session:
            return self._session(session).find(name)
        if default_session in self.sessions and (snap := self.sessions[default_session].find(name, missing_ok=True)):
            return snap
        found = [snap for s in self.sessions.values() if (snap := s.find(name, missing_ok=True))]
        if len(found) == 1:
            return found[0]
        if not found:
            raise KeyError(f"no snapshot {name!r} in any session (`talo snaps -s SESSION` lists them)")
        raise KeyError(f"snapshot name {name!r} is ambiguous: {', '.join(s.ref for s in found)}; use SESSION:NAME")

    async def _wait(self, job: Job, timeout: float | None) -> dict:
        if timeout is None or timeout > 0:
            try:
                await asyncio.wait_for(job.done.wait(), timeout)
            except asyncio.TimeoutError:
                pass
        return job.to_dict()

    async def _submit(self, session: Session, kind: str, code: str = "", **params) -> Job:
        job = Job(self._new_job_id(), session.name, code, "", kind=kind, params=params)
        job.output = str(session.dir / "jobs" / f"{job.id}.out")
        Path(job.output).touch()
        self.jobs[job.id] = job
        await session.queue.put(job)
        return job

    # ---------------------------------------------------------------- requests

    async def op_ping(self, req):
        return {"pid": os.getpid(), "home": str(home()), "sessions": len(self.sessions)}

    async def op_new(self, req):
        name = req["name"]
        if name in self.sessions and self.sessions[name].state != "dead":
            raise ValueError(f"session {name!r} already exists")
        source = self._snapshot(req["from"], None) if req.get("from") else None
        python = source and self.sessions[source.session].python or req.get("python") or sys.executable
        session = Session(name, python, req.get("cwd") or os.getcwd(), req.get("env"), req.get("memory_budget"))
        self.sessions[name] = session
        try:
            await (session.start_from(source) if source else session.start())
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
            raise SessionDead(f"session {session.name} is dead: {session.death} (`talo checkout SNAPSHOT` to roll "
                              f"back, or `talo kill {session.name}` and create it again)")
        job = await self._submit(session, "execute", req["code"])
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

    async def _ask_idle(self, session: Session, msg_type: str, content: dict, timeout: float | None = 60) -> dict:
        if session.state == "busy":
            raise RuntimeError(f"session {session.name} is busy with job {session.current.id}; variables can be "
                               f"inspected once it finishes (`talo wait {session.current.id}`)")
        reply = await session.request(msg_type, content, timeout=timeout)
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

    async def _control(self, session: Session, kind: str, timeout: float, **params) -> dict:
        job = await self._submit(session, kind, **params)
        out = await self._wait(job, timeout)
        if out["status"] == "error":
            raise RuntimeError(out["error"]["evalue"])
        if out["status"] != "ok":
            return {"job": out}
        return out["result"]

    async def op_snap(self, req):
        session = self._session(req["session"])
        return await self._control(session, "snap", req.get("timeout", 300), name=req.get("name"),
                                   message=req.get("message", ""), strict=bool(req.get("strict")))

    async def op_snaps(self, req):
        session = self._session(req["session"])
        return {"snapshots": [s.to_dict() for s in session.snapshots.values()], "base": session.base,
                "jobs_since_base": list(session.jobs_since_base)}

    async def op_checkout(self, req):
        session = self._session(req["session"])
        snap = self._snapshot(req["name"], session.name)
        return await self._control(session, "checkout", req.get("timeout", 300), snap=snap,
                                   discard=bool(req.get("discard")), save_as=req.get("snap"))

    async def op_drop(self, req):
        session = self._session(req["session"])
        snap = session.find(req["name"])
        session.drop(snap)
        return {"dropped": snap.ref}

    async def op_tree(self, req):
        nodes = []
        for session in self.sessions.values():
            nodes += [{"kind": "snapshot", **s.to_dict()} for s in session.snapshots.values()]
            nodes.append({"kind": "worker", "session": session.name, "state": session.state, "pid": session.worker_pid,
                          "parent": session.base, "jobs_since_base": list(session.jobs_since_base)})
        return {"nodes": nodes}

    # ---------------------------------------------------------------- diff

    def _state(self, ref: str, session: str) -> tuple[str, Session | Snapshot]:
        """``@`` / ``SESSION:@`` -> a live worker; ``[SESSION:]NAME`` -> a snapshot."""
        owner, _, name = ref.rpartition(":")
        if name == "@":
            return f"{owner or session}:@", self._session(owner or session)
        snap = self._snapshot(ref, session)
        return snap.ref, snap

    async def _ask_state(self, state: Session | Snapshot, op: str, **params) -> dict:
        """Ask a live worker (kernel message) or a snapshot (its socket) for ``vars`` or ``fingerprint``."""
        if isinstance(state, Session):
            return await self._ask_idle(state, f"talo_{op}_request", params, timeout=None)
        if private_dirty(state.pid) is None:
            raise RuntimeError(f"snapshot {state.ref} (pid {state.pid}) is gone")
        reader, writer = await asyncio.open_unix_connection(state.socket, limit=wire.MAX_LINE)
        try:
            writer.write(json.dumps({"op": op, **params}).encode() + b"\n")
            await writer.drain()
            reply = json.loads(await reader.readline())
        finally:
            writer.close()
        if not reply.get("ok"):
            raise RuntimeError(f"snapshot {state.ref}: {reply.get('error')}")
        return reply

    async def op_diff(self, req):
        session = req.get("session") or "default"
        (ref_a, a), (ref_b, b) = self._state(req["a"], session), self._state(req.get("b") or "@", session)
        limit = req.get("max_bytes", DIFF_MAX_BYTES)
        va, vb = await asyncio.gather(self._ask_state(a, "vars"), self._ask_state(b, "vars"))
        va = {v["name"]: v for v in va["variables"]}
        vb = {v["name"]: v for v in vb["variables"]}
        changed, same, not_compared = [], [], []
        for name in sorted(va.keys() & vb.keys()):
            x, y = va[name], vb[name]
            fields = [k for k in sorted(x.keys() | y.keys()) if k not in ("name", "nbytes", "nbytes_partial")
                      and x.get(k) != y.get(k)]
            if fields:
                changed.append({"name": name, "a": x, "b": y, "fields": fields})
            elif limit is not None and max(x.get("nbytes") or 0, y.get("nbytes") or 0) > limit:
                not_compared.append({"name": name, "reason": "size", "nbytes": max(x.get("nbytes") or 0,
                                                                                  y.get("nbytes") or 0)})
            else:
                same.append(name)
        if same:
            fa, fb = await asyncio.gather(self._ask_state(a, "fingerprint", names=same),
                                          self._ask_state(b, "fingerprint", names=same))
            fa, fb, unchanged = fa["fingerprints"], fb["fingerprints"], 0
            for name in same:
                x, y = fa.get(name, {}), fb.get(name, {})
                if "hash" not in x or "hash" not in y:
                    not_compared.append({"name": name, "reason": x.get("error") or y.get("error") or "missing"})
                elif x["hash"] != y["hash"]:
                    changed.append({"name": name, "a": va[name], "b": vb[name], "fields": ["content"]})
                else:
                    unchanged += 1
        else:
            unchanged = 0
        return {"a": ref_a, "b": ref_b, "added": [vb[n] for n in sorted(vb.keys() - va.keys())],
                "removed": [va[n] for n in sorted(va.keys() - vb.keys())],
                "changed": sorted(changed, key=lambda c: c["name"]), "unchanged": unchanged,
                "not_compared": not_compared, "max_bytes": limit}

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
