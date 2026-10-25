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
NOTE_PREVIEW = 500  # characters of each note shown by brief (talo notes shows them in full)
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


MEMORY_FIELDS = {"Rss:": "rss", "Pss:": "pss", "Private_Clean:": "private_clean", "Private_Dirty:": "private_dirty"}
MEMORY_LIMIT_INTERVAL = 0.5  # seconds between samples of a session with --memory-limit
MEMORY_LIMIT_GRACE = 3.0     # seconds an interrupted job gets to stop before its worker is killed


def memory(pid: int | None) -> dict | None:
    """rss / pss / private_clean / private_dirty bytes of a process (``/proc/PID/smaps_rollup``); None if unknown."""
    if not pid:
        return None
    out = {}
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                key = line.split(maxsplit=1)[0] if line.strip() else ""
                if key in MEMORY_FIELDS:
                    out[MEMORY_FIELDS[key]] = int(line.split()[1]) * 1024
    except OSError:
        return None
    return out or None


def machine_memory() -> dict:
    out = {}
    with open("/proc/meminfo") as f:
        for line in f:
            key, value = line.split(":", 1)
            if key in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"):
                out[key] = int(value.split()[0]) * 1024
    return {"total": out.get("MemTotal"), "available": out.get("MemAvailable"), "swap_total": out.get("SwapTotal"),
            "swap_free": out.get("SwapFree")}


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
    outputs: list = field(default_factory=list)  # files of rich outputs: {"mime", "path"}
    owner: str = "user"                        # who submitted it ($TALO_AGENT / --owner)
    key: str | None = None                     # idempotency key (run --key)
    expect_rev: int | None = None              # refuse to start unless the session is at this revision
    rev: int | None = None                     # the session's revision when the job started
    termination: str | None = None             # why Talo stopped the job ("memory_limit")
    progress: dict | None = None               # the last talo.progress() of a running job
    progress_at: float | None = None           # time.monotonic() of that update
    atomic: dict | None = None                 # run --atomic: {"outcome": committed | restored | restore_failed, ...}
    interrupt_requested: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def to_dict(self) -> dict:
        out = {k: getattr(self, k) for k in ("id", "session", "kind", "status", "submitted", "started", "finished",
                                            "execution_count", "result", "error", "metrics", "output", "outputs", "owner", "key",
                                            "rev", "atomic", "termination")}
        out["progress"] = self.progress
        if self.status == "running":
            out["last_progress_age_s"] = round(time.monotonic() - self.progress_at, 1) if self.progress_at else None
            try:
                out["last_output_age_s"] = round(max(0.0, time.time() - os.stat(self.output).st_mtime), 1)
            except OSError:
                out["last_output_age_s"] = None
            ages = [a for a in (out["last_progress_age_s"], out["last_output_age_s"]) if a is not None]
            out["last_activity_age_s"] = min(ages) if ages else None
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
    owner: str = "user"
    created: float = field(default_factory=time.time)
    hold: socket.socket | None = field(default=None, repr=False)

    @property
    def ref(self) -> str:
        return f"{self.session}:{self.id}"

    def to_dict(self) -> dict:
        return {"id": self.id, "name": self.name, "session": self.session, "ref": self.ref, "parent": self.parent,
                "pid": self.pid, "created": self.created, "message": self.message, "owner": self.owner,
                "execution_count": self.execution_count, "jobs": self.jobs, "threads": self.threads,
                "private_bytes": private_dirty(self.pid), "alive": private_dirty(self.pid) is not None}


class SessionDead(RuntimeError):
    pass


class StaleRevision(RuntimeError):
    pass


class Session:
    def __init__(self, name: str, python: str, cwd: str, env: dict | None = None, memory_budget: int | None = None,
                 memory_limit: int | None = None):
        self.name, self.python, self.cwd = name, python, cwd
        self.env = env or {}
        self.memory_budget = memory_budget
        self.memory_limit = memory_limit   # soft limit on the PSS of the worker plus its snapshots (--memory-limit)
        self.over_limit: str | None = None  # why the session is over its limit without anything to kill
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
        self.rev = 0                        # increases whenever the state may change (execution, checkout)
        self.owners: dict[str, str] = {}    # job id -> owner, for refusals that name whose work would be lost
        self.checkpoint: Snapshot | None = None  # the internal checkpoint of a running --atomic job
        self.vars_seen: dict | None = None  # the last variable summary: {"rev", "time", "variables"} (for brief)
        self.tasks: list[asyncio.Task] = []

    # ---------------------------------------------------------------- workers

    async def start(self) -> None:
        """Start a fresh worker process."""
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        env = {"MPLBACKEND": "Agg", **os.environ, **self.env, "PYTHONUNBUFFERED": "1"}  # figures become files
        with open(self.dir / "worker.log", "ab") as errlog:
            proc = await asyncio.create_subprocess_exec(
                self.python, str(WORKER), "--fd", str(child.fileno()), "--session", self.name,
                cwd=self.cwd, env=env, pass_fds=(child.fileno(),), stdin=asyncio.subprocess.DEVNULL,
                stdout=errlog, stderr=errlog, start_new_session=True)
        child.close()
        reader, writer = await asyncio.open_unix_connection(sock=parent, limit=wire.MAX_LINE)
        await self._attach(reader, writer, proc=proc)
        self._start_tasks()

    async def start_from(self, snap: Snapshot) -> None:
        """Start this session with a worker forked from a snapshot (possibly another session's)."""
        await self._spawn_from(snap)
        self._start_tasks()

    def _start_tasks(self) -> None:
        self.tasks.append(asyncio.create_task(self._execute_loop()))
        if self.memory_limit:
            self.tasks.append(asyncio.create_task(self._watch_memory()))

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
                if job.expect_rev is not None and job.expect_rev != self.rev:
                    since = [j for j in self.jobs_since_base]
                    raise StaleRevision(f"session {self.name} is at revision {self.rev}, not {job.expect_rev}; "
                                        f"base {self.base or 'session start'}, jobs since: {', '.join(since) or 'none'} "
                                        f"(`talo brief` shows the current state)")
                if job.kind in ("execute", "checkout"):
                    self.rev += 1
                job.rev = self.rev
                self.owners[job.id] = job.owner
                if job.kind == "execute" and job.params.get("atomic"):
                    self._finish(job, await self._execute_atomic(job))
                elif job.kind == "execute":
                    self._finish(job, await self._execute(job))
                elif job.kind == "snap":
                    self._finish(job, "ok", result=(await self._snap(**job.params, owner=job.owner)).to_dict())
                elif job.kind == "checkout":
                    self._finish(job, "ok", result=await self._checkout(**job.params, owner=job.owner))
            except Exception as error:  # noqa: BLE001 - a failed control job is reported, the session goes on
                if not job.done.is_set():
                    self._finish(job, "error", error={"ename": type(error).__name__, "evalue": str(error),
                                                      "traceback": []})
            finally:
                self.current = None
                if self.state != "dead":
                    self.state = "idle"

    async def _execute(self, job: Job) -> str:
        """Run an execute job on the worker; returns its status (the caller finishes the job)."""
        if self.state == "dead":
            job.error = {"ename": "SessionDead", "evalue": self.death, "traceback": []}
            return "lost"
        msg = wire.message("execute_request", {"code": job.code, "silent": False, "store_history": True,
                                               "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
                           session=self.name, metadata={"talo_output": job.output, "talo_job": job.id})
        q = self._send(msg)
        try:
            while True:
                got = await q.get()
                if got is None:  # the worker died mid-job
                    job.error = {"ename": "WorkerDied", "evalue": self.death, "traceback": []}
                    return "lost"
                kind = wire.msg_type(got)
                if kind == "execute_input":
                    job.execution_count = got["content"].get("execution_count")
                elif kind == "execute_result":
                    job.result = {"data": got["content"]["data"], "metadata": got["content"].get("metadata", {})}
                    job.outputs += [{"mime": m, "path": p} for m, p in job.result["metadata"].get("talo_files", {}).items()]
                elif kind == "display_data":
                    job.outputs += [{"mime": m, "path": p}
                                    for m, p in got["content"].get("metadata", {}).get("talo_files", {}).items()]
                elif kind == "error":
                    job.error = dict(got["content"])
                elif kind == "talo_progress":
                    job.progress, job.progress_at = dict(got["content"]), time.monotonic()
                elif kind == "execute_reply":
                    status = got["content"]["status"]
                    job.metrics = dict(got.get("metadata", {}))
                    if status == "error" and job.interrupt_requested and (job.error or {}).get("ename") == "KeyboardInterrupt":
                        status = "interrupted"
                    self.jobs_since_base.append(job.id)
                    return status
        finally:
            self.waiters.pop(msg["header"]["msg_id"], None)

    async def _execute_atomic(self, job: Job) -> str:
        """run --atomic: checkpoint, execute, then commit or restore (see "run --atomic" in docs/design.md)."""
        if self.state == "dead":
            return await self._execute(job)
        try:
            checkpoint = await self._fork(f"c{job.id}", strict=False)
        except Exception as error:  # noqa: BLE001 - no code ran
            job.error = {"ename": "CheckpointFailed", "evalue": str(error), "traceback": []}
            job.atomic = {"outcome": "not_started", "error": str(error)}
            return "error"
        base, before = self.base, list(self.jobs_since_base)
        self.checkpoint = checkpoint
        try:
            status = await self._execute(job)
            if status == "ok":
                job.atomic = {"outcome": "committed"}
                return status
            try:
                await self._replace_from(checkpoint)
                self.base, self.jobs_since_base = base, before  # the failed job leaves the lineage
                job.atomic = {"outcome": "restored"}
            except Exception as error:  # noqa: BLE001
                job.atomic = {"outcome": "restore_failed", "error": f"{type(error).__name__}: {error}"}
            return status
        finally:
            self.checkpoint = None
            self._release(checkpoint)

    async def _replace_from(self, snap: "Snapshot") -> None:
        """Replace the worker with a fork of ``snap``; the old worker is shut down only once the new one answers."""
        old_writer, old_proc, old_alive = self.writer, self.proc, self.state != "dead"
        await self._spawn_from(snap)
        if old_alive and old_writer is not None:
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

    def held(self) -> list:
        return [*self.snapshots.values(), *([self.checkpoint] if self.checkpoint else [])]

    def memory_in_use(self) -> int:
        """PSS of the worker plus its snapshots: shared pages are counted once across the family."""
        pids = [self.worker_pid if self.state != "dead" else None] + [s.pid for s in self.held()]
        return sum((memory(pid) or {}).get("pss", 0) for pid in pids if pid)

    async def _watch_memory(self) -> None:
        """Enforce --memory-limit (see "Resources on shared machines" in docs/design.md)."""
        while True:
            await asyncio.sleep(MEMORY_LIMIT_INTERVAL)
            used = self.memory_in_use()
            if used <= self.memory_limit:
                self.over_limit = None
                continue
            job, generation, pid = self.current, self.generation, self.worker_pid
            snaps = sum((memory(s.pid) or {}).get("pss", 0) for s in self.held())
            if job is None or job.kind != "execute" or job.status != "running" or snaps > self.memory_limit:
                self.over_limit = (f"{used >> 20} MB in use (snapshots {snaps >> 20} MB) over the limit of "
                                   f"{self.memory_limit >> 20} MB; drop snapshots or free variables")
                continue
            if job.termination != "memory_limit":  # first time over: interrupt, and give it a moment to stop
                log.warning("session %s: %d MB over the limit of %d MB; interrupting job %s", self.name, used >> 20,
                            self.memory_limit >> 20, job.id)
                job.termination = "memory_limit"
                self.interrupt(job)
                deadline = time.monotonic() + MEMORY_LIMIT_GRACE
                while time.monotonic() < deadline and not job.done.is_set() and self.memory_in_use() > self.memory_limit:
                    await asyncio.sleep(0.1)
                if job.done.is_set() or self.memory_in_use() <= self.memory_limit:
                    continue  # stopped, or freed enough; if it grows again it is killed
            if not job.done.is_set() and self.generation == generation:
                log.warning("session %s: job %s still over the memory limit; killing the worker", self.name, job.id)
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await job.done.wait()

    async def _snap(self, name: str | None = None, message: str = "", strict: bool = False,
                    owner: str = "user") -> Snapshot:
        if self.state == "dead":
            raise SessionDead(f"session {self.name} is dead ({self.death}); check out a snapshot first")
        sid = f"s{self.next_snapshot}"
        name = name or sid
        if self.find(name, missing_ok=True):
            raise ValueError(f"session {self.name} already has a snapshot named {name!r}")
        snap = await self._fork(sid, strict, name=name, message=message, owner=owner)
        self.next_snapshot += 1
        self.snapshots[sid] = snap
        self.base, self.jobs_since_base = snap.ref, []
        return snap

    async def _fork(self, sid: str, strict: bool, *, name: str | None = None, message: str = "",
                    owner: str = "user") -> Snapshot:
        """Fork the worker into a held snapshot process (named snapshots and internal checkpoints alike)."""
        if self.memory_budget and self.memory_in_use() > self.memory_budget:
            biggest = sorted(self.snapshots.values(), key=lambda s: -(private_dirty(s.pid) or 0))[:3]
            hint = ", ".join(f"{s.name} ({(private_dirty(s.pid) or 0) >> 20} MB)" for s in biggest) or "none"
            raise MemoryError(f"session {self.name} uses {self.memory_in_use() >> 20} MB, over its budget of "
                              f"{self.memory_budget >> 20} MB; snapshots that could be dropped: {hint}")
        if self.memory_limit and self.memory_in_use() > self.memory_limit:
            raise MemoryError(f"session {self.name} uses {self.memory_in_use() >> 20} MB, over its memory limit of "
                              f"{self.memory_limit >> 20} MB; `talo resources` shows what to drop")
        path = str(socket_for(self.dir / "snapshots" / f"{sid}.sock"))
        reply = await self.request("talo_snapshot_request", {"id": sid, "socket": path, "strict": strict}, timeout=120)
        content = reply["content"]
        if content.get("status") != "ok":
            raise RuntimeError(f"{content.get('ename')}: {content.get('evalue')}")
        snap = Snapshot(sid, name or sid, self.name, path, content["pid"], self.base, list(self.jobs_since_base),
                        content.get("execution_count", 0), content.get("threads", []), message, owner)
        snap.hold = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        snap.hold.connect(path)
        snap.hold.sendall(b'{"op": "hold"}\n')
        return snap

    def _release(self, snap: Snapshot) -> None:
        """Let a snapshot process exit and remove its files."""
        if snap.hold is not None:
            snap.hold.close()  # the snapshot exits when its hold connection closes
        for path in (Path(snap.socket), Path(snap.socket).with_suffix(".children")):
            path.unlink(missing_ok=True)

    def find(self, name: str, missing_ok: bool = False) -> Snapshot | None:
        for snap in self.snapshots.values():
            if name in (snap.id, snap.name):
                return snap
        if missing_ok:
            return None
        raise KeyError(f"session {self.name} has no snapshot {name!r} (`talo snaps -s {self.name}` lists them)")

    async def _checkout(self, snap: Snapshot, discard: bool = False, save_as: str | None = None,
                        owner: str = "user") -> dict:
        if self.jobs_since_base and self.state != "dead" and not discard and not save_as:
            by = sorted({self.owners.get(j, "user") for j in self.jobs_since_base})
            raise RuntimeError(f"jobs {', '.join(self.jobs_since_base)} (by {', '.join(by)}) ran since this worker's base "
                               f"({self.base or 'session start'}); use --discard to drop that state or "
                               f"--snap NAME to save it first")
        if save_as and self.state != "dead":
            await self._snap(save_as, message=f"saved before checking out {snap.name}", owner=owner)
        if private_dirty(snap.pid) is None:
            raise RuntimeError(f"snapshot {snap.name} (pid {snap.pid}) is gone")
        await self._retire()
        await self._spawn_from(snap)
        return self.to_dict()

    def drop(self, snap: Snapshot) -> None:
        self._release(snap)
        self.snapshots.pop(snap.id, None)

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
                "jobs_since_base": list(self.jobs_since_base), "snapshots": len(self.snapshots), "rev": self.rev,
                "memory_budget": self.memory_budget, "memory_limit": self.memory_limit, "over_limit": self.over_limit}


class Supervisor:
    def __init__(self):
        self.sessions: dict[str, Session] = {}
        self.jobs: dict[str, Job] = {}
        self.keys: dict[tuple[str, str], tuple] = {}  # (session, idempotency key) -> (job id, request fingerprint)
        self.epoch = wire.new_id()[:12]               # changes when the supervisor restarts (keys do not survive it)
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
        return {**job.to_dict(), "epoch": self.epoch}

    async def _submit(self, session: Session, kind: str, code: str = "", *, owner: str | None = None,
                      key: str | None = None, fingerprint: dict | None = None, expect_rev: int | None = None,
                      **params) -> Job:
        job = Job(self._new_job_id(), session.name, code, "", kind=kind, params=params, owner=owner or "user",
                  key=key, expect_rev=expect_rev)
        if key is not None:
            self.keys[(session.name, key)] = (job.id, fingerprint)
        job.output = str(session.dir / "jobs" / f"{job.id}.out")
        Path(job.output).touch()
        self.jobs[job.id] = job
        await session.queue.put(job)
        return job

    # ---------------------------------------------------------------- requests

    async def op_ping(self, req):
        return {"pid": os.getpid(), "home": str(home()), "sessions": len(self.sessions), "epoch": self.epoch}

    async def op_new(self, req):
        name = req["name"]
        if name in self.sessions and self.sessions[name].state != "dead":
            raise ValueError(f"session {name!r} already exists")
        source = self._snapshot(req["from"], None) if req.get("from") else None
        if source:  # a fork keeps the snapshot's interpreter and working directory
            python, cwd = self.sessions[source.session].python, self.sessions[source.session].cwd
        else:
            python, cwd = req.get("python") or sys.executable, req.get("cwd") or os.getcwd()
        session = Session(name, python, cwd, req.get("env"), req.get("memory_budget"), req.get("memory_limit"))
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

    def _deduplicated(self, session: Session, req: dict, fingerprint: dict) -> Job | None:
        """The job already submitted under ``req["key"]``, or None; an error if the key was used for another request."""
        key = req.get("key")
        if key is None or (session.name, key) not in self.keys:
            return None
        job_id, known = self.keys[(session.name, key)]
        if known != fingerprint:
            raise ValueError(f"key {key!r} was used for job {job_id} with a different request; use a new key")
        return self.jobs[job_id]

    async def op_run(self, req):
        session = self._session(req["session"])
        fingerprint = {"op": "run", "code": req["code"], "expect_rev": req.get("expect_rev"),
                       "atomic": bool(req.get("atomic"))}
        if job := self._deduplicated(session, req, fingerprint):
            return {**await self._wait(job, req.get("timeout", 30)), "deduplicated": True}
        if session.state == "dead":
            raise SessionDead(f"session {session.name} is dead: {session.death} (`talo checkout SNAPSHOT` to roll "
                              f"back, or `talo kill {session.name}` and create it again)")
        job = await self._submit(session, "execute", req["code"], owner=req.get("owner"), key=req.get("key"),
                                 fingerprint=fingerprint, expect_rev=req.get("expect_rev"),
                                 **({"atomic": True} if req.get("atomic") else {}))
        return await self._wait(job, req.get("timeout", 30))

    async def op_job(self, req):
        return await self._wait(self._job(req["id"]), req.get("wait", 0))

    async def op_jobs(self, req):
        name, last = req.get("session"), req.get("last")
        jobs = [j for j in self.jobs.values() if not name or j.session == name]
        return {"jobs": [{**j.to_dict(), "code_preview": j.code[:200]} for j in (jobs[-int(last):] if last else jobs)],
                "total": len(jobs)}

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
            data = f.read(max(0, min(limit, size - offset)))  # read(n) allocates n bytes up front
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

    async def _vars(self, session: Session, pattern: str | None = None) -> dict:
        rev = session.rev
        reply = await self._ask_idle(session, "talo_vars_request", {"pattern": pattern})
        if not pattern:
            session.vars_seen = {"rev": rev, "time": time.time(), "variables": reply["variables"]}
        return {**reply, "rev": rev}

    async def op_vars(self, req):
        return await self._vars(self._session(req["session"]), req.get("pattern"))

    async def op_show(self, req):
        return await self._ask_idle(self._session(req["session"]), "talo_show_request",
                                    {"name": req["name"], "rows": req.get("rows", 10)})

    async def op_history(self, req):
        path = self._session(req["session"]).dir / "history.jsonl"
        lines = path.read_text().splitlines() if path.exists() else []
        return {"history": [json.loads(x) for x in lines[-int(req.get("last", 20)):]]}

    async def _control(self, session: Session, kind: str, timeout: float, req: dict, **params) -> dict:
        fingerprint = {"op": kind, "expect_rev": req.get("expect_rev"),
                       **{k: (v.ref if isinstance(v, Snapshot) else v) for k, v in params.items()}}
        if job := self._deduplicated(session, req, fingerprint):
            out = {**await self._wait(job, timeout), "deduplicated": True}
        else:
            job = await self._submit(session, kind, owner=req.get("owner"), key=req.get("key"), fingerprint=fingerprint,
                                     expect_rev=req.get("expect_rev"), **params)
            out = await self._wait(job, timeout)
        if out["status"] == "error":
            raise RuntimeError(out["error"]["evalue"])
        if out["status"] != "ok":
            return {"job": out}
        return out["result"]

    async def op_snap(self, req):
        session = self._session(req["session"])
        return await self._control(session, "snap", req.get("timeout", 300), req, name=req.get("name"),
                                   message=req.get("message", ""), strict=bool(req.get("strict")))

    async def op_snaps(self, req):
        session = self._session(req["session"])
        return {"snapshots": [s.to_dict() for s in session.snapshots.values()], "base": session.base,
                "jobs_since_base": list(session.jobs_since_base)}

    async def op_checkout(self, req):
        session = self._session(req["session"])
        snap = self._snapshot(req["name"], session.name)
        return await self._control(session, "checkout", req.get("timeout", 300), req, snap=snap,
                                   discard=bool(req.get("discard")), save_as=req.get("snap"))

    async def op_drop(self, req):
        session = self._session(req["session"])
        snap = session.find(req["name"])
        session.drop(snap)
        return {"dropped": snap.ref}

    async def op_complete(self, req):
        return await self._ask_idle(self._session(req["session"]), "complete_request",
                                    {"code": req["code"], "cursor_pos": req.get("cursor_pos", len(req["code"]))})

    # ---------------------------------------------------------------- export

    def _records(self) -> dict:
        """Every job record of every session's history, by job id."""
        records = {}
        for path in (home() / "sessions").glob("*/history.jsonl"):
            for line in path.read_text().splitlines():
                rec = json.loads(line)
                records[rec["id"]] = rec
        return records

    async def op_export(self, req):
        """The job records that produced a state, oldest first (see "export" in docs/design.md)."""
        session = self._session(req["session"])
        records = self._records()
        if req.get("all"):
            path = session.dir / "history.jsonl"
            return {"jobs": [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else [],
                    "notes": self._notes(session)}
        snaps = {r["result"]["ref"]: r for r in records.values() if r["kind"] == "snap" and r["status"] == "ok"}
        if req.get("snap"):
            base, tail = self._snapshot(req["snap"], session.name).ref, []
        else:
            base, tail = session.base, list(session.jobs_since_base)
        chain = []
        while base:
            snap = snaps.get(base)
            if snap is None:
                raise KeyError(f"no history record for snapshot {base}")
            chain = [snap["result"]["jobs"], snap] + chain
            base = snap["result"]["parent"]
        out = []
        for part in chain + [tail]:
            if isinstance(part, dict):
                out.append(part)
            else:
                out += [records[j] for j in part if j in records]
        return {"jobs": out, "notes": self._notes(session)}

    # ---------------------------------------------------------------- notes and brief

    async def op_note(self, req):
        session = self._session(req["session"])
        if req.get("job"):
            self._job(req["job"])  # must exist
        note = {"id": f"n{len(self._notes(session)) + 1}", "time": time.time(), "owner": req.get("owner") or "user", "rev": session.rev, "base": session.base,
                "job": req.get("job"), "text": req["text"]}
        with open(session.dir / "notes.jsonl", "a") as f:
            f.write(json.dumps(note, ensure_ascii=False) + "\n")
        return {"note": note}

    def _notes(self, session: Session) -> list:
        path = session.dir / "notes.jsonl"
        return [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else []

    async def op_notes(self, req):
        notes = self._notes(self._session(req["session"]))
        last = req.get("last")
        return {"notes": notes[-int(last):] if last else notes, "total": len(notes)}

    async def op_brief(self, req):
        """What an agent needs to continue the session's work, every list bounded (see "Resuming" in design.md)."""
        session = self._session(req["session"])
        n_notes, n_jobs, n_vars = int(req.get("notes", 10)), int(req.get("jobs", 8)), int(req.get("vars", 15))
        notes = self._notes(session)
        jobs = [j for j in self.jobs.values() if j.session == session.name]
        finished = [j for j in jobs if j.status not in ("queued", "running")]

        def job_line(j: Job) -> dict:
            d = {k: v for k, v in j.to_dict().items() if k in ("id", "kind", "status", "elapsed_s", "owner", "rev")}
            d["code_preview"] = j.code[:120]
            if j.error:
                d["error"] = f"{j.error.get('ename')}: {str(j.error.get('evalue'))[:200]}"
            return d

        seen = session.vars_seen
        if session.state == "idle":
            await self._vars(session)
            seen = session.vars_seen
        if seen:
            rows = sorted(seen["variables"], key=lambda v: -(v.get("nbytes") or 0))
            variables = {"largest": [{k: v for k, v in r.items() if k != "value"} for r in rows[:n_vars]],
                         "total": len(rows), "observed_rev": seen["rev"], "observed_at": seen["time"],
                         "stale": seen["rev"] != session.rev}
        else:
            variables = {"unavailable": f"session is {session.state} and no summary was taken yet"}

        def cut(note: dict) -> dict:
            return {**note, "text": note["text"][:NOTE_PREVIEW]} if len(note["text"]) > NOTE_PREVIEW else note
        return {
            "session": session.to_dict(),
            "private_bytes": private_dirty(session.worker_pid),
            "notes": {"last": [cut(n) for n in notes[-n_notes:]], "total": len(notes), "more": "talo notes"},
            "snapshots": [{k: s.to_dict()[k] for k in ("name", "ref", "parent", "message", "owner", "private_bytes")}
                          for s in session.snapshots.values()],
            "variables": variables,
            "recent_jobs": {"last": [job_line(j) for j in finished[-n_jobs:]], "total": len(finished)},
            "active_jobs": [job_line(j) for j in jobs if j.status in ("queued", "running")],
        }

    async def op_resources(self, req):
        sessions = []
        for s in self.sessions.values():
            if req.get("session") and s.name != req["session"]:
                continue
            procs = [{"kind": "worker", "pid": s.worker_pid, "memory": memory(s.worker_pid) if s.state != "dead" else None}]
            procs += [{"kind": "snapshot", "name": x.name, "ref": x.ref, "pid": x.pid, "memory": memory(x.pid)}
                      for x in s.snapshots.values()]
            if s.checkpoint:
                procs.append({"kind": "checkpoint", "pid": s.checkpoint.pid, "memory": memory(s.checkpoint.pid)})
            known = [p["memory"]["pss"] for p in procs if p["memory"] and "pss" in p["memory"]]
            sessions.append({"name": s.name, "state": s.state, "processes": procs,
                             "pss_total": sum(known), "pss_unknown": len(known) < len(procs),
                             "memory_limit": s.memory_limit, "memory_budget": s.memory_budget,
                             "over_limit": s.over_limit})
        return {"sampled_at": time.time(), "machine": machine_memory(), "sessions": sessions,
                "pss_total": sum(x["pss_total"] for x in sessions)}

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
