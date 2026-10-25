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
import collections
import json
import logging
import os
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from talo import ephemeral, wire
from talo.paths import home, shared_socket_path, socket_for, socket_path

log = logging.getLogger("talo")
WORKER = Path(__file__).with_name("worker.py")
EVENTS_KEPT = 100_000  # events kept in the journal (older cursors are refused with CursorExpired)
CALL_PARALLEL = 8  # service calls of one session running at once (more wait)
BRIEF_MAX_BYTES = 16_000  # upper bound for the serialized brief
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


def blake(text: str) -> str:
    import hashlib
    return hashlib.blake2b(text.encode(errors="replace"), digest_size=12).hexdigest()


def case_seed(map_id: str, index: int) -> int:
    """A seed per map case: forks inherit identical random generator states, so cases should seed from this."""
    import hashlib
    return int.from_bytes(hashlib.blake2b(f"{map_id}:{index}".encode(), digest_size=4).digest(), "little")


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
    displays: list = field(default_factory=list)  # display() / figures in order: {offset, end, data, files}
    owner: str = "user"                        # who submitted it ($TALO_AGENT / --owner)
    session_id: str | None = None              # the incarnation of the session it was submitted to
    key: str | None = None                     # idempotency key (run --key)
    expect_rev: int | None = None              # refuse to start unless the session is at this revision
    rev: int | None = None                     # the session's revision when the job started
    termination: str | None = None             # why Talo stopped the job ("memory_limit")
    phase: str | None = None                   # preparing | executing | restoring | committing (signals only while executing)
    exec_pid: int | None = None                # the worker running the user code, the only process ever signalled
    handle: object = field(default=None, repr=False)  # a "fork" job's internal snapshot
    staged: list = field(default_factory=list)  # services staged with talo.expose, published if the job succeeds
    published: dict | None = None              # {"versions": [...]} or {"error": ...} after publication
    autosnap: dict | None = None               # the snapshot taken after it by talo autosnap
    progress: dict | None = None               # the last talo.progress() of a running job
    progress_at: float | None = None           # time.monotonic() of that update
    atomic: dict | None = None                 # run --atomic: {"outcome": committed | restored | restore_failed, ...}
    interrupt_requested: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def to_dict(self) -> dict:
        out = {k: getattr(self, k) for k in ("id", "session", "kind", "status", "submitted", "started", "finished",
                                            "execution_count", "result", "error", "metrics", "output", "outputs", "displays", "owner", "key",
                                            "rev", "atomic", "termination", "phase", "published", "autosnap")}
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
    pins: set = field(default_factory=set)  # holders that need this snapshot alive (maps, service versions, handoffs)
    auto: bool = False                      # taken by talo autosnap (dropped when older than the last N)
    hold: socket.socket | None = field(default=None, repr=False)

    @property
    def ref(self) -> str:
        return f"{self.session}:{self.id}"

    def to_dict(self) -> dict:
        return {"id": self.id, "name": self.name, "session": self.session, "ref": self.ref, "parent": self.parent,
                "pid": self.pid, "created": self.created, "message": self.message, "owner": self.owner,
                "pins": sorted(self.pins), "auto": self.auto,
                "execution_count": self.execution_count, "jobs": self.jobs, "threads": self.threads,
                "private_bytes": private_dirty(self.pid), "alive": private_dirty(self.pid) is not None}


@dataclass
class MapRun:
    """A fan-out of one snapshot into ephemeral workers (see "Fan-out" in docs/design.md)."""
    id: str
    session: str
    code: str
    cases: list
    snap: "Snapshot | None"   # None while the internal snapshot of the current state is being taken
    owns_snap: bool           # an internal snapshot of the current state, released when the map ends
    parallel: int
    memory_limit: int | None
    case_memory_limit: int | None
    json_limit: int
    owner: str
    dir: Path
    status: str = "running"   # (preparing ->) running -> finished (every case ran) / interrupted / error
    task: object = field(default=None, repr=False)
    blocked: str | None = None  # why no further case is starting right now
    rows: dict = field(default_factory=dict)      # index -> result row
    running: dict = field(default_factory=dict)   # index -> pid
    interrupt_requested: bool = False
    started: float = field(default_factory=time.time)
    finished: float | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def summary(self, rows: int | None = 50) -> dict:
        counts: dict = {}
        for r in self.rows.values():
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        ordered = [self.rows[i] for i in sorted(self.rows)]
        return {"id": self.id, "kind": "map", "session": self.session, "status": self.status, "blocked": self.blocked,
                "from": ("the current state" if self.snap is None else self.snap.ref if self.owns_snap
                         else f"{self.snap.name} ({self.snap.ref})"),
                "cases": len(self.cases), "finished_cases": len(self.rows), "running": sorted(self.running),
                "counts": counts, "owner": self.owner, "started": self.started, "finished": self.finished,
                "elapsed_s": round((self.finished or time.time()) - self.started, 3),
                "rows": ordered[:rows] if rows is not None else ordered, "rows_total": len(ordered),
                "results": str(self.dir / "results.jsonl")}


class SessionDead(RuntimeError):
    pass


class StaleRevision(RuntimeError):
    pass


class CursorExpired(RuntimeError):
    pass


class Forbidden(RuntimeError):
    pass


def visible_only(reply: dict, sessions: set) -> dict:
    """A listing narrowed to the sessions a shared user was granted."""
    def keep(item) -> bool:
        if not isinstance(item, dict):
            return True
        name = item.get("session", item.get("name"))
        if isinstance(name, str) and "." in name and "session" not in item:  # a service: "session.function"
            name = name.split(".")[0]
        return name in sessions
    out = dict(reply)
    for key in ("sessions", "nodes", "services", "handoffs", "maps", "records", "events", "jobs"):
        if isinstance(out.get(key), list):
            out[key] = [x for x in out[key] if keep(x)]
    if "sessions" in out and isinstance(out["sessions"], int):
        out["sessions"] = len(sessions)
    return out


# What each sharing level allows (see "Sharing a session" in docs/design.md). Operations that name a session are
# checked against the grant; operations on ids are checked against the session that owns the job, map or call.
# Listings: always narrowed to the granted sessions, whatever the request names.
SHARE_LISTINGS = {"ping", "ls", "tree", "services", "handoffs", "maps", "ledger", "events", "jobs", "resources"}
# Read: metadata the supervisor already has. No live inspection: completion, show, diff and fresh variable
# summaries call methods of the owner's objects (properties, __repr__, keys, pickling), so they need `run`.
SHARE_READ = {"brief", "vars", "snaps", "history", "notes", "job", "output", "resume"}
SHARE_CALL = SHARE_READ | {"call"}
SHARE_RUN = SHARE_CALL | {"run", "interrupt", "snap", "checkout", "drop", "reload", "map", "note", "record", "handoff",
                          "export", "expose", "unexpose", "show", "diff", "complete"}
SHARE_LEVELS = ("read", "call", "run")
SHARE_STATE_PARAMS = {"diff": ("a", "b"), "map": ("from",), "checkout": ("name",), "handoff": ("snap",),
                      "export": ("snap",)}


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
        self.id = wire.new_id()[:12]        # unique per incarnation: a recreated session with the same name is another
        self.rev = 0                        # increases whenever the state may change (execution, checkout, reload)
        self.owners: dict[str, str] = {}    # job id -> owner, for refusals that name whose work would be lost
        self.emit = lambda event_type, /, **fields: None
        self.on_record = lambda job, content: None   # set by the supervisor: talo.record() from a job
        self.on_publish = lambda job, snap: None      # set by the supervisor: register new service versions
        self.internal_snaps: list = []               # service versions, map sources (counted in memory)
        self.ephemeral: dict[str, int] = {}          # running map cases and calls: "m3:7" / "c12" -> pid
        self.call_slots = asyncio.Semaphore(CALL_PARALLEL)
        self.on_handoff = None                       # set by the supervisor: run a queued handoff
        self.on_finish = lambda job: None            # set by the supervisor: fill in the outcome of the job's records  # set by the supervisor: append to the event journal
        self.autosnap: dict | None = None  # {"keep": N} while talo autosnap is on
        self.checkpoint: Snapshot | None = None  # the internal checkpoint of a running --atomic job
        self.vars_seen: dict | None = None  # the last variable summary: {"rev", "time", "variables"} (for brief)
        self.tasks: list[asyncio.Task] = []

    # ---------------------------------------------------------------- workers

    async def start(self) -> None:
        """Start a fresh worker process."""
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        env = {"MPLBACKEND": "Agg", **os.environ, **self.env, "PYTHONUNBUFFERED": "1",  # figures become files
               "TALO_SOCKET": str(socket_path())}                                  # talo.call reaches us here
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
        """Make this connection the session's worker channel, replacing the previous worker only once the new one
        has answered: if the handshake fails, the session is left exactly as it was."""
        try:
            msg = wire.message("kernel_info_request", {}, session=self.name)
            writer.write(wire.encode(msg))
            await writer.drain()

            async def handshake() -> dict:
                while True:
                    line = await reader.readline()
                    if not line:
                        raise RuntimeError("the new worker closed its connection before answering")
                    got = wire.decode(line)
                    if got.get("parent_header", {}).get("msg_id") == msg["header"]["msg_id"] \
                            and wire.msg_type(got) == "kernel_info_reply":
                        return got
            info = await asyncio.wait_for(handshake(), 60)
        except BaseException:
            writer.close()
            if proc is not None and proc.returncode is None:
                proc.kill()
            raise
        self.generation += 1
        self.proc, self.children_file, self.writer = proc, children_file, writer
        self.worker_pid = info["content"].get("talo_pid")
        self.death = None
        self.state = "busy" if self.current is not None else "idle"  # an atomic restore is still part of its job
        self.tasks.append(asyncio.create_task(self._read(reader, self.generation)))

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
            await self._died(generation)

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

    async def _died(self, generation: int) -> None:
        death = f"the worker process {describe_exit(await self._exit_code())}"
        if generation != self.generation:  # replaced while we waited for the exit status: not this worker's death
            return
        self.death = death
        self.state = "dead"
        log.warning("session %s: %s", self.name, self.death)
        self.emit("session.died", session=self.name, death=death)
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
                if job.kind in ("execute", "checkout", "reload"):
                    self.rev += 1
                job.rev = self.rev
                self.owners[job.id] = job.owner
                if job.kind == "execute":
                    status = await (self._execute_atomic(job) if job.params.get("atomic") else self._execute(job))
                    if status == "ok" and job.staged:
                        await self._publish(job)
                    if status == "ok" and self.autosnap and job.execution_count and not job.params.get("silent"):
                        await self._autosnap(job)
                    self._finish(job, status)
                elif job.kind == "snap":
                    self._finish(job, "ok", result=(await self._snap(**job.params, owner=job.owner)).to_dict())
                elif job.kind == "checkout":
                    self._finish(job, "ok", result=await self._checkout(**job.params, owner=job.owner))
                elif job.kind == "handoff":
                    self._finish(job, "ok", result=await self.on_handoff(job))
                elif job.kind == "fork":  # an internal snapshot of the current state, for map
                    job.handle = await self._fork(f"f{job.id}", strict=False)
                    self._finish(job, "ok", result={"ref": job.handle.ref, "pid": job.handle.pid})
                elif job.kind == "reload":
                    if self.state == "dead":
                        raise SessionDead(f"session {self.name} is dead ({self.death})")
                    reply = (await self.request("talo_reload_request", job.params, timeout=600))["content"]
                    if any(m["status"] in ("reloaded", "failed") for m in reply.get("modules", [])):
                        self.jobs_since_base.append(job.id)  # module state changed (maybe partly): part of the lineage
                    failed = [m for m in reply.get("modules", []) if m["status"] == "failed"]
                    if failed:
                        job.error = {"ename": "ReloadFailed", "traceback": [],
                                     "evalue": "; ".join(f"{m['module']}: {m['error']}" for m in failed)}
                    self._finish(job, "error" if failed else "ok", result=reply)
            except Exception as error:  # noqa: BLE001 - a failed control job is reported, the session goes on
                if not job.done.is_set():
                    try:
                        self._finish(job, "error", error={"ename": type(error).__name__, "evalue": str(error),
                                                          "traceback": []})
                    except Exception:  # noqa: BLE001 - the queue must survive a failure to record a job
                        log.exception("session %s: could not finish job %s", self.name, job.id)
                        job.status = "error"
                        job.done.set()
            finally:
                self.current = None
                if self.state != "dead":
                    self.state = "idle"

    async def _execute(self, job: Job) -> str:
        """Run an execute job on the worker; returns its status (the caller finishes the job)."""
        if self.state == "dead":
            job.error = {"ename": "SessionDead", "evalue": self.death, "traceback": []}
            return "lost"
        silent = bool(job.params.get("silent"))
        msg = wire.message("execute_request", {"code": job.code, "silent": silent, "store_history": not silent,
                                               "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
                           session=self.name, metadata={"talo_output": job.output, "talo_job": job.id})
        job.phase, job.exec_pid = "executing", self.worker_pid
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
                    meta = got["content"].get("metadata", {})
                    job.outputs += [{"mime": m, "path": p} for m, p in meta.get("talo_files", {}).items()]
                    job.displays.append({"offset": meta.get("talo_offset"), "end": meta.get("talo_end"),
                                         "data": {k: v for k, v in got["content"]["data"].items()},
                                         "files": meta.get("talo_files", {})})
                elif kind == "error":
                    job.error = dict(got["content"])
                elif kind == "talo_progress":
                    job.progress, job.progress_at = dict(got["content"]), time.monotonic()
                elif kind == "talo_record":
                    self.on_record(job, got["content"])
                elif kind == "talo_expose":
                    if got["content"]["name"] not in job.staged:
                        job.staged.append(got["content"]["name"])
                elif kind == "execute_reply":
                    status = got["content"]["status"]
                    job.metrics = dict(got.get("metadata", {}))
                    if status == "error" and job.interrupt_requested and (job.error or {}).get("ename") == "KeyboardInterrupt":
                        status = "interrupted"
                    if job.termination == "memory_limit" and status == "ok":  # it caught the interrupt: still stopped
                        status = "interrupted"
                        job.error = {"ename": "MemoryLimit", "traceback": [],
                                     "evalue": "the session went over its memory limit; the job caught the interrupt"}
                    self.jobs_since_base.append(job.id)
                    return status
        finally:
            job.exec_pid = None
            self.waiters.pop(msg["header"]["msg_id"], None)

    async def _execute_atomic(self, job: Job) -> str:
        """run --atomic: checkpoint, execute, then commit or restore (see "run --atomic" in docs/design.md)."""
        if self.state == "dead":
            return await self._execute(job)
        job.phase = "preparing"
        try:
            checkpoint = await self._fork(f"c{job.id}", strict=False)
        except Exception as error:  # noqa: BLE001 - no code ran
            job.error = {"ename": "CheckpointFailed", "evalue": str(error), "traceback": []}
            job.atomic = {"outcome": "not_started", "error": str(error)}
            return "error"
        keep = False
        try:
            if job.interrupt_requested:  # interrupted while the checkpoint was taken: never start the code
                job.error = {"ename": "KeyboardInterrupt", "evalue": "interrupted before it started", "traceback": []}
                job.atomic = {"outcome": "not_started", "error": "interrupted"}
                return "interrupted"
            base, before = self.base, list(self.jobs_since_base)
            self.checkpoint = checkpoint
            status = await self._execute(job)
            if status == "ok":
                job.phase = "committing"
                job.atomic = {"outcome": "committed"}
                return status
            job.phase = "restoring"
            try:
                await self._replace_from(checkpoint)
                self.base, self.jobs_since_base = base, before  # the failed job leaves the lineage
                job.atomic = {"outcome": "restored"}
            except Exception as error:  # noqa: BLE001 - keep the checkpoint so the state can still be recovered
                keep = True
                checkpoint.name = f"restore-{job.id}"
                checkpoint.message = f"state before job {job.id}; restoring it failed"
                self.snapshots[checkpoint.id] = checkpoint
                job.atomic = {"outcome": "restore_failed", "error": f"{type(error).__name__}: {error}",
                              "checkpoint": checkpoint.ref}
            return status
        finally:
            self.checkpoint = None
            if not keep:
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
        record = {k: v for k, v in job.to_dict().items() if k != "result"}
        record["code"] = job.code
        if job.kind == "execute":
            record["result_text"] = (job.result or {}).get("data", {}).get("text/plain")
        else:
            record["result"] = job.result
        with open(self.dir / "history.jsonl", "a") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        job.done.set()  # the record is written first: whoever wakes up can read it
        self.on_finish(job)
        self.emit("job.finished", session=self.name, job=job.id, kind=job.kind, status=status, owner=job.owner,
                  rev=job.rev)

    # ---------------------------------------------------------------- snapshots

    def held(self) -> list:
        return [*self.snapshots.values(), *self.internal_snaps, *([self.checkpoint] if self.checkpoint else [])]

    async def _autosnap(self, job: Job) -> None:
        """talo autosnap: a snapshot after each successful execution, named @N, the last `keep` kept."""
        name = f"@{job.execution_count}"
        old = self.find(name, missing_ok=True)
        if old is not None:  # the same count on an abandoned branch (after a checkout)
            if old.pins or old.ref == self.base:
                name = f"@{job.execution_count}.{job.id}"
            else:
                self.drop(old)
        try:
            snap = await self._snap(name, message=f"after execution {job.execution_count} (job {job.id})",
                                    owner=job.owner)
        except Exception as error:  # noqa: BLE001 - the code ran; only the snapshot failed
            job.autosnap = {"error": f"{type(error).__name__}: {error}"}
            return
        snap.auto = True
        job.autosnap = {"snapshot": snap.ref, "name": snap.name}
        autos = [s for s in self.snapshots.values() if s.auto]
        for s in autos[:-self.autosnap["keep"]]:
            if not s.pins and s.ref != self.base:
                self.drop(s)

    async def _publish(self, job: Job) -> None:
        """Publish the services a successful job staged, as new versions on one snapshot of the state it left."""
        try:
            snap = await self._fork(f"v{job.id}", strict=False, name=f"services of job {job.id}", owner=job.owner)
        except Exception as error:  # noqa: BLE001 - the code succeeded; only the publication failed
            job.published = {"error": f"{type(error).__name__}: {error}"}
            return
        self.internal_snaps.append(snap)
        job.published = {"versions": self.on_publish(job, snap)}

    def memory_usage(self) -> tuple[int, list]:
        """(PSS of the worker plus its snapshots, pids whose memory could not be read). Shared pages are counted once
        across the family. A dead worker is not counted; a live process that cannot be measured is listed, not
        treated as zero."""
        pids = [self.worker_pid if self.state != "dead" else None] + [s.pid for s in self.held()] \
            + list(self.ephemeral.values())
        known, unknown = 0, []
        for pid in filter(None, pids):
            m = memory(pid)
            if m is None or "pss" not in m:
                if os.path.exists(f"/proc/{pid}"):
                    unknown.append(pid)
                continue
            known += m["pss"]
        return known, unknown

    def memory_in_use(self) -> int:
        return self.memory_usage()[0]

    async def _watch_memory(self) -> None:
        """Enforce --memory-limit (see "Resources on shared machines" in docs/design.md)."""
        while True:
            await asyncio.sleep(MEMORY_LIMIT_INTERVAL)
            used, unknown = self.memory_usage()
            if used <= self.memory_limit:
                self.over_limit = (f"enforcement degraded: memory of pids {unknown} cannot be read; {used >> 20} MB "
                                   "known") if unknown else None
                continue
            job, generation, pid = self.current, self.generation, self.worker_pid
            snaps = sum((memory(s.pid) or {}).get("pss", 0) for s in self.held())
            if job is None or job.kind != "execute" or job.phase != "executing" or snaps > self.memory_limit:
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
            if not job.done.is_set() and self.generation == generation and job.phase == "executing" \
                    and job.exec_pid == pid:
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
        self.emit("snapshot.created", session=self.name, snapshot=snap.ref, name=snap.name, owner=owner)
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
        self.emit("snapshot.dropped", session=self.name, snapshot=snap.ref, name=snap.name)

    # ---------------------------------------------------------------- lifecycle

    def interrupt(self, job: Job) -> str:
        if job.status == "queued":
            job.status, job.finished = "cancelled", time.time()
            job.done.set()
            return "cancelled"
        if job.status == "running" and job.kind == "execute":
            job.interrupt_requested = True
            if job.phase != "executing" or not job.exec_pid:  # a checkpoint or restore is in progress
                return "interrupt deferred" if job.phase == "preparing" else "finishing"
            try:
                os.kill(job.exec_pid, signal.SIGINT)
            except ProcessLookupError:
                pass
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
                "id": self.id,
                "memory_budget": self.memory_budget, "memory_limit": self.memory_limit, "over_limit": self.over_limit}


class Supervisor:
    def __init__(self):
        self.sessions: dict[str, Session] = {}
        self.jobs: dict[str, Job] = {}
        self.keys: dict[tuple[str, str], tuple] = {}  # (session id, idempotency key) -> (job id, request fingerprint)
        self.events: collections.deque = collections.deque(maxlen=EVENTS_KEPT)
        self.event_seq = 0
        self.event_added = asyncio.Event()
        self.maps: dict[str, MapRun] = {}
        self.next_map = 1
        self.ledger: dict[str, dict] = self._load_ledger()
        self.services: dict[str, dict] = {}   # "session.name" -> {"versions": {n: version}, "latest": n}
        self.version_counter: dict[str, int] = {}
        self.calls: dict[str, dict] = {}
        self.next_call = 1
        self.grants: dict[int, dict] = {}   # uid -> {"user", "sessions": {name: level}}
        self.shared_server = None
        self.handoffs: dict[str, dict] = {}
        self.next_handoff = 1   # record id -> record (shared by all sessions)
        self.last_rev: dict[str, int] = {}             # session name -> its revision when it was closed
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
                  key=key, expect_rev=expect_rev, session_id=session.id)
        if key is not None:
            self.keys[(session.id, key)] = (job.id, fingerprint)
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
        if name in self.sessions:  # replacing a dead session: its revisions are not reused
            self.last_rev[name] = self.sessions[name].rev
        source = self._snapshot(req["from"], None) if req.get("from") else None
        if source:  # a fork keeps the snapshot's interpreter and working directory
            python, cwd = self.sessions[source.session].python, self.sessions[source.session].cwd
        else:
            python, cwd = req.get("python") or sys.executable, req.get("cwd") or os.getcwd()
        session = Session(name, python, cwd, req.get("env"), req.get("memory_budget"), req.get("memory_limit"))
        session.rev = self.last_rev.get(name, 0)  # revisions never repeat for a name, so a stale --expect-rev fails
        session.emit = self.emit
        session.on_record = lambda job, content: self._add_record(content, session, job=job)
        session.on_finish = self._job_outcome
        session.on_handoff = lambda job: self._do_handoff(session, job)
        session.on_publish = lambda job, snap: self._register_versions(session, job, snap)
        self.sessions[name] = session
        try:
            await (session.start_from(source) if source else session.start())
            self.emit("session.created", session=name, id=session.id, owner=req.get("owner") or "user",
                      **({"from": source.ref} if source else {}))
        except Exception:
            self.sessions.pop(name, None)
            await session.stop()
            raise
        return session.to_dict()

    async def op_ls(self, req):
        return {"sessions": [s.to_dict() for s in self.sessions.values()]}

    async def op_kill(self, req):
        session = self._session(req["name"])
        self._kill_maps(session.name)
        calls = [c for c in self.calls.values() if c["session"] == session.name and not c["done"].is_set()]
        for c in calls:
            c["cancel"] = True
            if c["pid"]:
                try:
                    os.kill(c["pid"], signal.SIGKILL)
                except ProcessLookupError:
                    pass
        waiting = [m.done.wait() for m in self.maps.values() if m.session == session.name and not m.done.is_set()]
        waiting += [c["done"].wait() for c in calls]
        if waiting:  # the children must be gone before their snapshots are released
            try:
                await asyncio.wait_for(asyncio.gather(*waiting), 15)
            except asyncio.TimeoutError:
                log.warning("session %s: some map cases or calls did not stop within 15s", session.name)
        for svc in [s for s in self.services.values() if s["session"] == session.name]:
            for n in list(svc["versions"]):
                self._drop_version(svc, n)
            del self.services[svc["name"]]
        await session.stop()
        self.last_rev[session.name] = session.rev
        del self.sessions[req["name"]]
        self.emit("session.killed", session=session.name, id=session.id)
        return {"killed": req["name"]}

    def _deduplicated(self, session: Session, req: dict, fingerprint: dict) -> Job | None:
        """The job already submitted under ``req["key"]``, or None; an error if the key was used for another request."""
        key = req.get("key")
        if key is None or (session.id, key) not in self.keys:
            return None
        job_id, known = self.keys[(session.id, key)]
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
                                 **({"atomic": True} if req.get("atomic") else {}),
                                 **({"silent": True} if req.get("silent") else {}))
        return await self._wait(job, req.get("timeout", 30))

    async def op_job(self, req):
        if str(req["id"]) in self.calls:
            return await self._wait_call(self.calls[str(req["id"])], req.get("wait", 0))
        if str(req["id"]) in self.maps:
            return await self._wait_map(self.maps[str(req["id"])], req.get("wait", 0))
        return await self._wait(self._job(req["id"]), req.get("wait", 0))

    async def op_jobs(self, req):
        name, last = req.get("session"), req.get("last")
        jobs = [j for j in self.jobs.values() if (not name or j.session == name)
                and ("_visible" not in req or j.session in req["_visible"])]
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
        if str(req["id"]) in self.calls:
            call = self.calls[str(req["id"])]
            call["cancel"] = True  # set first: a call still being spawned is interrupted when it starts
            if call["pid"]:
                try:
                    os.kill(call["pid"], signal.SIGINT)
                except ProcessLookupError:
                    pass
            return {"id": call["id"], "result": "interrupting" if call["status"] == "running" else call["status"]}
        if str(req["id"]) in self.maps:
            m = self.maps[str(req["id"])]
            m.interrupt_requested = True
            for pid in m.running.values():
                try:
                    os.kill(pid, signal.SIGINT)
                except ProcessLookupError:
                    pass
            return {"id": m.id, "result": "interrupting" if m.status == "running" else m.status}
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
        session = self._session(req["session"])
        if req.get("_cached"):  # a shared reader: the last summary, never a live inspection
            seen = session.vars_seen
            if not seen:
                raise RuntimeError(f"no variable summary of session {session.name} has been taken yet")
            rows = [v for v in seen["variables"] if not req.get("pattern") or req["pattern"] in v["name"]]
            return {"status": "ok", "variables": rows, "rev": seen["rev"], "stale": seen["rev"] != session.rev,
                    "observed_at": seen["time"]}
        return await self._vars(session, req.get("pattern"))

    async def op_show(self, req):
        session = self._session(req["session"])
        rev = session.rev
        reply = await self._ask_idle(session, "talo_show_request", {"name": req["name"], "rows": req.get("rows", 10)},
                                     timeout=120)
        return {**reply, "rev": rev}

    async def op_history(self, req):
        path = self._session(req["session"]).dir / "history.jsonl"
        lines = path.read_text().splitlines() if path.exists() else []
        return {"history": [json.loads(x) for x in lines[-int(req.get("last", 20)):]]}

    async def _submit_control(self, session: Session, kind: str, timeout: float, req: dict, **params) -> dict:
        """As _control, but return the job itself (its result also when it failed)."""
        fingerprint = {"op": kind, "expect_rev": req.get("expect_rev"), **params}
        if job := self._deduplicated(session, req, fingerprint):
            return {"job": {**await self._wait(job, timeout), "deduplicated": True}}
        job = await self._submit(session, kind, owner=req.get("owner"), key=req.get("key"), fingerprint=fingerprint,
                                 expect_rev=req.get("expect_rev"), **params)
        return {"job": await self._wait(job, timeout)}

    async def _control(self, session: Session, kind: str, timeout: float, req: dict, fingerprint: dict | None = None,
                       **params) -> dict:
        fingerprint = fingerprint or {"op": kind, "expect_rev": req.get("expect_rev"), **params}
        if job := self._deduplicated(session, req, fingerprint):
            return self._control_reply({**await self._wait(job, timeout), "deduplicated": True})
        job = await self._submit(session, kind, owner=req.get("owner"), key=req.get("key"), fingerprint=fingerprint,
                                 expect_rev=req.get("expect_rev"), **params)
        return self._control_reply(await self._wait(job, timeout))

    def _control_reply(self, out: dict) -> dict:
        """A finished control job's result (with the job id, epoch and deduplicated flag), or the job while pending."""
        if out["status"] == "error":
            raise RuntimeError(out["error"]["evalue"])
        if out["status"] != "ok":
            return {"job": out}
        return {**out["result"], "job_id": out["id"], "epoch": out["epoch"], "deduplicated": out.get("deduplicated", False)}

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
        fingerprint = {"op": "checkout", "expect_rev": req.get("expect_rev"), "name": req["name"],
                       "discard": bool(req.get("discard")), "save_as": req.get("snap")}
        if job := self._deduplicated(session, req, fingerprint):  # before resolving: the target may be gone by now
            return self._control_reply({**await self._wait(job, req.get("timeout", 300)), "deduplicated": True})
        snap = self._snapshot(req["name"], session.name)
        return await self._control(session, "checkout", req.get("timeout", 300), req, fingerprint=fingerprint,
                                   snap=snap, discard=bool(req.get("discard")), save_as=req.get("snap"))

    async def op_reload(self, req):
        session = self._session(req["session"])
        params = {"modules": list(req.get("modules") or []), "changed": bool(req.get("changed")),
                  "root": req.get("root") or session.cwd}
        if req.get("dry_run"):  # only lists modules: not queued, does not change the revision
            return await self._ask_idle(session, "talo_reload_request", {**params, "dry_run": True})
        job = await self._submit_control(session, "reload", req.get("timeout", 300), req, **params)
        return job

    async def op_autosnap(self, req):
        session = self._session(req["session"])
        baseline = None
        if req.get("on") is not None:
            was_on = session.autosnap is not None
            session.autosnap = {"keep": max(1, int(req.get("keep") or 10))} if req["on"] else None
            if req["on"] and not was_on and (session.base is None or session.jobs_since_base):
                # a baseline, so that the first undo has a state to return to
                counts = [j.execution_count for j in self.jobs.values()
                          if j.session_id == session.id and j.kind == "execute" and j.execution_count]
                out = await self._control(session, "snap", req.get("timeout", 300), {}, name=f"@{counts[-1] if counts else 0}",
                                          message="autosnap baseline", strict=False)
                if "job" not in out:
                    session.find(out["name"]).auto = True
                    baseline = out["ref"]
        return {"session": session.name, "autosnap": session.autosnap, "baseline": baseline}

    async def op_undo(self, req):
        """Check out the state before the last execution: the base if jobs ran since it, else the base's parent."""
        session = self._session(req["session"])
        if session.jobs_since_base:
            target = session.base
        else:
            base = session.find(session.base.partition(":")[2], missing_ok=True) if session.base else None
            target = base.parent if base else None
        if not target:
            raise RuntimeError("nothing to undo: no snapshot before the last execution (`talo autosnap on` takes one "
                               "after every execution)")
        snap = self._snapshot(target, session.name)
        return await self._control(session, "checkout", req.get("timeout", 300), req, snap=snap, discard=True,
                                   save_as=None)

    async def op_drop(self, req):
        session = self._session(req["session"])
        snap = session.find(req["name"])
        if snap.pins:
            raise RuntimeError(f"snapshot {snap.name} is pinned by {', '.join(sorted(snap.pins))}; "
                               "it can be dropped once they end")
        session.drop(snap)
        return {"dropped": snap.ref}

    # ---------------------------------------------------------------- map

    async def op_map(self, req):
        session = self._session(req["session"])
        cases = req.get("cases")
        if cases is None:
            cases = list(range(int(req.get("range") or 0)))
        if not isinstance(cases, list) or not cases:
            raise ValueError("map needs a non-empty JSON list of cases (--cases) or --range N")
        parallel = int(req.get("parallel") or 4)
        if parallel < 1:
            raise ValueError("--parallel must be at least 1")
        fingerprint = {"code": req["code"], "cases": cases, "from": req.get("from"), "expect_rev": req.get("expect_rev"),
                       "parallel": parallel, "memory_limit": req.get("memory_limit"),
                       "case_memory_limit": req.get("case_memory_limit"), "max_result": req.get("max_result")}
        key = req.get("key")
        if key is not None and (session.id, "map:" + key) in self.keys:
            map_id, known = self.keys[(session.id, "map:" + key)]
            if known != fingerprint:
                raise ValueError(f"key {key!r} was used for map {map_id} with a different request; use a new key")
            out = await self._wait_map(self.maps[map_id], req.get("timeout", 30))
            return {**out, "map": {**out["map"], "deduplicated": True}}
        snap = self._snapshot(req["from"], session.name) if req.get("from") else None  # resolved once, now
        map_id = f"m{self.next_map}"
        self.next_map += 1
        m = MapRun(map_id, session.name, req["code"], cases, snap, snap is None, parallel,
                   req.get("memory_limit") or session.memory_limit, req.get("case_memory_limit"),
                   int(req.get("max_result") or 65536), req.get("owner") or "user", session.dir / "maps" / map_id,
                   status="running" if snap else "preparing")
        m.dir.mkdir(parents=True, exist_ok=True)
        if snap is not None:
            snap.pins.add(f"map {map_id}")
        self.maps[map_id] = m  # registered before the first await, so a retry finds it
        if key is not None:
            self.keys[(session.id, "map:" + key)] = (map_id, fingerprint)
        m.task = asyncio.create_task(self._prepare_and_run_map(session, m, req))
        return await self._wait_map(m, req.get("timeout", 30))

    async def _prepare_and_run_map(self, session: Session, m: MapRun, req: dict) -> None:
        if m.snap is None:  # the current state: an internal snapshot taken in the queue, after running jobs
            job = await self._submit(session, "fork", owner=m.owner, expect_rev=req.get("expect_rev"))
            await job.done.wait()
            if job.status != "ok" or m.interrupt_requested:
                if job.handle is not None:
                    session._release(job.handle)
                m.status = "interrupted" if m.interrupt_requested else "error"
                m.rows[-1] = {"index": -1, "status": "error",
                              "error": {"ename": (job.error or {}).get("ename", "Interrupted"),
                                        "evalue": f"could not snapshot the current state: {(job.error or {}).get('evalue')}"}}
                m.finished = time.time()
                m.done.set()
                self.emit("map.finished", session=m.session, map=m.id, status=m.status, counts={})
                return
            m.snap = job.handle
            m.snap.pins.add(f"map {m.id}")
            session.internal_snaps.append(m.snap)
            m.status = "running"
        await self._run_map(session, m)

    def _kill_maps(self, session: str | None = None) -> None:
        """Stop the cases of running maps (when their session or the supervisor goes away)."""
        for m in self.maps.values():
            if m.status in ("running", "preparing") and (session is None or m.session == session):
                m.interrupt_requested = True
                for pid in list(m.running.values()):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    async def op_maps(self, req):
        name = req.get("session")
        return {"maps": [m.summary(rows=0) for m in self.maps.values() if not name or m.session == name]}

    async def _wait_map(self, m: MapRun, timeout: float | None) -> dict:
        if timeout is None or timeout > 0:
            try:
                await asyncio.wait_for(m.done.wait(), timeout)
            except asyncio.TimeoutError:
                pass
        return {"map": m.summary(), "epoch": self.epoch}

    async def _run_map(self, session: Session, m: MapRun) -> None:
        sem = asyncio.Semaphore(m.parallel)
        results = open(m.dir / "results.jsonl", "a")

        def family_pss() -> int:
            return session.memory_usage()[0]  # worker, snapshots and every running case or call

        async def one(index: int, case) -> None:
            async with sem:
                while m.memory_limit and m.running and not m.interrupt_requested \
                        and (used := family_pss()) > m.memory_limit:  # admission; one case may always run
                    m.blocked = f"memory {used >> 20} MB over the limit of {m.memory_limit >> 20} MB"
                    await asyncio.sleep(0.2)
                m.blocked = None
                if m.interrupt_requested:
                    row = {"index": index, "case": case, "status": "cancelled"}
                else:
                    row = await self._run_case(m, index, case)
                m.rows[index] = row
                results.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                results.flush()
                self.emit("case.finished", session=m.session, map=m.id, index=index, status=row["status"])

        try:
            await asyncio.gather(*(one(i, c) for i, c in enumerate(m.cases)))
            m.status = "interrupted" if m.interrupt_requested else "finished"
        except Exception as error:  # noqa: BLE001
            log.exception("map %s failed", m.id)
            m.status = "error"
            m.rows.setdefault(-1, {"index": -1, "status": "error", "error": f"{type(error).__name__}: {error}"})
        finally:
            results.close()
            m.finished = time.time()
            m.snap.pins.discard(f"map {m.id}")
            if m.owns_snap:
                if m.snap in session.internal_snaps:
                    session.internal_snaps.remove(m.snap)
                session._release(m.snap)
            m.done.set()
            self.emit("map.finished", session=m.session, map=m.id, status=m.status, counts=m.summary(rows=0)["counts"])

    async def _run_case(self, m: MapRun, index: int, case) -> dict:
        session = self.sessions[m.session]
        output = str(m.dir / f"{index}.out")
        Path(output).touch()
        peak = {"pss": 0}
        stop = {"reason": None}

        def started(pid: int) -> None:
            m.running[index] = pid
            session.ephemeral[f"{m.id}:{index}"] = pid
            if m.interrupt_requested:  # interrupted while this case was being spawned
                try:
                    os.kill(pid, signal.SIGINT)
                except ProcessLookupError:
                    pass

        async def watch() -> None:
            killed_at = None
            while True:
                await asyncio.sleep(0.5)
                pid = m.running.get(index)
                pss = (memory(pid) or {}).get("pss") if pid else None
                if pss is None:
                    continue
                peak["pss"] = max(peak["pss"], pss)
                if m.case_memory_limit and pss > m.case_memory_limit:
                    stop["reason"] = "memory_limit"
                    sig = signal.SIGINT if killed_at is None else signal.SIGKILL
                    if killed_at is None or time.monotonic() - killed_at > MEMORY_LIMIT_GRACE:
                        try:
                            os.kill(pid, sig)
                        except ProcessLookupError:
                            pass
                        killed_at = killed_at or time.monotonic()

        watcher = asyncio.create_task(watch())
        try:
            def on_message(kind: str, content: dict) -> None:
                if kind == "talo_record":
                    self._add_record(content, session, m=m, index=index)

            out = await ephemeral.run(m.snap.socket, m.code, output, bind={"case": case},
                                      case={"map": m.id, "index": index, "seed": case_seed(m.id, index)},
                                      json_limit=m.json_limit, started=started, on_message=on_message)
        except Exception as error:  # noqa: BLE001
            out = {"status": "error", "error": {"ename": type(error).__name__, "evalue": str(error), "traceback": []}}
        finally:
            watcher.cancel()
            m.running.pop(index, None)
            session.ephemeral.pop(f"{m.id}:{index}", None)
        status = out["status"]
        if status == "error" and (out.get("error") or {}).get("ename") == "KeyboardInterrupt" \
                and (m.interrupt_requested or stop["reason"]):
            status = "interrupted"
        outcome = {"ok": "succeeded", "error": "failed"}.get(status, status)
        self._set_outcome(lambda r: r["producer"].get("map") == m.id and r["producer"].get("index") == index, outcome)
        error = out.get("error")
        if error:
            error = {"ename": error.get("ename"), "evalue": str(error.get("evalue"))[:500],
                     "traceback": (error.get("traceback") or [])[-20:]}
        return {"index": index, "case": case, "status": status, "result": out.get("result"),
                "result_error": out.get("result_error"), "text": out.get("text"), "error": error,
                "termination": stop["reason"], "elapsed_s": out.get("elapsed_s"), "sampled_peak_pss": peak["pss"] or None,
                "output": output, "outputs": out.get("outputs", [])}

    # ---------------------------------------------------------------- ledger

    def _load_ledger(self) -> dict:
        records, path = {}, home() / "ledger.jsonl"
        if path.exists():
            for line in path.read_text().splitlines():
                rec = json.loads(line)
                if "outcome_of" in rec:  # an outcome filled in later
                    if rec["outcome_of"] in records:
                        records[rec["outcome_of"]]["outcome"] = rec["outcome"]
                else:
                    records[rec["id"]] = rec
        for rec in records.values():  # producers of an earlier supervisor are gone; their ids will be reused
            if rec["outcome"] == "running":
                rec["outcome"] = "unknown"
        return records

    def _ledger_write(self, entry: dict) -> None:
        with open(home() / "ledger.jsonl", "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")

    def _add_record(self, content: dict, session: Session, *, job: Job | None = None, m: "MapRun | None" = None,
                    index: int | None = None, call: dict | None = None, owner: str | None = None) -> dict:
        key = content.get("key")
        if key is not None:
            for rec in self.ledger.values():
                if rec.get("key") == key and rec["session_id"] == session.id:
                    if rec["fields"] != (content.get("fields") or {}):
                        self.emit("record.conflict", session=session.name, record=rec["id"], key=key)
                    return rec  # an idempotent retry (a retry with other fields keeps the first, and says so)
        if call is not None:
            producer = {"kind": "call", "call": call["id"], "service": call["service"], "version": call["version"]}
            svc = self.services.get(call["service"])
            version = svc["versions"].get(call["version"]) if svc else None
            source, rev, code = (version["snapshot"].ref if version else None), None, json.dumps(call["args"])
        elif m is not None:
            producer = {"kind": "case", "map": m.id, "index": index}
            source, rev, code = m.snap.ref, None, m.code
        elif job is not None:
            producer = {"kind": "job", "job": job.id}
            source, rev, code = session.base, job.rev, job.code
        else:
            producer = {"kind": "shell"}
            source, rev, code = session.base, session.rev, ""
        rec = {"id": content.get("id") or "r" + wire.new_id()[:10], "key": key, "time": time.time(),
               "owner": owner or (job.owner if job else m.owner if m else call["owner"] if call else "user"),
               "session": session.name,
               "session_id": session.id, "producer": producer, "rev": rev, "source_snapshot": source,
               "code_hash": blake(code) if code else None, "modules": content.get("modules") or {},
               "fields": content.get("fields") or {}, "outcome": "running" if (job or m or call) else "reported"}
        self.ledger[rec["id"]] = rec
        self._ledger_write(rec)
        self.emit("record.added", session=session.name, record=rec["id"], owner=rec["owner"])
        return rec

    def _set_outcome(self, match, outcome: str) -> None:
        for rec in self.ledger.values():
            if rec["outcome"] == "running" and match(rec):
                rec["outcome"] = outcome
                self._ledger_write({"outcome_of": rec["id"], "outcome": outcome})

    def _job_outcome(self, job: Job) -> None:
        restored = (job.atomic or {}).get("outcome") == "restored"
        outcome = "rolled_back" if restored else {"ok": "succeeded", "error": "failed"}.get(job.status, job.status)
        self._set_outcome(lambda r: r["producer"].get("job") == job.id and r["producer"]["kind"] == "job", outcome)

    async def op_record(self, req):
        session = self._session(req["session"])
        fields = {}
        for k, v in (req.get("fields") or {}).items():
            fields[k] = v[:200] if isinstance(v, str) else v
        return {"record": self._add_record({"fields": fields, "key": req.get("key")}, session, owner=req.get("owner"))}

    async def op_ledger(self, req):
        recs = list(self.ledger.values())
        if "_visible" in req:
            recs = [r for r in recs if r["session"] in req["_visible"] and self.sessions[r["session"]].id == r["session_id"]]
        if req.get("session"):
            recs = [r for r in recs if r["session"] == req["session"]]
        if req.get("owner"):
            recs = [r for r in recs if r["owner"] == req["owner"]]
        if req.get("outcome"):
            recs = [r for r in recs if r["outcome"] == req["outcome"]]
        for key in req.get("has") or []:
            recs = [r for r in recs if key in r["fields"]]
        for cond in req.get("where") or []:
            k, _, v = cond.partition("=")
            recs = [r for r in recs if str(r["fields"].get(k)) == v]
        if req.get("sort"):
            k = req["sort"].lstrip("-")
            recs = [r for r in recs if isinstance(r["fields"].get(k), (int, float))]
            recs.sort(key=lambda r: r["fields"][k], reverse=req["sort"].startswith("-"))
        total = len(recs)
        last = int(req.get("last") or 50)
        recs = recs[:last] if req.get("sort") else recs[-last:]
        alive = {s.ref for sess in self.sessions.values() for s in sess.snapshots.values()}
        out = [{**r, "source_state": "none" if not r["source_snapshot"] else
                ("kept" if r["source_snapshot"] in alive else "gone")} for r in recs]
        return {"records": out, "total": total}

    # ---------------------------------------------------------------- services

    def _register_versions(self, session: Session, job: Job, snap: Snapshot) -> list:
        out = []
        for name in job.staged:
            full = f"{session.name}.{name}"
            svc = self.services.setdefault(full, {"name": full, "id": wire.new_id()[:12], "session": session.name,
                                                  "session_id": session.id, "function": name, "versions": {},
                                                  "latest": 0})
            n = self.version_counter.get(full, 0) + 1  # never reused, even after the service was removed
            self.version_counter[full] = n
            svc["versions"][n] = {"version": n, "snapshot": snap, "job": job.id, "owner": job.owner,
                                  "created": time.time(), "in_flight": set()}
            svc["latest"] = n
            snap.pins.add(f"service {full} v{n}")
            out.append({"service": full, "version": n, "snapshot": snap.ref})
            self.emit("service.published", session=session.name, service=full, version=n, job=job.id)
        return out

    def _service_info(self, svc: dict) -> dict:
        return {"name": svc["name"], "session": svc["session"], "latest": svc["latest"],
                "versions": [{"version": v["version"], "snapshot": v["snapshot"].ref, "job": v["job"],
                              "owner": v["owner"], "created": v["created"], "calls_in_flight": len(v["in_flight"]),
                              "pss": (memory(v["snapshot"].pid) or {}).get("pss")} for v in svc["versions"].values()]}

    async def op_services(self, req):
        return {"services": [self._service_info(s) for s in self.services.values()
                             if not req.get("session") or s["session"] == req["session"]]}

    async def op_expose(self, req):
        """Publish an existing function of the session (the shell form of talo.expose)."""
        name = req["name"]
        if not name.isidentifier():
            raise ValueError(f"service name {name!r} must be an identifier")
        return await self.op_run({**req, "code": f"talo.expose({name})"})

    async def op_unexpose(self, req):
        svc = self.services.get(req["service"])
        if svc is None:
            raise KeyError(f"no service {req['service']!r} (`talo services` lists them)")
        versions = [int(req["version"])] if req.get("version") else list(svc["versions"])
        for n in versions:
            v = svc["versions"].get(n)
            if v is None:
                raise KeyError(f"service {svc['name']} has no version {n}")
            if v["in_flight"]:
                raise RuntimeError(f"{svc['name']} v{n} has calls in flight: {', '.join(sorted(v['in_flight']))}")
        for n in versions:
            self._drop_version(svc, n)
        if not svc["versions"]:
            del self.services[svc["name"]]
        return {"removed": [f"{svc['name']} v{n}" for n in versions]}

    def _drop_version(self, svc: dict, n: int) -> None:
        v = svc["versions"].pop(n)
        snap = v["snapshot"]
        snap.pins.discard(f"service {svc['name']} v{n}")
        if not snap.pins and svc["session"] in self.sessions:
            session = self.sessions[svc["session"]]
            if snap in session.internal_snaps:
                session.internal_snaps.remove(snap)
            session._release(snap)

    async def op_call(self, req):
        svc = self.services.get(req["service"])
        if svc is None:
            raise KeyError(f"no service {req['service']!r} (`talo services` lists them)")
        key = req.get("key")
        fingerprint = {"service": svc["name"], "args": req.get("args") or {}, "version": req.get("version")}
        if key is not None and ("call", svc["id"], key) in self.keys:
            call_id, known = self.keys[("call", svc["id"], key)]
            if known != fingerprint:
                raise ValueError(f"key {key!r} was used for call {call_id} with a different request; use a new key")
            return {**await self._wait_call(self.calls[call_id], req.get("timeout", 30)), "deduplicated": True}
        n = int(req["version"]) if req.get("version") else svc["latest"]
        version = svc["versions"].get(n)
        if version is None:
            raise KeyError(f"service {svc['name']} has no version {n}")
        args = req.get("args") or {}
        if not isinstance(args, dict):
            raise ValueError("call arguments must be a JSON object")
        call_id = f"c{self.next_call}"
        self.next_call += 1
        session = self.sessions[svc["session"]]
        out_dir = session.dir / "calls"
        out_dir.mkdir(exist_ok=True)
        call = {"id": call_id, "kind": "call", "service": svc["name"], "session": svc["session"], "version": n,
                "args": args, "status": "running",
                "owner": req.get("owner") or "user", "started": time.time(), "finished": None, "result": None,
                "result_error": None, "error": None, "pid": None, "output": str(out_dir / f"{call_id}.out"),
                "cancel": False, "blocked": None, "done": asyncio.Event()}
        Path(call["output"]).touch()
        self.calls[call_id] = call
        if key is not None:
            self.keys[("call", svc["id"], key)] = (call_id, fingerprint)
        version["in_flight"].add(call_id)
        call["task"] = asyncio.create_task(self._run_call(svc, version, call, int(req.get("max_result") or 65536)))
        return await self._wait_call(call, req.get("timeout", 30))

    async def _run_call(self, svc: dict, version: dict, call: dict, json_limit: int) -> None:
        code = f"talo._serve_call({svc['function']!r}, __import__('json').loads({json.dumps(call['args'])!r}))"
        session = self.sessions[svc["session"]]

        def started(pid: int) -> None:
            call["pid"] = pid
            session.ephemeral[call["id"]] = pid
            if call["cancel"]:  # interrupted while it was being spawned
                try:
                    os.kill(pid, signal.SIGINT)
                except ProcessLookupError:
                    pass

        def on_message(kind: str, content: dict) -> None:
            if kind == "talo_record":
                self._add_record(content, session, call=call)

        try:
            async with session.call_slots:  # at most CALL_PARALLEL calls of a session at once
                while session.memory_limit and session.ephemeral and not call["cancel"] \
                        and session.memory_usage()[0] > session.memory_limit:
                    call["blocked"] = "memory over the session's limit"
                    await asyncio.sleep(0.2)
                call["blocked"] = None
                if call["cancel"]:
                    out = {"status": "cancelled", "error": None}
                else:
                    out = await ephemeral.run(version["snapshot"].socket, code, call["output"], json_limit=json_limit,
                                              started=started, on_message=on_message)
        except Exception as error:  # noqa: BLE001
            out = {"status": "error", "error": {"ename": type(error).__name__, "evalue": str(error), "traceback": []}}
        finally:
            version["in_flight"].discard(call["id"])
            session.ephemeral.pop(call["id"], None)
        outcome = {"ok": "succeeded", "error": "failed"}.get(out["status"], out["status"])
        self._set_outcome(lambda r: r["producer"].get("call") == call["id"], outcome)
        error = out.get("error")
        call.update(status=out["status"], result=out.get("result"), result_error=out.get("result_error"),
                    text=out.get("text"), elapsed_s=out.get("elapsed_s"), finished=time.time(), pid=None,
                    error={"ename": error.get("ename"), "evalue": str(error.get("evalue"))[:500],
                           "traceback": (error.get("traceback") or [])[-20:]} if error else None)
        call["done"].set()
        self.emit("call.finished", session=svc["session"], call=call["id"], service=svc["name"],
                  version=call["version"], status=call["status"], owner=call["owner"])

    async def _wait_call(self, call: dict, timeout: float | None) -> dict:
        if timeout is None or timeout > 0:
            try:
                await asyncio.wait_for(call["done"].wait(), timeout)
            except asyncio.TimeoutError:
                pass
        view = {k: v for k, v in call.items() if k not in ("done", "task", "cancel")}
        view["elapsed_s"] = round((call["finished"] or time.time()) - call["started"], 3)
        return {"call": view, "epoch": self.epoch}

    # ---------------------------------------------------------------- handoffs

    async def op_handoff(self, req):
        if req.get("close"):
            h = self._handoff(req["close"])
            self._close_handoff(h, "closed")
            return {"handoff": self._handoff_view(h)}
        session = self._session(req["session"])
        fingerprint = {"op": "handoff", "message": req.get("message", ""), "to": req.get("to"), "snap": req.get("snap"),
                       "expect_rev": req.get("expect_rev")}
        key = req.get("key")
        if key is not None and ("handoff", session.id, key) in self.keys:
            job_id, known = self.keys[("handoff", session.id, key)]
            if known != fingerprint:
                raise ValueError(f"key {key!r} was used for another handoff request; use a new key")
            return {**self._handoff_reply(await self._wait(self.jobs[job_id], req.get("timeout", 300))),
                    "deduplicated": True}
        hid = f"h{self.next_handoff}"
        self.next_handoff += 1
        # one queued operation: the snapshot, the pin, the cursors and the summary all see the same state
        job = await self._submit(session, "handoff", owner=req.get("owner"), expect_rev=req.get("expect_rev"),
                                 hid=hid, message=req.get("message", ""), to=req.get("to"), snap=req.get("snap"))
        if key is not None:
            self.keys[("handoff", session.id, key)] = (job.id, fingerprint)
        return self._handoff_reply(await self._wait(job, req.get("timeout", 300)))

    def _handoff_reply(self, out: dict) -> dict:
        if out["status"] == "error":
            raise RuntimeError(out["error"]["evalue"])
        if out["status"] != "ok":
            return {"job": out}
        return {"handoff": self._handoff_view(self.handoffs[out["result"]["handoff"]])}

    async def _do_handoff(self, session: Session, job: Job) -> dict:
        """Runs in the session's queue (the worker is idle)."""
        params = job.params
        hid = params["hid"]
        if params.get("snap"):
            snap = self._snapshot(params["snap"], session.name)
        else:
            snap = await session._snap(f"handoff-{hid}", message=f"handoff {hid}: {params['message']}"[:200],
                                       owner=job.owner)
        snap.pins.add(f"handoff {hid}")
        previous = [h for h in self.handoffs.values() if h["session_id"] == session.id]
        notes_from = previous[-1]["notes"]["upto"] if previous else 0
        records_from = previous[-1]["records"]["upto"] if previous else 0
        notes = self._notes(session)
        records = [r for r in self.ledger.values() if r["session_id"] == session.id]
        rows = (await session.request("talo_vars_request", {"pattern": None}, timeout=60))["content"]["variables"]
        session.vars_seen = {"rev": session.rev, "time": time.time(), "variables": rows}
        brief = await self.op_brief({"session": session.name, "notes": 0, "jobs": 8, "vars": 15, "_cached": True})
        h = {"id": hid, "session": session.name, "session_id": session.id, "time": time.time(), "job": job.id,
             "owner": job.owner, "to": params.get("to"), "message": params["message"],
             "snapshot": snap.ref, "snapshot_id": snap.id, "snapshot_name": snap.name, "rev": session.rev,
             "status": "open",
             "notes": {"count": len(notes) - notes_from, "last": notes[notes_from:][-10:], "upto": len(notes)},
             "records": {"count": len(records) - records_from, "ids": [r["id"] for r in records[records_from:]][-50:],
                         "upto": len(records)},
             "active_jobs": [j for j in brief["active_jobs"] if j["id"] != job.id], "variables": brief["variables"],
             "recent_jobs": brief["recent_jobs"],
             "continue": [f"talo resume {hid} --branch NAME   # a new session from {snap.ref}",
                          f"talo attach -s {session.name}            # the live session"]}
        self.handoffs[hid] = h
        with open(session.dir / "handoffs.jsonl", "a") as f:
            f.write(json.dumps(h, ensure_ascii=False, default=str) + "\n")
        self.emit("handoff.created", session=session.name, handoff=hid, to=h["to"], owner=h["owner"])
        return {"handoff": hid}

    def _handoff(self, hid: str) -> dict:
        if hid not in self.handoffs:
            raise KeyError(f"no handoff {hid!r} (`talo handoffs` lists them)")
        return self.handoffs[hid]

    def _handoff_snapshot(self, h: dict) -> Snapshot | None:
        """The handoff's snapshot, if the session incarnation that took it still holds it."""
        s = next((x for x in self.sessions.values() if x.id == h["session_id"]), None)
        return s.snapshots.get(h["snapshot_id"]) if s else None

    def _handoff_view(self, h: dict) -> dict:
        return {**h, "snapshot_available": self._handoff_snapshot(h) is not None}

    def _close_handoff(self, h: dict, status: str) -> None:
        h["status"] = status
        if (snap := self._handoff_snapshot(h)) is not None:
            snap.pins.discard(f"handoff {h['id']}")

    async def op_handoffs(self, req):
        return {"handoffs": [self._handoff_view(h) for h in self.handoffs.values()
                             if not req.get("session") or h["session"] == req["session"]]}

    async def op_resume(self, req):
        h = self._handoff(req["id"])
        out = {"handoff": self._handoff_view(h)}
        if req.get("branch"):
            if self._handoff_snapshot(h) is None:
                raise RuntimeError(f"the snapshot of handoff {h['id']} ({h['snapshot']}) is gone")
            session = await self.op_new({"name": req["branch"], "from": h["snapshot"], "owner": req.get("owner")})
            self._close_handoff(h, f"branched into {req['branch']}")
            out = {"handoff": self._handoff_view(h), "session": session}
        return out

    # ---------------------------------------------------------------- events

    def emit(self, event_type: str, /, **fields) -> None:
        self.event_seq += 1
        self.events.append({"seq": self.event_seq, "cursor": f"{self.epoch}:{self.event_seq}", "type": event_type,
                            "time": time.time(), **fields})
        self.event_added.set()
        self.event_added = asyncio.Event()

    def _cursor(self, text: str | None) -> int:
        if text is None:
            return self.event_seq  # from now on
        epoch, _, seq = str(text).rpartition(":")
        oldest = self.events[0]["seq"] if self.events else self.event_seq + 1
        if epoch and epoch != self.epoch:
            raise CursorExpired(f"cursor {text} is from another supervisor (epoch {epoch}, now {self.epoch}); events "
                                f"may have been missed: re-read the state (`talo brief`), then use `--after` with "
                                f"{self.epoch}:{self.event_seq}")
        if int(seq) < oldest - 1:
            raise CursorExpired(f"cursor {text} is older than the oldest event kept ({self.epoch}:{oldest}); events "
                                "were missed")
        return int(seq)

    async def op_events(self, req):
        after = 0 if req.get("all") else self._cursor(req.get("after"))
        if req.get("all") and self.events:
            after = self.events[0]["seq"] - 1
        session, kinds, limit = req.get("session"), req.get("type"), int(req.get("limit") or 100)
        kinds = set(kinds.split(",")) if isinstance(kinds, str) else set(kinds or [])
        deadline = time.monotonic() + float(req.get("wait") or 0)
        while True:
            if self.events and after < self.events[0]["seq"] - 1:  # overtaken while we waited
                raise CursorExpired(f"events after {self.epoch}:{after} were dropped from the journal while waiting")
            matched, scanned = [], after
            for e in self.events:
                if e["seq"] <= after:
                    continue
                if len(matched) >= limit:
                    break
                scanned = e["seq"]
                if (not session or e.get("session") == session) and (not kinds or e["type"] in kinds) \
                        and ("_visible" not in req or e.get("session") in req["_visible"]):
                    matched.append(e)
            after = scanned
            left = deadline - time.monotonic()
            if matched or left <= 0:
                return {"events": matched, "next": f"{self.epoch}:{after}", "has_more": after < self.event_seq,
                        "oldest": f"{self.epoch}:{self.events[0]['seq']}" if self.events else None}
            waiter = self.event_added
            try:
                await asyncio.wait_for(waiter.wait(), left)
            except asyncio.TimeoutError:
                pass

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
        self.emit("note.added", session=session.name, note=note["id"], owner=note["owner"])
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
        jobs = [j for j in self.jobs.values() if j.session_id == session.id]
        finished = [j for j in jobs if j.status not in ("queued", "running")]
        active = [j for j in jobs if j.status in ("queued", "running")]

        def job_line(j: Job) -> dict:
            d = {k: v for k, v in j.to_dict().items() if k in ("id", "kind", "status", "elapsed_s", "owner", "rev",
                                                                 "progress", "last_activity_age_s", "phase")}
            if d.get("progress"):
                d["progress"] = {**d["progress"], "note": str(d["progress"].get("note", ""))[:80]}
            d["code_preview"] = j.code[:120]
            if j.error:
                d["error"] = f"{j.error.get('ename')}: {str(j.error.get('evalue'))[:200]}"
            return d

        seen = session.vars_seen
        if session.state == "idle" and not req.get("_cached"):
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

        info = session.to_dict()
        since = info.pop("jobs_since_base")
        info["jobs_since_base"] = {"count": len(since), "last": since[-10:]}
        snaps = list(session.snapshots.values())
        n_snaps = 10
        while True:  # the whole reply is bounded: shrink the lists until it fits
            out = {
                "session": info,
                "private_bytes": private_dirty(session.worker_pid),
                "notes": {"last": [cut(n) for n in notes[-n_notes:]] if n_notes else [], "total": len(notes),
                          "more": "talo notes"},
                "snapshots": {"last": [{"name": s.name, "ref": s.ref, "parent": s.parent, "message": s.message[:120],
                                        "owner": s.owner, "private_bytes": private_dirty(s.pid)}
                                       for s in (snaps[-n_snaps:] if n_snaps else [])],
                              "total": len(snaps), "more": "talo snaps"},
                "variables": {**variables, "largest": variables["largest"][:n_vars]} if "largest" in variables else variables,
                "recent_jobs": {"last": [job_line(j) for j in finished[-n_jobs:]] if n_jobs else [],
                                "total": len(finished), "more": "talo jobs"},
                "active_jobs": [job_line(j) for j in active[:5]],
                "active_total": len(active),
            }
            if len(json.dumps(out, default=str)) <= BRIEF_MAX_BYTES or not (n_notes or n_jobs or n_vars or n_snaps):
                return out
            n_notes, n_jobs, n_vars, n_snaps = n_notes // 2, n_jobs // 2, n_vars // 2, n_snaps // 2

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
            procs += [{"kind": "internal", "name": x.name, "ref": x.ref, "pid": x.pid, "pins": sorted(x.pins),
                       "memory": memory(x.pid)} for x in s.internal_snaps]
            procs += [{"kind": "ephemeral", "name": name, "pid": pid, "memory": memory(pid)}
                      for name, pid in s.ephemeral.items()]
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

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, shared: bool = False) -> None:
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
                    visible = None
                    if shared:
                        req = self._authorize(writer, req)
                        visible = req.pop("_visible", None)
                    reply = {"ok": True, **await fn(req)}
                    if visible is not None:
                        reply = visible_only(reply, set(visible))
                except Exception as error:  # noqa: BLE001 - report to the client; the supervisor keeps running
                    msg = error.args[0] if isinstance(error, KeyError) and error.args else str(error)
                    reply = {"ok": False, "error": {"type": type(error).__name__, "message": msg}}
            writer.write(json.dumps(reply, ensure_ascii=False, default=str).encode() + b"\n")
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    # ---------------------------------------------------------------- sharing

    def _peer_uid(self, writer: asyncio.StreamWriter) -> int:
        import struct
        sock = writer.get_extra_info("socket")
        pid, uid, gid = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))
        return uid

    def _authorize(self, writer: asyncio.StreamWriter, req: dict) -> dict:
        """Check a request on the shared socket against the peer's grants, by the resource it actually acts on;
        returns the request with internal fields set (``_visible``, ``_cached``). Every request is recorded."""
        uid = self._peer_uid(writer)
        grant = self.grants.get(uid)
        op = req.get("op")
        req = {k: v for k, v in req.items() if not k.startswith("_")}  # internal fields are ours alone
        if grant is None:
            raise Forbidden(f"uid {uid} has no access to these sessions")
        user = grant["user"]
        req["owner"] = f"{user}@shared"
        # a grant belongs to one incarnation of a session: a session recreated under the same name is not shared
        levels = {name: g["level"] for name, g in grant["sessions"].items()
                  if name in self.sessions and self.sessions[name].id == g["id"]}
        req["_visible"] = sorted(levels)
        req["_cached"] = True

        def deny(why: str, session=None):
            self.emit("share.denied", user=user, op=op, session=session)
            raise Forbidden(f"{user} may not {op}: {why}")

        if op in SHARE_LISTINGS:
            self.emit("share.request", user=user, op=op)
            return req
        target = req.get("session")
        if op in ("job", "output", "interrupt"):
            ident = str(req.get("id"))
            target = (self.calls[ident]["session"] if ident in self.calls else
                      self.maps[ident].session if ident in self.maps else
                      self.jobs[ident].session if ident in self.jobs else None)
        elif op in ("call", "unexpose"):
            svc = self.services.get(str(req.get("service")))
            target = svc["session"] if svc else None
        elif op == "resume" or (op == "handoff" and req.get("close")):
            h = self.handoffs.get(str(req.get("id") or req.get("close")))
            target = h["session"] if h else None
            if op == "resume" and req.get("branch"):
                deny("creating a session (resume --branch) is not shared; resume without --branch shows the handoff",
                     target)
        level = levels.get(target)
        allowed = {"read": SHARE_READ, "call": SHARE_CALL, "run": SHARE_RUN}.get(level, set())
        if op not in allowed:
            deny(f"session {target} is not shared" if level is None else f"needs a higher level than {level}", target)
        for param in SHARE_STATE_PARAMS.get(op, ()):  # states named by the request stay inside shared sessions
            ref = req.get(param)
            if not ref:
                continue
            owner_session, _, name = str(ref).rpartition(":")
            if name == "@":
                ref_session = owner_session or target
            else:
                try:
                    ref_session = self._snapshot(str(ref), target).session
                except KeyError:
                    ref_session = owner_session or target
            if ref_session not in levels:
                deny(f"{ref} belongs to session {ref_session}, which is not shared", ref_session)
        if level == "run":
            req.pop("_cached")  # run is the owner's level: live inspection is allowed
        self.emit("share.request", user=user, op=op, session=target)
        return req

    async def op_share(self, req):
        import pwd
        user, level, name = req["user"], req.get("level", "read"), req["session"]
        if level not in SHARE_LEVELS:
            raise ValueError(f"level must be one of {', '.join(SHARE_LEVELS)}")
        self._session(name)
        uid = pwd.getpwnam(user).pw_uid
        grant = self.grants.setdefault(uid, {"user": user, "sessions": {}})
        grant["sessions"][name] = {"level": level, "id": self._session(name).id}
        await self._start_shared()
        self.emit("share.granted", user=user, session=name, level=level)
        warning = ("run lets them execute any code as you, with all of your account's access" if level == "run"
                   else None)
        return {"user": user, "uid": uid, "session": name, "level": level, "connect": str(shared_socket_path()),
                "warning": warning}

    async def op_unshare(self, req):
        import pwd
        uid = pwd.getpwnam(req["user"]).pw_uid
        grant = self.grants.get(uid, {"sessions": {}})
        removed = [s for s in list(grant["sessions"]) if not req.get("session") or s == req["session"]]
        for s in removed:
            del grant["sessions"][s]
        if not grant["sessions"]:
            self.grants.pop(uid, None)
        self.emit("share.revoked", user=req["user"], sessions=removed)
        return {"revoked": removed}

    async def op_shares(self, req):
        return {"shares": [{"user": g["user"], "uid": uid, "session": s, "level": x["level"],
                            "current": s in self.sessions and self.sessions[s].id == x["id"]}
                           for uid, g in self.grants.items() for s, x in g["sessions"].items()],
                "connect": str(shared_socket_path()) if self.shared_server else None}

    async def _start_shared(self) -> None:
        if self.shared_server is not None:
            return
        path = shared_socket_path()
        path.parent.mkdir(mode=0o711, exist_ok=True)
        os.chmod(path.parent, 0o711)
        path.unlink(missing_ok=True)
        self.shared_server = await asyncio.start_unix_server(lambda r, w: self.handle(r, w, shared=True),
                                                             path=str(path), limit=wire.MAX_LINE)
        os.chmod(path, 0o666)  # anyone may connect; every connection is checked by its uid (SO_PEERCRED)

    async def serve(self) -> None:
        path = socket_path()
        if path.exists():
            path.unlink()
        server = await asyncio.start_unix_server(self.handle, path=str(path), limit=wire.MAX_LINE)
        os.chmod(path, 0o600)
        log.info("supervisor started: pid %s, %s", os.getpid(), path)
        async with server:
            await self.stopping.wait()
        self._kill_maps()
        if self.shared_server is not None:
            self.shared_server.close()
            shared_socket_path().unlink(missing_ok=True)
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
