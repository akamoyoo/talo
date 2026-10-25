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
import re
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
AUTOSAVE_CHECK = float(os.environ.get("TALO_AUTOSAVE_CHECK", "15"))  # seconds between autosave checks
RETAINED_ERRORS = 5          # error-JOB snapshots kept by --retain-error (older unpinned ones are dropped)
GUARD_GRACE = 5.0           # seconds a job past its deadline or output limit gets to stop after the interrupt
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


DISPLAY_MARK_RE = __import__("re").compile("\x1b\\]5379;talo-display=\\d+\x07")


def strip_marks(text: str) -> str:
    """Output text without the display markers the worker writes (see Worker.display_bundle)."""
    return DISPLAY_MARK_RE.sub("", text)


def blake(text: str) -> str:
    import hashlib
    return hashlib.blake2b(text.encode(errors="replace"), digest_size=12).hexdigest()


def case_seed(map_id: str, index: int) -> int:
    """A seed per map case: forks inherit identical random generator states, so cases should seed from this."""
    import hashlib
    return int.from_bytes(hashlib.blake2b(f"{map_id}:{index}".encode(), digest_size=4).digest(), "little")


def descendants(pid: int, keep: set | frozenset = frozenset()) -> list[int]:
    """Processes started (directly or not) by ``pid``, from /proc, leaving out the processes in ``keep`` and
    everything below them."""
    children: dict[int, list[int]] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as f:
                ppid = int(f.read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            continue
        children.setdefault(ppid, []).append(int(entry))
    out, todo = [], [pid]
    while todo:
        for child in children.get(todo.pop(), []):
            if child in keep:
                continue
            out.append(child)
            todo.append(child)
    return out


def kill_tree(pid: int, keep: set | frozenset = frozenset(), sig: int = signal.SIGKILL) -> None:
    """Signal a process and everything it started (children first, so none is re-parented and missed), except the
    processes in ``keep`` and their subtrees: snapshots are forked by the worker, so they are its children too."""
    for p in [*reversed(descendants(pid, keep)), pid]:
        try:
            os.kill(p, sig)
        except (ProcessLookupError, PermissionError):
            pass


LIMIT_PROPERTIES = {"memory": ("MemoryMax", "memory"), "pids": ("TasksMax", "pids"), "cpu": ("CPUQuota", "cpu")}


def delegated_controllers() -> set:
    """cgroup v2 controllers the user's systemd instance may use for scopes."""
    uid = os.getuid()
    try:
        text = Path(f"/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service/cgroup.controllers").read_text()
    except OSError:
        return set()
    return set(text.split())


def scope_command(limits: dict, unit: str) -> list[str]:
    """``systemd-run --user --scope`` with the limits (``{"memory": "100G", "pids": "512", "cpu": "8"}``); raises if
    a limit cannot be enforced here."""
    import shutil
    if shutil.which("systemd-run") is None:
        raise RuntimeError("--limit needs systemd-run (a systemd user instance); use --memory-limit for a soft limit")
    available = delegated_controllers()
    cmd = ["systemd-run", "--user", "--scope", "--quiet", f"--unit={unit}", "--collect"]
    for key, value in limits.items():
        if key not in LIMIT_PROPERTIES:
            raise ValueError(f"unknown limit {key!r}: use memory, pids or cpu")
        prop, controller = LIMIT_PROPERTIES[key]
        if controller not in available:
            raise RuntimeError(f"the {controller} controller is not delegated to your systemd user instance "
                               f"(available: {', '.join(sorted(available)) or 'none'}), so {key}= cannot be enforced")
        if key == "cpu":
            value = f"{int(float(value) * 100)}%"
        cmd += ["-p", f"{prop}={value}"]
        if key == "memory":  # without this the kernel swaps the excess out instead of enforcing the limit
            cmd += ["-p", "MemorySwapMax=0"]
    return cmd + ["--"]


def memory_events_path(pid: int) -> Path | None:
    """The memory.events file of the cgroup ``pid`` is in."""
    try:
        rel = Path(f"/proc/{pid}/cgroup").read_text().strip().split("::", 1)[1]
    except (OSError, IndexError):
        return None
    return Path(f"/sys/fs/cgroup{rel}/memory.events")


def effective_limits(events: Path | None) -> dict | None:
    """memory.max, memory.swap.max, pids.max and cpu.max of the scope, as the kernel applies them."""
    if events is None:
        return None
    out = {}
    for name in ("memory.max", "memory.swap.max", "pids.max", "cpu.max"):
        try:
            out[name] = (events.parent / name).read_text().strip()
        except OSError:
            pass
    return out


def oom_kills(events: Path | None) -> int | None:
    try:
        for line in events.read_text().splitlines():
            if line.startswith("oom_kill "):
                return int(line.split()[1])
    except (OSError, AttributeError, ValueError):
        return None
    return None


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
    user_expressions: dict | None = None       # Jupyter user_expressions evaluated after the code
    retained: dict | None = None               # run --retain-error: the snapshot of the failed state
    progress: dict | None = None               # the last talo.progress() of a running job
    progress_at: float | None = None           # time.monotonic() of that update
    atomic: dict | None = None                 # run --atomic: {"outcome": committed | restored | restore_failed, ...}
    interrupt_requested: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def to_dict(self) -> dict:
        out = {k: getattr(self, k) for k in ("id", "session", "kind", "status", "submitted", "started", "finished",
                                            "execution_count", "result", "error", "metrics", "output", "outputs", "displays", "owner", "key",
                                            "rev", "atomic", "termination", "phase", "published", "autosnap",
                                            "user_expressions", "retained")}
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
SHARE_READ = {"brief", "vars", "snaps", "history", "notes", "job", "output", "resume", "artifacts", "trials"}
SHARE_CALL = SHARE_READ | {"call"}
SHARE_RUN = SHARE_CALL | {"run", "interrupt", "snap", "checkout", "drop", "reload", "map", "note", "record", "handoff",
                          "export", "expose", "unexpose", "show", "diff", "complete", "diagnose", "queue", "lease",
                          "verify", "review", "adopt", "reject", "memory", "autosnap", "undo", "save", "autosave"}
SHARE_LEVELS = ("read", "call", "run")
SHARE_STATE_PARAMS = {"diff": ("a", "b"), "map": ("from",), "checkout": ("name",), "handoff": ("snap",),
                      "export": ("snap",)}


class Session:
    def __init__(self, name: str, python: str, cwd: str, env: dict | None = None, memory_budget: int | None = None,
                 memory_limit: int | None = None, limits: dict | None = None, max_output: int | None = None):
        self.name, self.python, self.cwd = name, python, cwd
        self.env = env or {}
        self.memory_budget = memory_budget
        self.memory_limit = memory_limit   # soft limit on the PSS of the worker plus its snapshots (--memory-limit)
        self.limits = limits or {}         # hard limits of the systemd scope (--limit memory=,pids=,cpu=)
        self.max_output = max_output       # default bound of a job's output file (--max-output)
        self.scope: str | None = None
        self.oom_kills: int | None = None   # the scope's oom_kill count when the worker started
        self.memory_events: Path | None = None
        self.effective_limits: dict | None = None  # read back from the scope's cgroup
        self.limits_inherited_from: str | None = None
        self.last_checkpoint: str | None = None
        self.autosave_config: dict | None = None   # {"every": seconds, "keep": N}
        self.autosave_rev: int | None = None       # the revision of the last automatic checkpoint
        self.autosave_at = 0.0
        self.autosaving = False
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
        self.on_adopt = None                         # set by the supervisor: switch to a trial's state
        self.on_finish = lambda job: None            # set by the supervisor: fill in the outcome of the job's records  # set by the supervisor: append to the event journal
        self.autosnap: dict | None = None  # {"keep": N} while talo autosnap is on
        self.pending: list = []            # queued jobs taken off the queue but not yet allowed to start
        self.paused: dict | None = None    # talo queue pause: {"by", "reason", "since"}
        self.lease: dict | None = None     # talo lease: {"id", "owner", "until", "reason"}
        self.counts: list = []             # execution counts of finished executions, latest last
        self.resolve = None                # set by the supervisor: "session:id" -> Snapshot (any session)
        self.checkpoint: Snapshot | None = None  # the internal checkpoint of a running --atomic job
        self.vars_seen: dict | None = None  # the last variable summary: {"rev", "time", "variables"} (for brief)
        self.tasks: list[asyncio.Task] = []

    # ---------------------------------------------------------------- workers

    async def start(self) -> None:
        """Start a fresh worker process."""
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        env = {"MPLBACKEND": "Agg", **os.environ, **self.env, "PYTHONUNBUFFERED": "1",  # figures become files
               "TALO_SOCKET": str(socket_path())}                                  # talo.call reaches us here
        prefix = []
        if self.limits:  # the worker, its snapshots and all their forks live in one cgroup the kernel enforces
            self.scope = f"talo-{self.name}-{wire.new_id()[:8]}.scope"
            prefix = scope_command(self.limits, self.scope.removesuffix(".scope"))
        with open(self.dir / "worker.log", "ab") as errlog:
            proc = await asyncio.create_subprocess_exec(
                *prefix, self.python, str(WORKER), "--fd", str(child.fileno()), "--session", self.name,
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
        if self.limits and self.memory_events is None:
            self.memory_events = memory_events_path(self.worker_pid)
        if self.limits and self.effective_limits is None:
            self.effective_limits = effective_limits(self.memory_events)
        self.oom_kills = oom_kills(self.memory_events) if self.limits else None
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
        code = await self._exit_code()
        death = f"the worker process {describe_exit(code)}"
        now = oom_kills(self.memory_events) if self.limits.get("memory") else None
        if code == -signal.SIGKILL and now is not None and self.oom_kills is not None and now > self.oom_kills:
            death += (f"; the scope's OOM killer ran meanwhile (memory limit {self.limits['memory']}, "
                      f"oom_kill {self.oom_kills} -> {now}), most likely by the memory limit of its scope")
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

    def _eligible(self, job: Job) -> bool:
        if self.paused is not None:
            return False
        if self.lease is not None:
            if time.time() >= self.lease["until"]:
                self.emit("lease.expired", session=self.name, owner=self.lease["owner"])
                self.lease = None
            elif job.owner != self.lease["owner"]:
                return False
        return True

    async def _next_job(self) -> Job:
        """The oldest queued job that may start now (a pause or another owner's lease holds the others back, in
        order)."""
        while True:
            while not self.queue.empty():
                self.pending.append(self.queue.get_nowait())
            self.pending = [j for j in self.pending if j.status != "cancelled"]
            for i, job in enumerate(self.pending):
                if self._eligible(job):
                    return self.pending.pop(i)
            try:
                self.pending.append(await asyncio.wait_for(self.queue.get(), 1.0))
            except asyncio.TimeoutError:
                pass  # leases expire with time

    async def _execute_loop(self) -> None:
        while True:
            job = await self._next_job()
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
                if job.kind in ("execute", "checkout", "reload", "adopt", "undo"):
                    self.rev += 1
                job.rev = self.rev
                self.owners[job.id] = job.owner
                if job.kind == "execute":
                    autosnap = self.autosnap  # the setting when the job started, whatever changes meanwhile
                    status = await (self._execute_atomic(job) if job.params.get("atomic") else self._execute(job))
                    if status in ("error", "interrupted") and job.params.get("retain_error") and not job.params.get("atomic"):
                        await self._keep_error(job)
                    if status == "ok" and job.staged:
                        await self._publish(job)
                    if status == "ok" and autosnap and job.execution_count and not job.params.get("silent"):
                        await self._autosnap(job, autosnap)
                    self._finish(job, status)
                elif job.kind == "snap":
                    self._finish(job, "ok", result=(await self._snap(**job.params, owner=job.owner)).to_dict())
                elif job.kind == "checkout":
                    self._finish(job, "ok", result=await self._checkout(**job.params, owner=job.owner))
                elif job.kind == "autosnap":
                    self._finish(job, "ok", result=await self._set_autosnap(**job.params))
                elif job.kind == "adopt":
                    self._finish(job, "ok", result=await self.on_adopt(job))
                elif job.kind == "undo":
                    self._finish(job, "ok", result=await self._undo(job))
                elif job.kind == "handoff":
                    self._finish(job, "ok", result=await self.on_handoff(job))
                elif job.kind == "fork":  # an internal snapshot of the current state, for map
                    job.handle = await self._fork(f"f{job.id}", strict=False)
                    self._finish(job, "ok", result={"ref": job.handle.ref, "pid": job.handle.pid, "rev": self.rev,
                                                    "base": self.base, "jobs": list(self.jobs_since_base)})
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
        msg = wire.message("execute_request", {"code": job.code, "silent": silent,
                                               "store_history": job.params.get("store_history", True) and not silent,
                                               "user_expressions": job.params.get("user_expressions") or {},
                                               "allow_stdin": False, "stop_on_error": True},
                           session=self.name, metadata={"talo_output": job.output, "talo_job": job.id,
                                                        "talo_retain_error": bool(job.params.get("retain_error"))})
        job.phase, job.exec_pid = "executing", self.worker_pid
        q = self._send(msg)
        guard = asyncio.create_task(self._guard(job))  # deadline and output limit
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
                    job.displays.append({"n": meta.get("talo_display"), "offset": meta.get("talo_offset"),
                                         "end": meta.get("talo_end"),
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
                    job.user_expressions = got["content"].get("user_expressions") or None
                    job.metrics = dict(got.get("metadata", {}))
                    if status == "error" and job.interrupt_requested and (job.error or {}).get("ename") == "KeyboardInterrupt":
                        status = "interrupted"
                    limit = job.params.get("max_output") or self.max_output
                    if limit and not job.termination and os.path.getsize(job.output) > limit:
                        job.termination = "output_limit"  # finished between two checks: still over the limit
                    if job.termination and status == "ok":  # it caught Talo's interrupt: still stopped
                        status = "interrupted"
                        job.error = {"ename": {"memory_limit": "MemoryLimit", "deadline": "Deadline",
                                               "output_limit": "OutputLimit"}.get(job.termination, "Stopped"),
                                     "traceback": [], "evalue": f"stopped by Talo ({job.termination})"}
                    elif status == "error" and job.termination and (job.error or {}).get("ename") == "KeyboardInterrupt":
                        status = "interrupted"
                    self.jobs_since_base.append(job.id)
                    self.counts.append(job.execution_count)
                    return status
        finally:
            guard.cancel()
            job.exec_pid = None
            self.waiters.pop(msg["header"]["msg_id"], None)

    async def _keep_error(self, job: Job, drop_reference: bool = True) -> None:
        """run --retain-error: snapshot the failed state (the worker holds the exception, its traceback and the
        frames' locals as `_talo_error`), then let the live worker drop that reference."""
        try:
            snap = await self._fork(f"e{job.id}", strict=False, name=f"error-{job.id}",
                                    message=f"state when job {job.id} failed; the exception is _talo_error",
                                    owner=job.owner)
        except Exception as error:  # noqa: BLE001 - reported separately; the job's own failure stands
            job.retained = {"error": f"{type(error).__name__}: {error}"}
            if drop_reference and self.state != "dead":  # do not keep the frames alive in the live worker
                await self.request("execute_request", {"code": "globals().pop('_talo_error', None)", "silent": True,
                                                       "store_history": False, "user_expressions": {},
                                                       "allow_stdin": False}, timeout=60)
            return
        self.snapshots[snap.id] = snap  # a named snapshot, but the session's base and lineage are unchanged
        job.retained = {"snapshot": snap.ref, "name": snap.name}
        errors = [s for s in self.snapshots.values() if s.name.startswith("error-")]
        for s in errors[:-RETAINED_ERRORS]:  # failed states hold their frames' memory: keep only the latest
            if not s.pins and s.ref != self.base:
                self.drop(s)
        self.emit("snapshot.created", session=self.name, snapshot=snap.ref, name=snap.name, owner=job.owner)
        if drop_reference and self.state != "dead":
            await self.request("execute_request", {"code": "globals().pop('_talo_error', None)", "silent": True,
                                                   "store_history": False, "user_expressions": {},
                                                   "allow_stdin": False}, timeout=60)

    async def _guard(self, job: Job) -> None:
        """Stop a job that passes its deadline (--deadline) or writes more than its output limit (--max-output): an
        interrupt, then, after a grace period, the worker and every process it started are killed."""
        deadline = job.params.get("deadline")
        limit = job.params.get("max_output") or self.max_output
        if not deadline and not limit:
            return
        started = time.monotonic()
        while job.phase == "executing":
            await asyncio.sleep(0.25)
            reason = None
            if deadline and time.monotonic() - started > deadline:
                reason = "deadline"
            elif limit:
                try:
                    if os.stat(job.output).st_size > limit:
                        reason = "output_limit"
                except OSError:
                    pass
            if reason is None:
                continue
            job.termination = reason
            pid = job.exec_pid
            self.interrupt(job)
            await asyncio.sleep(GUARD_GRACE)
            if job.phase == "executing" and job.exec_pid == pid and pid:
                log.warning("session %s: job %s ignored the interrupt (%s); killing it", self.name, job.id, reason)
                kill_tree(pid, self.snapshot_pids())
            return

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
            if job.params.get("retain_error") and status in ("error", "interrupted"):
                await self._keep_error(job, drop_reference=False)  # before the restore replaces this state
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
        record["options"] = {k: job.params[k] for k in ("silent", "store_history") if k in job.params}
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

    def snapshot_pids(self) -> set:
        """Snapshot processes of this session: never killed with a job's process tree."""
        return {s.pid for s in self.held() if s.pid}

    def held(self) -> list:
        return [*self.snapshots.values(), *self.internal_snaps, *([self.checkpoint] if self.checkpoint else [])]

    async def _autosnap(self, job: Job, config: dict) -> None:
        """talo autosnap: a snapshot after each successful execution, named @N, the last `keep` kept. Manual
        snapshots are never replaced; an automatic @N of an abandoned branch is replaced only once the new one
        exists."""
        name = f"@{job.execution_count}"
        try:
            snap = await self._snap(f"@{job.execution_count}.{job.id}",
                                    message=f"after execution {job.execution_count} (job {job.id})", owner=job.owner)
        except Exception as error:  # noqa: BLE001 - the code ran; only the snapshot failed
            job.autosnap = {"error": f"{type(error).__name__}: {error}"}
            return
        snap.auto = True
        old = self.find(name, missing_ok=True)
        if old is None:
            snap.name = name
        elif old.auto and not old.pins:
            self.drop(old)
            snap.name = name
        job.autosnap = {"snapshot": snap.ref, "name": snap.name}
        self._record_snapshot(snap)  # again, with its final name (the later record of a ref wins)
        autos = [s for s in self.snapshots.values() if s.auto]
        for s in autos[:-config["keep"]]:
            if not s.pins and s.ref != self.base:
                self.drop(s)

    async def _set_autosnap(self, on: bool, keep: int) -> dict:
        """In the queue: the setting and its baseline describe the same state."""
        baseline = None
        was_on = self.autosnap is not None
        self.autosnap = {"keep": max(1, int(keep or 10))} if on else None
        if on and not was_on and (self.base is None or self.jobs_since_base):
            counts = [n for n in self.counts if n]
            name = f"@{counts[-1] if counts else 0}"
            if self.find(name, missing_ok=True) is not None:
                name = f"{name}.base"
            snap = await self._snap(name, message="autosnap baseline")
            snap.auto = True
            baseline = snap.ref
        return {"session": self.name, "autosnap": self.autosnap, "baseline": baseline}

    async def _undo(self, job: Job) -> dict:
        """In the queue, so the target is decided after the jobs before it: the base if jobs ran since it, else the
        base's parent (possibly in another session, for a branch)."""
        target = self.base if self.jobs_since_base else (self.resolve(self.base).parent if self.base else None)
        if not target:
            raise RuntimeError("nothing to undo: no snapshot before the last execution (`talo autosnap on` takes one "
                               "after every execution)")
        return await self._checkout(self.resolve(target), discard=True, owner=job.owner)

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
                kill_tree(pid, self.snapshot_pids())
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
        self._record_snapshot(snap)
        self.emit("snapshot.created", session=self.name, snapshot=snap.ref, name=snap.name, owner=owner)
        return snap

    def _record_snapshot(self, snap: Snapshot) -> None:
        """A history line for every snapshot on a lineage (autosnaps, handoffs, trials, as well as `snap` jobs), so
        export and verify can rebuild the path to any state."""
        record = {"id": f"S{snap.ref}", "kind": "snap", "status": "ok", "session": self.name, "code": "",
                  "finished": time.time(), "result": snap.to_dict()}
        with open(self.dir / "history.jsonl", "a") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

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
        if job.status == "running" and job.kind in ("execute", "trial"):
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
        for job in [*self.pending, *iter(lambda: self.queue.get_nowait() if not self.queue.empty() else None, None)]:
            if job is not None and not job.done.is_set():  # queued or held back by a pause: answered, never run
                job.status, job.finished = "cancelled", time.time()
                job.error = {"ename": "SessionKilled", "evalue": f"session {self.name} was closed", "traceback": []}
                job.done.set()
        self.pending = []
        for snap in [*self.snapshots.values(), *self.internal_snaps]:
            self._release(snap)
        self.snapshots.clear()
        self.internal_snaps.clear()
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
                "created": self.created, "queued": self.queue.qsize() + len(self.pending),
                "paused": self.paused, "lease": self.lease,
                "current_job": self.current.id if self.current else None, "death": self.death, "base": self.base,
                "jobs_since_base": list(self.jobs_since_base), "snapshots": len(self.snapshots), "rev": self.rev,
                "id": self.id,
                "memory_budget": self.memory_budget, "memory_limit": self.memory_limit, "over_limit": self.over_limit,
                "limits": self.limits, "effective_limits": self.effective_limits, "scope": self.scope,
                "limits_inherited_from": self.limits_inherited_from, "last_checkpoint": self.last_checkpoint,
                "autosave": self.autosave_config,
                "max_output": self.max_output}


async def compare_states(ask_a, ask_b, limit: int | None) -> dict:
    """Variables added, removed and changed from state A to state B: first by summary, then, for variables up to
    ``limit`` bytes, by content fingerprint. ``ask_x(op, **params)`` answers ``vars`` and ``fingerprint``."""
    va, vb = await asyncio.gather(ask_a("vars"), ask_b("vars"))
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
    unchanged = 0
    if same:
        fa, fb = await asyncio.gather(ask_a("fingerprint", names=same), ask_b("fingerprint", names=same))
        fa, fb = fa["fingerprints"], fb["fingerprints"]
        for name in same:
            x, y = fa.get(name, {}), fb.get(name, {})
            if "hash" not in x or "hash" not in y:
                not_compared.append({"name": name, "reason": x.get("error") or y.get("error") or "missing"})
            elif x["hash"] != y["hash"]:
                changed.append({"name": name, "a": va[name], "b": vb[name], "fields": ["content"]})
            else:
                unchanged += 1
    return {"added": [vb[n] for n in sorted(vb.keys() - va.keys())],
            "removed": [va[n] for n in sorted(va.keys() - vb.keys())],
            "changed": sorted(changed, key=lambda c: c["name"]), "unchanged": unchanged,
            "not_compared": not_compared, "max_bytes": limit}


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
        self.store_ops = 0  # saves and restores in progress (store gc refuses meanwhile)
        self.services: dict[str, dict] = {}   # "session.name" -> {"versions": {n: version}, "latest": n}
        self.version_counter: dict[str, int] = {}
        self.calls: dict[str, dict] = {}
        self.next_call = 1
        self.grants: dict[int, dict] = {}   # uid -> {"user", "sessions": {name: level}}
        self.shared_server = None
        self.trials: dict[str, dict] = {}
        self.next_trial = 1
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
        inherited = {}
        if source:  # a fork keeps the snapshot's interpreter and working directory, and lives in its scope
            origin = self.sessions[source.session]
            python, cwd = origin.python, origin.cwd
            inherited = {"limits": origin.limits, "scope": origin.scope, "events": origin.memory_events}
        else:
            python, cwd = req.get("python") or sys.executable, req.get("cwd") or os.getcwd()
        limits = {k: str(v) for k, v in (req.get("limits") or {}).items()}
        if source and limits:
            raise ValueError("a session started --from a snapshot lives in its source's scope and limits")
        if limits:
            scope_command(limits, "check")  # refuse limits this machine cannot enforce, before starting anything
        session = Session(name, python, cwd, req.get("env"), req.get("memory_budget"), req.get("memory_limit"),
                          limits or inherited.get("limits"), req.get("max_output"))
        if inherited.get("scope"):
            session.scope, session.memory_events = inherited["scope"], inherited["events"]
            session.limits_inherited_from = source.session
        session.rev = self.last_rev.get(name, 0)  # revisions never repeat for a name, so a stale --expect-rev fails
        session.emit = self.emit
        session.on_record = lambda job, content: self._add_record(content, session, job=job)
        session.on_finish = self._job_outcome
        session.on_handoff = lambda job: self._do_handoff(session, job)
        session.on_adopt = lambda job: self._do_adopt(session, job)
        session.resolve = lambda ref: self._snapshot(ref, session.name)
        session.on_publish = lambda job, snap: self._register_versions(session, job, snap)
        self.sessions[name] = session
        try:
            await (session.start_from(source) if source else session.start())
            self._save_registry()
            self.emit("session.created", session=name, id=session.id, owner=req.get("owner") or "user",
                      **({"from": source.ref} if source else {}))
        except Exception:
            self.sessions.pop(name, None)
            await session.stop()
            raise
        return session.to_dict()

    async def op_ls_lost(self, req):
        return {"lost": self._lost()}

    async def op_ls(self, req):
        return {"sessions": [s.to_dict() for s in self.sessions.values()], "lost": self._lost()}

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
        for tr in [x for x in self.trials.values() if x["session_id"] == session.id]:
            if tr["status"] in ("preparing", "running"):
                tr["task"].cancel()
                if tr["job"].exec_pid:
                    kill_tree(tr["job"].exec_pid)
                tr["status"] = "cancelled"
                if not tr["job"].done.is_set():
                    tr["job"].status, tr["job"].finished = "cancelled", time.time()
                    tr["job"].done.set()
        for svc in [s for s in self.services.values() if s["session"] == session.name]:
            for n in list(svc["versions"]):
                self._drop_version(svc, n)
            del self.services[svc["name"]]
        await session.stop()
        self.last_rev[session.name] = session.rev
        del self.sessions[req["name"]]
        self.emit("session.killed", session=session.name, id=session.id)
        registry = self._load_registry()
        if session.name in registry:  # closed on purpose: not something to recover
            registry[session.name]["alive"] = False
            (home() / "sessions.json").write_text(json.dumps(registry, indent=1))
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
        if req.get("trial"):
            return await self._start_trial(session, req)
        fingerprint = {"op": "run", "code": req["code"], "expect_rev": req.get("expect_rev"),
                       "atomic": bool(req.get("atomic")), "silent": bool(req.get("silent")),
                       "store_history": req.get("store_history", True), "deadline": req.get("deadline"),
                       "retain_error": bool(req.get("retain_error")),
                       "max_output": req.get("max_output")}
        if job := self._deduplicated(session, req, fingerprint):
            return {**await self._wait(job, req.get("timeout", 30)), "deduplicated": True}
        if session.state == "dead":
            raise SessionDead(f"session {session.name} is dead: {session.death} (`talo checkout SNAPSHOT` to roll "
                              f"back, or `talo kill {session.name}` and create it again)")
        job = await self._submit(session, "execute", req["code"], owner=req.get("owner"), key=req.get("key"),
                                 fingerprint=fingerprint, expect_rev=req.get("expect_rev"),
                                 **({"atomic": True} if req.get("atomic") else {}),
                                 **({"silent": True} if req.get("silent") else {}),
                                 **({"deadline": float(req["deadline"])} if req.get("deadline") else {}),
                                 **({"retain_error": True} if req.get("retain_error") else {}),
                                 **({"max_output": int(req["max_output"])} if req.get("max_output") else {}),
                                 **({"store_history": False} if req.get("store_history") is False else {}),
                                 **({"user_expressions": req["user_expressions"]} if req.get("user_expressions") else {}))
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
        out = {"id": job.id, "offset": offset, "next_offset": offset + len(data), "size": size, "status": job.status}
        if req.get("raw"):  # bytes as base64: the reader decodes incrementally (UTF-8 split across reads)
            import base64
            return {**out, "data": base64.b64encode(data).decode()}
        return {**out, "text": strip_marks(data.decode(errors="replace"))}

    async def op_diagnose(self, req):
        """A running job's thread stacks, CPU, IO, memory and child processes, without stopping it."""
        job = self._job(req["id"])
        session = self._session(job.session)
        pid = job.exec_pid
        if job.status != "running" or not pid or job.phase != "executing":
            raise RuntimeError(f"job {job.id} is {job.status}" + (f" ({job.phase})" if job.phase else "") +
                               "; diagnose looks at jobs whose code is executing")

        def cpu_ticks() -> int | None:
            try:
                fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
                return int(fields[11]) + int(fields[12])
            except (OSError, IndexError, ValueError):
                return None
        stack_path = Path(job.output.removesuffix(".out") + ".stack")
        before, t0 = cpu_ticks(), time.monotonic()
        size = stack_path.stat().st_size if stack_path.exists() else 0
        if req.get("stack", True):
            os.kill(pid, signal.SIGUSR1)
        for _ in range(20):
            await asyncio.sleep(0.05)
            if stack_path.exists() and stack_path.stat().st_size > size:
                break
        await asyncio.sleep(max(0.0, 0.5 - (time.monotonic() - t0)))
        after = cpu_ticks()
        cpu = None
        if before is not None and after is not None:
            cpu = round(100 * (after - before) / os.sysconf("SC_CLK_TCK") / (time.monotonic() - t0), 1)
        stack = stack_path.read_text(errors="replace")[size:] if stack_path.exists() else ""
        stack = "\n".join(line for line in stack.splitlines() if str(WORKER) not in line)  # the user's frames only
        status = {}
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                k, _, v = line.partition(":")
                if k in ("State", "Threads", "VmRSS", "VmHWM"):
                    status[k] = v.strip()
        except OSError:
            pass
        io = {}
        try:
            for line in Path(f"/proc/{pid}/io").read_text().splitlines():
                k, _, v = line.partition(":")
                if k in ("rchar", "wchar", "read_bytes", "write_bytes"):
                    io[k] = int(v)
        except OSError:
            pass
        try:
            wchan = Path(f"/proc/{pid}/wchan").read_text().strip()
        except OSError:
            wchan = None
        children = []
        for c in descendants(pid, session.snapshot_pids()):
            try:
                cmd = Path(f"/proc/{c}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip()
            except OSError:
                continue
            if cmd:  # an empty command line is a zombie, already finished
                children.append({"pid": c, "cmdline": cmd[:200]})
        return {"job": job.id, "pid": pid, "elapsed_s": round(time.time() - (job.started or time.time()), 1),
                "cpu_percent": cpu, "status": status, "wchan": wchan, "io": io, "children": children,
                "stack": stack[-20_000:], "progress": job.progress,
                "last_output_age_s": job.to_dict().get("last_output_age_s")}

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
        if req.get("on") is None:
            return {"session": session.name, "autosnap": session.autosnap, "baseline": None}
        return await self._control(session, "autosnap", req.get("timeout", 300), req, on=bool(req["on"]),
                                   keep=req.get("keep") or 10)

    async def op_undo(self, req):
        session = self._session(req["session"])
        return await self._control(session, "undo", req.get("timeout", 300), req)

    async def op_queue(self, req):
        """Pause or resume starting jobs (never queued itself)."""
        session = self._session(req["session"])
        if req["action"] == "pause":
            session.paused = {"by": req.get("owner") or "user", "reason": req.get("reason") or "", "since": time.time()}
            self.emit("queue.paused", session=session.name, by=session.paused["by"])
        elif req["action"] == "resume":
            session.paused = None
            self.emit("queue.resumed", session=session.name)
        return {"session": session.name, "paused": session.paused, "queued": session.queue.qsize() + len(session.pending)}

    async def op_lease(self, req):
        """Give one owner the session for a while (never queued itself, so the holder is never stuck behind it)."""
        session = self._session(req["session"])
        owner = req.get("owner") or "user"
        lease = session.lease
        if lease is not None and time.time() >= lease["until"]:
            lease = session.lease = None
        if req.get("release"):
            if lease is not None and lease["owner"] != owner and not req.get("force"):
                raise RuntimeError(f"the lease is held by {lease['owner']}; --force releases it anyway")
            session.lease = None
            self.emit("lease.released", session=session.name, owner=owner)
            return {"session": session.name, "lease": None}
        if lease is not None and lease["owner"] != owner:
            raise RuntimeError(f"session {session.name} is leased by {lease['owner']} until "
                               f"{time.strftime('%H:%M:%S', time.localtime(lease['until']))}")
        ttl = float(req.get("ttl") or 1800)
        session.lease = {"id": (lease or {}).get("id") or wire.new_id()[:8], "owner": owner, "until": time.time() + ttl,
                         "reason": req.get("reason") or ""}
        self.emit("lease.taken", session=session.name, owner=owner, until=session.lease["until"])
        return {"session": session.name, "lease": session.lease}

    async def op_artifacts(self, req):
        """Files a job wrote with talo.artifact_path (regular files only; links are not followed)."""
        job = self._job(req["id"])
        session = self._session(job.session)
        root = session.dir / "artifacts" / job.id
        files = []
        if root.is_dir():
            for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
                for f in filenames:
                    path = Path(dirpath) / f
                    st = path.lstat()
                    if not path.is_symlink() and os.path.isfile(path):
                        files.append({"path": str(path.relative_to(root)), "bytes": st.st_size, "modified": st.st_mtime})
        return {"job": job.id, "root": str(root), "files": sorted(files, key=lambda f: f["path"]),
                "complete": job.status not in ("queued", "running")}

    async def op_memory(self, req):
        """Where the session's memory is: processes (measured) and objects (estimated), and what could be released."""
        session = self._session(req["session"])
        processes = (await self.op_resources({"session": session.name}))["sessions"][0]
        objects = None
        if session.state == "idle":
            reply = await self._ask_idle(session, "talo_vars_request", {"pattern": None, "extra": ["_", "Out"]})
            rows = sorted(reply["variables"], key=lambda v: -(v.get("nbytes") or 0))
            objects = {"largest": [{k: v[k] for k in ("name", "type", "nbytes", "nbytes_partial") if k in v}
                                   for v in rows[:15]], "count": len(rows), "estimated": True}

        def private(s: Snapshot) -> int | None:
            m = memory(s.pid)
            return None if m is None else m.get("private_dirty", 0) + m.get("private_clean", 0)
        candidates = []
        for s in list(session.snapshots.values()):
            if s.pins or s.ref == session.base:
                continue
            if s.auto or s.name.startswith("error-"):
                candidates.append({"action": "drop", "snapshot": s.ref, "name": s.name, "private_bytes": private(s),
                                   "why": "automatic snapshot" if s.auto else "retained failed state"})
        if objects and any(v["name"] == "Out" and (v.get("nbytes") or 0) > 0 for v in objects["largest"]):
            candidates.append({"action": "clear_out", "why": "results kept in Out and _", "private_bytes": None})
        applied = []
        if req.get("apply"):
            for c in candidates:
                if c["action"] == "drop":
                    s = next((x for x in session.snapshots.values() if x.ref == c["snapshot"]), None)
                    if s is not None and not s.pins and s.ref != session.base:  # checked again: pins may have changed
                        session.drop(s)
                        applied.append(c)
                elif c["action"] == "clear_out":  # a recorded execution through the queue, like any mutation
                    job = await self._submit(session, "execute", "Out.clear(); globals().pop('_', None)",
                                             owner=req.get("owner"), silent=True)
                    await job.done.wait()
                    if job.status == "ok":
                        applied.append({**c, "job": job.id})
        return {"session": session.name, "processes": processes, "objects": objects, "prune": candidates,
                "applied": applied, "note": "object sizes are estimates and do not add up to process memory"}

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
                        if sig == signal.SIGKILL:
                            kill_tree(pid)
                        else:
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

    def _lineage(self, records: dict, base: str | None, tail: list) -> list:
        """Job and snapshot records from the session's start to the state whose base is ``base`` with ``tail`` run
        since, oldest first. Raises KeyError if a snapshot on the way has no record (the lineage is incomplete)."""
        snaps = {r["result"]["ref"]: r for r in records.values() if r["kind"] == "snap" and r["status"] == "ok"}
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
            else:  # an adopted trial ran in a fork, but on this lineage it is an execution like any other
                missing = [j for j in part if j not in records]
                if missing:
                    raise KeyError(f"no history record for jobs {', '.join(missing)}")
                out += [{**records[j], "kind": "execute", "trial": True} if records[j]["kind"] == "trial" else records[j]
                        for j in part]
        return out

    async def op_export(self, req):
        """The job records that produced a state, oldest first (see "export" in docs/design.md)."""
        session = self._session(req["session"])
        records = self._records()
        if req.get("all"):
            path = session.dir / "history.jsonl"
            return {"jobs": [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else [],
                    "notes": self._notes(session)}
        if req.get("snap"):
            base, tail = self._snapshot(req["snap"], session.name).ref, []
        else:
            base, tail = session.base, list(session.jobs_since_base)
        out = self._lineage(records, base, tail)
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

    # ---------------------------------------------------------------- trials

    def _trial(self, session: Session, name: str) -> dict:
        mine = [tr for tr in self.trials.values() if tr["session_id"] == session.id]
        exact = [tr for tr in mine if tr["id"] == name]
        named = [tr for tr in mine if tr["name"] == name]
        open_ = [tr for tr in named if tr["status"] in ("preparing", "running", "ready", "adopting")]
        found = exact or open_ or named[-1:]
        if not found:
            raise KeyError(f"session {session.name} has no trial {name!r} (`talo trials` lists them)")
        return found[0]

    def _trial_view(self, tr: dict) -> dict:
        return {k: (v.ref if isinstance(v, Snapshot) else v) for k, v in tr.items() if k not in ("job", "task")} | {
            "job": tr["job"].to_dict()}

    async def _start_trial(self, session: Session, req: dict) -> dict:
        name = req["trial"]
        options = {k: req[k] for k in ("deadline", "max_output", "retain_error", "expect_rev") if req.get(k) is not None}
        if req.get("atomic"):
            raise ValueError("--trial already leaves the session unchanged when the code fails; --atomic does not apply")
        fingerprint = {"code": req["code"], "name": name, **options}
        key = req.get("key")
        if key is not None and ("trial", session.id, key) in self.keys:
            tid, known = self.keys[("trial", session.id, key)]
            if known != fingerprint:
                raise ValueError(f"key {key!r} was used for another trial; use a new key")
            return {**await self._wait(self.trials[tid]["job"], req.get("timeout", 30)), "trial": tid,
                    "deduplicated": True}
        if any(tr["session_id"] == session.id and tr["name"] == name and tr["status"] in ("preparing", "running", "ready")
               for tr in self.trials.values()):
            raise ValueError(f"session {session.name} already has an open trial {name!r}")
        tid = f"t{self.next_trial}"
        self.next_trial += 1
        job = Job(self._new_job_id(), session.name, req["code"], "", kind="trial", params={"trial": tid, **options},
                  owner=req.get("owner") or "user", session_id=session.id)
        job.output = str(session.dir / "jobs" / f"{job.id}.out")
        Path(job.output).touch()
        self.jobs[job.id] = job
        tr = {"id": tid, "name": name, "session": session.name, "session_id": session.id, "status": "preparing",
              "owner": job.owner, "job": job, "source": None, "source_snapshot": None, "snapshot": None,
              "created": time.time()}
        self.trials[tid] = tr
        if key is not None:
            self.keys[("trial", session.id, key)] = (tid, fingerprint)
        tr["task"] = asyncio.create_task(self._run_trial(session, tr))
        return {**await self._wait(job, req.get("timeout", 30)), "trial": tid}

    async def _run_trial(self, session: Session, tr: dict) -> None:
        job = tr["job"]
        fork = await self._submit(session, "fork", owner=job.owner, expect_rev=job.params.get("expect_rev"))
        await fork.done.wait()
        if job.status == "cancelled":  # interrupted while preparing: never run it
            if fork.handle is not None:
                session._release(fork.handle)
            tr["status"] = "cancelled"
            return
        if fork.status != "ok":
            tr["status"] = "failed"
            job.started = time.time()
            session._finish(job, "error", error={"ename": "TrialNotStarted", "evalue": str((fork.error or {}).get("evalue")),
                                                 "traceback": []})
            return
        source = fork.handle
        tr["source"] = {k: fork.result[k] for k in ("rev", "base", "jobs")}
        tr["source_snapshot"] = source
        source.pins.add(f"trial {tr['id']}")
        session.internal_snaps.append(source)
        tr["status"], job.status, job.started, job.phase = "running", "running", time.time(), "executing"
        channel, guard = None, None
        try:
            channel = await ephemeral.Channel.from_snapshot(source.socket)
            job.exec_pid = channel.pid
            session.ephemeral[f"trial {tr['id']}"] = channel.pid
            guard = asyncio.create_task(session._guard(job))  # --deadline / --max-output, as for any job
            got = await channel.execute(job.code, job.output, retain_error=bool(job.params.get("retain_error")))
            limit = job.params.get("max_output") or session.max_output
            if limit and not job.termination and os.path.getsize(job.output) > limit:
                job.termination = "output_limit"
            if job.termination and got["status"] == "ok":
                got = {**got, "status": "error", "error": {"ename": "Stopped", "evalue": f"stopped by Talo ({job.termination})"}}
            job.execution_count, job.result = got.get("execution_count"), got.get("result")
            for m in got["messages"]:
                if m["header"]["msg_type"] == "talo_record":
                    self._add_record(m["content"], session, job=job)
            if got["status"] != "ok":
                if job.params.get("retain_error"):  # keep the failed state of the fork, with _talo_error
                    sid = f"e{job.id}"
                    path = str(socket_for(session.dir / "snapshots" / f"{sid}.sock"))
                    reply, _ = await channel.request("talo_snapshot_request",
                                                     {"id": sid, "socket": path, "strict": False}, timeout=120)
                    if reply["content"].get("status") == "ok":
                        err = Snapshot(sid, f"error-{job.id}", session.name, path, reply["content"]["pid"],
                                       tr["source"]["base"], [*tr["source"]["jobs"], job.id], 0, [],
                                       f"state when trial {tr['name']} failed; the exception is _talo_error", job.owner)
                        err.hold = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                        err.hold.connect(path)
                        err.hold.sendall(b'{"op": "hold"}\n')
                        session.snapshots[sid] = err
                        job.retained = {"snapshot": err.ref, "name": err.name}
                tr["status"] = "failed"
                session._finish(job, "interrupted" if job.interrupt_requested or job.termination else "error",
                                error={k: (got["error"] or {}).get(k) for k in ("ename", "evalue", "traceback")})
                return
            sid = f"t{tr['id']}"
            path = str(socket_for(session.dir / "snapshots" / f"{sid}.sock"))
            reply, _ = await channel.request("talo_snapshot_request", {"id": sid, "socket": path, "strict": False},
                                             timeout=120)
            if reply["content"].get("status") != "ok":
                raise RuntimeError(f"{reply['content'].get('ename')}: {reply['content'].get('evalue')}")
            snap = Snapshot(sid, f"trial-{tr['name']}", session.name, path, reply["content"]["pid"], tr["source"]["base"],
                            [*tr["source"]["jobs"], job.id], reply["content"].get("execution_count", 0),
                            reply["content"].get("threads", []), f"trial {tr['name']} (job {job.id})", job.owner)
            snap.hold = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            snap.hold.connect(path)
            snap.hold.sendall(b'{"op": "hold"}\n')
            snap.pins.add(f"trial {tr['id']}")
            session.snapshots[sid] = snap
            tr["snapshot"], tr["status"] = snap, "ready"
            session._finish(job, "ok")              # the job's history record, for the lineage after adoption
            session._record_snapshot(snap)
        except Exception as error:  # noqa: BLE001
            tr["status"] = "failed"
            if not job.done.is_set():
                session._finish(job, "error", error={"ename": type(error).__name__, "evalue": str(error), "traceback": []})
        finally:
            if guard is not None:
                guard.cancel()
            job.exec_pid, job.phase = None, None
            session.ephemeral.pop(f"trial {tr['id']}", None)
            if channel is not None:
                await channel.close()
            if tr["status"] == "failed":
                self._close_trial(session, tr, "failed")
            self.emit("trial.finished", session=session.name, trial=tr["id"], name=tr["name"], status=tr["status"])

    def _close_trial(self, session: Session, tr: dict, status: str, keep_snapshot: bool = False) -> None:
        tr["status"] = status
        source = tr.get("source_snapshot")
        if source is not None:
            source.pins.discard(f"trial {tr['id']}")
            if source in session.internal_snaps:
                session.internal_snaps.remove(source)
            if not source.pins:
                session._release(source)
            tr["source_snapshot"] = None
        snap = tr.get("snapshot")
        if snap is not None:
            snap.pins.discard(f"trial {tr['id']}")
            if not keep_snapshot and not snap.pins and snap.ref != session.base and snap.id in session.snapshots:
                session.drop(snap)

    async def op_trials(self, req):
        session = self._session(req["session"])
        return {"trials": [self._trial_view(tr) for tr in self.trials.values() if tr["session_id"] == session.id]}

    async def op_review(self, req):
        session = self._session(req["session"])
        tr = self._trial(session, req["name"])
        out = {"trial": self._trial_view(tr), "current_rev": session.rev}
        if tr["status"] == "ready":
            if req.get("current"):
                ask_a = lambda op, **kw: self._ask_state(session, op, **kw)  # noqa: E731
            else:
                ask_a = lambda op, **kw: self._ask_state(tr["source_snapshot"], op, **kw)  # noqa: E731
            out["diff"] = await compare_states(ask_a, lambda op, **kw: self._ask_state(tr["snapshot"], op, **kw),
                                               req.get("max_bytes", DIFF_MAX_BYTES))
            out["diff_against"] = "current" if req.get("current") else "source"
        return out

    async def op_adopt(self, req):
        session = self._session(req["session"])
        tr = self._trial(session, req["name"])
        fingerprint = {"op": "adopt", "expect_rev": req.get("expect_rev"), "trial": tr["id"],
                       "discard": bool(req.get("discard"))}
        if self._deduplicated(session, req, fingerprint) is not None:  # a retry: before checking the trial's state
            return await self._control(session, "adopt", req.get("timeout", 300), req, trial=tr["id"],
                                       discard=bool(req.get("discard")))
        if tr["status"] != "ready":
            raise RuntimeError(f"trial {tr['name']} is {tr['status']}; only a ready trial can be adopted")
        return await self._control(session, "adopt", req.get("timeout", 300), req, trial=tr["id"],
                                   discard=bool(req.get("discard")))

    async def _do_adopt(self, session: Session, job: Job) -> dict:
        """In the queue: switch the session to a trial's state, if nothing changed since the trial started."""
        tr = self.trials[job.params["trial"]]
        if tr["status"] != "ready":
            raise RuntimeError(f"trial {tr['name']} is {tr['status']}")
        if session.rev - 1 != tr["source"]["rev"] and not job.params["discard"]:  # this job already counted one
            raise RuntimeError(f"the session changed since trial {tr['name']} started (revision {tr['source']['rev']} "
                               f"→ {session.rev - 1}); adopting would discard that work: use --discard, or run the "
                               "trial again")
        tr["status"] = "adopting"  # reject refuses from now on
        try:
            await session._replace_from(tr["snapshot"])  # the new worker answers before the old one is retired
        except Exception:
            tr["status"] = "ready"
            raise
        self._close_trial(session, tr, "adopted", keep_snapshot=True)
        self.emit("trial.adopted", session=session.name, trial=tr["id"], name=tr["name"])
        return {**session.to_dict(), "adopted": tr["name"]}

    async def op_reject(self, req):
        session = self._session(req["session"])
        tr = self._trial(session, req["name"])
        if tr["status"] in ("preparing", "running", "adopting"):
            raise RuntimeError(f"trial {tr['name']} is {tr['status']}" +
                               (f"; interrupt job {tr['job'].id} first" if tr["status"] != "adopting" else ""))
        if tr["status"] == "adopted":
            raise RuntimeError(f"trial {tr['name']} was adopted: its state is the session's now")
        self._close_trial(session, tr, "rejected")
        return {"trial": self._trial_view(tr)}

    # ---------------------------------------------------------------- checkpoints

    def _checkpoint_dir(self, session: str) -> Path:
        return home() / "checkpoints" / session

    def _manifest(self, ref: str, default_session: str | None) -> tuple[str, dict]:
        """``SESSION/NAME`` (or ``NAME`` of the default session) -> (ref, manifest)."""
        session, _, name = ref.rpartition("/")
        session = session or default_session
        path = self._checkpoint_dir(session) / f"{name}.json"
        if not path.exists():
            raise KeyError(f"no checkpoint {session}/{name} (`talo checkpoints` lists them)")
        return f"{session}/{name}", json.loads(path.read_text())

    async def op_save(self, req):
        session = self._session(req["session"])
        name = req.get("name") or time.strftime("%Y%m%d-%H%M%S")
        if not re.fullmatch(r"[\w.-]+", name):
            raise ValueError(f"checkpoint name {name!r}: letters, digits, '.', '-' and '_' only")
        fingerprint = {"op": "save", "name": name, "names": req.get("names"), "expect_rev": req.get("expect_rev")}
        key = req.get("key")
        if key is not None and ("save", session.id, key) in self.keys:
            job_id, known = self.keys[("save", session.id, key)]
            if known != fingerprint:
                raise ValueError(f"key {key!r} was used for another save; use a new key")
            return {**await self._wait(self.jobs[job_id], req.get("timeout", 30)), "deduplicated": True}
        if (self._checkpoint_dir(session.name) / f"{name}.json").exists():
            raise ValueError(f"checkpoint {session.name}/{name} exists; choose another name or drop it")
        job = Job(self._new_job_id(), session.name, f"# save {name}", "", kind="save",
                  params={"name": name, "names": req.get("names"), "auto": bool(req.get("auto")),
                          "expect_rev": req.get("expect_rev")},
                  owner=req.get("owner") or "user", session_id=session.id)
        job.output = str(session.dir / "jobs" / f"{job.id}.out")
        Path(job.output).touch()
        self.jobs[job.id] = job
        if key is not None:
            self.keys[("save", session.id, key)] = (job.id, fingerprint)
        asyncio.create_task(self._run_save(session, job))
        return await self._wait(job, req.get("timeout", 30))

    async def _run_save(self, session: Session, job: Job) -> None:
        """Snapshot through the queue, write from an ephemeral fork (the session keeps working), then the
        manifest, last: a checkpoint exists completely or not at all."""
        fork = await self._submit(session, "fork", owner=job.owner, expect_rev=job.params.get("expect_rev"))
        await fork.done.wait()
        job.started = time.time()
        if fork.status != "ok":
            session._finish(job, "error", error={"ename": "SaveNotStarted", "evalue": str((fork.error or {}).get("evalue")),
                                                 "traceback": []})
            return
        source = fork.handle
        self.store_ops += 1  # garbage collection waits for running saves and restores
        source.pins.add(f"save {job.id}")
        session.internal_snaps.append(source)
        job.status, job.phase = "running", "executing"
        channel = None
        try:
            channel = await ephemeral.Channel.from_snapshot(source.socket)
            job.exec_pid = channel.pid
            session.ephemeral[f"save {job.id}"] = channel.pid
            store = home() / "store"
            code = f"talo._save({job.params['names']!r}, {str(store)!r})"  # Python literals, not JSON
            got = await channel.execute(code, job.output, silent=True, json_limit=16 << 20)
            if got["status"] != "ok":
                error = got.get("error") or {}
                session._finish(job, "error", error={"ename": error.get("ename"), "evalue": error.get("evalue"),
                                                     "traceback": error.get("traceback") or []})
                return
            meta = (got.get("result") or {}).get("metadata", {}).get("talo_json") or {}
            if "value" not in meta:
                raise RuntimeError(f"the save did not report its units: {meta.get('error')}")
            saved = meta["value"]
            manifest = {"format": 2, "id": wire.new_id(), "session": session.name, "session_id": session.id,
                        "name": job.params["name"], "created": time.time(), "selection": job.params["names"],
                        "complete": not saved["skipped"] and not job.params["names"],
                        "auto": job.params["auto"], "owner": job.owner, "rev": fork.result["rev"],
                        "base": fork.result["base"], "jobs_since_base": fork.result["jobs"],
                        "python": session.python, "python_version": saved["python"], "cwd": session.cwd,
                        "limits": session.limits, "units": saved["units"], "skipped": saved["skipped"],
                        "modules": saved["modules"], "imports": saved["imports"], "versions": saved["versions"],
                        "bytes": sum(u["bytes"] for u in saved["units"]),
                        "new_bytes": sum(u["new_bytes"] for u in saved["units"])}
            d = self._checkpoint_dir(session.name)
            d.mkdir(parents=True, exist_ok=True)
            tmp = d / f".{job.params['name']}.json.tmp"
            with open(tmp, "w") as f:  # the manifest is published last, and durably: then the checkpoint exists
                f.write(json.dumps(manifest, indent=1))
                f.flush()
                os.fsync(f.fileno())
            if (d / f"{job.params['name']}.json").exists():
                raise RuntimeError(f"checkpoint {session.name}/{job.params['name']} was created meanwhile")
            os.replace(tmp, d / f"{job.params['name']}.json")
            dfd = os.open(d, os.O_RDONLY)
            os.fsync(dfd)
            os.close(dfd)
            session.last_checkpoint = f"{session.name}/{job.params['name']}"
            self._save_registry()
            job.result = {"checkpoint": session.last_checkpoint, **{k: manifest[k] for k in
                          ("bytes", "new_bytes", "skipped", "rev")}, "variables": sum(len(u["names"]) for u in saved["units"])}
            session._finish(job, "ok")
            self.emit("checkpoint.saved", session=session.name, checkpoint=session.last_checkpoint)
            if job.params["auto"] and session.autosave_config:  # automatic checkpoints beyond --keep go
                autos = sorted((json.loads(p.read_text()) for p in d.glob("*.json")), key=lambda m: m["created"])
                for m in [m for m in autos if m.get("auto")][:-session.autosave_config["keep"]]:
                    (d / f"{m['name']}.json").unlink(missing_ok=True)
        except Exception as error:  # noqa: BLE001
            if not job.done.is_set():
                session._finish(job, "error", error={"ename": type(error).__name__, "evalue": str(error), "traceback": []})
        finally:
            self.store_ops -= 1
            session.ephemeral.pop(f"save {job.id}", None)
            job.exec_pid, job.phase = None, None
            if channel is not None:
                await channel.close()
            source.pins.discard(f"save {job.id}")
            if source in session.internal_snaps:
                session.internal_snaps.remove(source)
            session._release(source)

    async def op_restore(self, req):
        """Load a checkpoint into a new session (--as) or an existing idle one, as a recorded, queued execution."""
        default = req.get("session")
        ref, manifest = self._manifest(req["checkpoint"], default or None)
        target = req.get("as") or default
        if not target:
            raise ValueError("restore needs --as NEW_SESSION or -s SESSION")
        if target not in self.sessions:
            await self.op_new({"name": target, "python": req.get("python") or manifest["python"],
                               "cwd": manifest["cwd"], "owner": req.get("owner")})
        if manifest.get("format") != 2:
            raise ValueError(f"checkpoint {ref} has format {manifest.get('format')}, which this Talo cannot read")
        units = manifest["units"]
        only = req.get("names") or None
        for u in units:
            if not re.fullmatch(r"[0-9a-f]{40}", str(u.get("hash"))):
                raise ValueError(f"checkpoint {ref} lists a malformed unit")
        code = (f"# --- restored from checkpoint {ref}\n"
                "import json as _talo_json\n"
                f"talo._load({str(home() / 'store')!r}, _talo_json.loads({json.dumps(json.dumps(units))}), "
                f"{only!r}, {manifest['modules']!r}, {manifest['versions']!r}, {manifest.get('imports') or {}!r})")
        self.store_ops += 1
        try:  # atomic: a unit that fails to load leaves the session as it was
            out = await self.op_run({"session": target, "code": code, "timeout": req.get("timeout", 300),
                                     "owner": req.get("owner"), "key": req.get("key"),
                                     "expect_rev": req.get("expect_rev"), "atomic": True})
            if out["status"] in ("queued", "running"):
                await self._wait(self.jobs[out["id"]], None)  # gc stays blocked until the restore is done
        finally:
            self.store_ops -= 1
        return {**out, "checkpoint": ref, "skipped": manifest["skipped"], "complete": manifest.get("complete")}

    def _save_registry(self) -> None:
        """sessions.json: what is needed to bring sessions back after a restart (see "talo recover")."""
        registry = self._load_registry()
        for s in self.sessions.values():
            registry[s.name] = {"python": s.python, "cwd": s.cwd, "limits": s.limits, "max_output": s.max_output,
                                "memory_limit": s.memory_limit, "autosave": s.autosave_config,
                                "last_checkpoint": s.last_checkpoint or registry.get(s.name, {}).get("last_checkpoint"),
                                "epoch": self.epoch, "alive": s.state != "dead"}
        path = home() / "sessions.json"
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(registry, indent=1))
        os.replace(tmp, path)

    def _load_registry(self) -> dict:
        path = home() / "sessions.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def _last_full_checkpoint(self, session: str) -> str | None:
        """The latest checkpoint of a session that saved all its variables (a selection is not a recovery point),
        read from the manifests themselves, so a crash between a manifest and the registry loses nothing."""
        best = None
        for path in self._checkpoint_dir(session).glob("*.json"):
            try:
                m = json.loads(path.read_text())
            except ValueError:
                continue
            if m.get("selection") is None and (best is None or m["created"] > best["created"]):
                best = m
        return f"{session}/{best['name']}" if best else None

    def _lost(self) -> list:
        """Sessions an earlier supervisor was running that are gone."""
        return [{"name": n, "python": r.get("python"), "cwd": r.get("cwd"),
                 "last_checkpoint": self._last_full_checkpoint(n)}
                for n, r in self._load_registry().items()
                if r.get("alive") and r.get("epoch") != self.epoch and n not in self.sessions]

    async def op_recover(self, req):
        """Recreate sessions lost in a restart, with their settings, and restore their last checkpoints."""
        registry = self._load_registry()
        names = [x["name"] for x in self._lost()] if req.get("all") else [req["name"]]
        out = []
        for name in names:
            r = registry.get(name)
            if r is None:
                raise KeyError(f"no record of session {name!r}")
            if name in self.sessions:
                raise ValueError(f"session {name} is running; recover brings back sessions that are gone")
            await self.op_new({"name": name, "python": r["python"], "cwd": r["cwd"], "limits": r.get("limits") or None,
                               "max_output": r.get("max_output"), "memory_limit": r.get("memory_limit"),
                               "owner": req.get("owner")})
            last = self._last_full_checkpoint(name)
            item = {"session": name, "checkpoint": last}
            if last:
                job = await self.op_restore({"checkpoint": last, "session": name,
                                             "timeout": req.get("timeout", 600), "owner": req.get("owner")})
                item["job"] = {k: job.get(k) for k in ("id", "status", "error")}
            if r.get("autosave"):
                self.sessions[name].autosave_config = r["autosave"]
            out.append(item)
        self._save_registry()
        return {"recovered": out}

    async def op_autosave(self, req):
        session = self._session(req["session"])
        if req.get("on") is not None:
            session.autosave_config = ({"every": float(req.get("every") or 1800), "keep": int(req.get("keep") or 3)}
                                       if req["on"] else None)
            self._save_registry()
        return {"session": session.name, "autosave": session.autosave_config}

    async def _autosave_loop(self) -> None:
        """Save idle sessions whose state changed, every `every` seconds at most (talo autosave)."""
        while True:
            await asyncio.sleep(AUTOSAVE_CHECK)
            for s in list(self.sessions.values()):
                cfg = s.autosave_config
                if not cfg or s.autosaving or s.state != "idle" or s.rev == s.autosave_rev or s.queue.qsize() or s.pending:
                    continue
                if time.time() - s.autosave_at < cfg["every"]:
                    continue
                s.autosave_at, s.autosaving = time.time(), True  # one autosave at a time per session
                try:
                    out = await self.op_save({"session": s.name, "name": "auto-" + time.strftime("%Y%m%d-%H%M%S"),
                                              "auto": True, "timeout": None, "owner": "autosave"})
                    if out["status"] == "ok":
                        s.autosave_rev = out["result"]["rev"]  # the revision the checkpoint captured
                    else:
                        log.warning("autosave of session %s: %s", s.name, (out.get("error") or {}).get("evalue"))
                except Exception:  # noqa: BLE001
                    log.exception("autosave of session %s failed", s.name)
                finally:
                    s.autosaving = False

    async def op_checkpoints(self, req):
        root = home() / "checkpoints"
        out = []
        for path in sorted(root.glob("*/*.json")) if root.exists() else []:
            if req.get("session") and path.parent.name != req["session"]:
                continue
            m = json.loads(path.read_text())
            out.append({k: m.get(k) for k in ("session", "name", "created", "auto", "rev", "bytes", "new_bytes",
                                                 "owner")} | {"variables": sum(len(u["names"]) for u in m["units"]),
                                                              "skipped": len(m["skipped"])})
        return {"checkpoints": out}

    async def op_checkpoint_drop(self, req):
        ref, _ = self._manifest(req["checkpoint"], req.get("session"))
        session, _, name = ref.partition("/")
        (self._checkpoint_dir(session) / f"{name}.json").unlink()
        return {"dropped": ref}

    async def op_store_gc(self, req):
        """Delete store units no checkpoint uses (and abandoned temporary ones older than an hour)."""
        import shutil
        if self.store_ops:
            raise RuntimeError(f"{self.store_ops} saves or restores are in progress; run gc when they are done")
        store = home() / "store"
        used_units, used_buffers = set(), set()
        for path in (home() / "checkpoints").glob("*/*.json"):
            used_units |= {u["hash"] for u in json.loads(path.read_text())["units"]}
        for h in used_units:
            index = store / "units" / h / "index.json"
            if index.exists():
                used_buffers |= {b for b, _ in json.loads(index.read_text())["buffers"]}
        freed, removed = 0, 0
        for kind, used in (("units", used_units), ("buffers", used_buffers)):
            folder = store / kind
            for entry in folder.iterdir() if folder.exists() else []:
                age = time.time() - entry.lstat().st_mtime
                if entry.name in used or (entry.name.startswith(".tmp-") and age < 3600):
                    continue
                size = (sum(f.stat().st_size for f in entry.iterdir() if f.is_file()) if entry.is_dir()
                        else entry.lstat().st_size)
                if not req.get("dry_run"):
                    shutil.rmtree(entry, ignore_errors=True) if entry.is_dir() else entry.unlink(missing_ok=True)
                freed, removed = freed + size, removed + 1
        return {"removed": removed, "freed_bytes": freed, "dry_run": bool(req.get("dry_run"))}

    # ---------------------------------------------------------------- verify

    async def op_verify(self, req):
        """Replay the lineage of the current state and compare (see "talo verify" in docs/design.md)."""
        session = self._session(req["session"])
        cold, limit = bool(req.get("cold")), req.get("max_bytes", DIFF_MAX_BYTES)
        job = await self._submit(session, "fork", owner=req.get("owner"))  # the target, after running jobs
        await job.done.wait()
        if job.status != "ok":
            raise RuntimeError(f"could not snapshot the current state: {(job.error or {}).get('evalue')}")
        target = job.handle
        target.pins.add(f"verify {job.id}")
        session.internal_snaps.append(target)
        report = {"mode": "cold" if cold else "hot", "target": {"rev": job.result["rev"], "base": target.parent,
                                                                "jobs_since_base": list(target.jobs)},
                  "source": None, "replayed": [], "hazards": [], "inconclusive": []}
        channel, source = None, None
        try:
            out_dir = session.dir / "verify" / job.id
            out_dir.mkdir(parents=True, exist_ok=True)
            try:
                lineage = self._lineage(self._records(), target.parent, list(target.jobs))
            except KeyError as error:
                report.update(status="inconclusive", inconclusive=[f"incomplete lineage: {error.args[0]}"])
                return report
            alive = {s.ref: s for x in self.sessions.values() for s in x.snapshots.values()}
            start = 0
            if not cold:
                marks = [i for i, r in enumerate(lineage) if r["kind"] == "snap" and r["result"]["ref"] in alive]
                if req.get("from"):
                    ref = self._snapshot(req["from"], session.name).ref
                    marks = [i for i in marks if lineage[i]["result"]["ref"] == ref]
                    if not marks:
                        raise RuntimeError(f"{req['from']} is not on the lineage of the current state")
                if not marks:
                    report.update(status="inconclusive",
                                  inconclusive=["no snapshot of the lineage is kept; `talo verify --cold` replays from the start"])
                    return report
                start = marks[0] + 1
                source = alive[lineage[marks[0]]["result"]["ref"]]
                source.pins.add(f"verify {job.id}")
                report["source"] = {"snapshot": source.ref, "name": source.name}
                channel = await ephemeral.Channel.from_snapshot(source.socket)
            else:
                env = {"MPLBACKEND": "Agg", **os.environ, **session.env, "PYTHONUNBUFFERED": "1"}
                prefix = scope_command(session.limits, f"talo-verify-{job.id}-{wire.new_id()[:6]}") \
                    if session.limits else None  # the session's limits apply to the replay too
                channel = await ephemeral.Channel.fresh(session.python, str(WORKER), session.cwd, env, prefix)
            session.ephemeral[f"verify {job.id}"] = channel.pid
            for rec in lineage[start:]:
                if rec["kind"] == "reload":
                    report["inconclusive"].append(f"job {rec['id']} reloaded modules; their sources may have changed")
                    continue
                if rec["kind"] != "execute":
                    continue
                if rec["status"] != "ok":
                    report["hazards"].append(f"job {rec['id']} failed ({rec['status']}) and may have changed the "
                                             "state partly; it is replayed too")
                options = rec.get("options") or {}
                got = await channel.execute(rec["code"], str(out_dir / f"{rec['id']}.out"),
                                            silent=bool(options.get("silent")),
                                            store_history=options.get("store_history", True))
                report["replayed"].append({"job": rec["id"], "original": rec["status"], "replay": got["status"]})
                if rec["status"] == "ok" and got["status"] != "ok":
                    error = got["error"] or {}
                    report.update(status="replay_failed", failed={"job": rec["id"], "ename": error.get("ename"),
                                                                    "evalue": str(error.get("evalue"))[:500]})
                    return report

            async def ask_replay(op, **params):
                reply, _ = await channel.request(f"talo_{op}_request", params, timeout=3600)
                return reply["content"]
            diff = await compare_states(ask_replay, lambda op, **kw: self._ask_state(target, op, **kw), limit)
            report["comparison"] = diff
            differs = diff["added"] or diff["removed"] or diff["changed"]
            report["status"] = ("mismatch" if differs else "inconclusive" if report["inconclusive"] or
                                diff["not_compared"] else "match")
            return report
        finally:
            session.ephemeral.pop(f"verify {job.id}", None)
            if channel is not None:
                await channel.close()
            if source is not None:
                source.pins.discard(f"verify {job.id}")
            target.pins.discard(f"verify {job.id}")
            if target in session.internal_snaps:
                session.internal_snaps.remove(target)
            session._release(target)

    async def op_diff(self, req):
        session = req.get("session") or "default"
        (ref_a, a), (ref_b, b) = self._state(req["a"], session), self._state(req.get("b") or "@", session)
        limit = req.get("max_bytes", DIFF_MAX_BYTES)
        out = await compare_states(lambda op, **kw: self._ask_state(a, op, **kw),
                                   lambda op, **kw: self._ask_state(b, op, **kw), limit)
        return {"a": ref_a, "b": ref_b, **out}

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
        if op in ("job", "output", "interrupt", "artifacts", "diagnose"):
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
        autosave = asyncio.create_task(self._autosave_loop())
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
