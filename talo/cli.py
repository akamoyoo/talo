"""The talo command line. ``--json`` prints machine-readable output (stable fields, for agents and scripts); the
default output is for humans. See ``talo --help``."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from talo import __version__
from talo.client import TaloError, call

DEFAULT_SESSION = "default"
TIMEOUT = 30.0
LINES = 40


def session_name(args) -> str:
    return args.session or os.environ.get("TALO_SESSION") or DEFAULT_SESSION


def human_bytes(n) -> str:
    if n is None:
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def tail_lines(text: str, lines: int) -> tuple[str, int]:
    rows = text.splitlines()
    if len(rows) <= lines:
        return text, 0
    return "\n".join(rows[-lines:]), len(rows) - lines


def emit(args, data: dict, human) -> None:
    if args.json:
        print(json.dumps(data, ensure_ascii=False, default=str))
    else:
        human(data)


# ---------------------------------------------------------------- jobs

def show_job(args, job: dict) -> None:
    """A job: the last lines of its output, its result or error, and a status line."""
    lines = getattr(args, "lines", LINES)
    out = call("output", id=job["id"], max_bytes=256 << 10)
    text, cut = tail_lines(out["text"], lines)
    if args.json:
        print(json.dumps({"job": job, "output": {"text": text, "lines_cut": cut, "bytes_cut": out["offset"],
                                                 "size": out["size"]}}, ensure_ascii=False, default=str))
        return
    if out["offset"] or cut:
        earlier = f"{cut} lines" + (f" / {human_bytes(out['offset'])}" if out["offset"] else "")
        print(f"... [{earlier} earlier; `talo tail {job['id']} --all` shows everything]")
    if text:
        print(text.rstrip("\n"))
    if job.get("error"):
        print("\n".join(job["error"].get("traceback") or [f"{job['error'].get('ename')}: {job['error'].get('evalue')}"]))
    for o in job.get("outputs") or []:
        print(f"[{o['mime']}: {o['path']}]")
    if job.get("result") and "data" in job["result"]:
        print(job["result"]["data"].get("text/plain", ""))
    elif job.get("result"):  # a finished snap / checkout
        print(json.dumps(job["result"], ensure_ascii=False, default=str)[:2000])
    sys.stdout.flush()  # output first, the status line (stderr) last
    print(status_line(job), file=sys.stderr)


def status_line(job: dict) -> str:
    status, jid = job["status"], job["id"]
    elapsed = f" {job['elapsed_s']:.2f}s" if job.get("elapsed_s") is not None else ""
    count = f" [{job['execution_count']}]" if job.get("execution_count") else ""
    if status in ("running", "queued"):
        return (f"[job {jid} {status}{elapsed}] `talo wait {jid}` for the result, `talo tail {jid}` for output, "
                f"`talo interrupt {jid}` to stop it")
    mem = ""
    m = job.get("metrics") or {}
    if m.get("talo_rss_after") and m.get("talo_rss_before"):
        delta = m["talo_rss_after"] - m["talo_rss_before"]
        mem = f", memory {human_bytes(m['talo_rss_after'])} ({'+' if delta >= 0 else '-'}{human_bytes(abs(delta))})"
    return f"[job {jid} {status}{count}{elapsed}{mem}]"


def exit_code(job: dict) -> int:
    """0 ok, 1 failed (error / interrupted / lost / cancelled), 3 still running or queued."""
    return {"ok": 0, "running": 3, "queued": 3}.get(job["status"], 1)


# ---------------------------------------------------------------- commands

def parse_size(text: str | None) -> int | None:
    """'200G', '512M', '1.5T' or a number of bytes."""
    if not text:
        return None
    units = {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}
    text = text.strip().upper().rstrip("B")
    if text and text[-1] in units:
        return int(float(text[:-1]) * units[text[-1]])
    return int(text)


def cmd_new(args):
    if args.from_ and (args.python or args.cwd):
        raise TaloError("BadArgument", "a session started --from a snapshot inherits its interpreter and working "
                                       "directory; --python / --cwd cannot change them")
    s = call("new", name=args.name, python=args.python or (None if args.from_ else sys.executable),
             cwd=os.path.abspath(args.cwd or os.getcwd()), memory_budget=parse_size(args.memory_budget),
             **({"from": args.from_} if args.from_ else {}))
    origin = f", forked from {s['base']}" if s.get("base") else ""
    emit(args, s, lambda d: print(f"session {d['name']} started: pid {d['pid']}, {d['python']}, cwd {d['cwd']}{origin}"))


def cmd_ls(args):
    def human(d):
        if not d["sessions"]:
            print("no sessions (`talo new NAME` creates one; `talo run` creates `default` on first use)")
        for s in d["sessions"]:
            extra = f", running job {s['current_job']}" if s["current_job"] else ""
            extra += f", {s['queued']} queued" if s["queued"] else ""
            extra += f": {s['death']}" if s["state"] == "dead" else ""
            print(f"{s['name']:16s} {s['state']:8s} pid {s['pid']}  memory {human_bytes(s['rss'])}  {s['python']}{extra}")
    emit(args, call("ls"), human)


def cmd_kill(args):
    emit(args, call("kill", name=args.name), lambda d: print(f"session {d['killed']} closed"))


def ensure_session(name: str, python: str | None = None) -> None:
    sessions = {s["name"]: s for s in call("ls")["sessions"]}
    if name not in sessions and name != DEFAULT_SESSION:  # a typo must not silently create a new empty session
        raise TaloError("NoSession", f"no session {name!r}; `talo new {name}` creates it (`talo ls` lists sessions)")
    if name not in sessions:
        s = call("new", name=name, python=python or sys.executable, cwd=os.getcwd())
        print(f"(new session {name}: pid {s['pid']}, {s['python']})", file=sys.stderr)


def read_code(args) -> str:
    if args.file:
        with open(args.file) as f:
            return f.read()
    if args.code == "-" or (args.code is None and not sys.stdin.isatty()):
        return sys.stdin.read()
    if args.code is None:
        raise SystemExit("nothing to run: `talo run 'x = 1'`, `talo run -f script.py`, or `talo run - < script.py`")
    return args.code


def cmd_run(args):
    name = session_name(args)
    code = read_code(args)
    ensure_session(name, args.python)
    job = call("run", session=name, code=code, timeout=args.timeout, key=args.key, expect_rev=args.expect_rev)
    show_job(args, job)
    return exit_code(job)


def cmd_wait(args):
    job = call("job", id=args.id, wait=args.timeout)
    show_job(args, job)
    return exit_code(job)


def cmd_tail(args):
    if args.follow and args.json:
        raise TaloError("BadArgument", "tail -f has no --json mode; use `talo wait ID --json` or "
                                       "`talo tail ID --json --offset N` to read output incrementally")
    if args.follow:
        offset = 0
        while True:
            out = call("output", id=args.id, offset=offset, max_bytes=1 << 20)
            sys.stdout.write(out["text"])
            sys.stdout.flush()
            offset = out["next_offset"]
            if out["status"] not in ("running", "queued") and offset >= out["size"]:
                break
            time.sleep(0.5)
        print(status_line(call("job", id=args.id)), file=sys.stderr)
        return
    if args.offset is not None:  # incremental reads: pass back next_offset
        out = call("output", id=args.id, offset=args.offset, max_bytes=256 << 10)
        emit(args, out, lambda d: sys.stdout.write(d["text"]))
        return
    out = call("output", id=args.id, max_bytes=(1 << 40) if args.all else 256 << 10)
    text, cut = (out["text"], 0) if args.all else tail_lines(out["text"], args.lines)
    if args.json:
        print(json.dumps({**out, "text": text, "lines_cut": cut}, ensure_ascii=False))
        return
    if cut or (out["offset"] and not args.all):
        print(f"... [{cut} lines earlier; --all shows everything]")
    print(text.rstrip("\n"))


def cmd_jobs(args):
    def human(d):
        for j in d["jobs"]:
            preview = j.get("code_preview", "").strip().splitlines()
            print(f"{status_line(j)}  {(preview[0] if preview else '')[:80]}")
    session = None if args.all_sessions else session_name(args)
    emit(args, call("jobs", session=session, last=args.last), human)


def cmd_interrupt(args):
    emit(args, call("interrupt", id=args.id), lambda d: print(f"job {d['id']}: {d['result']}"))


def cmd_vars(args):
    def human(d):
        rows = d["variables"]
        if not rows:
            print("(no variables)")
        for v in rows:
            shape = f" {tuple(v['shape'])}" if "shape" in v else (f" len={v['len']}" if "len" in v else "")
            dtype = f" {v['dtype']}" if "dtype" in v else ""
            value = f" = {v['value']}" if "value" in v else ""
            size = ("≥ " if v.get("nbytes_partial") else "") + human_bytes(v.get("nbytes"))
            print(f"{v['name']:20s} {v['type']}{shape}{dtype}  {size}{value}")
        sys.stdout.flush()
        print(f"(worker memory {human_bytes(d.get('rss'))})", file=sys.stderr)
    emit(args, call("vars", session=session_name(args), pattern=args.pattern), human)


def cmd_show(args):
    d = call("show", session=session_name(args), name=args.name, rows=args.rows)
    if d.get("status") == "error":
        if args.json:
            print(json.dumps(d, ensure_ascii=False))
        else:
            print(f"{d['ename']}: {d['evalue']}")
        return 1
    emit(args, d, lambda x: print(x["text"]))


def cmd_history(args):
    def human(d):
        for h in d["history"]:
            code = h["code"].strip().splitlines()
            more = " ..." if len(code) > 1 else ""
            print(f"{status_line(h)}  {(code[0] if code else '')[:100]}{more}")
    emit(args, call("history", session=session_name(args), last=args.last), human)


def pending(args, d: dict) -> int | None:
    """Queued operations (snap, checkout) return ``{"job": ...}`` when they did not finish within the timeout."""
    if "job" not in d:
        return None
    show_job(args, d["job"])
    return exit_code(d["job"])


def cmd_snap(args):
    d = call("snap", session=session_name(args), name=args.name, message=args.message or "", strict=args.strict,
             timeout=args.timeout, key=args.key)
    if (code := pending(args, d)) is not None:
        return code
    def human(x):
        threads = f"; {len(x['threads'])} other threads at fork time" if x.get("threads") else ""
        print(f"snapshot {x['name']} ({x['ref']}): pid {x['pid']}, after {len(x['jobs'])} jobs since "
              f"{x['parent'] or 'session start'}{threads}")
    emit(args, d, human)


def cmd_snaps(args):
    def human(d):
        if not d["snapshots"]:
            print("(no snapshots; `talo snap NAME` takes one)")
        for s in d["snapshots"]:
            here = "  <- base of the current worker" if s["ref"] == d["base"] else ""
            alive = "" if s["alive"] else "  (gone)"
            msg = f"  {s['message']}" if s["message"] else ""
            print(f"{s['name']:16s} {s['ref']:20s} parent {s['parent'] or '-':20s} private {human_bytes(s['private_bytes'])}"
                  f"  jobs {','.join(s['jobs']) or '-'}{msg}{alive}{here}")
        if d["jobs_since_base"]:
            print(f"(jobs since the base: {', '.join(d['jobs_since_base'])})")
    emit(args, call("snaps", session=session_name(args)), human)


def cmd_checkout(args):
    d = call("checkout", session=session_name(args), name=args.name, discard=args.discard, snap=args.snap,
             timeout=args.timeout, expect_rev=args.expect_rev, key=args.key)
    if (code := pending(args, d)) is not None:
        return code
    emit(args, d, lambda x: print(f"session {x['name']} now runs a fork of {x['base']} (pid {x['pid']})"))


def cmd_drop(args):
    emit(args, call("drop", session=session_name(args), name=args.name), lambda d: print(f"dropped {d['dropped']}"))


def cmd_attach(args):
    if args.json:
        raise TaloError("BadArgument", "attach is interactive and has no --json mode; use run / wait / tail")
    from talo.attach import Attach
    return Attach(session_name(args)).loop()


def cmd_export(args):
    from talo import export
    session = session_name(args)
    d = call("export", session=session, snap=args.snap, all=args.all)
    to = args.to or ("ipynb" if args.output and args.output.endswith(".ipynb") else "py")
    notes = "".join(f"\nnote {note_line(n)}" for n in d.get("notes", []))
    what = "every job" if args.all else (f"the path to snapshot {args.snap}" if args.snap else "lineage of the current state")
    text = (export.to_ipynb if to == "ipynb" else export.to_py)(d["jobs"], export.header(session, what) + notes)
    if not args.output:
        if args.json:
            print(json.dumps({"format": to, "jobs": len(d["jobs"]), "text": text}, ensure_ascii=False))
        else:
            sys.stdout.write(text)
        return
    with open(args.output, "w") as f:
        f.write(text)
    summary = {"path": os.path.abspath(args.output), "format": to, "jobs": len(d["jobs"])}
    emit(args, summary, lambda x: print(f"wrote {x['path']} ({x['jobs']} records, {x['format']})"))


def cmd_note(args):
    d = call("note", session=session_name(args), text=" ".join(args.text), job=args.job)
    emit(args, d, lambda x: print(f"noted at revision {x['note']['rev']}"))


def note_line(n: dict) -> str:
    job = f" · job {n['job']}" if n.get("job") else ""
    return f"[rev {n['rev']} · {n['owner']}{job}] {n['text']}"


def cmd_notes(args):
    def human(d):
        if not d["notes"]:
            print("(no notes; `talo note TEXT` adds one)")
        for n in d["notes"]:
            print(note_line(n))
    emit(args, call("notes", session=session_name(args), last=args.last), human)


def cmd_brief(args):
    def human(d):
        s = d["session"]
        since = len(s["jobs_since_base"])
        print(f"session {s['name']}  {s['state']}  rev {s['rev']}  base {s['base'] or '(session start)'}  "
              f"{since} job{'s' if since != 1 else ''} since  memory {human_bytes(s['rss'])} "
              f"(private {human_bytes(d['private_bytes'])})")
        if s.get("death"):
            print(f"dead: {s['death']}")
        notes = d["notes"]
        if notes["last"]:
            more = f" (last {len(notes['last'])} of {notes['total']}; `talo notes`)" if notes["total"] > len(notes["last"]) else ""
            print(f"notes{more}")
            for n in notes["last"]:
                print(f"  {note_line(n)[:300]}")
        if d["snapshots"]:
            print("snapshots  " + "  ·  ".join(
                f"{x['name']} ({x['ref']})" + (f" \"{x['message'][:60]}\"" if x["message"] else "") for x in d["snapshots"]))
        v = d["variables"]
        if "largest" in v:
            items = [f"{x['name']} {x['type']}" + (f" {tuple(x['shape'])}" if "shape" in x else "")
                     + f" {human_bytes(x.get('nbytes'))}" for x in v["largest"]]
            stale = f", as of rev {v['observed_rev']}" if v["stale"] else ""
            print(f"variables (largest {len(items)} of {v['total']}{stale})  " + " · ".join(items))
        else:
            print(f"variables  ({v['unavailable']})")

        def line(j):
            preview = (j["code_preview"].strip().splitlines() or [""])[0][:70]
            err = f"  {j['error'][:120]}" if j.get("error") else ""
            el = f" {j['elapsed_s']:.1f}s" if j.get("elapsed_s") is not None else ""
            kind = "" if j["kind"] == "execute" else f"{j['kind']} "
            return f"#{j['id']} {kind}{j['status']}{el} [{j['owner']}]  {preview}{err}"
        recent = d["recent_jobs"]
        if recent["last"]:
            print(f"recent jobs (last {len(recent['last'])} of {recent['total']})")
            for j in recent["last"]:
                print(f"  {line(j)}")
        for j in d["active_jobs"]:
            print(f"active  {line(j)}")
    emit(args, call("brief", session=session_name(args), notes=args.notes, jobs=args.jobs), human)


def cmd_tree(args):
    def human(d):
        nodes = d["nodes"]
        snaps = {n["ref"]: n for n in nodes if n["kind"] == "snapshot"}
        children: dict = {}
        roots: list = []  # (label, parent key) for the tops of the tree
        for n in nodes:
            parent = n["parent"]
            if parent is None:
                key = ("empty", n["session"])
            elif parent in snaps:
                key = parent
            else:
                key = ("dropped", parent)
            children.setdefault(key, []).append(n)
            if not isinstance(key, str) and key not in roots:
                roots.append(key)

        def label(n):
            if n["kind"] == "worker":
                k = len(n["jobs_since_base"])
                return f"* {n['session']:24s} {n['state']}, {k} job{'s' if k != 1 else ''} since"
            alive = "" if n["alive"] else "  (gone)"
            msg = f"  {n['message']}" if n["message"] else ""
            return (f"{n['name']:12s} {n['ref']:14s} {human_bytes(n['private_bytes'])} private"
                    f"   jobs {','.join(n['jobs']) or '-'}{msg}{alive}")

        def walk(key, prefix):
            kids = sorted(children.get(key, []), key=lambda n: (n["kind"] == "worker", n.get("created", 0)))
            for i, n in enumerate(kids):
                last = i == len(kids) - 1
                print(f"{prefix}{'└─ ' if last else '├─ '}{label(n)}")
                if n["kind"] == "snapshot":
                    walk(n["ref"], prefix + ("   " if last else "│  "))

        for root in roots:
            print(f"(empty start: {root[1]})" if root[0] == "empty" else f"(dropped {root[1]})")
            walk(root, "")
        if not nodes:
            print("(no sessions)")
    emit(args, call("tree"), human)


def cmd_diff(args):
    limit = None if args.content else parse_size(args.max_bytes)
    d = call("diff", session=session_name(args), a=args.a, b=args.b, max_bytes=limit)

    def brief(v):
        shape = f" {tuple(v['shape'])}" if "shape" in v else (f" len={v['len']}" if "len" in v else "")
        return f"{v['type']}{shape}{' ' + v['dtype'] if 'dtype' in v else ''}"

    def human(x):
        print(f"--- {x['a']}\n+++ {x['b']}")
        for v in x["added"]:
            print(f"+ {v['name']:20s} {brief(v)}  {human_bytes(v.get('nbytes'))}")
        for v in x["removed"]:
            print(f"- {v['name']:20s} {brief(v)}")
        for c in x["changed"]:
            if c["fields"] == ["content"]:
                what = f"{brief(c['b'])}  content"
            else:
                what = "; ".join(f"{k} {c['a'].get(k, '-')} -> {c['b'].get(k, '-')}" for k in c["fields"])
            print(f"~ {c['name']:20s} {what}")
        notes = f"{x['unchanged']} unchanged"
        large = [f"{n['name']} {human_bytes(n['nbytes'])}" for n in x["not_compared"] if n["reason"] == "size"]
        if large:
            notes += (f"; not compared by content (over {human_bytes(x['max_bytes'])}, --content compares them): "
                      + ", ".join(large))
        other = [f"{n['name']} ({n['reason']})" for n in x["not_compared"] if n["reason"] != "size"]
        if other:
            notes += "; not comparable: " + ", ".join(other)
        print(f"({notes})", file=sys.stderr)
    emit(args, d, human)


def cmd_stop(args):
    try:
        call("shutdown", autostart=False)
        stopped = True
    except TaloError as error:
        if error.type != "NotRunning":
            raise
        stopped = False
    emit(args, {"stopped": stopped}, lambda d: print("supervisor stopped (all sessions closed)" if d["stopped"]
                                                     else "the supervisor is not running"))


def cmd_daemon(args):
    from talo import supervisor
    supervisor.main()


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="talo", description="A resident in-memory Python session: load data once, "
                                                          "keep it in memory, drive it from the terminal.")
    ap.add_argument("--version", action="version", version=f"talo {__version__}")
    sub = ap.add_subparsers(dest="command", required=True)

    def add(name, fn, help_, session=True):
        p = sub.add_parser(name, help=help_, description=help_)
        p.add_argument("--json", action="store_true", help="machine-readable output (for agents and scripts)")
        p.add_argument("--owner", help="who is acting (default: $TALO_AGENT, else user); recorded on jobs and notes")
        if session:
            p.add_argument("-s", "--session", help=f"session name (default: $TALO_SESSION or {DEFAULT_SESSION})")
        p.set_defaults(fn=fn)
        return p

    p = add("new", cmd_new, "create a session", session=False)
    p.add_argument("name")
    p.add_argument("--python", help="interpreter for the worker (default: the one running talo)")
    p.add_argument("--cwd", help="working directory of the worker (default: the current directory)")
    p.add_argument("--from", dest="from_", metavar="[SESSION:]SNAPSHOT",
                   help="start from a snapshot instead of an empty interpreter (a branch)")
    p.add_argument("--memory-budget", metavar="SIZE",
                   help="refuse new snapshots when the worker plus its snapshots use more than this (e.g. 200G)")
    add("ls", cmd_ls, "list sessions", session=False)
    p = add("kill", cmd_kill, "close a session (its variables are freed)", session=False)
    p.add_argument("name")
    p = add("run", cmd_run, "run code in a session")
    p.add_argument("code", nargs="?", help="code to run; - reads it from stdin")
    p.add_argument("-f", "--file", help="run the code in this file")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT,
                   help=f"seconds to wait (default {TIMEOUT:.0f}); if the code is still running, the job id is "
                        "returned and the computation continues in the background")
    p.add_argument("-n", "--lines", type=int, default=LINES, help=f"show only the last N output lines (default {LINES})")
    p.add_argument("--python", help="interpreter for the session if it has to be created")
    p.add_argument("--key", help="idempotency key: retrying with the same key returns the job already submitted")
    p.add_argument("--expect-rev", type=int, metavar="N", help="refuse to run unless the session is at revision N")
    p = add("wait", cmd_wait, "wait for a job to finish", session=False)
    p.add_argument("id")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT)
    p.add_argument("-n", "--lines", type=int, default=LINES)
    p = add("tail", cmd_tail, "show a job's output", session=False)
    p.add_argument("id")
    p.add_argument("-n", "--lines", type=int, default=LINES)
    p.add_argument("--all", action="store_true", help="the whole output")
    p.add_argument("--offset", type=int, help="read from this byte offset (the reply's next_offset continues)")
    p.add_argument("-f", "--follow", action="store_true", help="follow the output until the job finishes")
    p = add("jobs", cmd_jobs, "list jobs")
    p.add_argument("--last", type=int, default=20)
    p.add_argument("--all-sessions", action="store_true", help="jobs of every session")
    p = add("interrupt", cmd_interrupt, "interrupt a running job (a queued job is cancelled)", session=False)
    p.add_argument("id")
    p = add("vars", cmd_vars, "list the session's variables (name, type, shape, memory)")
    p.add_argument("pattern", nargs="?", help="only names containing this string")
    p = add("show", cmd_show, "summarize a variable or expression")
    p.add_argument("name", help="a variable or expression, e.g. df, df.x, d['k']")
    p.add_argument("-r", "--rows", type=int, default=10)
    p = add("history", cmd_history, "execution history")
    p.add_argument("--last", type=int, default=20)
    p = add("snap", cmd_snap, "snapshot the session's in-memory state (queued after running jobs)")
    p.add_argument("name", nargs="?", help="snapshot name (default s1, s2, ...)")
    p.add_argument("-m", "--message")
    p.add_argument("--strict", action="store_true", help="refuse when the worker has other threads (see docs)")
    p.add_argument("--key", help="idempotency key: retrying with the same key returns the job already submitted")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT, help="seconds to wait (it stays queued after)")
    add("snaps", cmd_snaps, "list the session's snapshots with their private memory")
    p = add("checkout", cmd_checkout, "replace the session's worker with a fork of a snapshot (roll back)")
    p.add_argument("name", help="[SESSION:]SNAPSHOT")
    p.add_argument("--discard", action="store_true", help="drop the state of jobs run since the current base")
    p.add_argument("--snap", metavar="NAME", help="snapshot the current state under NAME before checking out")
    p.add_argument("--expect-rev", type=int, metavar="N", help="refuse unless the session is at revision N")
    p.add_argument("--key", help="idempotency key: retrying with the same key returns the job already submitted")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT, help="seconds to wait (it stays queued after)")
    p = add("drop", cmd_drop, "free a snapshot")
    p.add_argument("name")
    add("attach", cmd_attach, "an interactive prompt on the session (Ctrl-D detaches; the session keeps running)")
    p = add("export", cmd_export, "write the code that produced the current state as a .py script or .ipynb notebook")
    p.add_argument("-o", "--output", help="file to write (default: standard output)")
    p.add_argument("--to", choices=("py", "ipynb"), help="format (default: from the file name, else py)")
    p.add_argument("--snap", metavar="NAME", help="export the path to this snapshot instead")
    p.add_argument("--all", action="store_true", help="every job of the session, including rolled-back ones")
    p = add("brief", cmd_brief, "what an agent needs to resume the session's work (state, notes, variables, jobs)")
    p.add_argument("--notes", type=int, default=10, help="most recent notes shown")
    p.add_argument("--jobs", type=int, default=8, help="most recent finished jobs shown")
    p = add("note", cmd_note, "attach a note (goal, finding, next step) to the session's current state")
    p.add_argument("text", nargs="+")
    p.add_argument("--job", help="the job the note is about")
    p = add("notes", cmd_notes, "the session's notes")
    p.add_argument("--last", type=int)
    add("tree", cmd_tree, "all snapshots and live workers of all sessions, as a tree", session=False)
    p = add("diff", cmd_diff, "variables added, removed or changed between two states")
    p.add_argument("a", help="[SESSION:]SNAPSHOT, or @ / SESSION:@ for a live worker")
    p.add_argument("b", nargs="?", default="@", help="as A (default @: the session's live worker)")
    p.add_argument("--max-bytes", default="256M", help="compare content only for variables up to this size")
    p.add_argument("--content", action="store_true", help="compare the content of all variables, whatever their size")
    add("stop", cmd_stop, "stop the supervisor (closes all sessions)", session=False)
    add("daemon", cmd_daemon, "run the supervisor in the foreground (for debugging; normally started automatically)",
        session=False)
    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if getattr(args, "owner", None):
        os.environ["TALO_AGENT"] = args.owner  # read by every request (client.call)
    try:
        try:
            return args.fn(args) or 0
        except (ValueError, OSError) as error:  # bad sizes, unreadable files: before or instead of a request
            raise TaloError("BadArgument", str(error)) from None
    except TaloError as error:
        if getattr(args, "json", False):
            print(json.dumps({"error": {"type": error.type, "message": str(error)}}, ensure_ascii=False))
        else:
            print(f"talo: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
