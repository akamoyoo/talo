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
    if job.get("result"):
        print(job["result"]["data"].get("text/plain", ""))
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

def cmd_new(args):
    s = call("new", name=args.name, python=args.python or sys.executable, cwd=os.path.abspath(args.cwd or os.getcwd()))
    emit(args, s, lambda d: print(f"session {d['name']} started: pid {d['pid']}, {d['python']}, cwd {d['cwd']}"))


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
    job = call("run", session=name, code=code, timeout=args.timeout)
    show_job(args, job)
    return exit_code(job)


def cmd_wait(args):
    job = call("job", id=args.id, wait=args.timeout)
    show_job(args, job)
    return exit_code(job)


def cmd_tail(args):
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
    out = call("output", id=args.id, max_bytes=(1 << 30) if args.all else 256 << 10)
    text, cut = (out["text"], 0) if args.all else tail_lines(out["text"], args.lines)
    if args.json:
        print(json.dumps({**out, "text": text, "lines_cut": cut}, ensure_ascii=False))
        return
    if cut or (out["offset"] and not args.all):
        print(f"... [{cut} lines earlier; --all shows everything]")
    print(text.rstrip("\n"))


def cmd_jobs(args):
    def human(d):
        for j in d["jobs"][-args.last:]:
            print(status_line(j))
    emit(args, call("jobs", session=args.session), human)


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


def cmd_stop(args):
    try:
        call("shutdown", autostart=False)
        print("supervisor stopped (all sessions closed)")
    except TaloError as error:
        print(error)


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
        if session:
            p.add_argument("-s", "--session", help=f"session name (default: $TALO_SESSION or {DEFAULT_SESSION})")
        p.set_defaults(fn=fn)
        return p

    p = add("new", cmd_new, "create a session", session=False)
    p.add_argument("name")
    p.add_argument("--python", help="interpreter for the worker (default: the one running talo)")
    p.add_argument("--cwd", help="working directory of the worker (default: the current directory)")
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
    p = add("wait", cmd_wait, "wait for a job to finish", session=False)
    p.add_argument("id")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT)
    p.add_argument("-n", "--lines", type=int, default=LINES)
    p = add("tail", cmd_tail, "show a job's output", session=False)
    p.add_argument("id")
    p.add_argument("-n", "--lines", type=int, default=LINES)
    p.add_argument("--all", action="store_true", help="the whole output")
    p.add_argument("-f", "--follow", action="store_true", help="follow the output until the job finishes")
    p = add("jobs", cmd_jobs, "list jobs")
    p.add_argument("--last", type=int, default=20)
    p = add("interrupt", cmd_interrupt, "interrupt a running job (a queued job is cancelled)", session=False)
    p.add_argument("id")
    p = add("vars", cmd_vars, "list the session's variables (name, type, shape, memory)")
    p.add_argument("pattern", nargs="?", help="only names containing this string")
    p = add("show", cmd_show, "summarize a variable or expression")
    p.add_argument("name", help="a variable or expression, e.g. df, df.x, d['k']")
    p.add_argument("-r", "--rows", type=int, default=10)
    p = add("history", cmd_history, "execution history")
    p.add_argument("--last", type=int, default=20)
    add("stop", cmd_stop, "stop the supervisor (closes all sessions)", session=False)
    add("daemon", cmd_daemon, "run the supervisor in the foreground (for debugging; normally started automatically)",
        session=False)
    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args) or 0
    except TaloError as error:
        if getattr(args, "json", False):
            print(json.dumps({"error": {"type": error.type, "message": str(error)}}, ensure_ascii=False))
        else:
            print(f"talo: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
