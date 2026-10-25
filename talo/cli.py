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

MAX_RESULT = 10_000


def condense(text: str) -> str:
    """Output as an agent should see it: lines rewritten with carriage returns (progress bars) keep their final
    state, and runs of identical lines are folded into one with a count. The output file is never changed."""
    rows = []
    for line in text.split("\n"):
        if "\r" in line:
            parts = [x for x in line.split("\r") if x]
            line = parts[-1] if parts else ""
        rows.append(line)
    out, i = [], 0
    while i < len(rows):
        j = i
        while j + 1 < len(rows) and rows[j + 1] == rows[i]:
            j += 1
        out.append(rows[i])
        if j - i >= 2:
            out.append(f"[... repeated {j - i} more times]")
        elif j - i == 1:
            out.append(rows[i])
        i = j + 1
    return "\n".join(out)


def bound_result(job: dict, limit: int) -> dict:
    """The job with its result cut to ``limit`` characters of text/plain; rich representations other than text are
    replaced by their size (the full text, when long, is in the file named by ``talo_text_path``)."""
    result = job.get("result")
    if not result or "data" not in result:
        return job
    data, meta = dict(result["data"]), dict(result.get("metadata") or {})
    text = data.get("text/plain", "")
    if len(text) > limit:
        data["text/plain"] = text[:limit]
        meta["talo_shown_chars"] = limit
    for mime in [m for m in data if m != "text/plain"]:
        meta.setdefault("talo_omitted", {})[mime] = len(data.pop(mime))
    return {**job, "result": {"data": data, "metadata": meta}}


def show_job(args, job: dict) -> None:
    """A job: the last lines of its output, its result or error, and a status line."""
    lines = getattr(args, "lines", LINES)
    raw = getattr(args, "raw", False)
    out = call("output", id=job["id"], max_bytes=256 << 10)
    text, cut = tail_lines(out["text"] if raw else condense(out["text"]), lines)
    job = bound_result(job, getattr(args, "max_result", MAX_RESULT))
    if args.json:
        print(json.dumps({"job": job, "output": {"text": text, "lines_cut": cut, "bytes_cut": out["offset"],
                                                 "size": out["size"], "condensed": not raw}},
                         ensure_ascii=False, default=str))
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
        meta = job["result"].get("metadata") or {}
        if meta.get("talo_shown_chars"):
            where = f"; all of it in {meta['talo_text_path']}" if meta.get("talo_text_path") else ""
            print(f"[result cut to {meta['talo_shown_chars']} of {meta.get('talo_text_chars', '?')} characters{where}]")
    elif job.get("result"):  # a finished snap / checkout
        print(json.dumps(job["result"], ensure_ascii=False, default=str)[:2000])
    sys.stdout.flush()  # output first, the status line (stderr) last
    print(status_line(job), file=sys.stderr)


def status_line(job: dict) -> str:
    status, jid = job["status"], job["id"]
    elapsed = f" {job['elapsed_s']:.2f}s" if job.get("elapsed_s") is not None else ""
    count = f" [{job['execution_count']}]" if job.get("execution_count") else ""
    if status in ("running", "queued"):
        live = ""
        if (p := job.get("progress")) is not None:
            pct = f" {100 * p['done'] / p['total']:.0f}%" if p.get("total") else ""
            live += f", progress{pct} ({p['done']}{'/' + str(p['total']) if p.get('total') else ''})"
            live += f" {p['note']}" if p.get("note") else ""
        if job.get("last_activity_age_s") is not None:
            live += f", active {job['last_activity_age_s']:.0f}s ago"
        return (f"[job {jid} {status}{elapsed}{live}] `talo wait {jid}` for the result, `talo tail {jid}` for output, "
                f"`talo interrupt {jid}` to stop it")
    atomic = job.get("atomic") or {}
    if job.get("termination") == "memory_limit":
        status += ", stopped by the memory limit"
    if atomic.get("outcome") == "restored":
        status += ", state restored"
    elif atomic.get("outcome") in ("restore_failed", "not_started"):
        status += f", atomic {atomic['outcome']}: {atomic.get('error')}"
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
             memory_limit=parse_size(args.memory_limit),
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
    job = call("run", session=name, code=code, timeout=args.timeout, key=args.key, expect_rev=args.expect_rev,
               atomic=args.atomic)
    show_job(args, job)
    return exit_code(job)


def cmd_wait(args):
    job = call("job", id=args.id, wait=args.timeout)
    if "call" in job:
        return show_call(args, job["call"])
    if "map" in job:
        return show_map(args, job["map"])
    show_job(args, job)
    return exit_code(job)


# ---------------------------------------------------------------- map

def show_map(args, m: dict) -> int:
    code = 3 if m["status"] == "running" else (0 if m["status"] == "finished" and set(m["counts"]) <= {"ok"} else 1)
    if args.json:
        print(json.dumps({"map": m}, ensure_ascii=False, default=str))
        return code
    print(f"map {m['id']}: {m['cases']} cases from {m['from']}, {m['status']}")
    for r in m["rows"]:
        if r.get("status") == "ok":
            shown = json.dumps(r["result"], ensure_ascii=False, default=str) if r.get("result") is not None \
                else (r.get("text") or "")
        else:
            e = r.get("error") or {}
            shown = f"{e.get('ename')}: {e.get('evalue')}" if e else ""
        if r.get("termination"):
            shown += f"  [{r['termination']}]"
        el = f"{r['elapsed_s']:.1f}s" if r.get("elapsed_s") is not None else "-"
        print(f"{r['index']:>4}  {r['status']:11s} {el:>8}  {shown[:200]}")
    if m["rows_total"] > len(m["rows"]):
        print(f"... {m['rows_total'] - len(m['rows'])} more rows in {m['results']}")
    counts = ", ".join(f"{v} {k}" for k, v in sorted(m["counts"].items()))
    running = f"; running cases {', '.join(map(str, m['running']))}" if m["running"] else ""
    sys.stdout.flush()
    print(f"({m['finished_cases']}/{m['cases']} cases: {counts or 'none yet'}{running}; results in {m['results']})",
          file=sys.stderr)
    if m.get("blocked"):
        print(f"(waiting to start more cases: {m['blocked']})", file=sys.stderr)
    if m["status"] == "running":
        print(f"[map {m['id']} running {m['elapsed_s']:.1f}s] `talo wait {m['id']}`, `talo interrupt {m['id']}`",
              file=sys.stderr)
    return code


def show_call(args, c: dict) -> int:
    code = {"ok": 0, "running": 3}.get(c["status"], 1)
    if args.json:
        print(json.dumps({"call": c}, ensure_ascii=False, default=str))
        return code
    if c["status"] == "ok":
        print(json.dumps(c["result"], ensure_ascii=False, default=str) if c.get("result") is not None
              else (c.get("result_error") or c.get("text") or ""))
    elif c.get("error"):
        print("\n".join(c["error"].get("traceback") or [f"{c['error']['ename']}: {c['error']['evalue']}"]))
    sys.stdout.flush()
    tail = f"`talo wait {c['id']}`" if c["status"] == "running" else ""
    print(f"[call {c['id']} {c['status']}, {c['service']} v{c['version']}, {c['elapsed_s']:.2f}s] {tail}".rstrip(),
          file=sys.stderr)
    return code


def cmd_call(args):
    payload = json.loads(args.args) if args.args else {}
    d = call("call", service=args.service, args=payload, version=args.version, timeout=args.timeout, key=args.key,
             max_result=args.max_result)
    return show_call(args, d["call"])


def cmd_expose(args):
    job = call("expose", session=session_name(args), name=args.name, timeout=args.timeout)
    if args.json:
        print(json.dumps({"job": job}, ensure_ascii=False, default=str))
    elif job.get("published", {}) and "versions" in job["published"]:
        for v in job["published"]["versions"]:
            print(f"published {v['service']} v{v['version']}")
    else:
        show_job(args, job)
    return exit_code(job)


def cmd_services(args):
    def human(d):
        if not d["services"]:
            print("(no services; `talo.expose(fn)` in a job publishes one)")
        for s in d["services"]:
            for v in s["versions"]:
                latest = "  (latest)" if v["version"] == s["latest"] else ""
                print(f"{s['name']:28s} v{v['version']:<3} {human_bytes(v['pss'])} pss  calls in flight "
                      f"{v['calls_in_flight']}  job {v['job']}{latest}")
    emit(args, call("services", session=args.session), human)


def cmd_unexpose(args):
    emit(args, call("unexpose", service=args.service, version=args.version),
         lambda d: print("removed " + ", ".join(d["removed"])))


def show_handoff(h: dict) -> None:
    print(f"handoff {h['id']} from {h['owner']}" + (f" to {h['to']}" if h.get("to") else "") +
          f", {time.strftime('%Y-%m-%d %H:%M', time.localtime(h['time']))}, {h['status']}")
    print(f"message: {h['message']}")
    avail = "available" if h["snapshot_available"] else "GONE"
    print(f"state: {h['snapshot_name']} ({h['snapshot']}, {avail}), session {h['session']} at rev {h['rev']}")
    if h["notes"]["count"]:
        print(f"notes since the previous handoff ({h['notes']['count']}):")
        for n in h["notes"]["last"]:
            print(f"  {note_line(n)[:300]}")
    if h["records"]["count"]:
        print(f"ledger records since: {h['records']['count']} (`talo ledger -s {h['session']}`)")
    v = h.get("variables") or {}
    if "largest" in v:
        print("variables: " + " · ".join(f"{x['name']} {x['type']} {human_bytes(x.get('nbytes'))}" for x in v["largest"]))
    for j in h.get("active_jobs") or []:
        print(f"running at handoff: job {j['id']} {(j['code_preview'].strip().splitlines() or [''])[0][:70]}")
    print("continue with:")
    for c in h["continue"]:
        print(f"  {c}")


def cmd_handoff(args):
    if args.close:
        d = call("handoff", close=args.close)
        emit(args, d, lambda x: print(f"handoff {x['handoff']['id']} closed; its snapshot is no longer pinned"))
        return
    if not args.message:
        raise TaloError("BadArgument", "a handoff needs -m MESSAGE: what was done, what is open, what to do next")
    d = call("handoff", session=session_name(args), message=args.message, to=args.to, snap=args.snap, key=args.key,
             expect_rev=args.expect_rev)
    emit(args, d, lambda x: show_handoff(x["handoff"]))


def cmd_handoffs(args):
    def human(d):
        if not d["handoffs"]:
            print("(no handoffs)")
        for h in d["handoffs"]:
            print(f"{h['id']:5s} {h['status']:24s} {h['session']:12s} {h['owner']} -> {h.get('to') or '-'}  "
                  f"{h['message'][:80]}")
    emit(args, call("handoffs", session=args.session), human)


def cmd_resume(args):
    d = call("resume", id=args.id, branch=args.branch)

    def human(x):
        show_handoff(x["handoff"])
        if "session" in x:
            print(f"session {x['session']['name']} started from {x['handoff']['snapshot']}")
    emit(args, d, human)


def cmd_share(args):
    if args.list:
        def human(d):
            if not d["shares"]:
                print("(nothing shared)")
            for s in d["shares"]:
                print(f"{s['user']:12s} {s['session']:16s} {s['level']}")
            if d["connect"]:
                print(f"(they connect with `talo --connect {d['connect']} ...`)")
        emit(args, call("shares"), human)
        return
    if not args.user:
        raise TaloError("BadArgument", "share needs --user NAME (or --list)")
    d = call("share", session=session_name(args), user=args.user, level=args.level)

    def human(x):
        print(f"shared session {x['session']} with {x['user']} at level {x['level']}")
        if x.get("warning"):
            print(f"warning: {x['warning']}")
        print(f"they connect with: talo --connect {x['connect']} brief -s {x['session']}")
    emit(args, d, human)


def cmd_unshare(args):
    d = call("unshare", user=args.user, session=args.session)
    emit(args, d, lambda x: print("revoked: " + (", ".join(x["revoked"]) or "nothing")))


def cmd_map(args):
    code = read_code(args)
    cases = None
    if args.cases:
        text = sys.stdin.read() if args.cases == "-" else (open(args.cases).read() if os.path.exists(args.cases) else args.cases)
        cases = json.loads(text)
    d = call("map", session=session_name(args), code=code, cases=cases, range=args.range, **{"from": args.from_},
             parallel=args.parallel, timeout=args.timeout, memory_limit=parse_size(args.memory_limit),
             case_memory_limit=parse_size(args.case_memory_limit), max_result=args.max_result, key=args.key,
             expect_rev=args.expect_rev)
    return show_map(args, d["map"])


def cmd_maps(args):
    def human(d):
        if not d["maps"]:
            print("(no maps)")
        for m in d["maps"]:
            counts = ", ".join(f"{v} {k}" for k, v in sorted(m["counts"].items()))
            print(f"{m['id']:6s} {m['status']:11s} {m['finished_cases']}/{m['cases']} from {m['from']}  {counts}")
    emit(args, call("maps", session=args.session), human)


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
    shown = out["text"] if args.raw else condense(out["text"])
    text, cut = (shown, 0) if args.all else tail_lines(shown, args.lines)
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
        since = s["jobs_since_base"]["count"]
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
        snaps = d["snapshots"]
        if snaps["last"]:
            more = f" (last {len(snaps['last'])} of {snaps['total']})" if snaps["total"] > len(snaps["last"]) else ""
            print(f"snapshots{more}  " + "  ·  ".join(
                f"{x['name']} ({x['ref']})" + (f" \"{x['message'][:60]}\"" if x["message"] else "") for x in snaps["last"]))
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
            live = ""
            if (p := j.get("progress")) is not None:
                live += f", progress {p['done']}" + (f"/{p['total']}" if p.get("total") else "")
            if j.get("last_activity_age_s") is not None:
                live += f", active {j['last_activity_age_s']:.0f}s ago"
            print(f"active  {line(j)}{live}")
        if d["active_total"] > len(d["active_jobs"]):
            print(f"  ... {d['active_total'] - len(d['active_jobs'])} more (`talo jobs`)")
    emit(args, call("brief", session=session_name(args), notes=args.notes, jobs=args.jobs), human)


def cmd_resources(args):
    def human(d):
        m = d["machine"]
        print(f"machine: {human_bytes(m['available'])} available of {human_bytes(m['total'])}; "
              f"Talo sessions use {human_bytes(d['pss_total'])} (PSS)")
        for s in d["sessions"]:
            limit = f", limit {human_bytes(s['memory_limit'])}" if s["memory_limit"] else ""
            unknown = " (some unknown)" if s["pss_unknown"] else ""
            print(f"{s['name']:16s} {s['state']:8s} {human_bytes(s['pss_total'])}{unknown}{limit}")
            for p in s["processes"]:
                mem = p["memory"] or {}
                label = p["kind"] if p["kind"] != "snapshot" else f"snapshot {p['name']}"
                print(f"  {label:24s} pid {p['pid']}  pss {human_bytes(mem.get('pss'))}  rss {human_bytes(mem.get('rss'))}"
                      f"  private {human_bytes((mem.get('private_dirty') or 0) + (mem.get('private_clean') or 0)) if mem else '-'}")
            if s["over_limit"]:
                print(f"  over the limit: {s['over_limit']}")
    emit(args, call("resources", session=args.session), human)


def cmd_reload(args):
    if not args.modules and not args.changed:
        raise TaloError("BadArgument", "name the modules to reload, or use --changed")
    d = call("reload", session=session_name(args), modules=args.modules, changed=args.changed, root=args.root,
             dry_run=args.dry_run, timeout=args.timeout, expect_rev=args.expect_rev, key=args.key)
    if args.dry_run:
        emit(args, d, lambda x: print("would reload: " + (", ".join(x["selected"]) or "(nothing changed)")))
        return
    job = d["job"]
    if args.json:
        print(json.dumps(d, ensure_ascii=False, default=str))
        return exit_code(job)
    result = job.get("result") or {}
    if job["status"] in ("running", "queued"):
        print(status_line(job), file=sys.stderr)
        return exit_code(job)
    if not result.get("selected"):
        print("nothing to reload")
    for m in result.get("modules", []):
        print(f"{m['module']:30s} {m['status']}" + (f": {m['error']}" if m.get("error") else ""))
    for s in result.get("stale_bindings", []):
        print(f"stale: {s['name']} ({s['kind']} from the old {s['module']}.{s['attribute']}) -> {s['rebind']}")
    if job.get("error") and not result:
        print(f"{job['error']['ename']}: {job['error']['evalue']}")
    print(status_line(job), file=sys.stderr)
    return exit_code(job)


def cmd_events(args):
    d = call("events", after=args.after, wait=args.wait, session=args.session, type=args.type, limit=args.limit,
             all=args.all)

    def human(x):
        for e in x["events"]:
            fields = " ".join(f"{k}={v}" for k, v in e.items() if k not in ("seq", "cursor", "type", "time"))
            print(f"{e['cursor']}  {time.strftime('%H:%M:%S', time.localtime(e['time']))}  {e['type']:18s} {fields}")
        if not x["events"]:
            print("(no events)")
        print(f"(next: --after {x['next']}{'; more are ready' if x['has_more'] else ''})", file=sys.stderr)
    emit(args, d, human)


def parse_value(text: str):
    try:
        return json.loads(text)
    except ValueError:
        return text


def cmd_record(args):
    fields = {}
    for item in args.fields:
        k, sep, v = item.partition("=")
        if not sep:
            raise TaloError("BadArgument", f"expected KEY=VALUE, got {item!r}")
        fields[k] = parse_value(v)
    d = call("record", session=session_name(args), fields=fields, key=args.key)
    emit(args, d, lambda x: print(f"recorded {x['record']['id']}"))


def record_line(r: dict) -> str:
    fields = " ".join(f"{k}={v:.4g}" if isinstance(v, float) else f"{k}={v}" for k, v in r["fields"].items())
    p = r["producer"]
    where = {"job": f"job {p.get('job')}", "case": f"map {p.get('map')} case {p.get('index')}"}.get(p["kind"], p["kind"])
    src = f" · from {r['source_snapshot']} ({r['source_state']})" if r.get("source_snapshot") else ""
    return f"{r['id']}  {fields[:160]}  {where}{src} · {r['outcome']} · {r['owner']}"


def cmd_ledger(args):
    d = call("ledger", session=args.session, owner=args.owner_filter, outcome=args.outcome, has=args.has,
             where=args.where, sort=("-" if args.desc else "") + args.sort if args.sort else None, last=args.last)

    def human(x):
        if not x["records"]:
            print("(no records; `talo.record(name=value)` in code adds one)")
        for r in x["records"]:
            print(record_line(r))
        if x["total"] > len(x["records"]):
            print(f"({len(x['records'])} of {x['total']} records)", file=sys.stderr)
    emit(args, d, human)


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
    ap.add_argument("--connect", metavar="PATH", help="use a session shared by another user (the path `talo share` gave)")
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
    p.add_argument("--memory-limit", metavar="SIZE",
                   help="soft limit on the session's memory (worker plus snapshots, PSS): a job that exceeds it is "
                        "interrupted, then its worker killed (e.g. 100G)")
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
    p.add_argument("--raw", action="store_true", help="show the output as written (no condensing)")
    p.add_argument("--max-result", type=int, default=MAX_RESULT, help="characters of the result shown")
    p.add_argument("--atomic", action="store_true",
                   help="run inside a checkpoint: if the code fails, the session is restored to its state before it")
    p = add("wait", cmd_wait, "wait for a job to finish", session=False)
    p.add_argument("id")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT)
    p.add_argument("-n", "--lines", type=int, default=LINES)
    p.add_argument("--raw", action="store_true", help="show the output as written (no condensing)")
    p.add_argument("--max-result", type=int, default=MAX_RESULT, help="characters of the result shown")
    p = add("tail", cmd_tail, "show a job's output", session=False)
    p.add_argument("id")
    p.add_argument("-n", "--lines", type=int, default=LINES)
    p.add_argument("--all", action="store_true", help="the whole output")
    p.add_argument("--offset", type=int, help="read from this byte offset (the reply's next_offset continues)")
    p.add_argument("--raw", action="store_true", help="show the output as written (no condensing)")
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
    p = add("reload", cmd_reload, "reload edited project modules; report names still bound to the old code")
    p.add_argument("modules", nargs="*", help="modules to reload, in this order")
    p.add_argument("--changed", action="store_true",
                   help="also every imported module under --root whose source changed since it was loaded")
    p.add_argument("--root", help="directory for --changed (default: the session's working directory)")
    p.add_argument("--dry-run", action="store_true", help="only list the modules that would be reloaded")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT)
    p.add_argument("--expect-rev", type=int, metavar="N", help="refuse unless the session is at revision N")
    p.add_argument("--key", help="idempotency key")
    p = add("map", cmd_map, "run code once per case, each in a fork of a snapshot, in parallel")
    p.add_argument("code", nargs="?", help="code to run for each case (the variable `case` holds it)")
    p.add_argument("-f", "--file", help="read the code from this file")
    p.add_argument("--cases", help="JSON list of cases: inline, a file, or - for stdin")
    p.add_argument("--range", type=int, metavar="N", help="cases 0 .. N-1")
    p.add_argument("--from", dest="from_", metavar="SNAPSHOT", help="snapshot to fork (default: the current state)")
    p.add_argument("-j", "--parallel", type=int, default=4, help="cases running at once (default 4)")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT, help="seconds to wait; the map continues after")
    p.add_argument("--memory-limit", metavar="SIZE", help="start no case while the session and its cases use more")
    p.add_argument("--case-memory-limit", metavar="SIZE", help="stop a case that uses more than this")
    p.add_argument("--max-result", type=int, default=65536, help="bytes of JSON kept per result")
    p.add_argument("--key", help="idempotency key")
    p.add_argument("--expect-rev", type=int, metavar="N", help="without --from: refuse unless the session is at revision N")
    add("maps", cmd_maps, "fan-outs and their progress")
    p = add("share", cmd_share, "let another user on this machine read, call or run in a session")
    p.add_argument("--user", help="the user to share with")
    p.add_argument("--level", choices=("read", "call", "run"), default="read",
                   help="read: inspect; call: also call exposed services; run: also run any code (as you)")
    p.add_argument("--list", action="store_true", help="list what is shared")
    p = add("unshare", cmd_unshare, "revoke a share")
    p.add_argument("--user", required=True)
    p = add("handoff", cmd_handoff, "hand the session's work over: snapshot, notes, records, running jobs, message")
    p.add_argument("-m", "--message", help="what was done, what is open, what to do next")
    p.add_argument("--to", help="who it is for (attribution only)")
    p.add_argument("--snap", metavar="NAME", help="hand over this snapshot instead of taking a new one")
    p.add_argument("--close", metavar="ID", help="close a handoff (its snapshot is no longer pinned)")
    p.add_argument("--key", help="idempotency key")
    p.add_argument("--expect-rev", type=int, metavar="N", help="refuse unless the session is at revision N")
    p = add("handoffs", cmd_handoffs, "handoffs and their status", session=False)
    p.add_argument("-s", "--session")
    p = add("resume", cmd_resume, "show a handoff; --branch NAME starts a session from its snapshot", session=False)
    p.add_argument("id")
    p.add_argument("--branch", metavar="NAME", help="create a session from the handoff's snapshot (closes it)")
    p = add("call", cmd_call, "call a service of a session (SESSION.NAME) with JSON arguments", session=False)
    p.add_argument("service")
    p.add_argument("args", nargs="?", help="JSON object of keyword arguments")
    p.add_argument("--version", type=int, help="service version (default: the latest, resolved now)")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT)
    p.add_argument("--key", help="idempotency key")
    p.add_argument("--max-result", type=int, default=65536, help="bytes of JSON kept")
    p = add("expose", cmd_expose, "publish a function of the session as a service (like talo.expose)")
    p.add_argument("name", help="name of a function in the session")
    p.add_argument("-t", "--timeout", type=float, default=TIMEOUT)
    p = add("services", cmd_services, "services, their versions, memory and calls in flight", session=False)
    p.add_argument("-s", "--session", help="only this session's services")
    p = add("unexpose", cmd_unexpose, "remove a service (or one version) that has no calls in flight", session=False)
    p.add_argument("service")
    p.add_argument("--version", type=int)
    p = add("record", cmd_record, "add a record to the experiment ledger from the shell")
    p.add_argument("fields", nargs="+", metavar="KEY=VALUE", help="values are parsed as JSON when possible")
    p.add_argument("--key", help="idempotency key")
    p = add("ledger", cmd_ledger, "the experiment ledger of all sessions, with where each record came from",
            session=False)
    p.add_argument("-s", "--session", help="only this session")
    p.add_argument("--owner-of", dest="owner_filter", metavar="OWNER", help="only records by this owner")
    p.add_argument("--outcome", help="running, succeeded, failed, interrupted, rolled_back or reported")
    p.add_argument("--has", action="append", metavar="KEY", help="only records with this field (repeatable)")
    p.add_argument("--where", action="append", metavar="KEY=VALUE", help="only records where KEY equals VALUE")
    p.add_argument("--sort", metavar="KEY", help="sort on a numeric field")
    p.add_argument("--desc", action="store_true", help="with --sort: largest first")
    p.add_argument("--last", type=int, default=50)
    p = add("events", cmd_events, "the event journal: wait for jobs, maps, snapshots, notes ... without polling",
            session=False)
    p.add_argument("--after", metavar="CURSOR", help="events after this cursor (default: only new events)")
    p.add_argument("--all", action="store_true", help="from the oldest event kept")
    p.add_argument("-w", "--wait", type=float, default=0, help="seconds to wait for the first matching event")
    p.add_argument("-s", "--session", help="only events of this session")
    p.add_argument("--type", help="only these event types (comma-separated), e.g. job.finished,map.finished")
    p.add_argument("--limit", type=int, default=100)
    add("resources", cmd_resources, "memory of every session's worker and snapshots, and of the machine")
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
    if args.connect:
        os.environ["TALO_CONNECT"] = args.connect
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
