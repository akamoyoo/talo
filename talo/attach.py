"""``talo attach``: an interactive prompt on a resident session (see "talo attach" in docs/design.md).

Every input is submitted as an ordinary job through the supervisor, so a human at this prompt and an agent using
``talo run`` share the session, its queue and its history.
"""

from __future__ import annotations

import codeop
import shlex
import sys
import time

from talo.client import TaloError, call
from talo.paths import home

POLL_S = 0.1
# talo commands available as %name inside attach; those marked True act on the attached session (-s is added)
MAGICS = {"vars": True, "show": True, "snap": True, "snaps": True, "checkout": True, "drop": True, "diff": True,
          "history": True, "export": True, "jobs": True, "tree": False, "ls": False}
HELP = """%vars [PATTERN]        variables          %show EXPR          summarize a value
%snap [NAME] [-m MSG]  take a snapshot    %snaps / %tree      snapshots
%checkout NAME         roll back          %diff A [B]         compare states
%history / %jobs       what ran           %export [-o FILE]   write a .py / .ipynb
%detach (or Ctrl-D)    leave; the session keeps running"""


class Attach:
    def __init__(self, session: str):
        self.session = session
        self.mine: set[str] = set()
        self.seen = self._last_job_id()
        self.count = self._last_count()

    # ---------------------------------------------------------------- state

    def _jobs(self) -> list:
        return call("jobs", session=self.session)["jobs"]

    def _last_job_id(self) -> int:
        return max((int(j["id"]) for j in self._jobs()), default=0)

    def _last_count(self) -> int:
        counts = [j["execution_count"] for j in self._jobs() if j.get("execution_count")]
        return counts[-1] if counts else 0

    def report_others(self) -> None:
        """Mention jobs that other clients submitted since the last prompt."""
        others = [j for j in self._jobs() if int(j["id"]) > self.seen and j["id"] not in self.mine]
        self.seen = max([self.seen, *(int(j["id"]) for j in others)])
        if others:
            ids = ", ".join(j["id"] for j in others)
            print(f"[job{'s' if len(others) > 1 else ''} {ids} from another client; %history shows them]")

    # ---------------------------------------------------------------- readline

    def setup_readline(self) -> None:
        try:
            import readline
        except ImportError:  # no line editing on this platform; input() still works
            return
        self.history_file = home() / "sessions" / self.session / "input_history"
        try:
            readline.read_history_file(self.history_file)
        except OSError:
            pass
        readline.set_history_length(10_000)
        readline.set_completer_delims(" \t\n`~!@#$%^&*()-=+{}\\|;:,<>/?")
        readline.set_completer(self.complete)
        if "libedit" in (readline.__doc__ or ""):  # macOS and some Python builds use libedit's syntax
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")

    def save_history(self) -> None:
        try:
            import readline
            readline.write_history_file(self.history_file)
        except (ImportError, OSError, AttributeError):
            pass

    def complete(self, text: str, state: int):
        if state == 0:
            import readline
            line, end = readline.get_line_buffer(), readline.get_endidx()
            try:
                reply = call("complete", session=self.session, code=line[:end], cursor_pos=end)
                start = reply["cursor_start"]
                # readline replaces `text` (from its own word start): rebase the matches onto it
                prefix = line[end - len(text):start] if start >= end - len(text) else ""
                self.matches = [prefix + m for m in reply["matches"]]
            except TaloError:  # busy or gone: no completion
                self.matches = []
        return self.matches[state] if state < len(self.matches) else None

    # ---------------------------------------------------------------- input and execution

    def read_cell(self) -> str | None:
        """One complete input (several lines for a block); None on Ctrl-D."""
        lines = []
        while True:
            prompt = f"[{self.count + 1}]: " if not lines else " " * (len(str(self.count + 1)) + 2) + "...: "
            try:
                line = input(prompt)
            except EOFError:
                print()
                return None
            except KeyboardInterrupt:
                print()
                lines = []
                continue
            lines.append(line)
            source = "\n".join(lines)
            if len(lines) == 1 and line.lstrip().startswith("%"):
                return source
            try:
                if codeop.compile_command(source, "<input>", "single") is None:
                    continue
            except (SyntaxError, ValueError, OverflowError):
                pass  # submitted anyway: the worker reports the error with a traceback
            if len(lines) > 1 and line.strip():
                continue  # a block ends with an empty line, as in the standard interpreter
            return source

    def run(self, code: str) -> None:
        job = call("run", session=self.session, code=code, timeout=0)
        self.mine.add(job["id"])
        offset, interrupted = 0, False
        while True:
            try:
                out = call("output", id=job["id"], offset=offset, max_bytes=1 << 20)
                sys.stdout.write(out["text"])
                sys.stdout.flush()
                offset = out["next_offset"]
                if out["status"] not in ("running", "queued") and offset >= out["size"]:
                    break
                time.sleep(POLL_S)
            except KeyboardInterrupt:
                if interrupted:  # a second Ctrl-C: stop waiting, the job goes on in the background
                    print(f"\n[job {job['id']} keeps running; `talo wait {job['id']}`]")
                    return
                interrupted = True
                call("interrupt", id=job["id"])
        job = call("job", id=job["id"])
        for o in job.get("outputs") or []:
            print(f"[{o['mime']}: {o['path']}]")
        if job.get("error"):
            print("\n".join(job["error"].get("traceback") or
                            [f"{job['error'].get('ename')}: {job['error'].get('evalue')}"]))
        elif job.get("result"):
            print(job["result"]["data"].get("text/plain", ""))
        if job.get("execution_count"):
            self.count = job["execution_count"]

    def magic(self, line: str) -> bool:
        """Run a %command; False to detach."""
        from talo.cli import main
        words = shlex.split(line.lstrip()[1:])
        if not words or words[0] == "help":
            print(HELP)
            return True
        if words[0] in ("detach", "exit", "quit"):
            return False
        if words[0] not in MAGICS:
            print(f"unknown command %{words[0]}; %help lists them")
            return True
        argv = words + (["-s", self.session] if MAGICS[words[0]] else [])
        try:
            main(argv)
        except SystemExit:  # argparse errors and --help
            pass
        sys.stdout.flush()
        sys.stderr.flush()
        self.seen = self._last_job_id()  # jobs the command queued (snap, checkout) are ours
        if words[0] == "checkout":
            self.count = self._last_count()
        return True

    def loop(self) -> int:
        info = next((s for s in call("ls")["sessions"] if s["name"] == self.session), None)
        if info is None:
            print(f"talo: no session {self.session!r} (`talo new {self.session}` creates it)", file=sys.stderr)
            return 2
        from talo.cli import human_bytes
        rss = f", {human_bytes(info['rss'])}" if info.get("rss") else ""
        print(f"talo {self.session} ({info['state']}{rss}) — Ctrl-C interrupts, Ctrl-D detaches, %help for commands")
        self.setup_readline()
        try:
            while True:
                self.report_others()
                cell = self.read_cell()
                if cell is None:
                    break
                if not cell.strip():
                    continue
                try:
                    if cell.lstrip().startswith("%"):
                        if not self.magic(cell):
                            break
                    else:
                        self.run(cell)
                except TaloError as error:
                    print(f"talo: {error}", file=sys.stderr)
        finally:
            self.save_history()
        print("detached; the session keeps running")
        return 0
