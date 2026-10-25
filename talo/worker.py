"""talo worker: runs code inside the user's interpreter; all variables live in this process.

Started by the supervisor as ``python <talo>/worker.py --fd N --session NAME``. File descriptor N is a Unix socket
connected to the supervisor; the message format is described in ``wire.py``. The worker is single-threaded and handles
messages one at a time: execute code, list variables, summarize a variable, shut down.

While code runs, fds 1 and 2 are redirected to the job's output file (the supervisor passes its path in the
``execute_request`` metadata), so output written directly to the file descriptors by C extensions is captured too and
the supervisor can read it while the job is still running. Interrupts arrive as SIGINT (a KeyboardInterrupt in user
code).

Standard library only and executed by path: talo does not need to be installed in the target environment, and no
directory is added to the user's ``sys.path``.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import linecache
import os
import pickle
import re
import rlcompleter
import select
import signal
import socket
import sys
import threading
import time
import traceback
import warnings
from pathlib import Path


def _load_wire():
    spec = importlib.util.spec_from_file_location("_talo_wire", Path(__file__).with_name("wire.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wire = _load_wire()

RESULT_LIMIT = 100_000  # characters of a result's text/plain kept; longer results are truncated and flagged
TRACEBACK_LIMIT = 50_000
CELL_FILENAME = "<talo-{n}>"


def rss_bytes() -> int:
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except OSError:
        return -1


def truncate(text: str, limit: int) -> tuple[str, int]:
    """(truncated text, number of characters cut): keeps the head and the tail, with a marker line in between."""
    if len(text) <= limit:
        return text, 0
    head, tail = text[: limit * 3 // 4], text[-limit // 4:]
    cut = len(text) - len(head) - len(tail)
    return f"{head}\n… [{cut} characters cut] …\n{tail}", cut


BINARY_REPRS = (("_repr_png_", "image/png", ".png"), ("_repr_jpeg_", "image/jpeg", ".jpg"),
                ("_repr_svg_", "image/svg+xml", ".svg"))


def mime_bundle(value, save=None) -> tuple[dict, dict]:
    """(data, metadata): always text/plain; also text/html / text/markdown when the object provides
    ``_repr_html_`` / ``_repr_markdown_`` (skipped if they fail). Images (``_repr_png_`` / ``_repr_jpeg_`` /
    ``_repr_svg_``) are written with ``save(data, suffix) -> path`` and listed in ``metadata["talo_files"]``."""
    data, meta = {}, {}
    try:
        text = repr(value)
    except Exception as error:  # noqa: BLE001 - a user object's __repr__ may raise anything
        text = f"<repr failed: {type(error).__name__}: {error}>"
    data["text/plain"], cut = truncate(text, RESULT_LIMIT)
    if cut:
        meta["talo_truncated"] = cut
    for method, mime in (("_repr_html_", "text/html"), ("_repr_markdown_", "text/markdown")):
        fn = getattr(type(value), method, None)
        if fn is None:
            continue
        try:
            out = fn(value)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(out, str):
            data[mime] = truncate(out, RESULT_LIMIT)[0]
    if save is not None:
        for method, mime, suffix in BINARY_REPRS:
            fn = getattr(type(value), method, None)
            if fn is None:
                continue
            try:
                out = fn(value)
            except Exception:  # noqa: BLE001
                continue
            if isinstance(out, tuple):  # (data, metadata), as IPython allows
                out = out[0]
            if isinstance(out, str):
                out = out.encode()
            if isinstance(out, bytes) and out:
                meta.setdefault("talo_files", {})[mime] = save(out, suffix)
    return data, meta


# ---------------------------------------------------------------- completion

_KEY = re.compile(r"([A-Za-z_][\w.]*)\[(['\"])([^'\"]*)$")   # d["pre  -> complete the keys of d
_NAME = re.compile(r"[A-Za-z_][\w.]*$|$")


def complete(namespace: dict, code: str, cursor_pos: int) -> dict:
    """Jupyter ``complete_reply`` content: names and attributes (rlcompleter), and string keys after ``name[\"``.
    Only plain names and attribute chains are evaluated, so completion never runs a call or an indexing."""
    before = code[:cursor_pos]
    if m := _KEY.search(before):
        matches = []
        try:
            obj = eval(m.group(1), namespace)  # a dotted name only (the regex allows nothing else)
            keys = obj.keys() if hasattr(obj, "keys") else []
            matches = sorted(k for k in map(str, keys) if k.startswith(m.group(3)))[:500]
        except Exception:  # noqa: BLE001
            pass
        return {"status": "ok", "matches": matches, "cursor_start": cursor_pos - len(m.group(3)),
                "cursor_end": cursor_pos, "metadata": {}}
    token = _NAME.search(before).group(0)
    completer, matches = rlcompleter.Completer(namespace), []
    for i in range(1000):
        try:
            match = completer.complete(token, i)
        except Exception:  # noqa: BLE001 - a property raising during attribute listing
            break
        if match is None:
            break
        matches.append(match)
    return {"status": "ok", "matches": sorted(set(matches)), "cursor_start": cursor_pos - len(token),
            "cursor_end": cursor_pos, "metadata": {}}


# ---------------------------------------------------------------- variable summaries

SIZE_DEPTH = 4         # how deep estimate_nbytes looks into containers and object attributes
SIZE_BUDGET = 20_000   # at most this many objects visited per estimate


def _buffer_nbytes(value) -> int | None:
    """Size of objects that know their own buffer size (no traversal), else None."""
    nbytes = getattr(type(value), "nbytes", None) and getattr(value, "nbytes", None)  # numpy, pyarrow, torch
    if isinstance(nbytes, int):
        return nbytes
    usage = getattr(value, "memory_usage", None)  # pandas DataFrame / Series
    if callable(usage) and hasattr(value, "shape"):
        try:
            total = usage(index=True, deep=False)
            return int(total.sum()) if hasattr(total, "sum") else int(total)
        except Exception:  # noqa: BLE001
            return None
    return None


def estimate_nbytes(value) -> tuple[int | None, bool]:
    """(bytes, complete): memory held by ``value``, following containers and object attributes a few levels deep
    and counting each object once. ``complete`` is False when the depth or object budget cut the walk short."""
    total, complete, seen, visited = 0, True, set(), 0
    stack = [(value, 0)]
    while stack:
        obj, depth = stack.pop()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        visited += 1
        if visited > SIZE_BUDGET:
            return total, False
        known = _buffer_nbytes(obj)
        if known is not None:
            total += known
            continue
        try:
            total += sys.getsizeof(obj)
        except Exception:  # noqa: BLE001
            complete = False
            continue
        if isinstance(obj, (str, bytes, bytearray, int, float, complex, bool, type(None), type, type(sys))) \
                or callable(obj):
            continue
        if isinstance(obj, dict):
            children = [*obj.keys(), *obj.values()]
        elif isinstance(obj, (list, tuple, set, frozenset)):
            children = list(obj)
        elif hasattr(obj, "__dict__"):
            children = list(vars(obj).values())
        else:
            continue
        if not children:
            continue
        if depth >= SIZE_DEPTH:
            complete = False
            continue
        stack.extend((c, depth + 1) for c in children)
    return total, complete


def _type_name(value) -> str:
    t = type(value)
    module = t.__module__
    return t.__qualname__ if module in ("builtins", "__main__") else f"{module.split('.')[0]}.{t.__qualname__}"


def describe(name: str, value) -> dict:
    """A one-line variable summary: name, type, shape / length, dtype, memory estimate."""
    nbytes, complete = estimate_nbytes(value)
    out = {"name": name, "type": _type_name(value), "nbytes": nbytes}
    if not complete:
        out["nbytes_partial"] = True  # at least this much
    shape = getattr(value, "shape", None)
    if isinstance(shape, tuple):
        out["shape"] = list(shape)
    elif hasattr(value, "__len__") and not isinstance(value, (str, bytes)):
        try:
            out["len"] = len(value)
        except Exception:  # noqa: BLE001
            pass
    dtype = getattr(value, "dtype", None)
    if dtype is not None:
        out["dtype"] = str(dtype)
    if hasattr(value, "dtypes") and hasattr(value, "columns"):  # DataFrame
        try:
            out["columns"] = [str(c) for c in list(value.columns)[:50]]
        except Exception:  # noqa: BLE001
            pass
    if isinstance(value, (int, float, complex, bool, str, bytes)) or value is None:
        out["value"] = truncate(repr(value), 200)[0]
    return out


def is_user_variable(name: str, value) -> bool:
    """Names the user created: not private names, modules, or classes / functions imported from other modules."""
    if name.startswith("_") or name in ("In", "Out", "exit", "quit", "get_ipython", "display"):
        return False
    if isinstance(value, type(sys)):
        return False
    if isinstance(value, type) or callable(value) and hasattr(value, "__module__") and hasattr(value, "__qualname__"):
        return getattr(value, "__module__", None) == "__main__"
    return True


def show(value, rows: int) -> str:
    """A text preview of one value: tables show their first rows and column types, arrays show basic statistics,
    anything else its (truncated) repr."""
    head = getattr(value, "head", None)
    if callable(head) and hasattr(value, "shape"):  # pandas DataFrame / Series
        parts = [f"{_type_name(value)} shape={tuple(value.shape)}"]
        dtypes = getattr(value, "dtypes", None)
        if dtypes is not None and hasattr(dtypes, "items"):
            parts.append("dtypes: " + ", ".join(f"{k}={v}" for k, v in list(dtypes.items())[:50]))
        elif dtypes is not None:
            parts.append(f"dtype: {dtypes}")
        parts.append(head(rows).to_string())
        return "\n".join(parts)
    if hasattr(value, "shape") and hasattr(value, "dtype"):  # numpy-like
        parts = [f"{_type_name(value)} shape={tuple(value.shape)} dtype={value.dtype}"]
        try:
            import numpy as np  # only reached when the user's environment has numpy
            flat = np.asarray(value).ravel()
            if flat.size and flat.dtype.kind in "biuf":
                finite = flat[np.isfinite(flat)] if flat.dtype.kind == "f" else flat
                parts.append(f"min={finite.min() if finite.size else 'nan'} max={finite.max() if finite.size else 'nan'} "
                             f"nan={int(flat.size - finite.size)}")
            parts.append(repr(flat[:rows]))
        except Exception:  # noqa: BLE001
            parts.append(truncate(repr(value), 2000)[0])
        return "\n".join(parts)
    return truncate(repr(value), 5000)[0]


class _HashWriter:
    """A file-like sink for ``pickle.Pickler`` that hashes what is written instead of keeping it."""

    def __init__(self, h):
        self.write = h.update


def fingerprint(value) -> dict:
    """``{"hash": hex}``: BLAKE2b of the object's pickle (protocol 5). Out-of-band buffers (NumPy arrays, and the data
    of pandas objects) are hashed in place, without being copied into the pickle; ``{"error": ...}`` when the object
    cannot be pickled."""
    h = hashlib.blake2b(digest_size=16)

    def buffer(b: pickle.PickleBuffer) -> bool:
        raw = b.raw()
        h.update(len(raw).to_bytes(8, "little"))
        h.update(raw)
        return False  # out of band: not written into the stream

    try:
        pickle.Pickler(_HashWriter(h), protocol=5, buffer_callback=buffer).dump(value)
    except Exception as error:  # noqa: BLE001
        return {"error": f"cannot be pickled ({type(error).__name__})"}
    return {"hash": h.hexdigest()}


def fingerprints(namespace: dict, names: list) -> dict:
    return {name: fingerprint(namespace[name]) if name in namespace else {"error": "missing"} for name in names}


# ---------------------------------------------------------------- snapshots

def other_threads() -> list[str]:
    """Names of the threads in this process other than the calling one (from /proc/self/task/*/comm)."""
    me = str(threading.get_native_id())
    names = []
    for tid in os.listdir("/proc/self/task"):
        if tid == me:
            continue
        try:
            with open(f"/proc/self/task/{tid}/comm") as f:
                names.append(f.read().strip())
        except OSError:
            pass
    return names


def _reply(conn: socket.socket, obj: dict) -> None:
    conn.sendall(json.dumps(obj, default=str).encode() + b"\n")


class Snapshot:
    """The snapshot side of a fork: a suspended copy of the worker that serves requests on its own socket.

    ``serve`` only returns in a process created by ``spawn``: that process is the session's new worker, and
    ``serve`` hands it the connection it should use."""

    def __init__(self, worker: "Worker", listener: socket.socket, path: str):
        self.worker, self.listener, self.path = worker, listener, path
        self.holds: list[socket.socket] = []
        self.children_file = path[: -len(".sock")] + ".children" if path.endswith(".sock") else path + ".children"

    def _reap(self, *_):
        """SIGCHLD: record how spawned workers ended, so the supervisor can report it."""
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if pid == 0:
                return
            with open(self.children_file, "a") as f:
                f.write(f"{pid} {os.waitstatus_to_exitcode(status)}\n")

    def serve(self) -> socket.socket:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGCHLD, self._reap)
        while True:
            readable, _, _ = select.select([self.listener, *self.holds], [], [])
            for sock in readable:
                if sock in self.holds:  # the supervisor closed its hold connection: it is gone or dropped us
                    if not sock.recv(1):
                        os._exit(0)
                    continue
                conn, _ = self.listener.accept()
                line = conn.makefile("rb").readline()
                if not line:
                    conn.close()
                    continue
                req = json.loads(line)
                op = req.get("op")
                if op == "hold":
                    self.holds.append(conn)
                elif op == "info":
                    _reply(conn, {"ok": True, "pid": os.getpid(), "execution_count": self.worker.execution_count})
                    conn.close()
                elif op == "vars":
                    rows = [describe(k, v) for k, v in list(self.worker.namespace.items()) if is_user_variable(k, v)]
                    _reply(conn, {"ok": True, "variables": rows})
                    conn.close()
                elif op == "fingerprint":
                    _reply(conn, {"ok": True, "fingerprints": fingerprints(self.worker.namespace, req.get("names", []))})
                    conn.close()
                elif op == "spawn":
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", DeprecationWarning)  # see "Fork safety" in docs/design.md
                        pid = os.fork()
                    if pid == 0:
                        return self._become_worker(conn)
                    conn.close()
                elif op == "exit":
                    _reply(conn, {"ok": True})
                    os._exit(0)
                else:
                    _reply(conn, {"ok": False, "error": f"unknown op {op!r}"})
                    conn.close()

    def _become_worker(self, conn: socket.socket) -> socket.socket:
        self.listener.close()
        for h in self.holds:
            h.close()
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.default_int_handler)
        _reply(conn, {"ok": True, "pid": os.getpid()})
        return conn


# ---------------------------------------------------------------- execution

class Worker:
    def __init__(self, sock: socket.socket, session: str):
        self.sock = sock
        self.rfile = sock.makefile("rb")
        self.wfile = sock.makefile("wb")
        self.session = session
        self.execution_count = 0
        self.namespace = {"__name__": "__main__", "__builtins__": __builtins__, "In": [""], "Out": {},
                          "display": self.display}
        self.parent: dict | None = None    # the execute_request being run, for display_data
        self.out_path: str | None = None   # its output file; images are written next to it
        self.files = 0
        self.pyplot_patched = False

    # ---------------------------------------------------------------- rich output

    def save(self, data: bytes, suffix: str) -> str:
        self.files += 1
        path = f"{self.out_path.removesuffix('.out')}-{self.files}{suffix}"
        with open(path, "wb") as f:
            f.write(data)
        return path

    def display(self, *objs, **_):
        """As IPython's ``display``: each object becomes a ``display_data`` message; its text goes to the output,
        its images to files."""
        for obj in objs:
            data, meta = mime_bundle(obj, self.save if self.out_path else None)
            if "talo_files" not in meta:
                print(data["text/plain"])
            if self.parent is not None:
                self.send("display_data", {"data": data, "metadata": meta, "transient": {}}, self.parent)

    def flush_figures(self, *_, **__):
        """Save and close the open matplotlib figures (Jupyter's inline backend does the same after each cell)."""
        plt = sys.modules.get("matplotlib.pyplot")
        if plt is None:
            return
        if not self.pyplot_patched:
            plt.show = self.flush_figures
            self.pyplot_patched = True
        if not self.out_path:
            return
        import io
        for num in plt.get_fignums():
            fig, buf = plt.figure(num), io.BytesIO()
            try:
                fig.savefig(buf, format="png", bbox_inches="tight")
            except Exception as error:  # noqa: BLE001
                print(f"[talo: figure {num} could not be saved: {type(error).__name__}: {error}]", file=sys.stderr)
                continue
            path = self.save(buf.getvalue(), ".png")
            self.send("display_data", {"data": {"text/plain": repr(fig)}, "metadata": {"talo_files": {"image/png": path}},
                                       "transient": {}}, self.parent)
        plt.close("all")

    def send(self, msg_type: str, content: dict, parent: dict, metadata: dict | None = None) -> None:
        wire.send(self.wfile, wire.message(msg_type, content, parent=parent, session=self.session, metadata=metadata))

    def status(self, state: str, parent: dict) -> None:
        self.send("status", {"execution_state": state}, parent)

    def run_code(self, code: str, n: int):
        """Run a cell; when its last statement is an expression, return that value (as IPython does)."""
        filename = CELL_FILENAME.format(n=n)
        # register the source so tracebacks can show the failing line (as IPython does)
        linecache.cache[filename] = (len(code), None, code.splitlines(keepends=True), filename)
        tree = ast.parse(code, filename=filename, mode="exec")
        last = None
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            last = ast.Expression(tree.body.pop().value)
        if tree.body:
            exec(compile(tree, filename, "exec"), self.namespace)
        if last is not None:
            return True, eval(compile(last, filename, "eval"), self.namespace)
        return False, None

    def execute(self, msg: dict) -> None:
        content, meta = msg["content"], msg["metadata"]
        code = content.get("code", "")
        silent = bool(content.get("silent", False))
        if not silent:
            self.execution_count += 1
        n = self.execution_count
        self.status("busy", msg)
        self.send("execute_input", {"code": code, "execution_count": n}, msg)
        out_path = meta.get("talo_output")
        self.parent, self.out_path, self.files = msg, out_path, 0
        saved = None
        if out_path:
            sys.stdout.flush()
            sys.stderr.flush()
            fd = os.open(out_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            saved = (os.dup(1), os.dup(2))
            os.dup2(fd, 1)
            os.dup2(fd, 2)
            os.close(fd)
        rss_before, started = rss_bytes(), time.time()
        reply = {"status": "ok", "execution_count": n, "user_expressions": {}, "payload": []}
        result = None
        try:
            has_value, value = self.run_code(code, n)
            if has_value and value is not None:
                result = value
                if not silent:
                    self.namespace["_"] = value
                    self.namespace["Out"][n] = value
            if not silent:
                self.namespace["In"].append(code)
        except BaseException as error:  # noqa: BLE001 - report KeyboardInterrupt / SystemExit instead of exiting
            frames = traceback.TracebackException.from_exception(error)
            frames.stack = traceback.StackSummary.from_list(
                [f for f in frames.stack if not f.filename.endswith("talo/worker.py")])  # drop the worker's own frames
            text, _ = truncate("".join(frames.format()), TRACEBACK_LIMIT)
            reply = {"status": "error", "execution_count": n, "ename": type(error).__name__,
                     "evalue": str(error), "traceback": text.splitlines()}
        finally:
            try:
                self.flush_figures()  # also after an error, as Jupyter does
            except Exception:  # noqa: BLE001
                pass
            sys.stdout.flush()
            sys.stderr.flush()
            if saved:
                os.dup2(saved[0], 1)
                os.dup2(saved[1], 2)
                os.close(saved[0])
                os.close(saved[1])
        if reply["status"] == "error":
            self.send("error", {k: reply[k] for k in ("ename", "evalue", "traceback")}, msg)
        elif result is not None:
            data, data_meta = mime_bundle(result, self.save if out_path else None)
            self.send("execute_result", {"execution_count": n, "data": data, "metadata": data_meta}, msg)
        metrics = {"talo_duration_s": round(time.time() - started, 6), "talo_rss_before": rss_before,
                   "talo_rss_after": rss_bytes()}
        self.send("execute_reply", reply, msg, metadata=metrics)
        self.status("idle", msg)

    def snapshot(self, msg: dict) -> None:
        """Fork a snapshot of the current state (see "Snapshots" in docs/design.md). In the parent this replies with
        the snapshot's pid; in a worker later spawned from the snapshot, it returns with ``self.sock`` replaced."""
        content = msg["content"]
        threads = other_threads()
        if content.get("strict") and threads:
            self.send("talo_snapshot_reply", {"status": "error", "ename": "ThreadsPresent",
                                              "evalue": f"{len(threads)} other threads: {', '.join(threads[:10])}",
                                              "threads": threads}, msg)
            return
        path = content["socket"]
        if os.path.exists(path):
            os.unlink(path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        os.chmod(path, 0o600)
        listener.listen(16)
        sys.stdout.flush()
        sys.stderr.flush()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # see "Fork safety" in docs/design.md
            pid = os.fork()
        if pid:
            listener.close()
            self.send("talo_snapshot_reply", {"status": "ok", "pid": pid, "threads": threads,
                                              "execution_count": self.execution_count}, msg)
            return
        # the snapshot process: drop the worker's connection and wait; serve() returns only in a spawned worker
        self.rfile.close()
        self.wfile.close()
        self.sock.close()
        conn = Snapshot(self, listener, path).serve()
        self.sock, self.rfile, self.wfile = conn, conn.makefile("rb"), conn.makefile("wb")

    def handle(self, msg: dict) -> bool:
        """Handle one message; return False to exit."""
        kind = wire.msg_type(msg)
        if kind == "execute_request":
            self.execute(msg)
        elif kind == "talo_snapshot_request":
            self.snapshot(msg)
        elif kind == "kernel_info_request":
            self.send("kernel_info_reply", {
                "status": "ok", "protocol_version": wire.PROTOCOL_VERSION, "implementation": "talo",
                "implementation_version": wire.VERSION, "banner": f"talo worker (Python {sys.version.split()[0]})",
                "language_info": {"name": "python", "version": sys.version.split()[0], "mimetype": "text/x-python",
                                  "file_extension": ".py"},
                "talo_pid": os.getpid(), "talo_executable": sys.executable}, msg)
        elif kind == "talo_vars_request":
            pattern = msg["content"].get("pattern")
            rows = [describe(k, v) for k, v in list(self.namespace.items())
                    if is_user_variable(k, v) and (not pattern or pattern in k)]
            self.send("talo_vars_reply", {"status": "ok", "variables": rows, "rss": rss_bytes()}, msg)
        elif kind == "complete_request":
            c = msg["content"]
            self.send("complete_reply", complete(self.namespace, c.get("code", ""),
                                                 int(c.get("cursor_pos", len(c.get("code", ""))))), msg)
        elif kind == "talo_fingerprint_request":
            self.send("talo_fingerprint_reply", {"status": "ok", "fingerprints": fingerprints(
                self.namespace, msg["content"].get("names", []))}, msg)
        elif kind == "talo_show_request":
            name, rows = msg["content"]["name"], int(msg["content"].get("rows", 10))
            try:
                value = eval(compile(name, "<talo-show>", "eval"), self.namespace)  # expressions such as df.x, d["k"]
                self.send("talo_show_reply", {"status": "ok", "summary": describe(name, value),
                                              "text": show(value, rows)}, msg)
            except Exception as error:  # noqa: BLE001
                self.send("talo_show_reply", {"status": "error", "ename": type(error).__name__,
                                              "evalue": str(error)}, msg)
        elif kind == "shutdown_request":
            self.send("shutdown_reply", {"status": "ok", "restart": False}, msg)
            return False
        else:
            self.send("error", {"ename": "UnknownMessage", "evalue": kind, "traceback": []}, msg)
        return True

    def serve(self) -> None:
        while True:
            try:
                msg = wire.recv(self.rfile)
            except KeyboardInterrupt:  # interrupt while idle: ignore
                continue
            if msg is None:
                return
            try:
                if not self.handle(msg):
                    return
            except KeyboardInterrupt:  # interrupt landed between messages
                continue


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fd", type=int, required=True)
    ap.add_argument("--session", default="")
    args = ap.parse_args(argv)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    sock = socket.socket(fileno=args.fd)
    sys.argv = [""]  # user code sees the same sys.argv as in an interactive interpreter
    Worker(sock, args.session).serve()


if __name__ == "__main__":
    # When run by path, sys.path[0] is talo's own directory: replace it with "" (the current directory), as in an
    # interactive interpreter, so talo's files never shadow the user's modules.
    sys.path[0] = ""
    main()
