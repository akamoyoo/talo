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
import faulthandler
import hashlib
import importlib
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
import types
import warnings
from pathlib import Path


def _load_wire():
    spec = importlib.util.spec_from_file_location("_talo_wire", Path(__file__).with_name("wire.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wire = _load_wire()

RESULT_FILE_MIN = 10_000  # results longer than this are also saved in full next to the job's output
RESULT_LIMIT = 100_000  # characters of a result's text/plain kept; longer results are truncated and flagged
TRACEBACK_LIMIT = 50_000
CELL_FILENAME = "<talo-{n}>"
SHOW_TIMEOUT = 60.0  # seconds an inspection may take before its fork is killed
DISPLAY_MARK = "\x1b]5379;talo-display={n}\x07"  # OSC sequence marking a display in the output (ignored by terminals)
OUT_KEEP = 3  # results kept in Out (as IPython's _, __, ___); `_` is the last one


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
    if save is not None and len(text) > RESULT_FILE_MIN:  # clients show a prefix; the whole text stays readable
        meta["talo_text_path"], meta["talo_text_chars"] = save(text.encode(errors="replace"), ".txt"), len(text)
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


def inspection_expr(text: str) -> ast.Expression:
    """Parse an expression for ``show``: names, attributes and constant subscripts only (``df``, ``df.x``,
    ``d["k"]``, ``a[0]``), so that inspecting never calls user code that could change the state unrecorded (properties
    and ``__getitem__`` still run). Anything else raises ValueError, pointing to ``talo run``."""
    tree = ast.parse(text.strip(), mode="eval")

    def ok(node) -> bool:
        if isinstance(node, ast.Name):
            return True
        if isinstance(node, ast.Attribute):
            return ok(node.value)
        if isinstance(node, ast.Subscript):
            index = node.slice
            parts = index.elts if isinstance(index, ast.Tuple) else [index]
            simple = all(isinstance(x, ast.Constant) or isinstance(x, ast.UnaryOp) and isinstance(x.operand, ast.Constant)
                         or isinstance(x, ast.Slice) and all(v is None or isinstance(v, ast.Constant)
                                                              for v in (x.lower, x.upper, x.step)) for x in parts)
            return simple and ok(node.value)
        return False

    if not ok(tree.body):
        raise ValueError(f"show only evaluates names, attributes and constant subscripts; use `talo run {text!r}` "
                         "for other expressions (it is recorded in the history)")
    return tree


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
    if not token:  # nothing typed: listing every name would only flood the front end
        return {"status": "ok", "matches": [], "cursor_start": cursor_pos, "cursor_end": cursor_pos, "metadata": {}}
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
    if name.startswith("_") or name in ("In", "Out", "exit", "quit", "get_ipython", "display", "talo"):
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
            arr = np.asarray(value)
            if arr.size and arr.dtype.kind in "biuf":
                parts.append(array_stats(np, arr))
            parts.append(repr(arr.reshape(-1)[:rows] if arr.flags.c_contiguous else arr.flat[:rows]))
        except Exception:  # noqa: BLE001
            parts.append(truncate(repr(value), 2000)[0])
        return "\n".join(parts)
    return truncate(repr(value), 5000)[0]


def array_stats(np, arr) -> str:
    """min / max / NaN count, computed in chunks of about a million elements, so that previewing a large array never
    allocates a copy of it."""
    rows = arr.reshape(1, -1) if arr.ndim == 0 else (arr if arr.ndim > 1 else arr.reshape(-1, 1))
    step = max(1, (1 << 20) // max(1, rows[0].size))
    lo = hi = None
    nan = 0
    for i in range(0, rows.shape[0], step):
        chunk = rows[i:i + step]
        if chunk.dtype.kind == "f":
            bad = int(np.count_nonzero(~np.isfinite(chunk)))
            nan += bad
            if bad == chunk.size:
                continue
            if bad:  # only this chunk is copied
                finite = np.where(np.isfinite(chunk), chunk, np.nan)
                a, b = np.nanmin(finite), np.nanmax(finite)
            else:
                a, b = chunk.min(), chunk.max()
        else:
            a, b = chunk.min(), chunk.max()
        lo = a if lo is None or a < lo else lo
        hi = b if hi is None or b > hi else hi
    return f"min={'nan' if lo is None else lo} max={'nan' if hi is None else hi} non_finite={nan} (exact)"


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
        _CodePickler(_HashWriter(h), protocol=5, buffer_callback=buffer).dump(value)
    except Exception as error:  # noqa: BLE001
        return {"error": f"cannot be pickled ({type(error).__name__})"}
    return {"hash": h.hexdigest()}


def _code_key(code) -> tuple:
    """What a code object does, without where it was written: replayed cells have other names and line numbers."""
    consts = tuple(_code_key(c) if hasattr(c, "co_code") else repr(c) for c in code.co_consts)
    return (code.co_name, code.co_code, consts, code.co_names, code.co_varnames, code.co_freevars)


def _function_key(fn):
    """A function's identity for comparisons: its source when known (bytecode is not stable: CPython compiles the
    same function differently depending on what else is in the cell), else its code."""
    import inspect
    import textwrap
    try:
        return textwrap.dedent(inspect.getsource(fn))
    except (OSError, TypeError):
        return _code_key(fn.__code__)


class _CodePickler(pickle.Pickler):
    """Functions and classes defined in the session cannot be pickled by reference (they are not importable): they
    are fingerprinted by their code instead."""

    def reducer_override(self, obj):
        import types
        if isinstance(obj, types.FunctionType) and obj.__module__ == "__main__":
            return str, (repr(("function", obj.__qualname__, _function_key(obj), obj.__defaults__,
                                obj.__kwdefaults__)),)
        if isinstance(obj, type) and obj.__module__ == "__main__":
            methods = sorted((k, _function_key(v)) for k, v in vars(obj).items() if hasattr(v, "__code__"))
            return str, (repr(("class", obj.__qualname__, [b.__qualname__ for b in obj.__bases__], methods)),)
        return NotImplemented


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


def _wait_child(pid: int) -> None:
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:  # in a snapshot, the SIGCHLD handler may have reaped it already
        pass


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
                elif op == "vars":  # inspected in a disposable fork: user code it triggers cannot change the snapshot
                    ns = self.worker.namespace
                    out = self.worker.in_fork(lambda: {"status": "ok", "variables": [
                        describe(k, v) for k, v in list(ns.items()) if is_user_variable(k, v)]})
                    _reply(conn, {"ok": out.get("status") == "ok", **out,
                                  **({} if out.get("status") == "ok" else {"error": out.get("evalue")})})
                    conn.close()
                elif op == "fingerprint":
                    ns, names = self.worker.namespace, req.get("names", [])
                    out = self.worker.in_fork(lambda: {"status": "ok", "fingerprints": fingerprints(ns, names)},
                                              timeout=3600)
                    _reply(conn, {"ok": out.get("status") == "ok", **out,
                                  **({} if out.get("status") == "ok" else {"error": out.get("evalue")})})
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

def _plain(value):
    """NumPy / pandas scalars and small arrays as plain Python values, for JSON."""
    if hasattr(value, "item") and getattr(value, "shape", None) == ():
        return value.item()
    if hasattr(value, "tolist") and getattr(value, "size", 0) <= 10_000:
        return value.tolist()
    if hasattr(value, "to_dict") and getattr(value, "size", 0) <= 10_000:
        return value.to_dict()
    raise TypeError(f"{type(value).__name__} is not JSON-serializable")


def json_result(value, limit: int) -> dict:
    """``{"value": ...}`` when the value is JSON-serializable within ``limit`` bytes, else ``{"error": ...}``."""
    try:
        text = json.dumps(value, default=_plain, allow_nan=True)
    except (TypeError, ValueError) as error:
        return {"error": f"not JSON: {error}"[:300]}
    if len(text) > limit:
        return {"error": f"result is {len(text)} bytes of JSON, over the limit of {limit}"}
    return {"value": json.loads(text)}


# ---------------------------------------------------------------- checkpoints

_RESTORE = {"ns": {}}  # the session namespace functions saved by source are rebuilt in (set by load_units)


def _rebuild_function(name: str, source: str, defaults=None, kwdefaults=None, attrs=None):
    """Unpickling a function saved by its source: its code compiled from the source, bound to the session's
    namespace as globals (without assigning any name there), with its saved defaults and attributes."""
    import types
    filename = f"<talo-restored-{name}>"
    linecache.cache[filename] = (len(source), None, source.splitlines(keepends=True), filename)  # for tracebacks
    module = compile(source, filename, "exec")
    code = next(c for c in module.co_consts if hasattr(c, "co_name") and c.co_name == name)
    fn = types.FunctionType(code, _RESTORE["ns"], name, defaults)
    fn.__kwdefaults__ = kwdefaults
    fn.__dict__.update(attrs or {})
    return fn





def _rebuild_view(root, offset: int, shape: tuple, strides: tuple, dtype):
    """Unpickling a NumPy view saved with its base: a view of the restored base, so they share memory again."""
    import numpy as np
    return np.ndarray(shape, dtype=np.dtype(dtype), buffer=root, offset=offset, strides=strides)


def _root(arr):
    while getattr(arr, "base", None) is not None and type(arr.base).__name__ == "ndarray":
        arr = arr.base
    return arr


class _SavePickler(pickle.Pickler):
    """Functions defined in the session are saved by their source (cells keep it in linecache); NumPy views of an
    array saved in the same unit are saved as views of it."""

    def __init__(self, *args, roots: set | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.roots = roots or set()

    def reducer_override(self, obj):
        import inspect
        import types
        if type(obj).__name__ == "ndarray" and obj.base is not None:
            root = _root(obj)
            if id(root) in self.roots and root.flags.c_contiguous and root is not obj:
                offset = obj.__array_interface__["data"][0] - root.__array_interface__["data"][0]
                return _rebuild_view, (root, offset, obj.shape, obj.strides, obj.dtype)
        if isinstance(obj, types.FunctionType) and obj.__module__ == "__main__" \
                and obj.__code__.co_filename.startswith(("<talo-", "<talo-restored-")):  # cells, not Talo's own
            if obj.__name__ == "<lambda>" or obj.__code__.co_freevars:
                raise pickle.PicklingError(f"{obj.__qualname__}: lambdas and closures cannot be saved by source")
            try:
                source = inspect.getsource(obj)
            except (OSError, TypeError) as error:
                raise pickle.PicklingError(f"{obj.__qualname__}: source not available") from error
            import textwrap
            source = textwrap.dedent(source)
            if source.lstrip().startswith("@") or obj.__qualname__ != obj.__name__:
                raise pickle.PicklingError(f"{obj.__qualname__}: decorated or nested functions cannot be saved by source")
            return _rebuild_function, (obj.__name__, source, obj.__defaults__, obj.__kwdefaults__, dict(obj.__dict__))
        return NotImplemented


def _alias_root(obj):
    """Objects that must be saved together to stay the same object or share memory after restoring."""
    base = obj
    while getattr(base, "base", None) is not None and type(base.base).__name__ == "ndarray":
        base = base.base
    return id(base)


def _file_digest(path: str) -> str:
    h = hashlib.blake2b(digest_size=20)
    with open(path, "rb") as f:
        while chunk := f.read(1 << 24):
            h.update(chunk)
    return h.hexdigest()


def _makedirs_durable(path: str) -> None:
    """makedirs whose new directory entries survive a crash (each parent is synced after its child appears)."""
    missing = []
    p = os.path.abspath(path)
    while not os.path.isdir(p):
        missing.append(p)
        p = os.path.dirname(p)
    for d in reversed(missing):
        os.makedirs(d, mode=0o700, exist_ok=True)
        _fsync_dir(os.path.dirname(d))


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


CHECKPOINT_FORMAT = 2


def _install(tmp: str, final: str, parent: str) -> bool:
    """Move a completely written file or directory into place; False if the same content is there already."""
    if os.path.exists(final):
        import shutil
        shutil.rmtree(tmp) if os.path.isdir(tmp) else os.unlink(tmp)
        return False
    os.rename(tmp, final)
    _fsync_dir(parent)  # the rename itself survives a crash
    return True


def _put_buffer(raw: memoryview, buffers: str) -> tuple[str, int, bool]:
    """Store one buffer by its content: (hash, size, newly written)."""
    digest = hashlib.blake2b(raw, digest_size=20).hexdigest()
    final = os.path.join(buffers, digest)
    if os.path.lexists(final):
        if not os.path.islink(final) and os.path.getsize(final) == raw.nbytes and _file_digest(final) == digest:
            return digest, raw.nbytes, False
        os.unlink(final)  # damaged or not a regular file: write it again
    tmp = os.path.join(buffers, f".tmp-{os.getpid()}-{wire.new_id()[:8]}")
    with open(tmp, "wb") as f:
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    return digest, raw.nbytes, _install(tmp, final, buffers)


def save_units(namespace: dict, names: list, store: str) -> dict:
    """Write the variables ``names`` into the content-addressed ``store``; returns the units and what was skipped.
    Every out-of-band buffer (NumPy arrays, the data of pandas objects) is stored by its own content in
    ``store/buffers/HASH``, so a variable whose arrays did not change writes nothing new; a unit
    (``store/units/HASH``) holds the pickle stream and the list of its buffers."""
    import json as _json
    groups: dict = {}
    for name in names:
        groups.setdefault(_alias_root(namespace[name]), []).append(name)
    units, skipped = [], []
    units_dir, buffers_dir = os.path.join(store, "units"), os.path.join(store, "buffers")
    _makedirs_durable(units_dir)
    _makedirs_durable(buffers_dir)
    for members in groups.values():
        tmp = os.path.join(units_dir, f".tmp-{os.getpid()}-{wire.new_id()[:8]}")
        os.makedirs(tmp)
        hs = hashlib.blake2b(digest_size=20)
        buffers, new_bytes = [], 0
        try:
            def buffer(b: pickle.PickleBuffer) -> bool:
                nonlocal new_bytes
                digest, size, new = _put_buffer(b.raw(), buffers_dir)
                buffers.append([digest, size])
                new_bytes += size if new else 0
                return False
            with open(os.path.join(tmp, "stream"), "wb") as fs:
                class Tee:
                    def write(self, data):
                        hs.update(data)
                        return fs.write(data)
                roots = {id(_root(namespace[n])) for n in members if type(namespace[n]).__name__ == "ndarray"}
                _SavePickler(Tee(), protocol=5, buffer_callback=buffer, roots=roots).dump(
                    {n: namespace[n] for n in sorted(members, key=lambda n: getattr(namespace[n], "base", None) is not None)})
                fs.flush()
                os.fsync(fs.fileno())
            digest = _unit_digest(hs, buffers)
            with open(os.path.join(tmp, "index.json"), "w") as f:
                _json.dump({"format": CHECKPOINT_FORMAT, "buffers": buffers, "names": members}, f)
                f.flush()
                os.fsync(f.fileno())
            _fsync_dir(tmp)
            stream_bytes = os.path.getsize(os.path.join(tmp, "stream"))
            new = _install(tmp, os.path.join(units_dir, digest), units_dir)
            units.append({"hash": digest, "names": members, "bytes": stream_bytes + sum(s for _, s in buffers),
                          "new_bytes": new_bytes + (stream_bytes if new else 0),
                          "buffer_bytes": sum(s for _, s in buffers), "stream_bytes": stream_bytes})
        except OSError:  # the storage failed (disk full, I/O error): the checkpoint fails, nothing is "skipped"
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        except Exception as error:  # noqa: BLE001 - this unit cannot be serialized: skipped, the others are saved
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
            skipped += [{"name": n, "reason": f"{type(error).__name__}: {error}"[:200]} for n in members]
    return {"units": units, "skipped": skipped}


def _unit_digest(stream_hash, buffers: list) -> str:
    """A unit's identity: its stream and the identities of its buffers, in order."""
    h = hashlib.blake2b(stream_hash.digest(), digest_size=20)
    for digest, size in buffers:
        h.update(bytes.fromhex(digest) + size.to_bytes(8, "little"))
    return h.hexdigest()


def load_units(namespace: dict, store: str, units: list, only: list | None = None, mapped: bool = False,
               verify: bool = True) -> dict:
    """Read units back into ``namespace``, checking every buffer's and stream's hash; buffers become ordinary
    writable memory, a separate allocation for each restored buffer. With ``mapped``, buffers are mapped read-only
    from their files instead (shared datasets): no copy, shared with every other reader."""
    import json as _json
    hex40 = re.compile(r"[0-9a-f]{40}")
    for unit in units:  # checked before anything is unpickled: names, hashes, no link in the way
        if not hex40.fullmatch(str(unit.get("hash"))) or not all(
                isinstance(n, str) and n.isidentifier() for n in unit.get("names", [])):
            raise ValueError(f"malformed checkpoint unit {str(unit)[:200]}")
    if any(os.path.islink(p) for p in (store, os.path.join(store, "units"), os.path.join(store, "buffers"))):
        raise ValueError("the checkpoint store goes through a symbolic link; refusing to load from it")
    _RESTORE["ns"] = namespace
    loaded, total = [], 0
    for unit in units:
        if only and not set(unit["names"]) & set(only):
            continue
        d = os.path.join(store, "units", unit["hash"])
        if os.path.islink(d):
            raise ValueError(f"unit {unit['hash']} is a symbolic link; refusing to load it")
        index = _json.loads(open(os.path.join(d, "index.json")).read())
        if index.get("format") != CHECKPOINT_FORMAT or not all(
                hex40.fullmatch(str(h)) and isinstance(s, int) and s >= 0 for h, s in index["buffers"]):
            raise ValueError(f"unit {unit['hash']} has an unknown format or malformed buffers")
        with open(os.path.join(d, "stream"), "rb") as f:
            stream = f.read()
        views = []
        for digest, size in index["buffers"]:
            path = os.path.join(store, "buffers", digest)
            if os.path.islink(path) or os.path.getsize(path) != size:
                raise ValueError(f"unit {unit['hash']} ({', '.join(unit['names'])}) is corrupt: buffer {digest} "
                                 "is missing, truncated or a link")
            if mapped:
                import mmap
                with open(path, "rb") as f:
                    data = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ) if size else b""
            else:
                data = bytearray(size)
                with open(path, "rb") as f:
                    f.readinto(data)
            if verify and hashlib.blake2b(data, digest_size=20).hexdigest() != digest:
                raise ValueError(f"unit {unit['hash']} ({', '.join(unit['names'])}) is corrupt: buffer {digest} "
                                 "does not match its hash")
            views.append(memoryview(data))
        if verify and _unit_digest(hashlib.blake2b(stream, digest_size=20), index["buffers"]) != unit["hash"]:
            raise ValueError(f"unit {unit['hash']} ({', '.join(unit['names'])}) is corrupt: its stream does not "
                             "match its hash")
        values = pickle.loads(stream, buffers=views)
        for name, value in values.items():
            if not only or name in only:
                namespace[name] = value
                loaded.append(name)
        total += unit["bytes"]
    return {"loaded": loaded, "bytes": total}


# ---------------------------------------------------------------- reload

def _source_hash(path: str) -> str | None:
    try:
        with open(path, "rb") as f:
            return hashlib.blake2b(f.read(), digest_size=12).hexdigest()
    except OSError:
        return None


def _patch_function(old, new) -> str | None:
    """Give the old function the new one's code, so every reference to it runs the new code; None when done."""
    if not isinstance(old, types.FunctionType) or not isinstance(new, types.FunctionType):
        return "not a plain Python function in both versions"
    if old.__code__.co_freevars != new.__code__.co_freevars:
        return "its closure variables changed"
    old.__code__ = new.__code__
    old.__defaults__, old.__kwdefaults__ = new.__defaults__, new.__kwdefaults__
    old.__annotations__, old.__doc__ = new.__annotations__, new.__doc__
    old.__dict__.update(new.__dict__)
    return None


def _patch_in_place(old, new) -> str | None:
    """``reload --in-place``: update a definition of the previous version of a module from the new version, so
    references and instances made before the reload use the new code (``isinstance`` against the new class stays
    false for old instances). Methods are patched function by function, which keeps zero-argument ``super()``
    bound to the old class. Returns None when patched, else why not."""
    if new is None:
        return "removed from the new version"
    if not isinstance(old, type):
        return _patch_function(old, new)
    if not isinstance(new, type):
        return "no longer a class"
    for key, value in new.__dict__.items():
        if key in ("__dict__", "__weakref__", "__module__", "__qualname__"):
            continue
        current = old.__dict__.get(key)
        if isinstance(value, (classmethod, staticmethod)) and type(current) is type(value) \
                and _patch_function(current.__func__, value.__func__) is None:
            continue
        if _patch_function(current, value) is None:
            continue
        try:
            setattr(old, key, value)
        except (AttributeError, TypeError) as error:
            return f"cannot set {key}: {error}"
    for key in [k for k in old.__dict__ if k not in new.__dict__ and not (k.startswith("__") and k.endswith("__"))]:
        delattr(old, key)
    return None


def _excluded(path: str) -> bool:
    """Files of installed packages and interpreters, which ``reload --changed`` never selects."""
    real = os.path.realpath(path)
    prefixes = {os.path.realpath(p) for p in (sys.prefix, sys.base_prefix, sys.exec_prefix)}
    return ("site-packages" in real.split(os.sep) or "dist-packages" in real.split(os.sep)
            or any(part in (".venv", "venv", ".tox") for part in real.split(os.sep))
            or any(real.startswith(p + os.sep) for p in prefixes))


class Reloader:
    """Remembers the source hash of each module when it is first seen, right after the job that imported it, so that
    ``reload --changed`` can tell which modules changed on disk since they were loaded. If the file was modified
    after that job started, the hash may not describe the code in memory: the module is marked uncertain and
    ``--changed`` selects it."""

    def __init__(self):
        self.hashes: dict[str, str] = {}
        self.uncertain: set[str] = set()
        self.old_defs: dict[int, tuple] = {}  # id -> (module, attribute, object) of every replaced definition

    def record(self, job_started: float | None = None) -> None:
        for name, module in list(sys.modules.items()):
            path = getattr(module, "__file__", None)
            if name not in self.hashes and path and path.endswith(".py"):
                if (h := _source_hash(path)) is not None:
                    self.hashes[name] = h
                    try:
                        if job_started is not None and os.stat(path).st_mtime >= job_started - 1:
                            self.uncertain.add(name)
                    except OSError:
                        pass

    def changed(self, root: str) -> list[str]:
        root = os.path.realpath(root)
        out = []
        for name, module in list(sys.modules.items()):  # import order: dependencies usually come first
            path = getattr(module, "__file__", None)
            if not path or not path.endswith(".py") or _excluded(path):
                continue
            if not os.path.realpath(path).startswith(root + os.sep):
                continue
            if name in self.hashes and (name in self.uncertain or _source_hash(path) != self.hashes[name]):
                out.append(name)
        return out

    def reload(self, namespace: dict, names: list[str], in_place: bool = False) -> dict:
        report, previous, patched, skipped = [], {}, [], []
        for name in names:
            module = sys.modules.get(name)
            path = getattr(module, "__file__", None) if module else None
            if module is None:
                report.append({"module": name, "status": "failed", "error": "not imported in this session"})
                continue
            if not path or not path.endswith(".py"):
                report.append({"module": name, "status": "unsupported", "error": "not a Python source module"})
                continue
            old_hash, new_hash = self.hashes.get(name), _source_hash(path)
            previous[name] = dict(vars(module))
            cached = getattr(module, "__cached__", None)
            if cached and old_hash != new_hash:
                # the .pyc is validated by mtime and size only: an edit within the same second that keeps the size
                # would otherwise reload the old bytecode
                try:
                    os.unlink(cached)
                except OSError:
                    pass
            try:
                importlib.reload(module)
            except Exception as error:  # noqa: BLE001 - a syntax or import-time error in the user's module
                report.append({"module": name, "status": "failed", "old_hash": old_hash, "new_hash": new_hash,
                               "error": f"{type(error).__name__}: {error}"})
                continue
            uncertain = name in self.uncertain
            self.hashes[name] = new_hash
            self.uncertain.discard(name)
            report.append({"module": name, "status": "reloaded" if old_hash != new_hash or uncertain else "unchanged",
                           "old_hash": old_hash, "new_hash": new_hash})
            current = vars(module)
            for attr, obj in previous[name].items():  # remember replaced definitions of every version
                if (isinstance(obj, type) or callable(obj) and hasattr(obj, "__code__")) \
                        and current.get(attr) is not obj and getattr(obj, "__module__", None) == name:
                    reason = _patch_in_place(obj, current.get(attr)) if in_place else "not requested"
                    if reason is None:
                        patched.append({"module": name, "attribute": attr,
                                        "kind": "class" if isinstance(obj, type) else "function"})
                        continue
                    if in_place:
                        skipped.append({"module": name, "attribute": attr, "reason": reason})
                    self.old_defs[id(obj)] = (name, attr, obj)  # the reference keeps the id from being reused
        out = {"modules": report, "stale_bindings": self.stale(namespace), "scan_scope": "direct_namespace"}
        if in_place:
            out.update(patched=patched, skipped_in_place=skipped)
        return out

    def stale(self, namespace: dict) -> list:
        """Names bound directly to a function or class of any earlier version of a reloaded module, or to an
        instance of such a class."""
        out = []
        for name, value in list(namespace.items()):
            if name.startswith("_"):
                continue
            if id(value) in self.old_defs and self.old_defs[id(value)][2] is value:
                module_name, attr, _ = self.old_defs[id(value)]
                out.append({"name": name, "kind": "definition", "module": module_name, "attribute": attr,
                            "rebind": f"from {module_name} import {attr}" + (f" as {name}" if name != attr else "")})
            elif id(type(value)) in self.old_defs and self.old_defs[id(type(value))][2] is type(value):
                module_name, attr, _ = self.old_defs[id(type(value))]
                out.append({"name": name, "kind": "instance", "module": module_name, "attribute": attr,
                            "rebind": f"recreate {name}: it is an instance of the old {module_name}.{attr}"})
        return out


class Helper:
    """The ``talo`` object in the user's namespace."""

    PROGRESS_INTERVAL = 0.2  # seconds between progress messages (the last update of a job is always sent)

    def __init__(self, worker: "Worker"):
        self._worker = worker
        self._last = 0.0
        self.case = None  # in a map case: {"map": id, "index": i}

    def progress(self, done, total=None, note: str = "") -> None:
        """Report progress of the running job: ``talo.progress(i, n, "day 2024-03-01")``. Shown by ``talo jobs``,
        ``wait`` and ``brief``; rate-limited, so it is cheap to call in a loop."""
        now = time.monotonic()
        final = total is not None and done >= total
        if self._worker.parent is None or (now - self._last < self.PROGRESS_INTERVAL and not final):
            return
        self._last = now
        self._worker.send("talo_progress", {"done": done, "total": total, "note": str(note)[:200]},
                          self._worker.parent)

    def record(self, key: str | None = None, **fields) -> str:
        """Add a record to the experiment ledger: ``talo.record(ic=0.012, factor="gap_mom")``. Values are JSON scalars
        or strings (cut to 200 characters). The supervisor adds where the record came from (job or map case,
        revision, snapshot, code hashes) and later the outcome of the job. Returns the record id."""
        if self._worker.parent is None:
            raise RuntimeError("talo.record can only be used while code runs in a Talo session")
        clean = {}
        for name, value in fields.items():
            value = _plain(value) if hasattr(value, "item") else value
            if isinstance(value, str):
                value = value[:200]
            elif not (value is None or isinstance(value, (bool, int, float))):
                raise TypeError(f"talo.record: {name} must be a number, bool, None or string, not {type(value).__name__}")
            clean[name] = value
        # with a key, the id is derived from it, so a retry returns the id of the record that exists
        record_id = ("rk" + hashlib.blake2b(f"{self._worker.session}:{key}".encode(), digest_size=5).hexdigest()
                     if key is not None else "r" + wire.new_id()[:10])
        modules = {name: h for name, h in self._worker.reloader.hashes.items()
                   if (m := sys.modules.get(name)) is not None and getattr(m, "__file__", None)
                   and not _excluded(m.__file__) and os.path.realpath(m.__file__).startswith(os.getcwd() + os.sep)}
        self._worker.send("talo_record", {"id": record_id, "key": key, "fields": clean, "modules": dict(list(modules.items())[:50])},
                          self._worker.parent)
        return record_id

    def expose(self, fn, name: str | None = None) -> str:
        """Offer ``fn`` as a service of this session: other sessions and agents call it with JSON arguments
        (``talo call SESSION.NAME '{...}'``). The service is published when the running job ends successfully, as a
        new version that keeps the session's state at that moment; calls run in forks of it."""
        if self._worker.parent is None:
            raise RuntimeError("talo.expose can only be used while code runs in a Talo session")
        if not callable(fn):
            raise TypeError("talo.expose needs a function")
        name = name or getattr(fn, "__name__", None)
        if not name or not name.isidentifier():
            raise ValueError(f"service name {name!r} must be an identifier")
        self._worker.services[name] = fn
        self._worker.send("talo_expose", {"name": name}, self._worker.parent)
        return name

    def artifact_path(self, name: str = "") -> str:
        """A path in this job's own directory for files it writes (``sessions/S/artifacts/JOB/NAME``): rolling back
        memory never removes files, so each job's outputs are kept apart, listed by ``talo artifacts JOB`` and
        published deliberately. ``name`` must be relative and stay inside the directory."""
        out = self._worker.out_path
        if not out:
            raise RuntimeError("talo.artifact_path can only be used while code runs in a Talo session")
        if os.path.isabs(name) or ".." in Path(name).parts:
            raise ValueError(f"artifact name {name!r} must be a relative path inside the job's directory")
        base = Path(out)
        root = (base.parent.parent / "artifacts" / base.stem) if base.parent.name == "jobs" \
            else base.with_suffix(".artifacts")
        if root.is_symlink() or any(p.is_symlink() for p in root.parents if p != Path("/")):
            raise ValueError(f"the artifact directory {root} goes through a symbolic link")
        path = root / name
        (path.parent if name else path).mkdir(parents=True, exist_ok=True)
        real_root = os.path.realpath(root)
        if os.path.realpath(path) != real_root and not os.path.realpath(path).startswith(real_root + os.sep):
            raise ValueError(f"artifact name {name!r} leads outside the job's directory (through a link)")
        return str(path)

    def _save(self, names: list | None, store: str) -> dict:
        """talo save, in a fork of the session: write variables to the store (see "Checkpoints" in design.md)."""
        ns = self._worker.namespace
        selection = names  # None: everything, including what imports bound
        names = names or [k for k, v in list(ns.items()) if is_user_variable(k, v) and k != "talo"]
        missing = [n for n in names if n not in ns]
        out = save_units(ns, [n for n in names if n in ns], store)
        out["skipped"] += [{"name": n, "reason": "no such variable"} for n in missing]
        cwd = os.path.realpath(os.getcwd())
        out["imports"] = {}
        for k, v in list(ns.items()):  # names bound by import statements: brought back by importing again
            if k.startswith("_") or k in ("In", "Out", "talo", "display") or (selection and k not in selection):
                continue
            if isinstance(v, type(sys)):
                out["imports"][k] = {"module": v.__name__}
            elif not is_user_variable(k, v) and getattr(v, "__module__", None) not in (None, "__main__"):
                out["imports"][k] = {"module": v.__module__, "attr": getattr(v, "__qualname__", k)}
        out["modules"] = sorted(name for name, m in list(sys.modules.items())
                                if (f := getattr(m, "__file__", None)) and not _excluded(f)
                                and os.path.realpath(f).startswith(cwd + os.sep))
        out["versions"] = {m: getattr(sys.modules[m], "__version__", None) for m in ("numpy", "pandas", "pyarrow")
                           if m in sys.modules}
        out["python"] = sys.version.split()[0]
        return out

    def _load(self, store: str, units: list, only: list | None = None, modules: list | None = None,
              versions: dict | None = None, imports: dict | None = None) -> dict:
        """talo restore: import the session's modules and names bound by imports, then read the units back."""
        import importlib
        failed = []
        for name, spec in (imports or {}).items():
            if only and name not in only:
                continue
            try:
                value = importlib.import_module(spec["module"])
                for part in spec.get("attr", "").split(".") if spec.get("attr") else []:
                    value = getattr(value, part)
                self._worker.namespace[name] = value
            except Exception as error:  # noqa: BLE001
                failed.append(f"{name} ({spec['module']}): {type(error).__name__}: {error}"[:200])
        if failed:
            raise ImportError("cannot restore the names bound by imports: " + "; ".join(failed))
        for m in modules or []:
            try:
                importlib.import_module(m)
            except Exception as error:  # noqa: BLE001
                failed.append(f"{m}: {type(error).__name__}: {error}"[:200])
        out = load_units(self._worker.namespace, store, units, only)
        out["module_errors"] = failed
        out["version_changes"] = {m: [v, getattr(sys.modules.get(m), "__version__", None)]
                                  for m, v in (versions or {}).items()
                                  if m in sys.modules and getattr(sys.modules[m], "__version__", None) != v}
        return out

    def _datasets(self) -> str:
        root = os.environ.get("TALO_DATASETS")
        if not root:
            raise RuntimeError("shared datasets need $TALO_DATASETS (set for workers started by Talo)")
        return root

    def _store_lock(self, root: str):
        """Shared while publishing or mapping, exclusive for garbage collection: gc never removes buffers that a
        publication has written but not yet listed, or that a reader is about to map."""
        import fcntl
        os.makedirs(root, mode=0o700, exist_ok=True)
        lock = open(os.path.join(root, ".gc-lock"), "a")
        fcntl.flock(lock, fcntl.LOCK_SH)
        return lock

    def publish(self, name: str, value, key: str | None = None) -> int:
        """Publish ``value`` as a new, immutable version of the shared dataset ``name`` (``talo.get(name)`` in any
        session maps its buffers without copying); returns the version number. Unchanged buffers are shared with
        earlier versions. Publishing takes effect at once: a later failure or rollback of the job does not undo it.
        With ``key``, publishing again under the same key returns the version already published. Version numbers
        are never reused, even after versions are dropped."""
        import fcntl
        import json as _json
        import shutil
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            raise ValueError(f"dataset name {name!r}: letters, digits, '.', '-' and '_' only")
        root = self._datasets()
        folder = os.path.join(root, "datasets", name)
        _makedirs_durable(folder)
        with self._store_lock(root), open(os.path.join(folder, ".lock"), "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)  # one writer of this dataset's versions and pointer at a time
            manifests = [_json.loads(open(os.path.join(folder, f)).read()) for f in os.listdir(folder)
                         if re.fullmatch(r"v\d+\.json", f)]
            if key is not None:  # a retry: before anything that could fail for another reason
                done = [m for m in manifests if m.get("key") == key]
                if done:
                    self._dataset_message(name, done[0]["version"], "published", deduplicated=True)
                    return done[0]["version"]
            estimate, free = _buffer_nbytes(value) or 0, shutil.disk_usage(root).free
            if estimate > free:
                raise OSError(f"publishing {name} needs about {estimate >> 20} MB; {root} has {free >> 20} MB free")
            out = save_units({"value": value}, ["value"], os.path.join(root, "store"))
            if out["skipped"]:
                raise ValueError(f"cannot publish {name}: {out['skipped'][0]['reason']}")
            unit = out["units"][0]
            counter = os.path.join(folder, "counter")
            last = int(open(counter).read()) if os.path.exists(counter) else 0
            version = max([last, *(m["version"] for m in manifests)]) + 1
            manifest = {"format": CHECKPOINT_FORMAT, "dataset": name, "version": version, "created": time.time(),
                        "key": key, "unit": unit, "type": type(value).__name__,
                        "shape": list(getattr(value, "shape", []) or []), "session": self._worker.session,
                        "job": (self._worker.parent or {}).get("metadata", {}).get("talo_job"), "pid": os.getpid()}
            for fname, content in (("counter", str(version)), (f"v{version}.json", _json.dumps(manifest, indent=1)),
                                   ("latest", str(version))):
                tmp = os.path.join(folder, f".{fname}.{os.getpid()}.tmp")
                with open(tmp, "w") as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, os.path.join(folder, fname))  # counter, the version, then the pointer to it
            _fsync_dir(folder)
        hints = self._sharing_hints(value, unit["stream_bytes"], unit["bytes"])
        self._dataset_message(name, version, "published", bytes=unit["bytes"], new_bytes=unit["new_bytes"],
                              mapped_bytes=unit["buffer_bytes"], private_bytes=unit["stream_bytes"], hints=hints)
        if hints:
            print(f"talo.publish({name!r}): {unit['stream_bytes'] >> 10} KB of it is rebuilt by every reader; "
                  + "; ".join(hints), file=sys.stderr)
        return version

    @staticmethod
    def _sharing_hints(value, private: int, total: int) -> list:
        """Why part of a published value cannot be shared by mapping, and what to do about it."""
        if private < (1 << 20) or private < total * 0.05:
            return []
        kind = type(value).__module__.split(".")[0]
        hints = []
        if kind == "pandas":
            frame = value.to_frame() if hasattr(value, "to_frame") and not hasattr(value, "columns") else value
            objects = [str(c) for c, d in getattr(frame, "dtypes", {}).items() if str(d) == "object"]
            if objects:
                hints.append(f"object columns {', '.join(objects[:10])} are rebuilt by every reader: store strings as "
                             "'str' / 'string[pyarrow]' (pyarrow installed) and other values as typed columns")
            index = getattr(value, "index", None)
            if index is not None and getattr(index, "tz", None) is not None:
                hints.append("a time zone-aware index is rebuilt by every reader: keep the times as a column, or "
                             "an index without time zone (tz_convert(None)) plus a note of the zone")
            elif index is not None and str(getattr(index, "dtype", "")) == "object":
                hints.append("an object index is rebuilt by every reader: use a typed index or reset_index()")
        elif kind == "polars":
            hints.append("polars frames are pickled whole: publish value.to_arrow() and read with pl.from_arrow(), "
                         "both without copying")
        elif kind == "torch":
            hints.append("torch tensors are pickled whole: publish tensor.numpy() and read with torch.from_numpy()")
        if not hints:
            hints.append("Python objects (lists, dicts, strings, object arrays) are rebuilt by every reader; "
                         "NumPy arrays, numeric and Arrow-backed pandas columns and pyarrow tables are shared")
        return hints

    def _dataset_message(self, name: str, version: int, action: str, **extra) -> None:
        if self._worker.parent is not None:
            self._worker.send("talo_dataset", {"name": name, "version": int(version), "action": action, **extra},
                              self._worker.parent)

    def get(self, name: str, version: int | None = None, copy: bool = False, verify: bool = False):
        """The shared dataset ``name`` (the latest version, resolved now, or ``version``): its buffers mapped
        read-only, shared with every other reader; ``copy=True`` gives private writable memory."""
        import json as _json
        root = self._datasets()
        folder = os.path.join(root, "datasets", name)
        with self._store_lock(root):  # held from resolving the version until its buffers are mapped: gc waits
            if version is None:
                try:
                    version = int(open(os.path.join(folder, "latest")).read())
                except FileNotFoundError:
                    raise LookupError(f"no dataset {name!r} (`talo datasets` lists them)") from None
            try:
                manifest = _json.loads(open(os.path.join(folder, f"v{int(version)}.json")).read())
            except FileNotFoundError:
                raise LookupError(f"dataset {name!r} has no version {version}") from None
            box: dict = {}
            load_units(box, os.path.join(root, "store"), [manifest["unit"]], mapped=not copy, verify=verify)
        self._dataset_message(name, version, "read", copy=copy)
        value = box["value"]
        if not copy and type(value).__module__.startswith("pandas") and hasattr(value, "copy"):
            # pandas copies a block on write only when another object shares it: hand out a shallow copy that keeps
            # the mapped original alive (for as long as the copy lives), so editing it copies the edited block
            original, value = value, value.copy(deep=False)
            object.__setattr__(value, "_talo_mapped", original)
        return value

    def _serve_call(self, name: str, kwargs: dict):
        """Run a service function (in a fork of a service version)."""
        self.in_call = True
        if name not in self._worker.services:
            raise LookupError(f"this version has no service {name!r}")
        return self._worker.services[name](**kwargs)

    def call(self, service: str, args: dict | None = None, *, version: int | None = None,
             timeout: float | None = None, **kwargs):
        """Call a service of a session (``"session.name"``) with JSON arguments (``args`` and keyword arguments
        together; put arguments named ``version`` or ``timeout`` in ``args``); returns its result or raises."""
        kwargs = {**(args or {}), **kwargs}
        if self.case is not None or getattr(self, "in_call", False):
            raise RuntimeError("talo.call cannot be used inside a map case or a service call (nested calls can "
                               "deadlock)")
        path = os.environ.get("TALO_SOCKET")
        if not path:
            raise RuntimeError("talo.call needs $TALO_SOCKET (set for workers started by Talo)")
        request = {"op": "call", "service": service, "args": kwargs, "timeout": timeout, "version": version,
                   "owner": os.environ.get("TALO_AGENT") or "user"}
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(path)
            s.sendall(json.dumps(request).encode() + b"\n")
            reply = json.loads(s.makefile("rb").readline() or b'{"ok": false, "error": {"message": "disconnected"}}')
        if not reply.get("ok"):
            raise RuntimeError(f"talo.call {service}: {reply['error']['message']}")
        call = reply["call"]
        if call["status"] == "ok":
            if call.get("result_error"):
                raise RuntimeError(f"talo.call {service}: {call['result_error']}")
            return call["result"]
        if call["status"] in ("running", "queued"):
            raise TimeoutError(f"talo.call {service}: call {call['id']} still running (`talo wait {call['id']}`)")
        error = call.get("error") or {}
        raise RuntimeError(f"talo.call {service} failed: {error.get('ename')}: {error.get('evalue')}")

    def __repr__(self) -> str:
        return "<talo helper: talo.progress, talo.record, talo.expose, talo.call>"


class Worker:
    def __init__(self, sock: socket.socket, session: str):
        self.sock = sock
        self.rfile = sock.makefile("rb")
        self.wfile = sock.makefile("wb")
        self.session = session
        self.execution_count = 0
        self.namespace = {"__name__": "__main__", "__builtins__": __builtins__, "In": [""], "Out": {},
                          "display": self.display, "talo": Helper(self)}
        # `import talo` (in a cell or in a module the session imports) gives this helper, whether or not the
        # interpreter has the talo package installed; the worker never imports the package itself
        sys.modules["talo"] = self.namespace["talo"]
        self.parent: dict | None = None    # the execute_request being run, for display_data
        self.out_path: str | None = None   # its output file; images are written next to it
        self.files = 0
        self.displays = 0
        self.unstored = 0  # executions not stored in the history (silent), for their cell names
        self.pyplot_patched = False
        self.services: dict = {}  # name -> function exposed with talo.expose
        self.reloader = Reloader()
        self.reloader.record()

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
            self.display_bundle(data, meta, print_text="talo_files" not in meta)

    def display_bundle(self, data: dict, meta: dict, print_text: bool) -> None:
        """Send a display_data. Its place among the printed output is a marker written into the output file
        (``DISPLAY_MARK``, an escape sequence terminals ignore), so a front end can put it exactly where it happened;
        its text representation follows the marker, for the CLI, and ``talo_end`` says where that text ends."""
        self.displays += 1
        meta["talo_display"] = self.displays
        start = self.output_offset()
        if start is not None:
            sys.stdout.write(DISPLAY_MARK.format(n=self.displays))
            if print_text:
                print(data["text/plain"])  # the CLI shows the text in place; Jupyter front ends show the bundle
            meta["talo_offset"], meta["talo_end"] = start, self.output_offset()
        if self.parent is not None:
            self.send("display_data", {"data": data, "metadata": meta, "transient": {}}, self.parent)

    def user_expressions(self, expressions: dict) -> dict:
        """Jupyter's user_expressions: each evaluated after the code, as a mime bundle or an error."""
        out = {}
        for name, expr in expressions.items():
            try:
                data, meta = mime_bundle(eval(compile(str(expr), "<user-expression>", "eval"), self.namespace))
                out[name] = {"status": "ok", "data": data, "metadata": meta}
            except Exception as error:  # noqa: BLE001
                out[name] = {"status": "error", "ename": type(error).__name__, "evalue": str(error), "traceback": []}
        return out

    def output_offset(self) -> int | None:
        """Bytes written to the job's output so far: where a display sits among the printed text."""
        if not self.out_path:
            return None
        sys.stdout.flush()
        sys.stderr.flush()
        try:
            return os.fstat(1).st_size
        except OSError:
            return None

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
            self.display_bundle({"text/plain": repr(fig)}, {"talo_files": {"image/png": path}}, print_text=False)
        plt.close("all")

    def send(self, msg_type: str, content: dict, parent: dict, metadata: dict | None = None) -> None:
        wire.send(self.wfile, wire.message(msg_type, content, parent=parent, session=self.session, metadata=metadata))

    def status(self, state: str, parent: dict) -> None:
        self.send("status", {"execution_state": state}, parent)

    def run_code(self, code: str, n: int, stored: bool = True):
        """Run a cell; when its last statement is an expression, return that value (as IPython does). A cell that is
        not stored in the history (silent) gets its own name, so it never replaces the source of cell n."""
        if stored:
            filename = CELL_FILENAME.format(n=n)
        else:
            self.unstored += 1
            filename = f"<talo-silent-{self.unstored}>"
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
        store = content.get("store_history", True) and not silent
        if store:
            self.execution_count += 1
        n = self.execution_count
        self.status("busy", msg)
        self.send("execute_input", {"code": code, "execution_count": n}, msg)
        out_path = meta.get("talo_output")
        self.parent, self.out_path, self.files, self.displays = msg, out_path, 0, 0
        stack_file = None
        if out_path:  # talo diagnose: SIGUSR1 dumps every thread's stack here, even inside C code
            stack_file = open(out_path.removesuffix(".out") + ".stack", "w")
            faulthandler.register(signal.SIGUSR1, file=stack_file, all_threads=True, chain=False)
        if "talo_bind" in meta:  # an ephemeral worker: names the code expects (map's `case`)
            self.namespace.update(meta["talo_bind"])
            self.namespace["talo"].case = meta.get("talo_case")
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
        self.reloader.record()  # modules imported by earlier messages (not by this job)
        reply = {"status": "ok", "execution_count": n, "user_expressions": {}, "payload": []}
        result = None
        try:
            has_value, value = self.run_code(code, n, stored=store)
            if has_value and value is not None:
                result = value
                if store:
                    self.namespace["_"] = value
                    out = self.namespace["Out"]
                    out[n] = value
                    for k in sorted(out)[:-OUT_KEEP]:  # older results would pin large objects in memory
                        del out[k]
            if store:
                self.namespace["In"].append(code)
        except BaseException as error:  # noqa: BLE001 - report KeyboardInterrupt / SystemExit instead of exiting
            if meta.get("talo_retain_error"):  # run --retain-error: the frames stay reachable for a snapshot
                tb = error.__traceback__
                while tb is not None and tb.tb_frame.f_code.co_filename == __file__:  # start at the user's code
                    tb = tb.tb_next
                self.namespace["_talo_error"] = error.with_traceback(tb)
            frames = traceback.TracebackException.from_exception(error)
            frames.stack = traceback.StackSummary.from_list(
                [f for f in frames.stack if not f.filename.endswith("talo/worker.py")])  # drop the worker's own frames
            text, _ = truncate("".join(frames.format()), TRACEBACK_LIMIT)
            reply = {"status": "error", "execution_count": n, "ename": type(error).__name__,
                     "evalue": str(error), "traceback": text.splitlines()}
        finally:
            if stack_file is not None:
                faulthandler.unregister(signal.SIGUSR1)
                stack_file.close()
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
        self.reloader.record(started)
        if reply["status"] == "error":
            self.send("error", {k: reply[k] for k in ("ename", "evalue", "traceback")}, msg)
        elif result is not None:
            data, data_meta = mime_bundle(result, self.save if out_path else None)
            if meta.get("talo_json_result") is not None:
                data_meta["talo_json"] = json_result(result, int(meta["talo_json_result"]))
            self.send("execute_result", {"execution_count": n, "data": data, "metadata": data_meta}, msg)
        metrics = {"talo_duration_s": round(time.time() - started, 6), "talo_rss_before": rss_before,
                   "talo_rss_after": rss_bytes()}
        if reply["status"] == "ok" and content.get("user_expressions"):
            reply["user_expressions"] = self.user_expressions(content["user_expressions"])
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

    def show(self, content: dict) -> dict:
        name, rows = content["name"], int(content.get("rows", 10))
        try:
            value = eval(compile(inspection_expr(name), "<talo-show>", "eval"), self.namespace)
            return {"status": "ok", "summary": describe(name, value), "text": show(value, rows)}
        except Exception as error:  # noqa: BLE001
            return {"status": "error", "ename": type(error).__name__, "evalue": str(error)}

    def in_fork(self, fn, timeout: float = SHOW_TIMEOUT) -> dict:
        """Run ``fn()`` in a disposable fork and return its result: properties, ``__getitem__`` or ``__repr__`` that
        change state while inspecting change only the copy."""
        r, w = os.pipe()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # see "Fork safety" in docs/design.md
            pid = os.fork()
        if pid == 0:
            os.close(r)
            try:
                data = json.dumps(fn(), default=str).encode()
            except BaseException as error:  # noqa: BLE001
                data = json.dumps({"status": "error", "ename": type(error).__name__, "evalue": str(error)}).encode()
            with os.fdopen(w, "wb") as f:
                f.write(data)
            os._exit(0)
        os.close(w)
        chunks, deadline = [], time.monotonic() + timeout
        with os.fdopen(r, "rb") as f:
            while True:
                left = deadline - time.monotonic()
                if left <= 0 or not select.select([f], [], [], left)[0]:
                    os.kill(pid, signal.SIGKILL)
                    _wait_child(pid)
                    return {"status": "error", "ename": "Timeout", "evalue": f"inspection took over {timeout:.0f}s"}
                chunk = os.read(f.fileno(), 1 << 20)
                if not chunk:
                    break
                chunks.append(chunk)
        _wait_child(pid)
        try:
            return json.loads(b"".join(chunks))
        except ValueError:
            return {"status": "error", "ename": "InspectionFailed", "evalue": "the inspection process died"}

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
            rows += [describe(k, self.namespace[k]) for k in msg["content"].get("extra") or [] if k in self.namespace]
            self.send("talo_vars_reply", {"status": "ok", "variables": rows, "rss": rss_bytes()}, msg)
        elif kind == "complete_request":
            c = msg["content"]
            self.send("complete_reply", complete(self.namespace, c.get("code", ""),
                                                 int(c.get("cursor_pos", len(c.get("code", ""))))), msg)
        elif kind == "talo_reload_request":
            c = msg["content"]
            names = list(c.get("modules") or [])
            if c.get("changed"):
                names += [n for n in self.reloader.changed(c.get("root") or os.getcwd()) if n not in names]
            if c.get("dry_run"):
                self.send("talo_reload_reply", {"status": "ok", "selected": names, "dry_run": True}, msg)
            else:
                self.send("talo_reload_reply", {"status": "ok", "selected": names,
                                                **self.reloader.reload(self.namespace, names,
                                                                       in_place=bool(c.get("in_place")))}, msg)
        elif kind == "talo_fingerprint_request":
            self.send("talo_fingerprint_reply", {"status": "ok", "fingerprints": fingerprints(
                self.namespace, msg["content"].get("names", []))}, msg)
        elif kind == "talo_show_request":
            self.send("talo_show_reply", self.in_fork(lambda: self.show(msg["content"])), msg)
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
    signal.signal(signal.SIGUSR1, signal.SIG_IGN)  # talo diagnose between executions: harmless, never fatal
    sock = socket.socket(fileno=args.fd)
    sys.argv = [""]  # user code sees the same sys.argv as in an interactive interpreter
    Worker(sock, args.session).serve()


if __name__ == "__main__":
    # When run by path, sys.path[0] is talo's own directory: replace it with "" (the current directory), as in an
    # interactive interpreter, so talo's files never shadow the user's modules.
    sys.path[0] = ""
    main()
