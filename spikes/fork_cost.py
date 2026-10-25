"""Measurement: what do fork() snapshots and rollbacks of in-memory state cost?

For resident data of a given size (a numpy array with every page written):
- the time fork() itself takes (copying page tables);
- after a snapshot (a suspended child), how much memory each side holds privately (Private_Dirty in
  /proc/<pid>/smaps_rollup) when the live process changes nothing / touches 1% of the pages in place / rewrites
  everything in place / creates a new array of the same size;
- rollback: fork a process holding the snapshot-time state, corrupt the live data, check the copy is intact, time it;
- thread safety: after a multi-threaded BLAS matmul in the parent, fork and matmul again in the child (hang = timeout).

    python spikes/fork_cost.py --gb 1 4 16
Prints one JSON line per size.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

import numpy as np

PAGE = os.sysconf("SC_PAGE_SIZE")


def private_dirty_mb(pid: int) -> float:
    with open(f"/proc/{pid}/smaps_rollup") as f:
        for line in f:
            if line.startswith("Private_Dirty:"):
                return int(line.split()[1]) / 1024
    return float("nan")


def rss_mb(pid: int) -> float:
    with open(f"/proc/{pid}/smaps_rollup") as f:
        for line in f:
            if line.startswith("Rss:"):
                return int(line.split()[1]) / 1024
    return float("nan")


def hold_snapshot():
    """Fork a suspended snapshot process that waits in pause() until SIGTERM. Returns (pid, fork seconds)."""
    t = time.perf_counter()
    pid = os.fork()
    if pid == 0:
        signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
        while True:
            signal.pause()
    return pid, time.perf_counter() - t


def blas_after_fork(timeout_s: float = 20.0) -> str:
    """After multi-threaded BLAS in the parent, fork and matmul in the child: ok / hang / error."""
    a = np.random.default_rng(0).standard_normal((1500, 1500))
    a @ a  # start BLAS's thread pool
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        try:
            (a @ a).sum()
            os.write(w, b"ok")
        except Exception:  # noqa: BLE001
            os.write(w, b"error")
        os._exit(0)
    os.close(w)
    deadline = time.time() + timeout_s
    import select
    out = b""
    while time.time() < deadline:
        ready, _, _ = select.select([r], [], [], 0.5)
        if ready:
            out = os.read(r, 16)
            break
    if not out:
        os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    return out.decode() or "hang"


def run(gb: float) -> dict:
    n = int(gb * (1 << 30) / 8)
    t = time.perf_counter()
    data = np.arange(n, dtype=np.float64)  # every page is really written
    alloc_s = time.perf_counter() - t
    out = {"gb": gb, "alloc_s": round(alloc_s, 2), "parent_private_mb_before": round(private_dirty_mb(os.getpid()))}
    for case in ("none", "touch_1pct", "touch_all", "new_array"):
        snap, fork_s = hold_snapshot()
        t = time.perf_counter()
        extra = None
        if case == "touch_1pct":
            data[:: (PAGE // 8) * 100] += 1.0  # one value per 100 pages: about 1% of the pages become dirty
        elif case == "touch_all":
            data += 1.0
        elif case == "new_array":
            extra = data * 2.0
        work_s = time.perf_counter() - t
        out[case] = {"fork_ms": round(fork_s * 1000, 1), "work_s": round(work_s, 2),
                     "snapshot_private_mb": round(private_dirty_mb(snap)),
                     "parent_private_mb": round(private_dirty_mb(os.getpid()))}
        os.kill(snap, signal.SIGTERM)
        os.waitpid(snap, 0)
        del extra
        # restore in place (no full temporary copy) so the next case starts from the same state
        if case == "touch_1pct":
            data[:: (PAGE // 8) * 100] -= 1.0
        elif case == "touch_all":
            data -= 1.0
    # Rollback: fork a copy (the state at snapshot time), corrupt the live data, then let the copy continue and check
    # its data; in talo the copy becomes the session's new worker, here it just reports.
    r, w = os.pipe()
    t = time.perf_counter()
    pid = os.fork()
    if pid == 0:
        os.close(w)
        os.read(r, 1)  # wait until the live process has corrupted its data
        ok = bool(data[0] == 0.0 and data[-1] == n - 1)
        os._exit(0 if ok else 1)
    os.close(r)
    data[0] = -1.0
    data[-1] = -1.0
    os.write(w, b"x")
    _, status = os.waitpid(pid, 0)
    out["rollback_child_sees_snapshot"] = os.waitstatus_to_exitcode(status) == 0
    out["rollback_ms"] = round((time.perf_counter() - t) * 1000, 1)
    out["blas_after_fork"] = blas_after_fork()
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--gb", nargs="+", type=float, default=[1, 4])
    args = ap.parse_args(argv)
    for gb in args.gb:
        print(json.dumps(run(gb), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    sys.exit(main())
