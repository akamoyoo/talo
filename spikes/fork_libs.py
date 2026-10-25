"""Measurement: are common objects intact after a fork() snapshot, and can a child still compute after the parent used
multi-threaded libraries?

- pandas: a DataFrame (numeric column, string column, datetime index) is snapshotted, then corrupted in the live
  process (drop a column, change values, shift the index); the snapshot checks that it still has the original;
- BLAS: a multi-threaded matmul in the parent before fork(), again in the child;
- thread pool: a concurrent.futures pool used before fork(); the child submits to the inherited pool object, whose
  worker threads do not exist in the child.

Prints ok / hang / mismatch / error for each case.
"""

from __future__ import annotations

import os
import select
import signal
import time

import numpy as np


def in_child(fn, timeout_s=20.0) -> str:
    """Run fn() in a forked child (True means pass); report hang after the timeout."""
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        try:
            os.write(w, b"ok" if fn() else b"mismatch")
        except Exception as error:  # noqa: BLE001
            os.write(w, f"error {type(error).__name__}: {error}"[:200].encode())
        os._exit(0)
    os.close(w)
    out, deadline = b"", time.time() + timeout_s
    while time.time() < deadline and not out:
        if select.select([r], [], [], 0.5)[0]:
            out = os.read(r, 256)
    if not out:
        os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    return out.decode() or "hang"


def pandas_case() -> str:
    import pandas as pd
    n = 2_000_000
    idx = pd.date_range("2024-01-01", periods=n, freq="s")
    df = pd.DataFrame({"x": np.arange(n, dtype=float), "s": pd.Series(np.arange(n) % 7).astype(str)}, index=idx)
    ref = df.copy(deep=True)
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:  # the snapshot: wait until the live process has corrupted its copy, then check ours
        os.close(w)
        os.read(r, 1)
        os._exit(0 if df.equals(ref) and list(df.columns) == ["x", "s"] and df.index.equals(ref.index) else 1)
    os.close(r)
    df.drop(columns=["s"], inplace=True)
    df["x"] *= -1
    df.index = df.index + pd.Timedelta("1D")
    os.write(w, b"x")
    _, status = os.waitpid(pid, 0)
    return "ok" if os.waitstatus_to_exitcode(status) == 0 else "mismatch"


def blas_case() -> str:
    a = np.random.default_rng(0).standard_normal((1500, 1500))
    ref = (a @ a).sum()
    return in_child(lambda: (a @ a).sum() == ref)


def thread_pool_case() -> str:
    from concurrent.futures import ThreadPoolExecutor
    pool = ThreadPoolExecutor(max_workers=4)
    assert pool.submit(sum, [1, 2, 3]).result() == 6  # the pool's threads now exist in the parent
    return in_child(lambda: pool.submit(sum, [1, 2, 3]).result(timeout=5) == 6)


if __name__ == "__main__":
    for name, fn in (("pandas", pandas_case), ("blas", blas_case), ("thread pool", thread_pool_case)):
        print(name, fn(), flush=True)
