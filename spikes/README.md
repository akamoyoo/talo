# Measurements (M0)

Before building snapshots on `fork()`, measure what they cost and what breaks. Machine: 192 cores, 755 GB RAM;
Python 3.12, numpy 2.5, pandas 3.0 / 3.1.

## Cost of `fork()` (`fork_cost.py`, numpy arrays with every page written)

| Resident data | `fork()` | touch 1% of the pages after a snapshot | rewrite everything after a snapshot | rollback |
|---|---|---|---|---|
| 1 GB | 2.7 ms | +11 MB | +1 GB | 59 ms |
| 8 GB | 6.9 ms | +83 MB | +8 GB | 0.3 s |
| 16 GB | 12 ms | +164 MB | +16 GB | 0.6 s |

- Taking a snapshot is nearly free (milliseconds; what grows with the data is copying page tables). Extra memory
  afterwards equals the pages modified after the snapshot, independent of the total.
- Rewriting everything in place (for example `df["x"] *= 2` over a whole column) makes the snapshot hold a full copy
  of the old data: that is copy-on-write by nature, hence accounting and budgets.
- Creating a new object (`y = x * 2`) does not make the snapshot grow (old pages are untouched); only the live process
  grows by the new object.
- After a rollback the data is exactly what it was at the snapshot (checked value by value).

## Libraries after `fork()` (`fork_libs.py`)

| Object / library | Result |
|---|---|
| pandas DataFrame (numeric column, string column, datetime index): after the snapshot the live process drops a column, changes values and the index | the snapshot is unchanged |
| numpy / OpenBLAS: multi-threaded matmul before `fork()`, again in the child | works, same result |
| `concurrent.futures.ThreadPoolExecutor` used before `fork()`; the child submits to the inherited pool | **hangs**: the pool's threads do not exist in the child (`TimeoutError`) |
| a C++ extension with its own scheduler thread pool (tested with a real-world analytics library) | **refused** by the library: it records the PID that created the pool and raises in a forked child |

- Python 3.12 emits a DeprecationWarning when a multi-threaded process calls `fork()`: locks held by other threads at
  fork time may never be released. Snapshots must be taken at idle points, after checking which threads exist
  (`/proc/self/task/*/comm`), with warnings or refusals for threads not known to be safe.
- Libraries that keep their own thread pools either break or refuse to run after a rollback unless they rebuild the
  pool in the child (for example in a `pthread_atfork` handler). Talo must detect and report this rather than hang.

## An incident during the measurements

The 32 GB run peaked at the `systemd-run -p MemoryMax=120G` limit; the resulting memory pressure made systemd-oomd
kill another service of the same user. Lesson: memory limits for large tests must be well above the expected peak
(about 3x the data here), and tests must avoid allocating full temporary copies.
