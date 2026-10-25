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

# Measurements: CUDA (GPU residency)

Can a session keep a model on the GPU, and what breaks when it forks? Machine: 8x H100 80GB (shared), driver
580.173, 256 cores; Python 3.10, torch 2.4.0+cu121, cupy 14.2; one GPU (`CUDA_VISIBLE_DEVICES=1`).

## When does `fork()` stop working, and what can the worker see? (`gpu_fork.py`)

Each row: a fresh process does the step, then forks; the child tries to create a CUDA context (driver API, through
ctypes) and a torch CUDA tensor. "Signals" are what a stdlib-only worker can read without importing anything.

| Step in the parent | Child can use CUDA | `/dev/nvidia*` mapped (`/proc/self/maps`) | `/dev/nvidia*` fds open | `libcuda.so` mapped | `torch.cuda.is_initialized()` |
|---|---|---|---|---|---|
| nothing | yes | no | no | no | – |
| `import torch` | yes | no | no | **yes** | False |
| `torch.cuda.is_available()` | **no** | yes | yes | yes | **False** |
| `torch.cuda.device_count()`, `get_device_name()` | no | yes | yes | yes | False / True |
| first CUDA tensor, model forward, after `del` + `empty_cache()` | no | yes | yes | yes | True |
| `is_available()` with `PYTORCH_NVML_BASED_CUDA_CHECK=1` | yes | no | **yes** | yes | False |
| `nvmlInit()` only (ctypes) | yes | no | **yes** | yes | – |
| `cuInit(0)` only (ctypes, no framework) | no | yes | yes | yes | – |
| cupy array | no | yes | yes | yes | – |

- **`/dev/nvidia*` in `/proc/self/maps` matches "fork is no longer usable for CUDA" in every case**, whatever the
  framework: it appears at `cuInit`, not at import, and not for NVML-only queries. Reading the maps of a worker with a
  model on the GPU (552 lines) takes 0.8 ms.
- The other signals are wrong: `libcuda.so` is mapped by `import torch` (a CPU-only torch user would be refused);
  open `/dev/nvidia*` files also come from NVML (false positive); torch's flag is False after `is_available()`
  (false negative); thread names (`cuda00001400006` after `cuInit`, `cuda00001800007` after NVML) are internal.
- Nothing hangs: the child gets errors (`Cannot re-initialize CUDA in forked subprocess`, `cuInit` returns 3,
  `CUDA error: initialization error` on the parent's tensors). CPU work, including torch CPU ops, still works in the
  child, so a broken fork looks healthy until it touches the GPU.
- The state is sticky: `del` + `empty_cache()` does not unmap anything.
- A CUDA context costs about 0.5-0.6 GB of VRAM per process (612 MiB for torch, 522 MiB for cupy), reported by
  `nvidia-smi --query-compute-apps` only once a context exists; one query takes about 190 ms.

## Today's Talo with a CUDA session (`gpu_talo.sh`, `gpu_talo_fail.sh`)

A session imports torch, takes snapshot `cpu0`, then loads a 4 GiB model (64 x Linear(4096, 4096)) onto the GPU.

| Operation | Result |
|---|---|
| load the model | 8-10 s, once |
| inference, repeated `run` | 0.09 s first, then 0.02 s: residency works |
| `vars`, `show NAME` | work |
| `snap` | **reports success**; the snapshot cannot use CUDA |
| `checkout` of that snapshot | **reports success**; CPU variables are there, the model fails with `Cannot re-initialize CUDA` |
| `run --atomic` | the job ran and committed; a restore would land in a fork that cannot use CUDA |
| `run --trial` | fails with torch's error |
| `map` without `--from` | every case fails with torch's error |
| `save` | reports success, skipping `model` with a CUDA error message |
| `verify` | replays from `cpu0` (14 s, it reloads the model); `model` not compared |
| `expose` | **reports success**; every `call` fails with torch's error |
| `memory` | does not show the model's VRAM |
| `map --from cpu0` (snapshot taken before CUDA) | works: each case creates its own context (0.9 s) |
| `checkout cpu0 --discard` | works; the old worker's VRAM is released, the new worker can use CUDA |
| interrupt a long GPU loop | `KeyboardInterrupt` within seconds; the model is still usable |
| CUDA out of memory | an ordinary job error; the model is still usable |
| worker killed with SIGKILL | VRAM released at once; `ls` says `dead`; `checkout cpu0` recovers |
| supervisor killed with SIGKILL | the worker exits with it; VRAM released |
| `snap` thread report | 63 "other threads" right after `import torch` (its CPU pool), 2 after CUDA: thread counts do not tell CUDA apart |

Conclusion: residency already works; the problem is that every fork-based command still succeeds, or fails with a
library's message, instead of being refused. Refusing them when `/dev/nvidia*` is mapped in the worker covers every
case measured; snapshots taken before that point stay fully usable (`checkout`, `map --from`).
