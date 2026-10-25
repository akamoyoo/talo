# Talo design

Talo is a resident, in-memory Python session driven from the terminal: data is loaded once and stays in memory,
and the state can be snapshotted, rolled back and branched. Its first users are coding agents (Claude Code, Codex and
similar tools that run shell commands); its second users are people at a terminal. The goal is to be **an upgrade
of Jupyter**: everything a Jupyter kernel does, plus what Jupyter does not have — snapshots, rollback and branches of
the in-memory state, a command-line protocol designed for agents, reproducible execution history, and resource
accounting.

## Three guarantees, stated separately

1. **Resident.** When the CLI exits or the agent disconnects, the session's variables and running computations stay.
2. **Hot rollback** (M2). While the machine is up, the session can switch back to any snapshot of its in-memory
   state, or branch from it.
3. **Durable restore** (M9). After a process or machine restart, selected variables can be restored from disk.

Rollback is not a sandbox: files already written, network requests already sent and changes to external databases
are not undone.

## Architecture

```
talo CLI (humans, agents)       JupyterLab / VS Code (M7)     MCP (M7)
        │ JSON over a Unix socket       │ ZMQ bridge
        └───────────────┬───────────────┘
                 supervisor (one resident process per machine)
                 ├─ session table, job queues, execution history, output files, snapshot tree
                 ├─ session a: worker (runs code, holds the variables) + snapshot processes
                 └─ session b: ...
```

- **The supervisor holds no user data.** It routes, queues and records. When a worker dies, the supervisor is still
  there to say what happened (signal, likely OOM kill).
- **Workers are single-threaded** and run in any interpreter the user chooses (for example a project's virtualenv).
  The worker is a standard-library-only file executed by path, so the target environment does not need Talo installed,
  and nothing is added to the user's `sys.path`.
- **Output is captured at the file-descriptor level.** While a job runs, fds 1 and 2 point to the job's output file;
  output written by C extensions is captured, and it can be read while the job is still running.
- **Snapshots** (M2) are suspended copies of the worker made with `fork()` between two executions (copy-on-write).
  Rolling back means asking a snapshot to fork a new worker; the snapshot itself stays unchanged, so it can be checked
  out many times and branched. Snapshots form a tree.

### Snapshots, checkout and branches (M2)

**User model.** A session's state is a tree, like git commits:

```console
$ talo run -f load.py              # 30 GB in memory
$ talo snap loaded                 # snapshot "loaded" (milliseconds)
$ talo run 'df = df[df.price > 0]' # experiment ...
$ talo checkout loaded             # ... and go back: the worker is replaced by a fork of "loaded"
$ talo new idea2 --from loaded     # or explore in parallel: a second session forked from the same snapshot
$ talo snaps                       # the tree, with each snapshot's private memory
$ talo drop loaded                 # free it
```

- `snap [NAME] [-m MESSAGE]` is queued like an execution, so `run …; snap …` happens in order. Names are unique per
  session; unnamed snapshots get `s1`, `s2`, …
- `checkout NAME` replaces the session's worker with a fresh fork of the snapshot. If jobs ran since the worker's base
  snapshot, checkout refuses and lists them, unless `--discard` (drop that state) or `--snap NAME` (save it first).
- `new SESSION --from [SESSION:]NAME` starts an independent session from a snapshot (branching).
- Each session records its **base** (the snapshot its worker descends from, or none) and the jobs run since; snapshots
  record their parent snapshot, so the tree and "what happened since" are always known.
- Snapshots survive the death of the worker: after an out-of-memory kill, `checkout` brings back the last snapshot.
  They do not survive the supervisor (they exit when it goes away) or a reboot (that is M9).

**Mechanics.** On `talo_snapshot_request` the worker, at an idle point, forks. The child is the snapshot: it closes the
worker's connection, listens on its own Unix socket (`sessions/NAME/snapshots/ID.sock`) and serves a few requests:
`vars` (read-only, for listing and diffing), `spawn`, `exit`. To check out, the supervisor connects to that socket and
sends `spawn`; the snapshot forks again, and the grandchild closes the listening socket and keeps the accepted
connection as its channel to the supervisor — it is the new worker, with the snapshot's exact memory state. No file
descriptors need to be passed. The snapshot reaps its children and records their exit status in
`snapshots/ID.children`, so the supervisor can report how a forked worker died.

**Accounting.** The supervisor reads each snapshot's private memory from `/proc/PID/smaps_rollup` (`Private_Dirty`:
pages that only this process holds). With `new --memory-budget SIZE`, `snap` is refused when the worker's resident
memory plus all snapshots' private memory would exceed the budget, with a list of snapshots that could be dropped.

**Tree.** `talo tree` draws every snapshot of every session as one tree, following each snapshot's parent across
sessions (a branch made with `new --from` hangs under the snapshot it came from). Each session's live worker is a leaf
under its base, marked with `*` and the number of jobs run since. A snapshot whose parent was dropped hangs under a
`(dropped REF)` placeholder.

```console
$ talo tree
(empty start: default)
└─ loaded       default:s1   1.2 MB private   jobs 1,2
   ├─ clean     default:s2   164 MB private   jobs 3
   │  └─ * default           idle, 1 job since
   └─ * idea2                idle, 0 jobs since
```

**Diff.** `talo diff A [B]` compares the variables of two states. A state is a snapshot (`[SESSION:]NAME`) or a
session's live worker (`@` for the current session, `SESSION:@` for another one); `B` defaults to `@`. The result lists
variables that were added, removed or changed, and how many are unchanged:

```console
$ talo diff loaded
+ mask          ndarray (1000000,) bool        976.6 KB
~ df            DataFrame (1000000, 3) -> (998012, 3)
~ prices        ndarray (1000000,) float64     content
- tmp           list len=3
(12 unchanged; 1 not compared by content: bars 31.4 GB > 256 MB, use --content)
```

Two variables are compared first by their summaries (type, shape or length, dtype, columns, value preview). When the
summaries are equal, both sides compute a content fingerprint: a BLAKE2b hash of the object's pickle (protocol 5,
with large buffers such as NumPy and pandas data hashed in place, without copying). Equal fingerprints mean equal
pickled bytes. Hashing reads every byte (about 0.6 GB/s per side, both sides in parallel), so by default only variables up to
256 MB are compared by content; `--content` compares all of them, `--max-bytes SIZE` changes the limit, and variables
that were not compared are listed. Objects that cannot be pickled are reported as not comparable. Diffing a live
worker needs it to be idle, as for `vars`.

Costs measured in [spikes/README.md](../spikes/README.md):

- taking a snapshot: milliseconds, growing with the page tables (12 ms with 16 GB resident);
- extra memory after a snapshot: exactly the pages modified afterwards (touching 1% of 16 GB cost 164 MB; rewriting
  everything costs a full copy);
- rollback: well under a second at 16 GB.

Each snapshot's private memory is reported from `/proc/PID/smaps_rollup` (`Private_Dirty`); a session budget refuses
new snapshots that would exceed it and says which snapshots to drop.

### Fork safety

`fork()` in a multi-threaded process leaves only the calling thread in the child; locks held by other threads may stay
locked forever (Python 3.12 warns about this). Snapshots are taken only at idle points (between jobs). Before forking,
the worker lists its threads (`/proc/self/task/*/comm`) and reports them with the snapshot; with `snap --strict` it
refuses to fork while other threads exist. Thread pools that tolerate `fork()` (OpenBLAS registers handlers for it) are
common, so the default is to warn, not refuse. Libraries with their own thread pools may hang or refuse to run in a forked child
unless they rebuild the pool after `fork()`; GPU contexts, network connections and writable mappings are not restored
by a rollback.

## Messages: built on the Jupyter messaging protocol

All messages — CLI to supervisor and supervisor to worker — use the structure of the Jupyter messaging protocol
(5.3): `header` (`msg_id`, `msg_type`, `session`, `date`, `version`), `parent_header`, `metadata`, `content`.
Execution-related message types and fields follow the specification; Talo's own features are extension messages
prefixed `talo_`. The transport is newline-delimited JSON over Unix sockets; M7 adds a ZMQ bridge and a standard
kernel connection file so that Jupyter front ends can attach. See [protocol.md](protocol.md).

## A command line designed for agents

- Every command returns: `run` waits 30 seconds by default, then returns a job id; `wait`, `tail` and `interrupt`
  take it from there.
- `--json` gives stable fields, structured errors and truncation markers; the default output is for humans.
- Output is truncated to its last lines and results to a bounded size, always with a note saying how much was cut;
  the full output stays in the job's file and can be read by offset.
- One session runs one execution at a time; further execution requests queue. Inspection requests (`vars`, `show`)
  are answered only while the session is idle, never by touching the heap of a running computation.
- Exit codes: `0` ok, `1` the code failed, `2` Talo failed (with `--json`: `{"error": {"type", "message"}}`), `3`
  still running or queued — also for queued operations (`snap`, `checkout`) that did not finish within `-t`.
- Only the `default` session is created implicitly by `run`; any other name must exist (`talo new NAME`), so a typo
  never silently starts an empty session. A branch (`new --from`) inherits its snapshot's interpreter and directory.
- Results are kept in `_` and in `Out` for the last three executions only, so a large result that is no longer named
  does not stay in memory.

## Execution history

Every finished job — executions, snapshots and checkouts — appends one line to `sessions/NAME/history.jsonl`: code,
status, execution count, start and end times, resident memory before and after, result preview, error, and the files
of rich outputs. Snapshot records carry the snapshot's ref, parent and the jobs between them, so the path that led to
any state can be reconstructed from the history alone, even after snapshots are dropped.

## For humans (M3)

### `talo attach`: an interactive prompt on a resident session

```console
$ talo attach                       # the "default" session; -s NAME for another
talo default (Python 3.12, 31.4 GB) — Ctrl-C interrupts, Ctrl-D detaches, %help for commands
[4]: df.groupby("code").ret.mean().head()
...
[5]: %snap grouped
snapshot grouped (default:s3)
[5]: ^D
detached; the session keeps running
```

- Each input is submitted as an ordinary job: it is queued with jobs from other clients, appears in `history`, and
  its output is streamed as it is written. A human and an agent can work on the same session at the same time.
- Before each prompt, attach reports jobs that other clients ran in the meantime (`[jobs 7, 8 ran from another
  client; %history shows them]`).
- Multi-line input is detected as in the standard interactive interpreter (`codeop`): a block continues with `...:`
  until it is complete.
- Tab completes names, attributes and keys from the live namespace through the Jupyter `complete_request`. Input
  history is kept per session in `sessions/NAME/input_history` (readline).
- Ctrl-C while a job runs interrupts it; at the prompt it clears the line. Ctrl-D (or `%detach`) leaves; the session,
  its variables and its jobs stay.
- Lines starting with `%` run Talo commands on the same session: `%vars`, `%show df`, `%snap NAME`, `%snaps`,
  `%tree`, `%diff A [B]`, `%checkout NAME`, `%history`, `%export ...`; `%help` lists them. Python code that begins with
  `%` is not valid Python anyway, so there is no ambiguity.

### Rich outputs saved as files

Images and other binary outputs are written next to the job's output file (`jobs/ID-N.png`, `jobs/ID-N.svg`), and
the job lists them: `outputs: [{"mime": "image/png", "path": ...}]`. The CLI prints `[image/png: PATH]`; an agent
that can read images opens the file, a human opens it with any viewer.

- A result object with `_repr_png_`, `_repr_jpeg_` or `_repr_svg_` produces the image alongside its `text/plain`.
- `display(obj, ...)` is available in the namespace (as in Jupyter): each object becomes a `display_data` message;
  text goes to the job's output, images to files.
- matplotlib: the worker sets `MPLBACKEND=Agg` unless the user chose a backend. After each execution, figures that are
  still open are saved as PNG and closed, as Jupyter's inline backend does, and `plt.show()` does the same
  immediately.

### `talo export`: a replayable script or notebook

```console
$ talo export -o analysis.py                 # the code that produced the current state
$ talo export --to ipynb -o analysis.ipynb   # the same as a notebook, with outputs
$ talo export --snap loaded                  # the path to a snapshot
$ talo export --all                          # every job, in order, including rolled-back branches
```

- By default the export follows the **lineage** of the session's current state: the jobs between each snapshot on the
  chain of bases, across sessions for a branch, then the jobs since the base. Code that was rolled back is left out,
  so running the script from an empty interpreter reproduces the state (side effects outside the process aside).
- A failed job may have changed the state before failing; the export marks it as a **replay hazard** and says so
  in its header, because the script leaves out code whose effects the state still has (`run --atomic` avoids this).
- Failed and interrupted jobs are kept as comments (`# [job 4 failed: NameError: ...]`) in `.py`, and as cells with
  their error in `.ipynb`; snapshots appear as comments (`# --- snapshot loaded (default:s1)`) or Markdown cells.
- `.ipynb` is nbformat 4.5, written with the standard library. Outputs come from the job records and output files:
  streams (truncated to their last 100 KB, with a note), results, errors and saved images (embedded).

## Agents at scale (M4)

Agents lose their context (compaction, a new subagent), retry shell calls that timed out, run in parallel on one
session, and share machines with other people. M4 makes each of these safe and cheap.

### Resuming: `talo brief` and `talo note`

```console
$ talo brief                       # what a fresh agent needs to continue, in under ~8 KB
session default  idle  rev 42  base loaded (default:s1)  4 jobs since  memory 31.2 GB (private 1.1 GB)
notes
  [rev 12 · agent-a] goal: find intraday factors that survive a 5-minute delay
  [rev 40 · agent-a] gap momentum dies after the first 30 minutes; next: test sector-relative version
snapshots  loaded (default:s1) "panels 2021-2024"  ·  clean (default:s2)
variables (largest 15 of 37)  panel DataFrame (48M, 31) 28.4 GB · ...
recent jobs  #41 ok 3.2s  ic = evaluate(gap_mom)  ·  #42 error 0.1s  KeyError: 'sector'
running      #43 running 12m, progress 61% (412/672 days), last output 3s ago
```

- `talo note TEXT [--job ID]` appends a note to the session (`notes.jsonl`) with an id, time, owner, the revision it
  was recorded at, the base, and the job it is about. `talo notes` lists them; `brief` shows the most recent. Notes
  are kept with the session's history, so they survive rollbacks, and `export` writes them into the header.
- `brief` is built from what the supervisor already knows, so it answers while a job runs. Variables come from the
  worker when it is idle; otherwise the last summary is shown with the revision and time it was observed and
  `stale: true`. The whole reply is bounded (notes and previews are cut, lists are limited, and every list says how
  many items were left out and which command shows them).

### Several agents and retries: owners, keys and revisions

- **Owner.** Every request carries an owner: `--owner NAME`, else `$TALO_AGENT`, else `user`. Jobs, snapshots and notes
  record it, and listings show it. Owners are attribution, not locks: `checkout` that would discard other owners'
  jobs names them in its refusal, and `--discard` still overrides it.
- **Revision.** Each session has a revision number that increases when an operation that may change the state is
  dispatched to the worker: an execution (including from `attach`), a checkout, a reload. It never goes back: a failed
  or rolled-back job still consumed a revision. Snapshots do not change it. `run`, `checkout` and `reload` accept
  `--expect-rev N` and are refused when they reach the front of the queue if the revision is no longer `N`; the
  error says the current revision and the jobs in between. Inspection (`vars`, `show`, `brief`) reports the revision
  it observed. `show` only evaluates names, attributes and constant subscripts (`df`, `df.x`, `d["k"]`); anything else
  must go through `run`, so that it is recorded and counted. `show` runs in a disposable fork of the worker, so a
  property or `__getitem__` that changes state changes only the copy. The guarantee covers operations made through Talo:
  background threads or writes to shared files can still change the state without a revision.
- **Idempotency key.** `run --key K` returns the job already submitted under `K` in this session — whatever its
  outcome, even if it failed or was refused — with `deduplicated: true`. The key identifies the whole request (code
  and options that affect execution, such as `--atomic` and `--expect-rev`, but not `-t`): reusing it for a different
  request is an error, so a new attempt needs a new key. Keys belong to one incarnation of a session (a session recreated under the same name has a new id, and its
  revisions continue from the old one's). Keys are looked up before anything else (a retry after the
  worker died still returns the original job) and are kept for the lifetime of the supervisor; the reply carries the
  supervisor's `epoch`, so a retry can detect that the supervisor restarted in between. `snap` and `checkout` accept
  `--key` as well.

### Failing without leaving half a mutation: `run --atomic`

`run --atomic CODE` runs the job inside a checkpoint:

1. *prepare*: the worker forks an internal checkpoint. It is not a named snapshot: it does not change the session's
   base or the jobs since it, it does not appear in `snaps` or `tree`, and its memory counts against the budget. If it
   cannot be taken (budget, `--strict` threads), the job fails before running any code.
2. *execute* the code.
3. *commit* on success: the checkpoint is dropped and the job joins the lineage like any other.
   *restore* on error, interruption or a memory-limit kill: the worker is replaced by a fork of the checkpoint (the old
   worker is kept until the new one answers), so variables, `_`, `Out` and the execution counter are as before the job.
   The job stays in the history with its error but is left out of the lineage, so `export` does not replay it.

The job reports `atomic: {"outcome": "committed" | "restored" | "restore_failed", "error": ...}`; the session's queue
stays blocked until commit or restore has finished, so `wait` never returns in between. A shell call that times out
does not restore anything: the job keeps running. External side effects are not undone, and the limits of `fork()`
apply (see "Fork safety").

### Progress and activity

User code can call `talo.progress(done, total=None, note="")` (a `talo` object is available in the namespace, as
`display` is; it is hidden from `vars`). Updates are rate-limited to a few per second and belong to the running job.
Every running job reports `progress` and three ages measured on a monotonic clock: `last_output_age_s`,
`last_progress_age_s` and `last_activity_age_s` (the smaller of the two). They are evidence of activity, not a stall
detector: a native computation can be silent for an hour, so Talo never interrupts a job for being quiet.

### Output that respects the context window

The output shown by `run`, `wait` and `tail` (never the file, and never the byte offsets) is condensed: for lines
rewritten with carriage returns (progress bars) only the final state is kept, and runs of identical lines are folded
into one with a count (`[... repeated 120 times]`). Condensing applies to each reply separately. A result's text is
cut to `--max-result` characters (default 10 000); the full `text/plain` of results longer than that is saved next to the job's
output (`ID-N.txt`) and the reply gives its path and size (`talo_text_path`, `talo_text_chars`). Each reply is bounded as a whole. `--raw` shows the output
without condensing (the size limits still apply).

### Resources on shared machines

- `talo resources` reports, per session, the worker's and each snapshot's memory as separate named metrics: `rss`,
  `pss` (proportional: shared pages divided among the processes that map them), `private_dirty` and `private_clean`,
  with the time they were sampled; plus the machine's available memory. Unknown values are reported as unknown,
  never as zero. PSS sums are the honest total for a family of forks, but they are an apportionment, not the exact
  memory each process would free.
- `new --memory-limit SIZE` sets a **soft** limit on the PSS of the session's worker plus its snapshots. The supervisor
  samples it twice a second. When a job exceeds it, Talo interrupts the job, then kills the worker if it does not stop
  within a few seconds; the job ends with `termination: "memory_limit"` (distinct from a kernel out-of-memory kill), and
  an `--atomic` job is restored to its checkpoint. If the session is over the limit because of its snapshots alone,
  Talo does not kill anything: `snap` is refused and `brief` / `resources` say which snapshots to drop. Sampling can
  miss a fast allocation; a cgroup limit (for example `systemd-run --user -p MemoryMax=`) is the hard guarantee.
- `--memory-budget` (refusing new snapshots) stays as is and is measured the same way.

### Editing code while data stays loaded: `talo reload`

- `talo reload MODULE ...` reloads the named modules (`importlib.reload`), in the order given, as a queued operation
  (it increments the revision and accepts `--expect-rev` and `--key`). `talo reload --changed [--root DIR]` selects the
  imported modules whose source file is under `DIR` (default: the session's working directory, excluding virtual
  environments and `site-packages`) and changed since it was imported, judged by a content hash recorded at import
  time; `--dry-run` only lists them.
- The report gives, per module, `status` (`reloaded`, `unchanged`, `failed` with the error), old and new hashes, and
  `stale_bindings`: names in the session's namespace that are bound directly to a function or class from the module's
  previous version, or to an instance of such a class (`from factors import f` keeps the old `f`). Only the namespace
  itself is scanned (`scan_scope: "direct_namespace"`); references inside containers, closures and other objects are
  not followed. Talo does not rebind anything automatically.
- Extension modules cannot be reloaded and are reported as such.

## Across sessions (M5)

The expensive part of agent research is usually preparation: loading data, warming a model, reproducing a bug.
M5 lets many agents and tasks reuse one prepared state at once, keeps the evidence they produce tied to the state it
came from, and lets them wait for each other without polling.

### Ephemeral workers and pins

The building block: a worker forked from a snapshot that is *not* a session's worker. It runs one piece of code,
returns a bounded result, and exits; the snapshot is unchanged and the session it came from keeps working. Ephemeral
workers start in milliseconds and share all unmodified memory with the snapshot. Each one belongs to an operation
(a map case, a service call), is listed with it, and is counted in the session's memory.

Anything that needs a snapshot to stay alive **pins** it: a running map, a service version, a handoff. `drop` refuses
a pinned snapshot and names its holders; `kill` of a session stops its maps and calls first. Forks share nothing
with each other in memory, but they do share files, connections and writable mappings inherited from the snapshot.

### Fan-out: `talo map`

```console
$ talo map --from loaded -f evaluate.py --cases ideas.json --parallel 8
map m3: 24 cases from loaded (default:s1), finished
 #  status  time   result
 0  ok      41.2s  {"ic": 0.012, "rank_ic": 0.015}
 1  error   0.3s   KeyError: 'sector'
 ...
(24/24 cases: 22 ok, 2 error; results in sessions/default/maps/m3/results.jsonl)
```

- `--from SNAPSHOT` is resolved once, when the map is submitted, and pinned until the map ends. Without it, the map
  takes an internal snapshot of the current state through the queue (after running jobs; `--expect-rev` applies).
  The code and the cases are read once, at submission; `--key` makes the submission retry-safe.
- Each case runs the code with `case` bound to its element of the `--cases` JSON list (or `--range N` for
  `case = 0 .. N-1`), and `talo.case` gives `{map, index, seed}`. Every fork inherits the snapshot's random number
  generators in the same state, so cases that draw random numbers should seed from `talo.case["seed"]`.
- A case's **result** is the value of its last expression as JSON (NumPy and pandas scalars and small arrays are
  converted). A result that is not JSON-serializable, or larger than `--max-result` bytes, is reported as
  `result: null` with `result_error` and a text preview — never as cut JSON. Each row has `index`, `case`, `status`,
  `result`, `result_error`, `text`, `error`, `termination`, `elapsed_s`, `sampled_peak_pss` and its output path.
- At most `--parallel` cases run at once. While the session's memory (worker, snapshots and running cases, by PSS) is
  above `--memory-limit` (default: the session's limit), no further case starts, but one case always may, so a map
  never waits forever; the map reports `blocked` while it waits. A case over `--case-memory-limit` is interrupted,
  then killed. The limit on running cases together is admission only; a cgroup limit remains the hard guarantee.
- The map's status is `finished` (every case ran, whatever its outcome), `interrupted` or `error`; the exit code is 0
  only if every case is `ok`. `map` returns after `-t` seconds like `run`; `wait` and `interrupt` take its id.
  Interrupting cancels pending cases and interrupts running ones.
- Cases never change the session. To keep the state of a case, re-run it from the same snapshot: `talo new NAME
  --from SNAPSHOT` then the case's code with the same `case`.

### Sessions as services: `talo expose` and `talo call`

```python
# in the host session, after loading data
def evaluate(expr: str, horizon: int = 30) -> dict: ...
talo.expose(evaluate)      # published when this job ends successfully
```
```console
$ talo call default.evaluate '{"expr": "ret_5 - ret_30", "horizon": 60}'
{"ic": 0.009, "rank_ic": 0.011}  [call c17, default.evaluate v1, 3.1s]
```

- `talo.expose(fn, name=None)` stages a service in the running job. When the job ends **successfully**, the
  supervisor takes one snapshot of the host's state at that point and publishes every service staged by the job as a
  new **version** pinned to it; a failed, interrupted or rolled-back job publishes nothing, and a publication that
  fails is reported on the job separately from the code's outcome. `talo expose NAME` from the shell does the same
  for a function that already exists in the session.
- A call forks an ephemeral worker from its version's snapshot, so calls run in parallel, never see later changes of
  the host, and cannot change the host or each other. `--version V` selects a version; otherwise the latest is
  resolved once, at submission, and a retry with the same `--key` gets the same version and the same call.
- Versions stay until `talo unexpose NAME [--version V]`; `talo services` lists services, versions, their memory and
  calls in flight. A version with calls in flight cannot be removed.
- Arguments and results are JSON, bounded like map results. From Python, `talo.call(name, args=None, *, version=None,
  timeout=None, **kwargs)` returns the result or raises (`TimeoutError` names the call id to wait for). A call made from inside a
  service call or a map case is refused: nested synchronous calls are where deadlocks start.
- User code reaches the supervisor through the socket in `$TALO_SOCKET`, identifying the job or case it runs in.
- Deferred: services that run in the host's queue and change it (stateful services), and passing Python objects
  other than JSON.

### The experiment ledger: `talo.record`

```python
talo.record(ic=0.012, rank_ic=0.015, factor="gap_mom")
```
```console
$ talo ledger --has ic --sort ic --desc --last 5
r41  ic=0.0150 rank_ic=0.0180  map m3 case 7 · from loaded (default:s1) · succeeded · agent-a
r38  ic=0.0120 rank_ic=0.0110  job 211 · base default:s4 (dropped) · rolled back · agent-b
```

- `talo.record(**fields)` appends a record to the ledger shared by all sessions (`ledger.jsonl` in `TALO_HOME`). The
  record keeps the **reported fields** (JSON scalars and strings up to 200 characters) apart from the
  **provenance** the supervisor adds: id, time, owner, session id, the job, case or call that produced it, revision,
  source snapshot, and hashes of the code and of reloaded modules. `--key` makes a record idempotent.
- Each record shows the **outcome** of its producer, filled in when the producer ends: `running`, `succeeded`,
  `failed`, `interrupted`, `rolled_back`, or `unknown` when the supervisor restarted before the producer ended; records of failed work are kept but labelled. The ledger says whether
  the source snapshot still exists; the exact state of a map case or call is never kept, only the snapshot it came
  from.
- `talo ledger` filters (`--session`, `--owner`, `--where KEY=VALUE`, `--has KEY`, `--outcome`), sorts on one numeric
  field (`--sort KEY [--desc]`) and bounds its output. Records hold facts reported by code; interpretations belong in notes, which can cite them.

### Events: `talo events`

Things agents wait for are appended to an event journal: `job.finished`, `map.finished`, `case.finished`,
`call.finished`, `snapshot.created`, `snapshot.dropped`, `session.created`, `session.died`, `session.killed`,
`service.published`, `record.added`, `note.added`, `handoff.created`. An event is published only after the
operation's final result (including a rollback) is visible. Each event has a cursor `EPOCH:SEQ`.

`talo events [--after CURSOR] [--wait SECONDS] [--session S] [--type T] [--limit N]` returns events after the cursor,
waiting up to the given time for the first matching one. The reply's `next` advances past every event looked at,
matching or not, and `has_more` says whether more are ready. The journal keeps the last 100 000 events of the
supervisor's lifetime: a cursor older than `oldest` or from another epoch is refused with `CursorExpired`, so a
consumer always knows when it may have missed something.

### Sharing a session

- **One user, several clients.** An agent using the CLI, a human in `talo attach` and (M7) notebooks attached through
  the Jupyter bridge share the session's queue. Every client sees jobs that other clients ran as a one-line notice
  (owner, job, status, revision), not their full output; `--expect-rev` keeps each client from acting on a state it
  has not seen.
- **Other users on the same machine.** Running code in a session means running it as the session's owner, with all
  of that account's access, so sharing has levels:

  | Level | Allows |
  |---|---|
  | `read` | metadata Talo already holds: `ls`, `tree`, `snaps`, `jobs`, `job`, `output`, `history`, `brief` and `vars` (the last summary taken, never a live inspection), `notes`, `ledger`, `events`, `services`, `maps`, `handoffs`, `resume` (without `--branch`), `resources` |
  | `call` | `read`, plus calling the functions the owner exposed (`talo call`, JSON arguments only) |
  | `run` | also running code and everything that inspects or changes live state (`run`, `show`, `diff`, completion, `snap`, `checkout`, `map`, `reload`, `expose`, ...): equivalent to the owner's account, stated when granted |

  Live inspection is not offered at `read` because it calls methods of the owner's objects (properties,
  `__repr__`, `keys()`, pickling), which may have side effects. Creating, killing and sharing sessions are never
  offered on the shared socket. A grant belongs to one incarnation of a session: if the session is killed and
  recreated under the same name, the grant no longer applies.

  `talo share -s SESSION --user NAME --level LEVEL` opens a shared socket (`/tmp/talo-shared-UID/…`, in a directory
  others can traverse). Anyone may connect to it, but every connection is identified by its uid (`SO_PEERCRED`) and
  every request is checked against that uid's grants, so the file's permissions are not what protects it. Listings
  only show granted sessions; `run` excludes managing sessions, shares and the supervisor. `talo share --list` and
  `talo unshare --user NAME` manage grants; every grant and every request on a shared socket is recorded in the event journal.
  The other user connects with `talo --connect PATH ...`.
- **Other machines** are out of scope for M5: forward the socket over SSH, or (M7) use a Jupyter remote kernel.

### Handing over: `talo handoff`

`talo handoff -m "MESSAGE" [--to OWNER]` is a queued operation (it accepts `--expect-rev` and `--key`). It takes a
snapshot of the current state, or uses `--snap NAME`, and records a handoff: the snapshot, the revision it captured,
the message, the notes and ledger records since the session's previous handoff (by cursor, with ids rather than full
copies when long), the jobs that were running, and how to continue (`talo new NAME --from SNAPSHOT`, or `talo
attach`). `--to` is attribution only. `talo handoffs` lists handoffs; `talo resume ID` prints one, bounded, and says
whether its snapshot is still available. The snapshot stays pinned until `talo resume ID --branch NAME` has created
a session from it or the handoff is closed with `talo handoff --close ID`.

## Jupyter front ends and MCP (M7)

### The kernel bridge: `talo kernel`

```console
$ talo kernelspec install -s default      # "Talo: default" appears in JupyterLab, VS Code, jupyter console
$ jupyter lab                              # open any .ipynb, pick the kernel "Talo: default"
```

- `talo kernelspec install [-s SESSION] [--name NAME]` writes a Jupyter kernelspec whose command is
  `talo kernel --session SESSION -f {connection_file}`. Jupyter starts that process as "the kernel"; it is a bridge:
  it speaks the Jupyter messaging protocol (5.3) over ZMQ to the front end (shell, iopub, stdin and control channels,
  messages signed and verified as the connection file says, and the raw heartbeat echo) and Talo's control requests to the supervisor. The session
  itself, with its data, runs as always; the bridge holds nothing. It needs `pyzmq` (`pip install talo[jupyter]`);
  the worker stays standard-library only.
- Standard `.py` and `.ipynb` files: notebooks, VS Code's `# %%` cells in `.py` files and its Interactive Window all
  work through the same kernelspec. Several front ends can attach to one session, and agents keep using the CLI.
- Mapping: `execute_request` → a job (`run`, `silent` passed through); its output file is streamed as `stream`
  messages while it runs, with each `display()` and figure sent as `display_data` at the place in the text where it
  happened (the worker records the output offset of every display), then `execute_result` / `error`. Images are
  inlined (base64 for PNG and JPEG, text for SVG). `complete_request`, `inspect_request` (a `show` summary), `is_complete_request`, `history_request` (the
  session's history), `kernel_info_request` (`implementation: "talo"`) and `comm_info_request` (no comms) are
  answered. `interrupt_request` (control channel) interrupts the running job. `input()` is not supported.
- **Attaching from a notebook** connects to the live state, not to its code: the notebook document belongs to the
  front end, and the protocol has no way for a kernel to add cells to it. `talo notebook -s SESSION` writes the
  session's lineage as a notebook (code, outputs, images, the matching execution counts) whose metadata names the
  session's kernel, and installs that kernelspec: opening the file attaches to the session with the text matching
  the state. `%history` shows the code from inside any notebook.
- **Restarting the kernel** from the front end does not reset the session: the bridge reconnects to it and says so
  (`Talo kept session default (31 GB, rev 42); %checkout or talo kill resets it`). Data survives a front end restart,
  which is the point. `shutdown_request` likewise detaches; the session keeps running.
- **Jobs from other clients** are reported at the top of the next cell's output, one line each (owner, job, status,
  revision), as `talo attach` does.
- The kernel is labelled `Talo: SESSION (persistent)`: "Restart and Run All" re-runs the cells against the state
  that is still there. Execution counts follow the state: after a checkout they continue from the snapshot's count,
  which departs from Jupyter's monotonic counter but keeps the count meaningful for rollback. Comms, widgets,
  `clear_output` / display updates and debugging are not supported.

### Rollback inside notebooks: magics and autosnap

- A cell whose lines all start with `%` runs Talo commands on the attached session: `%snap NAME`, `%checkout NAME`,
  `%tree`, `%diff A [B]`, `%vars`, `%brief`, `%history`, `%undo`, `%autosnap on|off [--keep N]`, `%help`. (The worker
  is plain Python, not IPython; IPython magics such as `%timeit` are not provided.)
- `talo autosnap on [--keep N]` (or `%autosnap on`) makes the session take a snapshot after every successful
  execution, named `@N` after the execution count, keeping the last N (default 10) and dropping older ones unless
  something pins them. It is a session setting, so it works the same from the CLI, `attach` and notebooks. Costs:
  milliseconds per cell, plus the pages each later cell modifies.
- Turning autosnap on takes a baseline snapshot when the current state has none. `talo undo` (or `%undo`) checks
  out the preceding retained checkpoint: the base, if jobs ran since it (a failed execution leaves no autosnap, so
  undo returns to the state before it), otherwise the base's parent. `talo checkout @15` returns to the state after execution 15.
- After a checkout, the bridge prints which state the session is in (`state restored to @12; cells run after it are
  no longer in memory`); the notebook's text is unchanged, and the restored execution count shows which cells are
  no longer reflected in memory.

### MCP: `talo mcp`

`talo mcp [-s SESSION]` is a Model Context Protocol server over stdio (newline-delimited JSON-RPC, standard library
only). It implements the lifecycle of MCP revisions 2024-11-05 to 2025-11-25 (`initialize`, `notifications/initialized`,
`tools/list`, `tools/call`, `ping`); the 2026-07-28 revision, which drops `initialize`, is not yet supported. Requests
are handled concurrently, so `interrupt` works while a `wait` is pending; end of input detaches and leaves the
sessions running. Tool results put the data in `structuredContent` with a bounded JSON text copy; failed code is a
result with `isError`, a job still running is not an error. Its tools
are the agent-facing operations with the same arguments, results and errors as the CLI's `--json`: `run`, `wait`,
`interrupt`, `brief`, `vars`, `show`, `snap`, `checkout`, `undo`, `diff`, `map`, `call`, `record`, `ledger`,
`events`, `note`, `output`. Results are bounded as on the CLI. An MCP client that cannot run shell commands can thus use a
session as fully as a CLI agent.

## Trust and safety (M8)

Agents run long, expensive and sometimes wrong computations on shared machines, and hand results to people who must
decide whether to trust them. M8 makes results checkable before they are adopted, failures inspectable without
recomputing, and resource use bounded by the operating system rather than by polling.

### Is this state reproducible? `talo verify`

- `talo verify --hot [--from SNAPSHOT]` forks an ephemeral worker from a snapshot on the current lineage (default: the
  oldest one still kept), replays the lineage's jobs after it in order, and compares the result with the live state:
  variable by variable, by summary and content fingerprint (as `diff`, with its size limit).
- `talo verify --cold` starts a fresh interpreter (the session's), runs the exported script of the current lineage,
  and compares in the same way.
- The report lists variables that match, differ (with the differing fields), are missing on either side or could not
  be compared, the jobs replayed, and the **replay hazards**: failed jobs in the lineage whose partial effects the
  state keeps, reloads, and jobs that read files or the network cannot be detected and are not claimed. Verification
  never changes the session. A difference is reported, not explained: randomness without a fixed seed, time and
  external data are common causes.

### Trial, review, adopt

```console
$ talo run --trial clean -f clean.py      # runs in a fork of the current state; the session is not changed
$ talo review clean                        # its output, result, and diff against the current state
$ talo adopt clean                         # the session switches to the trial's state, atomically
$ talo reject clean                        # or drop it
```

- `run --trial NAME` takes an internal snapshot of the current state through the queue, runs the code in an ephemeral
  worker forked from it, and, if the code succeeds, keeps that worker's final state as a snapshot `trial:NAME`
  (pinned, counted in the session's memory). The session's own worker is free meanwhile.
- `review NAME` shows the trial job (status, output, result, records), and `diff` between the session's current state
  and the trial's.
- `adopt NAME` checks out the trial's snapshot; the trial's job joins the lineage as if it had run in the session.
  If the session's revision changed since the trial started, adopting would discard that work: it is refused unless
  `--discard`. `reject NAME` drops the trial. A failed trial keeps nothing (with `--retain-error`, its failed state).

### Hard limits and deadlines

- `new --limit memory=100G,cpu=8,pids=512` runs the worker in a systemd user scope (cgroup v2): `MemoryMax` (with
  `MemorySwapMax=0`, without which the excess is swapped out instead of enforced), `CPUQuota` and `TasksMax` apply to the worker, its snapshots and every fork of them together — the kernel enforces them, there
  is no sampling. Talo checks at `new` that a user systemd instance is available and that the needed controllers are
  delegated to it (often only `memory` and `pids` are; `cpu=` is then refused), and refuses otherwise (the soft
  `--memory-limit` remains for machines without it). An out-of-memory kill inside the scope is reported as such.
- `run --deadline SECONDS` bounds a job's run time independently of how long the client waits (`-t`): at the deadline
  Talo interrupts the job, then kills it after a grace period; the job ends with `termination: "deadline"`.
- `run --max-output SIZE` (and a session default) bounds a job's output file; a job that exceeds it is interrupted with
  `termination: "output_limit"`.
- When Talo kills a job's worker (deadline, limits), it also kills the processes the worker started (found through
  `/proc`), so child processes do not outlive the job.

### Diagnosing a job: `talo diagnose`, `--retain-error`

- `talo diagnose JOB` reports, without stopping the job: the stack of every thread (the worker dumps them with
  `faulthandler` on a signal), CPU use over a short interval, read and written bytes, resident memory, the process
  state and the processes it started.
- `run --retain-error` keeps the failed state: if the code fails, the worker keeps the exception (with its traceback
  and the frames' local variables) as `_talo_error`, Talo takes a snapshot `error-JOB` of that state, and the live
  worker drops the reference. `talo new debug --from error-JOB` then gives a session where the failure can be
  examined without running the expensive computation again. The traceback starts at the user's code
  (`_talo_error.__traceback__.tb_next.tb_frame.f_locals` are the locals of the next frame down); everything those
  frames reference stays in the snapshot's memory until it is dropped. With `--atomic`, the snapshot is taken before the
  restore.

### Files: `talo.artifact_path()`

`talo.artifact_path(name="")` returns a path in a directory of the running job (`sessions/S/artifacts/JOB/`), created
on demand. Rolling back memory never removes files; giving each job its own directory makes its outputs visible
(`talo artifacts JOB` lists them, with sizes) and lets a reviewer publish them deliberately
(`talo artifacts JOB --copy-to DIR`) instead of having an experiment overwrite shared files.

### A human takes over: `talo queue pause`, `talo lease`

- `talo queue pause [-m REASON]` stops the session from starting new jobs (running ones finish); submissions keep
  queueing and say why they wait. `talo queue resume` continues.
- `talo lease --ttl 30m [-m REASON]` gives the caller's owner the session for a while: only its jobs start; other
  owners' jobs wait in order and start when the lease ends (`talo lease --release`, or when it expires).
- `brief`, `jobs` and `events` show a pause or a lease.

### Where the memory went: `talo memory`

`talo memory` lists the session's memory by holder: the largest variables (their estimated sizes), objects retained
by `_` and `Out`, each snapshot's private memory and pins, map cases and calls, and the worker's resident memory not
attributed to any variable. `talo memory --prune` proposes what could be released — automatic and unpinned snapshots
that are not the base, the `Out` cache — with the estimated bytes of each, and applies it with `--apply`. Estimates
are labelled as such: shared pages, allocator caches and fragmentation make exact promises impossible.

## Known limitations

- Linux only (relies on `fork`, `/proc`, Unix sockets).
- Snapshots inherit the limits of `fork()` described above.
- A worker that runs out of memory is killed by the kernel; the supervisor reports it, and its variables are lost
  unless a snapshot or (M9) a durable checkpoint exists.
