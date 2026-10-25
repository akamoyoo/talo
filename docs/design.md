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
3. **Durable restore** (M8). After a process or machine restart, selected variables can be restored from disk.

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
  They do not survive the supervisor (they exit when it goes away) or a reboot (that is M8).

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

### Ephemeral workers

The building block: a worker forked from a snapshot that is *not* a session's worker. It runs one piece of code,
returns a bounded result, and exits; the snapshot is unchanged and the session it came from keeps working. Ephemeral
workers start in milliseconds and share all unmodified memory with the snapshot. They are listed by `talo jobs` with
their parent operation (`map` or `call`) and their memory is counted with the session's.

### Fan-out: `talo map`

```console
$ talo map --from loaded -f evaluate.py --cases ideas.json --parallel 8
map m3: 24 cases from default:loaded, 8 at a time
 #  status  time   result
 0  ok      41.2s  {"ic": 0.012, "rank_ic": 0.015}
 1  error   0.3s   KeyError: 'sector'
 ...
(24 cases: 22 ok, 2 error; results in sessions/default/maps/m3.jsonl)
```

- `--from SNAPSHOT` (default: the current state, through an internal checkpoint taken first) is forked once per case.
  Each case runs the code with `case` bound to its element of the `--cases` JSON list (or `--range N` for
  `case = 0 .. N-1`) and `talo.case` giving `{map, index}`.
- A case's **result** is the value of its last expression; it must be JSON-serializable (after conversion of NumPy
  scalars) and is cut to `--max-result` bytes, otherwise its `repr` is kept. Output goes to a file per case. Each
  result row has `index`, `case`, `status`, `result`, `error`, `elapsed_s`, `peak_pss` and its output path.
- Concurrency: at most `--parallel` cases run at once, and none is started while the session's memory (PSS of worker,
  snapshots and running cases) is above `--memory-limit` (default: the session's limit); a case over
  `--case-memory-limit` is stopped like a job over the memory limit.
- `map` returns after `-t` seconds like `run`, with a map id; `talo map-status ID` (and `wait`, `interrupt`) work on it.
  Interrupting a map cancels pending cases and interrupts running ones.
- Cases never change the session: there is nothing to roll back. A case that should be kept is re-run in the session.

### Sessions as services: `talo.expose` and `talo call`

```python
# in the host session, after loading data
def evaluate(expr: str, horizon: int = 30) -> dict: ...
talo.expose(evaluate)                      # service "default.evaluate", version 1
```
```console
$ talo call default.evaluate '{"expr": "ret_5 - ret_30", "horizon": 60}'
{"ic": 0.009, "rank_ic": 0.011}  [call c17, version 1, 3.1s]
```
```python
# in another session
r = talo.call("default.evaluate", expr="ret_5 - ret_30")
```

- `talo.expose(fn, name=None, mode="fork")` registers a function of the host session. Exposing takes an internal
  snapshot of the host: that is the **service version**. Calls fork ephemeral workers from it, so they run in
  parallel, never see later edits of the host, and cannot change the host or each other. Exposing again (after a
  fix or new data) makes a new version; running calls finish on their own version, and old versions are released
  when no call uses them. `talo services` lists services, versions, memory and calls in flight.
- `mode="queue"` instead runs each call as a job in the host's queue, for stateful services (a cache that should
  fill up, a model that learns); such calls change the host and count as revisions.
- Arguments and results are JSON. From Python, `talo.call` also accepts and returns picklable objects up to
  `max_bytes` (default 256 MB), passed through a file, so a session can fetch a computed table from a service.
- Calls accept `--key` (retry-safe) and `-t`; a call that does not finish in time returns its id like `run`.
- `talo.call` and the ledger functions reach the supervisor from user code through the socket named in
  `$TALO_SOCKET`; a call to a service of the same session is refused (it would wait for itself).

### The experiment ledger: `talo.record`

```python
talo.record(ic=0.012, rank_ic=0.015, factor="gap_mom", hypothesis="gap momentum survives a 5-minute delay")
```
```console
$ talo ledger --metric ic --sort -ic --last 5
r41  0.0150  map m3 case 7   default:loaded  agent-a  gap_mom_sector  (state: snapshot kept)
r38  0.0120  job 211         default:s4      agent-b  gap_mom         (state: dropped)
```

- `talo.record(**fields)` (and `talo record KEY=VALUE ...` from the shell) appends a record to the ledger shared by all
  sessions (`ledger.jsonl` in `TALO_HOME`): its fields (JSON scalars and short strings), id, time, owner, and where it
  came from — session, job, map case or call, revision, base snapshot, hashes of the job's code and of the reloaded
  modules' sources.
- `talo ledger` filters (`--session`, `--owner`, `--where KEY=VALUE`, `--has KEY`), sorts and bounds the records and
  says whether the state that produced each one still exists. `talo compare ID ID` shows two records side by side
  with the difference of their code and lineage.
- Records are facts reported by code; agents' interpretations belong in notes, which can cite record ids.

### Events: `talo events`

Everything that agents may wait for is appended to an event journal (`events.jsonl`): `job.finished`, `map.finished`,
`case.finished`, `call.finished`, `snapshot.created`, `snapshot.dropped`, `session.created`, `session.died`,
`service.exposed`, `record.added`, `note.added`, `handoff.created`. Each event has a sequence number.
`talo events --after N --wait SECONDS [--session S] [--type T]` returns the events after `N`, waiting up to the given
time for the first one; the reply's `next` is the cursor to pass back. Delivery is at least once; the journal is
kept for the supervisor's lifetime and capped in size.

### Handing over: `talo handoff`

`talo handoff -m "MESSAGE" [--to OWNER]` takes a snapshot of the current state (or names an existing one with
`--snap`) and writes a handoff: the brief, the notes and ledger records since the previous handoff, the running
jobs, the message, and how to continue (`talo new NAME --from SNAPSHOT`, or `talo attach`). `talo handoffs` lists
them and `talo resume ID` prints one, bounded, for the next agent or a human. The handoff's snapshot is pinned until
the handoff is closed (`talo resume ID --close`).

## Known limitations

- Linux only (relies on `fork`, `/proc`, Unix sockets).
- Snapshots inherit the limits of `fork()` described above.
- A worker that runs out of memory is killed by the kernel; the supervisor reports it, and its variables are lost
  unless a snapshot or (M8) a durable checkpoint exists.
