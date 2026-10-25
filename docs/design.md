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
3. **Durable restore** (M5). After a process or machine restart, selected variables can be restored from disk.

Rollback is not a sandbox: files already written, network requests already sent and changes to external databases
are not undone.

## Architecture

```
talo CLI (humans, agents)       JupyterLab / VS Code (M4)     MCP (M4)
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
  They do not survive the supervisor (they exit when it goes away) or a reboot (that is M5).

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
prefixed `talo_`. The transport is newline-delimited JSON over Unix sockets; M4 adds a ZMQ bridge and a standard
kernel connection file so that Jupyter front ends can attach. See [protocol.md](protocol.md).

## A command line designed for agents

- Every command returns: `run` waits 30 seconds by default, then returns a job id; `wait`, `tail` and `interrupt`
  take it from there.
- `--json` gives stable fields, structured errors and truncation markers; the default output is for humans.
- Output is truncated to its last lines and results to a bounded size, always with a note saying how much was cut;
  the full output stays in the job's file and can be read by offset.
- One session runs one execution at a time; further execution requests queue. Inspection requests (`vars`, `show`)
  are answered only while the session is idle, never by touching the heap of a running computation.
- Exit codes: `0` ok, `1` the code failed, `2` Talo failed, `3` still running.

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

## Known limitations

- Linux only (relies on `fork`, `/proc`, Unix sockets).
- Snapshots inherit the limits of `fork()` described above.
- A worker that runs out of memory is killed by the kernel; the supervisor reports it, and its variables are lost
  unless a snapshot or (M5) a durable checkpoint exists.
