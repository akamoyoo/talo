# talo design

talo is a resident, in-memory Python session driven from the terminal: data is loaded once and stays in memory,
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
  The worker is a standard-library-only file executed by path, so the target environment does not need talo installed,
  and nothing is added to the user's `sys.path`.
- **Output is captured at the file-descriptor level.** While a job runs, fds 1 and 2 point to the job's output file;
  output written by C extensions is captured, and it can be read while the job is still running.
- **Snapshots** (M2) are suspended copies of the worker made with `fork()` between two executions (copy-on-write).
  Rolling back means asking a snapshot to fork a new worker; the snapshot itself stays unchanged, so it can be checked
  out many times and branched. Snapshots form a tree.

### Snapshot mechanics (M2)

Before forking, the worker creates a socket pair, keeps one end in the child and hands the other end to the
supervisor (`SCM_RIGHTS`). The child is the snapshot: it closes its connection to the supervisor and waits on its own
end. To check it out, the supervisor sends it a fresh socket end; the snapshot forks again, and the grandchild becomes
the session's new worker using that socket. The snapshot reaps its children itself.

Costs measured in [spikes/README.md](../spikes/README.md):

- taking a snapshot: milliseconds, growing with the page tables (12 ms with 16 GB resident);
- extra memory after a snapshot: exactly the pages modified afterwards (touching 1% of 16 GB cost 164 MB; rewriting
  everything costs a full copy);
- rollback: well under a second at 16 GB.

Each snapshot's private memory is reported from `/proc/PID/smaps_rollup` (`Private_Dirty`); a session budget refuses
new snapshots that would exceed it and says which snapshots to drop.

### Fork safety

`fork()` in a multi-threaded process leaves only the calling thread in the child; locks held by other threads may stay
locked forever (Python 3.12 warns about this). Snapshots are taken only at idle points, and before forking the worker
inspects its threads (`/proc/self/task/*/comm`): known-safe thread pools (OpenBLAS) are allowed, unknown threads produce
a warning or a refusal with an explanation. Libraries with their own thread pools may hang or refuse to run in a forked child
unless they rebuild the pool after `fork()`; GPU contexts, network connections and writable mappings are not restored
by a rollback.

## Messages: built on the Jupyter messaging protocol

All messages — CLI to supervisor and supervisor to worker — use the structure of the Jupyter messaging protocol
(5.3): `header` (`msg_id`, `msg_type`, `session`, `date`, `version`), `parent_header`, `metadata`, `content`.
Execution-related message types and fields follow the specification; talo's own features are extension messages
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
- Exit codes: `0` ok, `1` the code failed, `2` talo failed, `3` still running.

## Execution history

Every finished job appends one line to `sessions/NAME/history.jsonl`: code, status, execution count, start and end
times, resident memory before and after, result preview and error. M3 exports a session's history (or the path to a
snapshot) as a replayable `.py` file or `.ipynb`.

## Known limitations

- Linux only (relies on `fork`, `/proc`, Unix sockets).
- Snapshots inherit the limits of `fork()` described above.
- A worker that runs out of memory is killed by the kernel; the supervisor reports it, and its variables are lost
  unless a snapshot or (M5) a durable checkpoint exists.
