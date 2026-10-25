# Roadmap

Each milestone is accepted against a real workflow: loading tens of gigabytes of market data once and iterating on
it from the terminal by a coding agent.

| Milestone | Scope | Acceptance | Status |
|---|---|---|---|
| **M0** Measurements | cost of `fork()` snapshots; libraries after `fork()` ([spikes](../spikes/README.md)) | numbers that justify the design | done |
| **M1** Resident execution | sessions (`new`, `ls`, `kill`); `run` with timeout → background jobs; `jobs`, `wait`, `tail`, `interrupt`; `vars`, `show`; history; Jupyter-shaped messages; `--json` | load a large dataset once and evaluate many ideas without reloading | done |
| **M2** Snapshots and rollback | `snap`, `checkout`, `tree`, `diff`, `drop`, branches; per-snapshot memory accounting and budgets; thread checks before forking | load → snapshot → corrupt a DataFrame → roll back → values, dtypes, index and aliasing intact | done |
| **M3** For humans | `attach` (an interactive prompt with completion and history on the same session); images saved as files; `export` to `.py` / `.ipynb` | a human and an agent working in the same session | done |
| **M4** Agents at scale | context recovery (`brief`, `note`); retry-safe submission (`run --key`); owners and revisions (`--expect-rev`) for several agents on one session; progress and activity; atomic runs (`run --atomic`); output that respects the context window; resource visibility and limits; `reload` of edited modules | a fresh agent (after context compaction) resumes an investigation from `talo brief` alone; three agents share one session without losing each other's work | done |
| **M5** Across sessions | `map`: fan one snapshot out into N forked trials under a memory budget, collecting bounded results; `expose` / `call`: a session as a local service other agents call (stateful calls queued, read-only calls in disposable forks); an experiment ledger (`record`, `compare`) linked to jobs, snapshots and code versions; an event journal (`events --after CURSOR --wait`); `handoff` / `resume`; sharing a session with other clients and other users (`share --level read / call / run`) | six agents test hypotheses from one prepared state with one copy of the data; a replacement agent continues an investigation from a handoff || done |
| **M6** Shared datasets | `publish` immutable versions of a variable into `/dev/shm` (the M9 format: buffers stored by content), `talo.get` maps them read-only in any session (one copy in memory, unchanged columns shared between versions), versions recorded on the jobs that read them | three sessions use one 2 GB dataset version with one copy in memory; a new version with one changed column adds only that column | |
| **M7** Jupyter compatibility | ZMQ bridge and a kernelspec (`talo kernel --session NAME`) so JupyterLab, VS Code (notebooks and `# %%` cells in `.py` files) and `jupyter console` attach to a running session, with standard `.py` / `.ipynb` files; MCP adapter over the same job and error contract; rollback inside notebooks: `%snap` / `%checkout` / `%tree` / `%diff` cell magics handled by the bridge (the front end stays connected while the worker behind it is replaced), `%autosnap` (a snapshot after every cell, the last N kept) with `%undo` and `%checkout @N`, and a notice when the notebook's text no longer matches the state; later, an optional VS Code extension for what Jupyter lacks (snapshot tree, checkout, variables with memory, jobs, brief), built on `--json` | open a Talo session that an agent is using in JupyterLab and in VS Code | done (tested through Jupyter's kernel manager with nbclient; JupyterLab and VS Code not yet tried by hand) |
| **M8** Trust and safety | `verify` (hot: replay from a snapshot and diff the result; cold: run the export in a fresh interpreter); `run --trial` / `review` / `adopt` (a result kept aside until accepted, then switched in atomically); hard limits through a cgroup v2 scope (memory, CPU, processes, output size) and `--deadline` separate from the client's wait; interrupting the whole process tree; `diagnose JOB --stack` (thread stacks without killing it) and `--retain-error` (keep the failed state in a fork to debug); `talo.artifact_path()` (per-job output directories, published after review); `queue pause` / `lease` for a human to take over; `memory explain` / `prune --dry-run` | an agent's 20-minute transformation is reviewed and adopted without recomputation; a stuck job is diagnosed without being killed; `verify` reports whether the current state is reproducible | done |
| **M9** Durability | checkpoints of variables (`save` / `restore`, content-addressed, aliasing kept), `recover` after a restart, `autosave` | after the supervisor is killed, `talo recover` brings a session with a 2 GB panel back and `verify`-style comparison shows the same values | done |

### Where Talo stands

Keeping a Python session resident and driving it from a CLI is no longer unique (for example agentnb drives a
resident IPython from the command line with background jobs; ipykernel 7 subshells allow inspection while a job
runs, sharing one namespace). Talo's difference is **cheap, inspectable, disposable branches of the in-memory state**:
snapshots, rollback, forks per case or call, and the provenance that ties results to them. Hidden state and
reproducibility remain the best-documented problem of notebooks (in a re-execution of about 860 000 GitHub notebooks,
24% ran without errors and 4% reproduced their results); reactive notebooks infer dependencies but cannot see
in-place mutation such as `list.append()`. M8's `verify` addresses this with execution, not inference.

### Agent needs behind M4 and M5

Collected from real agent work and design reviews; each is a problem that Jupyter, IPython, tmux or cached scripts do
not solve for an agent.

1. **Context recovery.** After compaction an agent no longer knows which state is trustworthy, why approaches failed,
   or what is still running. `talo brief` returns a bounded summary (state, variables by size, snapshots with
   messages, recent and running jobs, notes); `talo note TEXT [--job ID]` records intent and findings next to the
   state they refer to.
2. **Retry-safe submission.** A shell call times out after submitting a mutation but before printing the job id;
   retrying must not run it twice. `run --key K` returns the existing job for a known key.
3. **Several agents on one session.** Jobs carry an owner (`TALO_AGENT`); `checkout` names other owners' work it would
   discard; `run --expect-rev N` fails if the state changed underneath.
4. **Atomic runs.** `talo run --atomic CODE` runs inside a checkpoint and restores it on failure, so a failed
   half-mutation never leaks into the session.
5. **Progress and liveness.** `talo.progress(done, total)` from user code; jobs report progress and the time since the
   last output or progress (evidence of activity; Talo never interrupts a quiet job).
6. **Context-economical output.** Collapse carriage-return progress bars, fold repeated warnings, and cap the total
   size of every reply.
7. **Resources on shared machines.** `talo resources` (private and proportional memory per worker and snapshot,
   free memory, other users); a soft per-session memory limit enforced by Talo itself, with atomic jobs restored
   to their checkpoint.
8. **Editing code while data stays loaded.** `talo reload MODULE` with a report of objects still bound to old code.
9. **Fan-out from a prepared state (M5).** One agent prepares data or reproduces a hard bug; many trials start from
   exactly that state in forks, in parallel, without reloading. `talo map --from S --cases trials.json --parallel 4`.
10. **A session as a service (M5).** A warmed evaluator, solver or loaded model that every agent calls
    (`talo.expose(fn)`, `talo.call("session.fn", ...)`), pinned to a version so the owner's edits do not silently
    change other agents' experiments.
11. **Evidence that survives compaction (M5).** A ledger of hypotheses and metrics tied to the job, snapshot and code
    that produced them, and handoffs that separate observations from suggestions.
12. **Shared datasets (M6).** One session publishes an immutable version of a table; other sessions and agents map it
   without copying and move to a newer version explicitly. Live mutable sharing is out of scope.

Out of scope until after 1.0: GPU state, cross-machine migration, CRIU integration, automatic recomputation, merging
branches.
