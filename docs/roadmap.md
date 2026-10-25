# Roadmap

Each milestone is accepted against a real workflow: loading tens of gigabytes of market data once and iterating on
it from the terminal by a coding agent.

| Milestone | Scope | Acceptance | Status |
|---|---|---|---|
| **M0** Measurements | cost of `fork()` snapshots; libraries after `fork()` ([spikes](../spikes/README.md)) | numbers that justify the design | done |
| **M1** Resident execution | sessions (`new`, `ls`, `kill`); `run` with timeout → background jobs; `jobs`, `wait`, `tail`, `interrupt`; `vars`, `show`; history; Jupyter-shaped messages; `--json` | load a large dataset once and evaluate many ideas without reloading | done |
| **M2** Snapshots and rollback | `snap`, `checkout`, `tree`, `diff`, `drop`, branches; per-snapshot memory accounting and budgets; thread checks before forking | load → snapshot → corrupt a DataFrame → roll back → values, dtypes, index and aliasing intact | done |
| **M3** For humans | `attach` (an interactive prompt with completion and history on the same session); images saved as files; `export` to `.py` / `.ipynb` | a human and an agent working in the same session | done |
| **M4** Agents at scale | context recovery (`brief`, `note`); retry-safe submission (`run --key`); owners and preconditions for several agents on one session; progress and stall detection; disposable experiments (`try`); output that respects the context window; resource visibility and limits; `reload` of edited modules | a fresh agent (after context compaction) resumes an investigation from `talo brief` alone; three agents share one session without losing each other's work | |
| **M5** Across sessions | `map`: fan one snapshot out into N forked trials under a memory budget, collecting bounded results; `expose` / `call`: a session as a local service other agents call (stateful calls queued, read-only calls in disposable forks); an experiment ledger (`record`, `compare`) linked to jobs, snapshots and code versions; an event journal (`events --after CURSOR --wait`); `handoff` / `resume` | six agents test hypotheses from one prepared state with one copy of the data; a replacement agent continues an investigation from a handoff | |
| **M6** Shared datasets | `publish` immutable named versions of tables (Arrow IPC in sealed memory), `talo.get` in any session, explicit refresh, version retention and memory accounting | three agents use one 100 GB dataset version with one copy in memory; benchmarked against fork-only sharing and reloading | |
| **M7** Jupyter compatibility | ZMQ bridge and kernel connection file; JupyterLab / VS Code / `jupyter console` attach; MCP adapter over the same job and error contract | open a Talo session in JupyterLab | |
| **M8** Durability | save and restore selected variables and session metadata (notes, receipts) across restarts | recover data after a restart | |

### Agent needs behind M4 and M5

Collected from real agent work and design reviews; each is a problem that Jupyter, IPython, tmux or cached scripts do
not solve for an agent.

1. **Context recovery.** After compaction an agent no longer knows which state is trustworthy, why approaches failed,
   or what is still running. `talo brief` returns a bounded summary (state, variables by size, snapshots with
   messages, recent and running jobs, notes); `talo note TEXT [--job ID]` records intent and findings next to the
   state they refer to.
2. **Retry-safe submission.** A shell call times out after submitting a mutation but before printing the job id;
   retrying must not run it twice. `run --key K` returns the existing job for a known key.
3. **Several agents on one session.** Jobs carry an owner (`TALO_AGENT`); `checkout` refuses to discard other owners'
   work; `run --expect-base REF` fails if the state changed underneath.
4. **Disposable experiments.** `talo try CODE` runs in a fork and promotes the result only on success, so a failed
   half-mutation never leaks into the session.
5. **Progress and liveness.** `talo.progress(done, total)` from user code; jobs report progress, time since the last
   output, and a stall flag.
6. **Context-economical output.** Collapse carriage-return progress bars, fold repeated warnings, and cap the total
   size of every reply.
7. **Resources on shared machines.** `talo resources` (private and proportional memory per worker and snapshot,
   free memory, other users); per-session memory limits, with rollback to the last snapshot after an out-of-memory
   kill.
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
