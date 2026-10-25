# Roadmap

Each milestone is accepted against a real workflow: loading tens of gigabytes of market data once and iterating on
it from the terminal by a coding agent.

| Milestone | Scope | Acceptance | Status |
|---|---|---|---|
| **M0** Measurements | cost of `fork()` snapshots; libraries after `fork()` ([spikes](../spikes/README.md)) | numbers that justify the design | done |
| **M1** Resident execution | sessions (`new`, `ls`, `kill`); `run` with timeout → background jobs; `jobs`, `wait`, `tail`, `interrupt`; `vars`, `show`; history; Jupyter-shaped messages; `--json` | load a large dataset once and evaluate many ideas without reloading | done |
| **M2** Snapshots and rollback | `snap`, `checkout`, `tree`, `diff`, `drop`, branches; per-snapshot memory accounting and budgets; thread checks before forking | load → snapshot → corrupt a DataFrame → roll back → values, dtypes, index and aliasing intact | done |
| **M3** For humans | `attach` (an interactive prompt with completion and history on the same session); images saved as files; `export` to `.py` / `.ipynb` | a human and an agent working in the same session | |
| **M4** Jupyter compatibility | ZMQ bridge and kernel connection file; JupyterLab / VS Code / `jupyter console` attach; MCP adapter | open a Talo session in JupyterLab | |
| **M5** Durability | save and restore selected variables; `reload` changed project modules with a report of stale objects | recover data after a restart | |

Out of scope until after 1.0: GPU state, cross-machine migration, CRIU integration, automatic recomputation, merging
branches.
