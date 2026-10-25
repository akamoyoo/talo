# Changelog

## Unreleased

- `reload --in-place` updates the old functions and classes in place, so instances and references made before the
  reload (also inside containers) run the new code.
- `import talo` inside a session (in a cell or in a module it imports) gives the session's `talo` helper, also in
  interpreters without the talo package.
- `map --cases` also accepts JSON Lines, and reports malformed cases as a usage error.

## 0.1.0a1 — first release

Everything of the [roadmap](docs/roadmap.md), M0 to M9.

### Sessions and jobs

- A supervisor (one per `TALO_HOME`, started automatically, holding no user data) manages sessions, job queues and
  execution history; workers run in any interpreter, use the standard library only, speak Jupyter-shaped messages
  and capture output at the file-descriptor level.
- CLI: `run` (a timeout turns a job into a background job), `wait`, `tail`, `interrupt`, `jobs`, `vars`, `show`,
  `history`, `new`, `ls`, `kill`, `stop`; `--json` everywhere, structured errors, exit code 3 for running work.
- `attach`: an interactive prompt on a session, sharing its queue with other clients.
- Images and `display()` as files listed with the job; `export` of the code behind the current state as `.py` or
  `.ipynb`, with replay hazards marked.

### States

- Snapshots (`snap`), rollback (`checkout`), branches (`new --from`), `tree`, `diff` (by summary and content
  fingerprint), per-snapshot memory and budgets; `autosnap` and `undo`.
- `run --atomic`; `run --trial` with `review`, `adopt` and `reject`; `verify` (hot and cold replay agreement);
  `run --retain-error`.

### Agents at scale

- `brief`, notes, owners, revisions (`--expect-rev`), idempotency keys (`--key`), `talo.progress()`, condensed
  output and bounded results, `reload` with a stale-binding report.
- Across sessions: `map` (fan-out of a snapshot into ephemeral forks), services (`talo.expose`, `talo call`), the
  experiment ledger (`talo.record`, `ledger`), the event journal (`events`), `handoff` / `resume`, `share` with
  read / call / run levels.
- Shared datasets: `talo publish` / `talo.publish`, `talo.get` (immutable versions on `/dev/shm`, mapped read-only by
  every reader), `datasets`, `dataset drop | gc`, with hints when a value cannot be shared.

### Front ends

- `talo kernel` (a Jupyter kernel bridged to a session), `kernelspec install`, `notebook`, notebook magics;
  `talo mcp` (an MCP server over stdio).

### Safety and durability

- Hard limits through systemd scopes (`new --limit`), a soft `--memory-limit`, `run --deadline` / `--max-output`,
  `resources`, `memory`, `diagnose`, `queue pause | resume`, `lease`, `talo.artifact_path()` and `artifacts`.
- Checkpoints: `save`, `restore`, `checkpoints`, `checkpoint drop`, `store gc` (content-addressed, crash-consistent,
  aliasing and NumPy views kept, verified on reading); `recover` after a restart; `autosave`.

### Measurements

- The cost of `fork()` snapshots and the behaviour of common libraries after `fork()` (`spikes/`).
