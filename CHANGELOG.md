# Changelog

## Unreleased

### Added

- `attach`: an interactive prompt on a session (multi-line blocks, tab completion via `complete_request`,
  per-session input history, `%` commands, Ctrl-C interrupts, Ctrl-D detaches), sharing the job queue with other
  clients. Images (`_repr_png_` / `_repr_jpeg_` / `_repr_svg_`, `display()`, matplotlib figures) are saved as files
  listed in the job's `outputs`. `export` writes the lineage of the current state as `.py` or `.ipynb`.
- Snapshots (`snap`), rollback (`checkout`, with `--discard` / `--snap`), branches (`new --from`), `snaps`,
  `drop`; per-snapshot private memory, `--memory-budget`, `snap --strict`; snapshots survive a killed worker.
- `tree`: all snapshots and live workers of all sessions as one tree; `diff A [B]`: variables added, removed or
  changed between two snapshots or a snapshot and a live worker, by summary and by content fingerprint.
- Supervisor (one per `TALO_HOME`, started automatically) managing sessions, job queues and execution history.
- Worker running in any interpreter; standard-library only; Jupyter-shaped messages; fd-level output capture.
- CLI: `run` (timeout → background job), `wait`, `tail`, `interrupt`, `jobs`, `vars`, `show`, `history`, `new`, `ls`,
  `kill`, `stop`, `--json` everywhere.
- Measurements of `fork()` snapshot cost and of libraries after `fork()` (`spikes/`).
