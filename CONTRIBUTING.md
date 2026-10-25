# Contributing to Talo

Talo is pre-alpha: interfaces change, and design discussions come before code. Please open an issue describing the
problem or the idea before sending a large change.

## Development setup

```console
$ git clone git@github.com:akamoyoo/talo.git && cd talo
$ uv venv && uv pip install -e ".[dev]"
$ .venv/bin/pytest
```

Tests start their own supervisor under a temporary `TALO_HOME`, so they never touch your real sessions.

## Ground rules

- **The worker and `wire.py` use the standard library only.** They run inside users' interpreters.
- **Messages follow the Jupyter messaging protocol.** New standard-looking fields go into Talo extensions
  (`talo_*` message types or metadata keys), never into redefined Jupyter fields. Update
  [docs/protocol.md](docs/protocol.md) together with the code.
- **Every command must return.** No CLI command may block longer than its timeout.
- **Measure before optimizing memory or snapshot behavior**, with a small input first, then a representative large one,
  under a hard memory limit well above the expected peak.
- Code, comments, docs and commit messages are in English.

## Commit messages

One coherent change per commit. Summary line in the imperative (`Add snapshot memory accounting`), followed by a
short explanation of why.
