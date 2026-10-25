<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
    <img src="assets/logo.svg" alt="Talo" width="128" height="128">
  </picture>
</p>

<h1 align="center">Talo</h1>

<p align="center">
  <em>A resident Python session you drive from the terminal.<br>
  Load your data once. Keep it in memory. Snapshot, roll back, branch.</em>
</p>

<p align="center">
  <a href="#status">Status: pre-alpha</a> ·
  <a href="docs/design.md">Design</a> ·
  <a href="docs/protocol.md">Protocol</a> ·
  <a href="docs/roadmap.md">Roadmap</a> ·
  <a href="CONTRIBUTING.md">Contributing</a> ·
  <a href="LICENSE">Apache-2.0</a>
</p>

---

**Talo** (Finnish for *house*) is a long-lived Python process that keeps your variables in memory while you —
or your coding agent — send it code from the terminal, one command at a time. Think of it as a Jupyter kernel without
the notebook UI, built for workflows where the expensive part is getting the data into memory (illustrative session):

```console
$ talo run 'import pandas as pd; df = pd.read_parquet("trades.parquet")'   # once: 40 s, 30 GB
[job 1 ok [1] 41.20s, memory 30.4 GB (+30.3 GB)]

$ talo run 'df.groupby("symbol").price.std().nlargest(5)'                  # every idea after that: instant
symbol
BTCUSDT    812.4
...
[job 2 ok [2] 0.84s, memory 30.4 GB (+12.0 MB)]
```

Every command returns quickly: if the code is still running after the timeout, you get a job id and the computation
keeps going in the background. Output is captured at the file-descriptor level (so prints from C extensions show up
too), truncated sensibly for humans and agents alike, and kept in full on disk.

## Why Talo?

Data analysis by trial and error — and that is exactly how coding agents work — keeps paying two costs over and over:

1. **Reloading.** Every new idea becomes a new script that loads the same gigabytes again.
2. **No way back.** Once a step mutates a DataFrame in place, the only way to recover is to reload and redo.

Jupyter solves the first one for humans in a browser. Talo aims to be a superset of what a Jupyter kernel gives you,
driven from the shell, plus what Jupyter does not have:

| | Jupyter kernel | Talo |
|---|---|---|
| Variables stay in memory between executions | ✓ | ✓ |
| Rich outputs (HTML, images), `.ipynb` | ✓ | ✓ (M3) |
| Connect from JupyterLab / VS Code | ✓ | ✓ (M6, Jupyter messaging protocol) |
| Driven from the terminal by agents, non-blocking commands, background jobs | – | ✓ |
| Structured, truncated output with `--json` | – | ✓ |
| **Snapshots, rollback and branches of the in-memory state** | – | ✓ |
| Execution history with memory deltas, export of the code behind the current state (`.py` / `.ipynb`) | partial | ✓ |
| A human and an agent on the same live session | – | ✓ |
| Memory accounting and budgets per session | – | ✓ |

Snapshots use copy-on-write `fork()`: taking one costs milliseconds even with tens of gigabytes in memory, and it only
consumes extra memory for the pages you modify afterwards (measurements: [spikes/README.md](spikes/README.md)).

## Status

**Pre-alpha.** Resident execution (M1) and snapshots / rollback / branches / `tree` / `diff` (M2) and the interactive prompt, images as files and export (M3) work; tools for agents at scale (M4) are next, then shared datasets (M5) and Jupyter front ends (M6). Interfaces will change.
Linux only. See the [roadmap](docs/roadmap.md).

## Install

Talo has no runtime dependencies and needs Python ≥ 3.10.

```console
$ pip install -e .            # from a checkout; a PyPI release will follow
$ talo --version
```

The session's code runs in whichever interpreter you choose (`--python`), so your project's virtualenv does not need
Talo installed in it.

## Quick start

```console
$ talo run 'x = 41'                       # creates the "default" session on first use
$ talo run 'x + 1'
42
[job 2 ok [2] 0.00s, memory 17.6 MB (+64.0 KB)]

$ talo run -f load.py -t 5                # long job: returns after 5 s, keeps running
[job 3 running 5.00s] `talo wait 3` for the result, `talo tail 3` for output, `talo interrupt 3` to stop it
$ talo tail 3 -f                          # follow its output
$ talo wait 3

$ talo vars                               # what's in memory
df                   pandas.DataFrame (12000000, 8)  1.1 GB
x                    int  28 B = 41
$ talo show df -r 5                       # dtypes and the first rows

$ talo snap loaded                        # snapshot: milliseconds, even with tens of GB in memory
$ talo run 'df = df[df.price > 0]'        # experiment ...
$ talo checkout loaded --discard          # ... and roll back
$ talo new idea --from loaded             # or try another idea in parallel from the same state

$ talo new research --python ~/proj/.venv/bin/python --cwd ~/proj
$ talo run -s research 'import mylib'     # sessions are independent processes
$ talo ls
$ talo kill research
```

### For coding agents

Add `--json` to any command for stable, machine-readable output:

```console
$ talo run --json 'df.shape'
{"job": {"id": "7", "status": "ok", "execution_count": 4, "result": {"data": {"text/plain": "(12000000, 8)"}, ...},
 "output": {"text": "", "lines_cut": 0, ...}}
```

Exit codes: `0` ok, `1` the code failed (error, interrupted, lost), `2` Talo itself failed, `3` still running
(use `talo wait ID`). A typical agent loop never blocks for more than the timeout it chose.

## Commands

| Command | What it does |
|---|---|
| `talo run CODE` / `-f FILE` / `-` | run code in a session (`-s NAME`, `-t SECONDS`, `-n LINES`) |
| `talo wait ID` | wait for a job and print its result |
| `talo tail ID [-f] [--all]` | a job's output, optionally following it |
| `talo interrupt ID` | interrupt a running job (cancels a queued one) |
| `talo jobs` | jobs and their status |
| `talo vars [PATTERN]` | variables: type, shape, dtype, memory |
| `talo show EXPR` | a summary of a variable or expression (`df`, `df.x`, `d["k"]`) |
| `talo history` | finished jobs with duration and memory deltas |
| `talo snap [NAME]` | snapshot the in-memory state (milliseconds, copy-on-write) |
| `talo checkout NAME` | roll back: replace the worker with a fork of a snapshot (`--discard`, `--snap NAME`) |
| `talo new NAME --from SNAPSHOT` | branch: a new session forked from a snapshot |
| `talo snaps` / `drop NAME` | snapshots with their private memory; free one |
| `talo tree` | all snapshots and workers of all sessions as one tree |
| `talo diff A [B]` | variables added, removed or changed between two snapshots (or a snapshot and the live worker) |
| `talo attach` | an interactive prompt on the session: completion, history, `%snap` / `%diff` / ...; Ctrl-D detaches |
| `talo export [-o FILE]` | the code that produced the current state as `.py` or `.ipynb` (`--snap NAME`, `--all`) |
| `talo new NAME` / `ls` / `kill NAME` | manage sessions (`--python`, `--cwd`, `--memory-budget`) |
| `talo stop` | stop the supervisor and all sessions |

`talo COMMAND --help` documents every option.

## How it works

```
talo CLI (humans, agents)        JupyterLab / VS Code (M6)
        │  JSON over a Unix socket       │  ZMQ bridge
        └───────────────┬────────────────┘
                 supervisor  (one per machine; holds no user data)
                 ├─ sessions, job queues, history, output files
                 ├─ session "default": worker process + snapshots (M2)
                 └─ session "research": ...
```

- The **supervisor** starts automatically on first use and routes requests; it never holds your data, so it survives
  a worker crash and tells you what happened (for example an out-of-memory kill).
- Each session's **worker** is a single-threaded process running in your chosen interpreter. It is a single
  standard-library-only file executed by path.
- Messages use the structure of the **Jupyter messaging protocol** (`execute_request`, `execute_result`, `error`,
  `status`, ...), with Talo's own extensions prefixed `talo_`. See [docs/protocol.md](docs/protocol.md).

Details, trade-offs and known limitations are in [docs/design.md](docs/design.md).

## Development

```console
$ uv venv && uv pip install -e ".[dev]"
$ .venv/bin/pytest
```

See [CONTRIBUTING.md](CONTRIBUTING.md). Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## License

[Apache License 2.0](LICENSE). The Talo name and logo are not covered by the license.
