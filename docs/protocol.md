# Talo protocol

Talo has two message channels. Both carry newline-delimited JSON over Unix sockets.

| Channel | Between | Shape |
|---|---|---|
| Control | CLI (or any client) and supervisor, `$TALO_HOME/supervisor.sock` | one request line, one reply line |
| Kernel | supervisor and a session's worker (socket pair) | Jupyter messages |

## Kernel messages

Every message has the Jupyter structure:

```json
{"header": {"msg_id": "…", "msg_type": "execute_request", "session": "default", "username": "talo",
            "date": "2026-10-25T01:00:00+00:00", "version": "5.3"},
 "parent_header": {},
 "metadata": {},
 "content": {}}
```

Replies and side messages carry the request's header as `parent_header`.

### Standard messages (Jupyter 5.3)

| Request | Replies and side messages | Notes |
|---|---|---|
| `execute_request` (`code`, `silent`, `store_history`, …) | `status` busy, `execute_input`, `execute_result` or `error`, `execute_reply`, `status` idle | metadata `talo_output`: path of the job's output file (fds 1 and 2 are redirected to it). `execute_reply` metadata: `talo_duration_s`, `talo_rss_before`, `talo_rss_after` |
| `kernel_info_request` | `kernel_info_reply` | adds `talo_pid`, `talo_executable` |
| `complete_request` (`code`, `cursor_pos`) | `complete_reply` (`matches`, `cursor_start`, `cursor_end`) | names, attributes and dictionary keys of the live namespace |
| `shutdown_request` | `shutdown_reply` | the worker exits |

Interrupts are signals (SIGINT to the worker), as in Jupyter's `interrupt_mode: signal`. Output is delivered through
the job's output file rather than `stream` messages; the M4 bridge turns the file into `stream` messages for Jupyter
front ends.

`execute_result.data` is a mime bundle: `text/plain` always (truncated to 100 000 characters, with
`metadata.talo_truncated` set to the number of characters cut), plus `text/html` / `text/markdown` when the object
provides `_repr_html_` / `_repr_markdown_`. Binary representations (`image/png`, `image/jpeg`, `image/svg+xml`) are not sent inline: the worker
writes each one to a file next to the job's output file (`ID-N.png`, ...) and lists it in `metadata.talo_files`
(`{mime: path}`); the M4 bridge inlines them (base64) for Jupyter front ends. `display_data` messages (from
`display()` and matplotlib figures) follow the same rules. The supervisor collects the files of a job into its
`outputs` list.

### Talo extensions

| Request | Reply content |
|---|---|
| `talo_vars_request` (`pattern`) | `variables`: list of `{name, type, nbytes, shape | len, dtype, columns, value}`; `rss` |
| `talo_show_request` (`name`: a name, attribute or constant subscript such as `df.x` or `d["k"]`, `rows`) | `summary` (as above), `text` (preview) — or `status: error` with `ename`, `evalue` |
| `talo_progress` (worker → supervisor, child of the running `execute_request`): `done`, `total`, `note` | – (stored as the job's `progress`) |
| `talo_fingerprint_request` (`names`) | `fingerprints`: `{name: {"hash": hex} \| {"error": text}}` (BLAKE2b of the pickled object; see "Diff" in [design.md](design.md)) |
| `talo_snapshot_request` (`id`, `socket`: path to listen on, `strict`) | `pid` of the snapshot process, `threads`: names of the other threads present at fork time — or `status: error` (`strict` and other threads exist) |

### Snapshot sockets

A snapshot process listens on the path given in `talo_snapshot_request` and answers newline-delimited JSON requests,
one per connection:

| Request | Effect |
|---|---|
| `{"op": "info"}` | `{"ok": true, "pid": …, "execution_count": …}` |
| `{"op": "vars"}` | `{"ok": true, "variables": [...]}` (as `talo_vars_reply`; the snapshot's state is never modified) |
| `{"op": "fingerprint", "names": [...]}` | `{"ok": true, "fingerprints": {...}}` (as `talo_fingerprint_reply`) |
| `{"op": "spawn"}` | the snapshot forks; the child replies `{"ok": true, "pid": …}` and from then on speaks the kernel protocol on this connection as the session's new worker |
| `{"op": "hold"}` | no reply; the supervisor keeps this connection open, and the snapshot exits when it closes |
| `{"op": "exit"}` | the snapshot exits |

## Control requests

Request: `{"op": "<operation>", "owner": "...", ...parameters}` (`owner` defaults to `user`; the CLI sends `$TALO_AGENT`). Reply: `{"ok": true, ...result}` or
`{"ok": false, "error": {"type": "...", "message": "..."}}`.

| `op` | Parameters | Result |
|---|---|---|
| `ping` | – | `pid`, `home`, `sessions`, `epoch` (changes when the supervisor restarts) |
| `new` | `name`, `python`, `cwd`, `env`, `from` (`[session:]snapshot`), `memory_budget`, `memory_limit` (bytes) | the session |
| `ls` | – | `sessions` |
| `kill` | `name` | `killed` |
| `run` | `session`, `code`, `timeout` (seconds), `key` (idempotency), `expect_rev`, `atomic` | the job (with the supervisor's `epoch`), possibly still `running`; for a known `key`, the job already submitted, with `deduplicated: true` |
| `job` | `id`, `wait` (seconds) | the job |
| `jobs` | `session` (optional), `last` | `jobs` (the last `last`, each with `code_preview`: the first 200 characters of its code), `total` |
| `output` | `id`, `offset` (default: the end), `max_bytes` | `text`, `offset`, `next_offset`, `size`, `status` |
| `interrupt` | `id` | `result`: `interrupting`, `cancelled` or the final status |
| `vars` | `session`, `pattern` | as `talo_vars_reply`, plus the `rev` observed |
| `show` | `session`, `name`, `rows` | as `talo_show_reply` |
| `history` | `session`, `last` | `history` |
| `snap` | `session`, `name`, `message`, `strict`, `timeout`, `key` | the snapshot (queued behind running jobs), or `{"job": ...}` when it has not finished within `timeout` |
| `snaps` | `session` | `snapshots`, `base`, `jobs_since_base` |
| `checkout` | `session`, `name`, `discard`, `snap` (name to save the current state under first), `timeout`, `expect_rev`, `key` | the session, or `{"job": ...}` as for `snap` |
| `drop` | `session`, `name` | `dropped` |
| `complete` | `session`, `code`, `cursor_pos` | as `complete_reply` (answered only while idle) |
| `export` | `session`, `snap` (export the path to this snapshot), `all` (every job) | `jobs`: job records in order, each with `code`, `status`, `outputs`, and markers for snapshots (`kind: "snap"`) |
| `tree` | – | `nodes`: snapshots (`kind: "snapshot"`, as below) and live workers (`kind: "worker"`, `session`, `state`, `base`, `jobs_since_base`), each with `parent` (a snapshot ref or null) |
| `diff` | `session` (for `@` and bare names), `a`, `b` (default `@`), `max_bytes` (content limit, null for no limit) | `added`, `removed`: variable summaries; `changed`: `{name, a, b, fields}` (`fields` lists the differing summary keys, or `["content"]`); `unchanged`: count; `not_compared`: `{name, reason}` |
| `resources` | `session` (optional) | `machine` (`total`, `available`, swap), `sessions`: each with `processes` (worker, snapshots, checkpoint: `pid`, `memory`: `{rss, pss, private_clean, private_dirty}` or null when unknown), `pss_total`, `pss_unknown`, `memory_limit`, `over_limit`; `pss_total`, `sampled_at` |
| `note` | `session`, `text`, `job` | `note`: `{time, owner, rev, base, job, text}` |
| `notes` | `session`, `last` | `notes`, `total` |
| `brief` | `session`, `notes`, `jobs`, `vars` (how many of each) | `session`, `private_bytes`, `notes`, `snapshots`, `variables` (`largest`, `total`, or `skipped` while busy), `recent_jobs`, `active_jobs` |
| `shutdown` | – | `stopping` |

A **session** is `{name, state (starting | idle | busy | dead), python, cwd, pid, rss, created, queued, current_job,
death, base, jobs_since_base, snapshots, rev, memory_budget, memory_limit, over_limit}`. A **snapshot** is `{id, name, session, parent, pid, created, message, execution_count,
jobs (ids run between its parent and itself), private_bytes, threads, owner}`. A **job** is `{id, session, status (queued | running | ok | error | interrupted | lost | cancelled), submitted,
started, finished, elapsed_s, execution_count, result, error, metrics, output, outputs, owner, key, rev, atomic, termination (`memory_limit` when Talo stopped it), progress}`; a running job also has `last_output_age_s`,
`last_progress_age_s` and `last_activity_age_s`.

The control protocol is versioned together with Talo while it is pre-alpha; it will be frozen and versioned
separately at 1.0.
