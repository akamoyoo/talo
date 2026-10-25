"""M4: several agents, retries, resuming."""

import json


def run_json(talo, *args, env=None):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def test_owner_is_recorded_and_named_in_checkout_refusals(talo):
    talo("run", "x = 1")
    talo("snap", "base")
    talo("run", "x = 2", "--owner", "agent-b")
    rc, out = run_json(talo, "jobs", "--last", "1")
    assert out["jobs"][0]["owner"] == "agent-b"
    rc, out = run_json(talo, "checkout", "base")
    assert rc == 2 and "by agent-b" in out["error"]["message"]


def test_expect_rev_refuses_a_changed_state(talo):
    talo("run", "x = 1")
    rc, s = run_json(talo, "brief")
    rev = s["session"]["rev"]
    rc, out = run_json(talo, "run", "--expect-rev", str(rev), "x = 2")
    assert rc == 0 and out["job"]["rev"] == rev + 1
    rc, out = run_json(talo, "run", "--expect-rev", str(rev), "x = 3")  # someone (we) changed it meanwhile
    assert rc == 1 and out["job"]["error"]["ename"] == "StaleRevision"
    rc, out = run_json(talo, "run", "x")
    assert out["job"]["result"]["data"]["text/plain"] == "2"


def test_key_makes_retries_idempotent(talo):
    talo("run", "n = 0")
    rc, a = run_json(talo, "run", "--key", "step-1", "n += 1")
    rc, b = run_json(talo, "run", "--key", "step-1", "n += 1")
    assert a["job"]["id"] == b["job"]["id"] and b["job"]["deduplicated"] and not a["job"].get("deduplicated")
    rc, out = run_json(talo, "run", "n")
    assert out["job"]["result"]["data"]["text/plain"] == "1"
    rc, out = run_json(talo, "run", "--key", "step-1", "n += 2")
    assert rc == 2 and "different request" in out["error"]["message"]
    rc, out = run_json(talo, "run", "--key", "step-1", "--expect-rev", "1", "n += 1")  # same code, other options
    assert rc == 2 and "different request" in out["error"]["message"]
    rc, a = run_json(talo, "snap", "s", "--key", "snap-1")
    rc, b = run_json(talo, "snap", "s", "--key", "snap-1")
    assert rc == 0 and a["ref"] == b["ref"]  # the retry did not try to take a second snapshot named s


def test_show_only_inspects(talo):
    talo("run", "items = [1, 2, 3]\nd = {'k': [4, 5]}")
    assert talo("show", "d['k'][0]").returncode == 0
    rc, out = run_json(talo, "show", "items.pop()")
    assert out["status"] == "error" and "talo run" in out["evalue"]
    rc, out = run_json(talo, "run", "len(items)")
    assert out["job"]["result"]["data"]["text/plain"] == "3"


def test_brief_while_busy_shows_the_last_variables_as_stale(talo):
    talo("run", "import time\nx = 1")
    talo("brief")
    talo("run", "y = 2; time.sleep(3)", "-t", "0")
    rc, d = run_json(talo, "brief")
    assert rc == 0 and d["variables"]["stale"] and [v["name"] for v in d["variables"]["largest"]] == ["x"]
    assert d["active_jobs"][0]["status"] == "running"


def test_brief_and_notes(talo):
    talo("run", "import numpy as np\nbig = np.ones(1_000_000)\nsmall = 1")
    talo("note", "goal: test the brief")
    talo("snap", "loaded", "-m", "data loaded")
    talo("run", "oops = missing")
    rc, d = run_json(talo, "brief")
    assert rc == 0 and d["notes"]["last"][0]["text"] == "goal: test the brief"
    assert d["variables"]["largest"][0]["name"] == "big"
    assert d["snapshots"]["last"][0]["message"] == "data loaded"
    assert d["recent_jobs"]["last"][-1]["error"].startswith("NameError")
    text = talo("brief").stdout
    assert "rev " in text and "goal: test the brief" in text and "NameError" in text
    rc, d = run_json(talo, "notes")
    assert d["total"] == 1 and d["notes"][0]["owner"] == "user"


def test_atomic_run_restores_the_state_on_failure(talo):
    talo("run", "import numpy as np, pandas as pd\ndf = pd.DataFrame({'x': np.arange(5.0)})\nalias = df\nn = 0")
    rc, out = run_json(talo, "run", "--atomic", "df.drop(columns=['x'], inplace=True)\nn += 1\nraise ValueError('half')")
    job = out["job"]
    assert rc == 1 and job["status"] == "error" and job["atomic"]["outcome"] == "restored"
    rc, out = run_json(talo, "run", "list(df.columns), n, alias is df")
    assert out["job"]["result"]["data"]["text/plain"] == "(['x'], 0, True)"
    rc, snaps = run_json(talo, "snaps")
    assert snaps["snapshots"] == [] and job["id"] not in snaps["jobs_since_base"]  # no checkpoint left, not in lineage
    assert "raise ValueError" not in talo("export").stdout
    rc, out = run_json(talo, "run", "--atomic", "n = 5")
    assert out["job"]["atomic"]["outcome"] == "committed" and out["job"]["id"] in run_json(talo, "snaps")[1]["jobs_since_base"]


def test_atomic_run_restores_after_the_worker_dies(talo):
    talo("run", "x = 1")
    rc, out = run_json(talo, "run", "--atomic", "import os, signal\nx = 2\nos.kill(os.getpid(), signal.SIGKILL)")
    assert out["job"]["status"] == "lost" and out["job"]["atomic"]["outcome"] == "restored"
    rc, out = run_json(talo, "run", "x")
    assert rc == 0 and out["job"]["result"]["data"]["text/plain"] == "1"


def test_atomic_interrupt_restores(talo):
    talo("run", "import time\nx = 1")
    rc, out = run_json(talo, "run", "--atomic", "x = 2\ntime.sleep(30)", "-t", "1")
    jid = out["job"]["id"]
    talo("interrupt", jid)
    rc, out = run_json(talo, "wait", jid, "-t", "30")
    assert out["job"]["status"] == "interrupted" and out["job"]["atomic"]["outcome"] == "restored"
    assert run_json(talo, "run", "x")[1]["job"]["result"]["data"]["text/plain"] == "1"


def test_progress_and_activity_of_a_running_job(talo):
    rc, out = run_json(talo, "run", "import time\nfor i in range(30):\n    talo.progress(i + 1, 30, 'step')\n"
                                    "    time.sleep(0.1)", "-t", "1")
    assert rc == 3
    job = out["job"]
    assert 0 < job["progress"]["done"] < 30 and job["progress"]["total"] == 30 and job["last_activity_age_s"] < 1
    rc, out = run_json(talo, "wait", job["id"], "-t", "30")
    assert out["job"]["progress"]["done"] == 30  # the final update is never dropped
    rc, out = run_json(talo, "vars")
    assert "talo" not in {v["name"] for v in out["variables"]}


def test_output_is_condensed_and_results_bounded(talo):
    code = ("import sys\nfor i in range(100):\n    sys.stdout.write(f'\\rprogress {i}%')\nprint()\n"
            "for _ in range(50):\n    print('same warning')\nprint('end')\n'x' * 50_000")
    rc, out = run_json(talo, "run", code)
    text = out["output"]["text"]
    assert "progress 99%" in text and "progress 98%" not in text
    assert text.count("same warning") == 1 and "repeated 49 more times" in text
    result = out["job"]["result"]
    assert len(result["data"]["text/plain"]) == 10_000 and result["metadata"]["talo_text_chars"] == 50_002
    assert open(result["metadata"]["talo_text_path"]).read() == repr("x" * 50_000)
    raw = talo("tail", out["job"]["id"], "--raw", "--all").stdout
    assert raw.count("same warning") == 50


GROW = ("import numpy as np, time\nblocks = []\nfor i in range(40):\n"
        "    blocks.append(np.ones(5_000_000))  # 40 MB a step\n    time.sleep(0.1)")


def test_memory_limit_interrupts_a_growing_job(talo):
    talo("new", "lim", "--memory-limit", "300M")
    rc, out = run_json(talo, "run", "-s", "lim", GROW, "-t", "60")
    job = out["job"]
    assert job["status"] == "interrupted" and job["termination"] == "memory_limit"
    rc, out = run_json(talo, "resources", "-s", "lim")
    assert out["sessions"][0]["memory_limit"] == 300 << 20 and out["machine"]["available"] > 0


def test_memory_limit_kills_a_job_that_ignores_the_interrupt_and_atomic_restores(talo):
    talo("new", "lim", "--memory-limit", "300M")
    talo("run", "-s", "lim", "x = 1")
    stubborn = "while True:\n    try:\n" + "\n".join("        " + line for line in GROW.splitlines()) + \
               "\n    except KeyboardInterrupt:\n        pass"
    rc, out = run_json(talo, "run", "-s", "lim", "--atomic", "x = 2\n" + stubborn, "-t", "60")
    job = out["job"]
    assert job["status"] == "lost" and job["termination"] == "memory_limit" and job["atomic"]["outcome"] == "restored"
    rc, out = run_json(talo, "run", "-s", "lim", "x")
    assert out["job"]["result"]["data"]["text/plain"] == "1"


def test_reload_changed_modules_and_report_stale_bindings(talo, tmp_path):
    import os
    import time
    (tmp_path / "factors.py").write_text("def f():\n    return 1\n\nclass Model:\n    pass\n")
    os.utime(tmp_path / "factors.py", (time.time() - 60, time.time() - 60))  # written well before it is imported
    (tmp_path / "fresh.py").write_text("x = 1\n")                          # written just now: baseline uncertain
    (tmp_path / ".venv").mkdir()
    talo("new", "r", "--cwd", str(tmp_path))
    talo("run", "-s", "r", "import factors, fresh\nfrom factors import f\nm = factors.Model()\nf1 = f")
    rc, d = run_json(talo, "reload", "-s", "r", "--changed", "--dry-run")
    assert rc == 0 and d["selected"] == ["fresh"]
    (tmp_path / "factors.py").write_text("def f():\n    return 2\n\nclass Model:\n    pass\n")
    rc, d = run_json(talo, "reload", "-s", "r", "--changed", "--dry-run")
    assert d["selected"] == ["factors", "fresh"]
    rc, d = run_json(talo, "brief", "-s", "r")
    rev = d["session"]["rev"]
    rc, d = run_json(talo, "reload", "-s", "r", "--changed")
    result = d["job"]["result"]
    assert rc == 0 and d["job"]["rev"] == rev + 1 and result["modules"][0]["status"] == "reloaded"
    stale = {s["name"]: s for s in result["stale_bindings"]}
    assert stale["f"]["kind"] == "definition" and stale["f"]["rebind"] == "from factors import f"
    assert stale["m"]["kind"] == "instance"
    rc, out = run_json(talo, "run", "-s", "r", "factors.f(), f()")
    assert out["job"]["result"]["data"]["text/plain"] == "(2, 1)"
    # a third version: names bound to the first version are still reported
    (tmp_path / "factors.py").write_text("def f():\n    return 3\n\nclass Model:\n    pass\n\n\n")
    rc, d = run_json(talo, "reload", "-s", "r", "factors")
    assert {"f", "f1", "m"} <= {s["name"] for s in d["job"]["result"]["stale_bindings"]}
    # the reload is part of the lineage: checkout refuses to drop it silently, export mentions it
    talo("snap", "-s", "r", "after")
    (tmp_path / "factors.py").write_text("def f():\n    return 4\n")
    talo("reload", "-s", "r", "factors")
    rc, out = run_json(talo, "checkout", "-s", "r", "after")
    assert rc == 2 and "--discard" in out["error"]["message"]
    assert "# --- reload factors reloaded" in talo("export", "-s", "r").stdout
    (tmp_path / "factors.py").write_text("def f(:\n")
    rc, d = run_json(talo, "reload", "-s", "r", "factors")
    assert rc == 1 and d["job"]["result"]["modules"][0]["status"] == "failed"
    assert run_json(talo, "run", "-s", "r", "factors.f()")[1]["job"]["result"]["data"]["text/plain"] == "4"


def test_show_runs_in_a_disposable_fork(talo):
    talo("run", "class Sneaky:\n    @property\n    def x(self):\n        globals()['touched'] = True\n        return 1\n"
                "s = Sneaky()")
    rc, out = run_json(talo, "show", "s.x")
    assert out["status"] == "ok" and "rev" in out
    rc, out = run_json(talo, "run", "'touched' in globals()")
    assert out["job"]["result"]["data"]["text/plain"] == "False"


def test_brief_is_bounded(talo):
    long = "x" * 3000
    talo("run", "y = 1", "-n", "1")
    for i in range(30):
        talo("note", f"{i} {long}")
    rc, d = run_json(talo, "brief", "--notes", "30")
    assert rc == 0 and len(json.dumps(d)) <= 16_000 and d["notes"]["total"] == 30
    assert all(len(n["text"]) <= 500 for n in d["notes"]["last"])


def test_keys_and_revisions_do_not_cross_session_incarnations(talo):
    talo("new", "w")
    rc, a = run_json(talo, "run", "-s", "w", "--key", "k", "x = 1")
    rev = run_json(talo, "brief", "-s", "w")[1]["session"]["rev"]
    talo("kill", "w")
    talo("new", "w")
    rc, b = run_json(talo, "run", "-s", "w", "--key", "k", "x = 1")
    assert not b["job"].get("deduplicated") and b["job"]["id"] != a["job"]["id"]
    rc, out = run_json(talo, "run", "-s", "w", "--expect-rev", str(rev), "x = 2")
    assert out["job"]["error"]["ename"] == "StaleRevision"  # revisions continue across the recreation


def test_checkout_retry_after_its_target_was_dropped(talo):
    talo("run", "x = 1")
    talo("snap", "a")
    rc, first = run_json(talo, "checkout", "a", "--key", "co")
    talo("drop", "a")
    rc, again = run_json(talo, "checkout", "a", "--key", "co")
    assert rc == 0 and again["deduplicated"] and again["job_id"] == first["job_id"]


def test_memory_limit_wins_over_a_caught_interrupt(talo):
    talo("new", "lim", "--memory-limit", "300M")
    talo("run", "-s", "lim", "x = 1")
    swallow = "x = 2\ntry:\n" + "\n".join("    " + line for line in GROW.splitlines()) + \
              "\nexcept KeyboardInterrupt:\n    blocks = None"
    rc, out = run_json(talo, "run", "-s", "lim", "--atomic", swallow, "-t", "60")
    job = out["job"]
    assert job["status"] == "interrupted" and job["termination"] == "memory_limit"
    assert job["atomic"]["outcome"] == "restored"
    assert run_json(talo, "run", "-s", "lim", "x")[1]["job"]["result"]["data"]["text/plain"] == "1"


def test_deadline_output_limit_and_child_processes(talo):
    import os
    import time
    talo("run", "import time, subprocess, sys")
    rc, out = run_json(talo, "run", "time.sleep(60)", "--deadline", "1", "-t", "30")
    assert out["job"]["status"] == "interrupted" and out["job"]["termination"] == "deadline"
    rc, out = run_json(talo, "run", "while True:\n    print('x' * 1000)", "--max-output", "1M", "-t", "30")
    assert out["job"]["termination"] == "output_limit"
    # a job that ignores the interrupt and has a child: both are killed after the grace period
    code = ("child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(600)'])\n"
            "open('child.pid', 'w').write(str(child.pid))\n"
            "while True:\n    try:\n        time.sleep(60)\n    except KeyboardInterrupt:\n        pass")
    rc, out = run_json(talo, "run", code, "--deadline", "1", "-t", "30")
    assert out["job"]["status"] == "lost" and out["job"]["termination"] == "deadline"
    pid = int(open("child.pid").read())
    os.unlink("child.pid")
    time.sleep(0.5)
    assert not os.path.exists(f"/proc/{pid}") or open(f"/proc/{pid}/stat").read().split()[2] == "Z"


def test_hard_limits_through_a_systemd_scope(talo):
    import shutil
    import pytest
    if shutil.which("systemd-run") is None:
        pytest.skip("no systemd-run")
    rc, out = run_json(talo, "new", "hard", "--limit", "memory=300M,pids=64")
    if rc != 0:
        pytest.skip(out["error"]["message"])
    assert out["scope"].startswith("talo-hard-")
    rc, out = run_json(talo, "run", "-s", "hard", "import numpy as np\nblocks = [np.ones(10_000_000) for _ in range(10)]",
                       "-t", "60")
    assert out["job"]["status"] == "lost"
    rc, s = run_json(talo, "ls")
    death = [x for x in s["sessions"] if x["name"] == "hard"][0]["death"]
    assert "memory limit of its scope" in death
    rc, out = run_json(talo, "new", "nocpu", "--limit", "cpu=2")
    assert rc == 2 or out.get("scope")                 # refused where the cpu controller is not delegated


def test_killing_a_job_tree_spares_the_snapshots(talo):
    talo("run", "import time\nx = 1")
    talo("snap", "keep")
    rc, out = run_json(talo, "run", "x = 2\nwhile True:\n    try:\n        time.sleep(60)\n    except KeyboardInterrupt:\n        pass",
                       "--deadline", "1", "-t", "30")
    assert out["job"]["status"] == "lost"
    rc, out = run_json(talo, "checkout", "keep")             # the snapshot (a child of the worker) survived
    assert rc == 0 and run_json(talo, "show", "x")[1]["text"] == "1"


def test_diagnose_a_running_job(talo):
    talo("run", "import time\ndef inner():\n    time.sleep(30)\ndef outer():\n    inner()")
    rc, out = run_json(talo, "run", "outer()", "-t", "0.5")
    jid = out["job"]["id"]
    rc, d = run_json(talo, "diagnose", jid)
    assert rc == 0 and "inner" in d["stack"] and "outer" in d["stack"] and d["cpu_percent"] is not None
    rc, d = run_json(talo, "jobs", "--last", "1")
    assert d["jobs"][0]["status"] == "running"            # diagnosing did not stop it
    talo("interrupt", jid)


def test_retain_error_keeps_the_failed_state(talo):
    talo("run", "def compute(n):\n    partial = [i * i for i in range(n)]\n    raise ValueError('bad input')")
    rc, out = run_json(talo, "run", "result = compute(5)", "--retain-error")
    name = out["job"]["retained"]["name"]
    assert out["job"]["status"] == "error" and name.startswith("error-")
    assert run_json(talo, "run", "'_talo_error' in globals()")[1]["job"]["result"]["data"]["text/plain"] == "False"
    talo("new", "debug", "--from", name)
    rc, out = run_json(talo, "run", "-s", "debug", "_talo_error.__traceback__.tb_next.tb_frame.f_locals['partial']")
    assert out["job"]["result"]["data"]["text/plain"] == "[0, 1, 4, 9, 16]"
    rc, out = run_json(talo, "run", "--atomic", "--retain-error", "x = 1\nraise KeyError('k')")
    assert out["job"]["atomic"]["outcome"] == "restored" and out["job"]["retained"]["name"].startswith("error-")
