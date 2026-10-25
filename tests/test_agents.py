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
    assert d["snapshots"][0]["message"] == "data loaded"
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
