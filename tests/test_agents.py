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
