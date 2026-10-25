"""M8: trial, review, adopt."""

import json
import time


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def x(talo):
    return run_json(talo, "show", "x")[1]["text"]


def test_trial_review_adopt(talo):
    talo("run", "import time\nx = 1\nitems = [1]")
    rc, out = run_json(talo, "run", "--trial", "double", "x = x * 2\nitems.append(2)\nprint('done')\nx")
    assert rc == 0 and out["job"]["trial"] == "t1" and out["job"]["kind"] == "trial" and out["job"]["status"] == "ok"
    assert x(talo) == "1"                                            # the session is unchanged
    rc, r = run_json(talo, "review", "double")
    assert r["trial"]["status"] == "ready"
    assert sorted(c["name"] for c in r["diff"]["changed"]) == ["items", "x"]
    assert "done" in talo("review", "double").stdout
    rc, out = run_json(talo, "adopt", "double")
    assert rc == 0 and x(talo) == "2"
    script = talo("export").stdout
    assert script.count("x = x * 2") == 1                            # the trial's job is in the lineage once
    rc, t = run_json(talo, "trials")
    assert t["trials"][0]["status"] == "adopted"


def test_adopt_refuses_when_the_session_moved_on(talo):
    talo("run", "x = 1")
    talo("run", "--trial", "a", "x = 10")
    talo("run", "x = 5")                                             # work done in the session meanwhile
    rc, out = run_json(talo, "adopt", "a")
    assert rc == 2 and "discard" in out["error"]["message"] and x(talo) == "5"
    rc, out = run_json(talo, "adopt", "a", "--discard")
    assert rc == 0 and x(talo) == "10"


def test_failed_trial_keeps_nothing_and_reject(talo):
    talo("run", "x = 1")
    rc, out = run_json(talo, "run", "--trial", "bad", "x = 2\nraise ValueError('no')")
    assert out["job"]["status"] == "error"
    assert run_json(talo, "trials")[1]["trials"][0]["status"] == "failed"
    assert run_json(talo, "snaps")[1]["snapshots"] == []
    talo("run", "--trial", "maybe", "x = 3")
    rc, out = run_json(talo, "reject", "maybe")
    assert rc == 0 and run_json(talo, "snaps")[1]["snapshots"] == [] and x(talo) == "1"


def test_session_stays_usable_while_a_trial_runs(talo):
    talo("run", "import time\nx = 1")
    rc, out = run_json(talo, "run", "--trial", "slow", "time.sleep(3)\nx = 99", "-t", "0")
    t0 = time.time()
    rc, d = run_json(talo, "run", "y = 2")
    assert rc == 0 and time.time() - t0 < 2.5                        # not queued behind the trial
    rc, d = run_json(talo, "wait", out["job"]["id"], "-t", "30")
    assert d["job"]["status"] == "ok"
