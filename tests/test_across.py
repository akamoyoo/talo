"""M5: work across sessions."""

import json
import time


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def test_map_forks_a_snapshot_per_case(talo):
    talo("run", "import numpy as np\ndata = np.arange(1000.0)")
    talo("snap", "loaded")
    code = ("data = data * case\n{'case': case, 'sum': float(data.sum()), 'inv': 1 / case, 'idx': talo.case['index'],"
            " 'seed': talo.case['seed'] > 0}")
    rc, d = run_json(talo, "map", "--from", "loaded", "--cases", "[1, 2, 0]", code, "-j", "2")
    m = d["map"]
    assert rc == 1 and m["status"] == "finished" and m["counts"] == {"ok": 2, "error": 1}
    rows = {r["index"]: r for r in m["rows"]}
    assert rows[1]["result"] == {"case": 2, "sum": 999000.0, "inv": 0.5, "idx": 1, "seed": True}
    assert rows[2]["error"]["ename"] == "ZeroDivisionError"
    assert run_json(talo, "run", "float(data.sum())")[1]["job"]["result"]["data"]["text/plain"] == "499500.0"
    lines = open(m["results"]).read().splitlines()
    assert len(lines) == 3


def test_map_from_the_current_state_leaves_the_session_alone(talo):
    talo("run", "x = 10")
    rev = run_json(talo, "brief")[1]["session"]["rev"]
    rc, d = run_json(talo, "map", "--range", "4", "x + case")
    assert rc == 0 and [r["result"] for r in d["map"]["rows"]] == [10, 11, 12, 13]
    b = run_json(talo, "brief")[1]
    assert b["session"]["rev"] == rev and b["snapshots"]["total"] == 0  # the internal snapshot is gone
    rc, d = run_json(talo, "run", "--expect-rev", str(rev), "x")
    assert rc == 0


def test_map_runs_in_the_background_and_can_be_interrupted(talo):
    talo("run", "import time")
    rc, d = run_json(talo, "map", "--range", "6", "time.sleep(20)", "-j", "2", "-t", "1")
    mid = d["map"]["id"]
    assert rc == 3 and d["map"]["status"] == "running" and len(d["map"]["running"]) == 2
    talo("interrupt", mid)
    rc, d = run_json(talo, "wait", mid, "-t", "30")
    m = d["map"]
    assert m["status"] == "interrupted" and m["counts"].get("interrupted") == 2 and m["counts"].get("cancelled") == 4


def test_map_case_memory_limit(talo):
    talo("run", "import numpy as np, time")
    talo("snap", "base")
    grow = "blocks = []\nfor i in range(40):\n    blocks.append(np.ones(5_000_000))\n    time.sleep(0.1)\n'done'"
    rc, d = run_json(talo, "map", "--from", "base", "--range", "1", grow, "--case-memory-limit", "200M", "-t", "60")
    row = d["map"]["rows"][0]
    assert row["status"] == "interrupted" and row["termination"] == "memory_limit" and row["sampled_peak_pss"] > 200 << 20


def test_a_snapshot_used_by_a_running_map_cannot_be_dropped(talo):
    talo("run", "import time")
    talo("snap", "base")
    rc, d = run_json(talo, "map", "--from", "base", "--range", "1", "time.sleep(10)", "-t", "0.5")
    rc, out = run_json(talo, "drop", "base")
    assert rc == 2 and d["map"]["id"] in out["error"]["message"]
    talo("interrupt", d["map"]["id"])


def test_map_key_returns_the_same_map(talo):
    talo("run", "x = 1")
    rc, a = run_json(talo, "map", "--range", "2", "x + case", "--key", "k")
    rc, b = run_json(talo, "map", "--range", "2", "x + case", "--key", "k")
    assert a["map"]["id"] == b["map"]["id"] and b["map"]["deduplicated"]


def test_events_cursor_wait_and_filters(talo):
    talo("run", "x = 1")
    rc, d = run_json(talo, "events", "--all", "--type", "job.finished")
    assert rc == 0 and d["events"][-1]["type"] == "job.finished" and d["events"][-1]["status"] == "ok"
    cursor = run_json(talo, "events")[1]["next"]
    rc, d = run_json(talo, "events", "--after", cursor, "--wait", "0.2")
    assert d["events"] == [] and d["next"] == cursor
    talo("run", "import time\ntime.sleep(1)", "-t", "0")
    t0 = time.time()
    rc, d = run_json(talo, "events", "--after", cursor, "--wait", "20", "--type", "job.finished")
    assert d["events"] and d["events"][0]["status"] == "ok" and time.time() - t0 < 10
    rc, d = run_json(talo, "events", "--after", "0123:5")
    assert rc == 2 and d["error"]["type"] == "CursorExpired"
    talo("snap", "s")
    talo("note", "hello")
    kinds = [e["type"] for e in run_json(talo, "events", "--after", cursor, "--limit", "50")[1]["events"]]
    assert "snapshot.created" in kinds and "note.added" in kinds


def test_ledger_records_provenance_and_outcomes(talo):
    talo("run", "x = 1")
    talo("snap", "base")
    talo("run", "talo.record(ic=0.012, factor='gap')")
    talo("run", "--atomic", "talo.record(ic=0.5, factor='bad')\nraise ValueError('oops')")
    talo("map", "--from", "base", "--range", "3", "talo.record(ic=case / 100, factor='m')\n1 / (case - 2)")
    talo("record", "note=manual", "score=3")
    rc, d = run_json(talo, "ledger", "--has", "ic", "--sort", "ic", "--desc")
    recs = d["records"]
    assert rc == 0 and [r["fields"]["ic"] for r in recs] == [0.5, 0.02, 0.012, 0.01, 0.0]
    by_factor = {}
    for r in recs:
        by_factor.setdefault(r["fields"]["factor"], []).append(r)
    assert by_factor["bad"][0]["outcome"] == "rolled_back"
    assert by_factor["gap"][0]["outcome"] == "succeeded" and by_factor["gap"][0]["producer"]["kind"] == "job"
    cases = {r["producer"]["index"]: r for r in by_factor["m"]}
    assert cases[2]["outcome"] == "failed" and cases[0]["outcome"] == "succeeded"
    assert cases[0]["source_snapshot"] == "default:s1" and cases[0]["source_state"] == "kept"
    rc, d = run_json(talo, "ledger", "--where", "note=manual")
    assert d["records"][0]["fields"]["score"] == 3 and d["records"][0]["outcome"] == "reported"


def test_services_versions_calls_and_pins(talo):
    talo("new", "host")
    code = ("import time\ndata = [1, 2, 3]\ndef total(scale=1, nap=0):\n    time.sleep(nap)\n    data.append(99)  # only the fork\n"
            "    return sum(data) * scale\ntalo.expose(total)")
    rc, out = run_json(talo, "run", "-s", "host", code)
    assert out["job"]["published"]["versions"][0] == {"service": "host.total", "version": 1, "snapshot": "host:v1"}
    rc, c = run_json(talo, "call", "host.total", '{"scale": 2}')
    assert rc == 0 and c["call"]["result"] == 210 and c["call"]["version"] == 1
    talo("run", "-s", "host", "data[:] = [10]")
    assert run_json(talo, "call", "host.total")[1]["call"]["result"] == 105      # v1 never sees later changes
    talo("expose", "-s", "host", "total")
    assert run_json(talo, "call", "host.total")[1]["call"]["result"] == 109      # v2
    assert run_json(talo, "call", "host.total", "--version", "1")[1]["call"]["result"] == 105
    assert run_json(talo, "run", "-s", "host", "data")[1]["job"]["result"]["data"]["text/plain"] == "[10]"
    # a failing job publishes nothing
    rc, out = run_json(talo, "run", "-s", "host", "talo.expose(total, 'never')\nraise ValueError('no')")
    assert out["job"]["published"] is None and "host.never" not in {s["name"] for s in run_json(talo, "services")[1]["services"]}
    # another session calls from Python; nested calls from a map case are refused
    talo("new", "other")
    rc, out = run_json(talo, "run", "-s", "other", "talo.call('host.total', scale=3)")
    assert out["job"]["result"]["data"]["text/plain"] == "327"
    rc, d = run_json(talo, "map", "-s", "other", "--range", "1", "talo.call('host.total')")
    assert "nested" in d["map"]["rows"][0]["error"]["evalue"]
    # a version with a call in flight cannot be removed; its snapshot is pinned
    rc, c = run_json(talo, "call", "host.total", '{"nap": 3}', "-t", "0.5")
    rc, out = run_json(talo, "unexpose", "host.total", "--version", "2")
    assert rc == 2 and c["call"]["id"] in out["error"]["message"]
    rc, out = run_json(talo, "wait", c["call"]["id"], "-t", "30")
    assert out["call"]["status"] == "ok"
    rc, out = run_json(talo, "unexpose", "host.total")
    assert rc == 0 and run_json(talo, "services")[1]["services"] == []


def test_handoff_and_resume(talo):
    talo("run", "x = 41")
    talo("note", "goal: reach 42")
    talo("run", "talo.record(progress=0.5)")
    rc, d = run_json(talo, "handoff", "-m", "x is almost there; add one", "--to", "agent-b")
    h = d["handoff"]
    assert rc == 0 and h["snapshot_available"] and h["notes"]["count"] == 1 and h["records"]["count"] == 1
    rc, out = run_json(talo, "drop", h["snapshot_name"])
    assert rc == 2 and "handoff" in out["error"]["message"]
    talo("run", "x = 0")                                   # the live session moves on
    rc, r = run_json(talo, "resume", h["id"], "--branch", "next")
    assert rc == 0 and r["session"]["name"] == "next" and r["handoff"]["status"].startswith("branched")
    assert run_json(talo, "run", "-s", "next", "x + 1")[1]["job"]["result"]["data"]["text/plain"] == "42"
    rc, d = run_json(talo, "handoff", "-m", "second")
    assert d["handoff"]["notes"]["count"] == 0             # only what happened since the previous handoff
    assert run_json(talo, "drop", h["snapshot_name"])[0] == 0   # no longer pinned
