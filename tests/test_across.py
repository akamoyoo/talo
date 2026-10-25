"""M5: work across sessions."""

import json
import time


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def test_map_forks_a_snapshot_per_case(talo):
    talo("run", "import numpy as np\ndata = np.arange(1000.0)")
    talo("snap", "loaded")
    code = "data = data * case\n{'case': case, 'sum': float(data.sum()), 'inv': 1 / case, 'idx': talo.case['index']}"
    rc, d = run_json(talo, "map", "--from", "loaded", "--cases", "[1, 2, 0]", code, "-j", "2")
    m = d["map"]
    assert rc == 1 and m["status"] == "ok" and m["counts"] == {"ok": 2, "error": 1}
    rows = {r["index"]: r for r in m["rows"]}
    assert rows[1]["result"] == {"case": 2, "sum": 999000.0, "inv": 0.5, "idx": 1}
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
    assert row["status"] == "interrupted" and row["termination"] == "memory_limit" and row["peak_pss"] > 200 << 20


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
