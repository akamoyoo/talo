"""M10: snapshots on disk."""

import json
import time


def run_json(talo, *args):
    out = talo(*args, "--json")
    assert out.stdout, out.stderr
    return out.returncode, json.loads(out.stdout)


def value(talo, expr, *extra):
    rc, out = run_json(talo, "run", *extra, expr)
    assert rc == 0, out
    return out["job"]["result"]["data"]["text/plain"]


def snaps(talo, *extra):
    return {s["name"]: s for s in run_json(talo, "snaps", *extra)[1]["snapshots"]}


def test_demote_and_check_out_from_disk(talo):
    talo("run", "import numpy as np\na = np.arange(1_000_000.0)\nb = a[10:20]\nd = {'k': [1, 2]}\n"
                "def f(x, k=3):\n    return x * k")
    talo("snap", "loaded")
    talo("run", "a[:] = -1\nextra = 1")
    talo("snap", "later")  # the worker's base is now `later`, so `loaded` can move to disk
    assert snaps(talo)["loaded"]["storage"] == "memory"
    rc, d = run_json(talo, "snapshot", "demote", "loaded")
    assert rc == 0 and d["job"]["status"] == "ok", d
    r = d["job"]["result"]
    assert r["storage"] == "disk" and r["variables"] >= 4 and r["bytes"] > 8_000_000
    s = snaps(talo)["loaded"]
    assert s["storage"] == "disk" and s["private_bytes"] is None and s["alive"]
    rc, diff = run_json(talo, "diff", "loaded", "later")
    assert {c["name"] for c in diff["changed"]} == {"a", "b"} and [v["name"] for v in diff["added"]] == ["extra"]
    rc, out = run_json(talo, "checkout", "loaded")
    assert rc == 0 and out["restore"] == "values", out
    assert value(talo, "float(a.sum()), b[:2].tolist(), d, f(2), 'extra' in globals()") == \
        "(499999500000.0, [10.0, 11.0], {'k': [1, 2]}, 6, False)"
    assert value(talo, "np.shares_memory(a, b)") == "True"  # a view saved with its base is a view again
    assert snaps(talo)["loaded"]["storage"] == "disk"  # it stays on disk; a new `snap` takes a hot one
    assert "In" in value(talo, "'In' if len(In) > 1 else 'empty'")


def test_demotion_fails_closed_and_respects_bases_and_pins(talo):
    talo("run", "g = (i for i in range(3))\nx = 1")
    talo("snap", "gen")
    talo("run", "x = 2")
    talo("snap", "top")
    rc, d = run_json(talo, "snapshot", "demote", "gen")
    assert rc == 1 and "can be saved" in d["job"]["error"]["evalue"]
    s = snaps(talo)["gen"]
    assert s["storage"] == "memory" and "g:" in s["demote_blocked"]
    rc, out = run_json(talo, "checkout", "gen", "--discard")  # still a hot snapshot: exact state
    assert rc == 0 and out.get("restore") is None and value(talo, "next(g)") == "0"
    out = talo("snapshot", "demote", "gen")  # the base of the live worker
    assert out.returncode == 2 and "base of the live worker" in out.stderr
    rc, d = run_json(talo, "snapshot", "demote", "top", "--dry-run")
    assert d["eligible"] and d["private_bytes"] is not None


def test_cold_snapshots_refuse_forks_and_own_their_manifest(talo):
    talo("run", "x = 41")
    talo("snap", "a")
    talo("run", "x = 42")
    talo("snap", "b")
    assert run_json(talo, "snapshot", "demote", "a")[0] == 0
    out = talo("map", "--from", "a", "--range", "2", "x + case")
    assert out.returncode == 2 and "on disk" in out.stderr
    rc, d = run_json(talo, "checkpoints", "--all")
    mine = [c for c in d["checkpoints"] if c["kind"] == "snapshot"]
    assert len(mine) == 1 and mine[0]["name"].startswith("snapshot:a@")
    assert not [c for c in run_json(talo, "checkpoints")[1]["checkpoints"] if c["kind"] != "checkpoint"]
    out = talo("checkpoint", "drop", f"default/{mine[0]['name']}")
    assert out.returncode == 2 and "talo drop" in out.stderr
    rc, out = run_json(talo, "store", "gc", "--dry-run")
    assert out["removed"] == 0  # the snapshot's units are kept
    talo("drop", "a")
    assert not [c for c in run_json(talo, "checkpoints", "--all")[1]["checkpoints"] if c["kind"] != "checkpoint"]


def test_branch_from_a_cold_snapshot(talo):
    talo("run", "import numpy as np\nv = np.ones(10)")
    talo("snap", "base")
    talo("run", "v = v * 2")
    talo("snap", "next")
    assert run_json(talo, "snapshot", "demote", "base")[0] == 0
    assert talo("new", "br", "--from", "default:base").returncode == 0
    assert value(talo, "float(v.sum())", "-s", "br") == "10.0"
    assert value(talo, "float(v.sum())") == "20.0"


def test_policy_keeps_the_session_under_its_budget(talo, monkeypatch):
    monkeypatch.setenv("TALO_POLICY_IDLE", "0")
    monkeypatch.setenv("TALO_POLICY_INTERVAL", "0.5")
    talo("new", "p", "--memory-budget", "600M", "--snapshot-policy", "demote")
    talo("run", "-s", "p", "import numpy as np\nx = np.zeros(25_000_000)")  # 200 MB
    for i in range(3):  # each snapshot ends up holding its own 200 MB once the worker rewrites x
        talo("snap", "-s", "p", f"k{i}")
        talo("run", "-s", "p", f"x = x + {i + 1}")
    deadline = time.time() + 60
    while time.time() < deadline:
        s = snaps(talo, "-s", "p")
        if sum(v["storage"] == "disk" for v in s.values()) >= 1 and \
                run_json(talo, "memory", "-s", "p")[1]["processes"]["pss_total"] <= 600 << 20:
            break
        time.sleep(1)
    s = snaps(talo, "-s", "p")
    assert sum(v["storage"] == "disk" for v in s.values()) >= 1, s
    events = run_json(talo, "events", "--all", "--type", "snapshot.demoted")[1]["events"]
    assert events and all(e["session"] == "p" for e in events)
    disk = next(n for n, v in s.items() if v["storage"] == "disk")
    out = talo("checkout", "-s", "p", disk, "--discard")  # restoring 200 MB next to the rest exceeds the budget
    assert out.returncode == 2 and "over its budget" in out.stderr
    talo("snapshot", "policy", "-s", "p", "--memory-budget", "2G")
    assert talo("checkout", "-s", "p", disk, "--discard").returncode == 0
    assert value(talo, "float(x[0])", "-s", "p") == {"k0": "0.0", "k1": "1.0", "k2": "3.0"}[disk]


def test_cold_checkout_stays_in_the_session_scope(talo):
    import shutil
    import pytest
    if shutil.which("systemd-run") is None or shutil.which("busctl") is None:
        pytest.skip("no systemd user instance")
    if talo("new", "lim", "--limit", "memory=2G").returncode != 0:
        pytest.skip("memory limits are not enforceable here")
    talo("run", "-s", "lim", "x = 1")
    talo("snap", "-s", "lim", "a")
    talo("run", "-s", "lim", "x = 2")
    talo("snap", "-s", "lim", "b")
    assert run_json(talo, "snapshot", "demote", "-s", "lim", "a")[0] == 0
    scope = open(f"/proc/{run_json(talo, 'ls')[1]['sessions'][0]['pid']}/cgroup").read()
    assert run_json(talo, "checkout", "-s", "lim", "a")[0] == 0
    pid = next(s for s in run_json(talo, "ls")[1]["sessions"] if s["name"] == "lim")["pid"]
    assert open(f"/proc/{pid}/cgroup").read() == scope
    assert value(talo, "x", "-s", "lim") == "1"
    rc, d = run_json(talo, "resources", "-s", "lim")
    assert not d["sessions"][0]["pss_unknown"] and d["sessions"][0]["on_disk"][0]["name"] == "a"


def test_share_clients_cannot_restore_from_disk(talo):
    import getpass
    import subprocess
    import sys
    talo("run", "x = 1")
    talo("snap", "a")
    talo("run", "x = 2")
    talo("snap", "b")
    assert run_json(talo, "snapshot", "demote", "a")[0] == 0
    path = run_json(talo, "share", "--user", getpass.getuser(), "--level", "run")[1]["connect"]

    def shared(*args):
        out = subprocess.run([sys.executable, "-m", "talo.cli", "--connect", path, *args, "--json"],
                             capture_output=True, text=True, timeout=60)
        return out.returncode, json.loads(out.stdout)
    rc, d = shared("checkout", "a", "--discard")
    assert rc == 2 and "share clients" in d["error"]["message"]
    rc, d = shared("snapshot", "demote", "b")
    assert rc == 2
    talo("checkout", "a")  # the owner may
    talo("run", "y = 1")
    talo("snap", "c")      # c's parent is a (on disk); undo from c goes to a
    rc, d = shared("undo")
    assert rc == 2 and "share clients" in d["error"]["message"]
    assert value(talo, "x") == "1"


def test_drop_waits_for_a_demotion_and_interrupt_keeps_it_hot(talo):
    talo("run", "import numpy as np\nbig = np.random.default_rng(0).random(30_000_000)")  # 240 MB to write
    talo("snap", "a")
    talo("run", "z = 1")
    talo("snap", "b")
    rc, d = run_json(talo, "snapshot", "demote", "a", "-t", "0")
    jid = d["job"]["id"]
    out = talo("drop", "a")
    assert out.returncode == 2 and "demote" in out.stderr  # pinned by its demotion
    talo("interrupt", jid)
    rc, d = run_json(talo, "wait", jid, "-t", "60")
    s = snaps(talo)["a"]
    if d["job"]["status"] == "error":  # interrupted before the commit: still in memory and usable
        assert s["storage"] == "memory" and d["job"]["error"]["ename"] == "DemoteFailed"
        assert value(talo, "1") == "1" and talo("checkout", "a", "--discard").returncode == 0
    else:                              # the commit came first
        assert s["storage"] == "disk"
    assert run_json(talo, "store", "gc", "--dry-run")[0] == 0


def test_a_queued_demotion_can_be_cancelled(talo):
    talo("run", "import numpy as np\nbig = np.random.default_rng(1).random(30_000_000)")
    talo("snap", "a")
    talo("run", "z = 1")
    talo("snap", "b")
    talo("run", "z = 2")
    talo("snap", "c")
    first = run_json(talo, "snapshot", "demote", "a", "-t", "0")[1]["job"]["id"]
    rc, d = run_json(talo, "snapshot", "demote", "b", "-t", "0")
    second = d["job"]["id"]
    talo("interrupt", second)
    run_json(talo, "wait", first, "-t", "120")
    rc, d = run_json(talo, "wait", second, "-t", "60")
    s = snaps(talo)
    if d["job"]["status"] == "cancelled":
        assert s["b"]["storage"] == "memory"
    assert run_json(talo, "store", "gc", "--dry-run")[0] == 0  # no store user left behind
