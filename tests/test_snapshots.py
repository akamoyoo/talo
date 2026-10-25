"""M2: snapshots, checkout and branches."""

import json
import os
import signal
import time

from talo.client import call


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def result(talo, code, *args):
    rc, out = run_json(talo, "run", code, *args)
    assert rc == 0, out
    return out["job"]["result"]["data"]["text/plain"]


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split()[2] != "Z"
    except OSError:
        return False


def test_rollback_restores_values_dtypes_index_and_aliasing(talo):
    talo("run", "import numpy as np, pandas as pd\n"
                "df = pd.DataFrame({'x': np.arange(100_000, dtype=float), 's': pd.Series(np.arange(100_000) % 7).astype(str)},"
                " index=pd.date_range('2024-01-01', periods=100_000, freq='s'))\n"
                "alias = df\nref = df.copy(deep=True)")
    rc, snap = run_json(talo, "snap", "loaded")
    assert rc == 0 and snap["name"] == "loaded" and snap["jobs"] == ["1"]
    talo("run", "df.drop(columns=['s'], inplace=True)\ndf['x'] *= -1\ndf.index = df.index + pd.Timedelta('1D')\n"
                "del ref")
    rc, _ = run_json(talo, "checkout", "loaded", "--discard")
    assert rc == 0
    assert result(talo, "df.equals(ref) and df.index.equals(ref.index) and list(df.dtypes.astype(str)) == "
                        "list(ref.dtypes.astype(str)) and alias is df") == "True"


def test_checkout_refuses_to_lose_unsaved_work(talo):
    talo("run", "x = 1")
    talo("snap", "one")
    talo("run", "x = 2")
    rc, out = run_json(talo, "checkout", "one")
    assert rc == 2 and "--discard" in out["error"]["message"]
    rc, _ = run_json(talo, "checkout", "one", "--snap", "two")
    assert rc == 0 and result(talo, "x") == "1"
    rc, snaps = run_json(talo, "snaps")
    by_name = {s["name"]: s for s in snaps["snapshots"]}
    assert set(by_name) == {"one", "two"} and by_name["two"]["parent"] == by_name["one"]["ref"]
    assert snaps["base"] == by_name["one"]["ref"]
    rc, _ = run_json(talo, "checkout", "two", "--discard")
    assert rc == 0 and result(talo, "x") == "2"


def test_checkout_can_be_repeated_from_the_same_snapshot(talo):
    talo("run", "items = []")
    talo("snap", "empty")
    for i in range(3):
        talo("run", f"items.append({i})")
        assert run_json(talo, "checkout", "empty", "--discard")[0] == 0
        assert result(talo, "items") == "[]"


def test_branch_sessions_are_independent(talo):
    talo("run", "import numpy as np\na = np.arange(5)")
    talo("snap", "base")
    rc, branch = run_json(talo, "new", "idea", "--from", "base")
    assert rc == 0 and branch["base"] == "default:s1"
    assert result(talo, "a[0] = 99\nint(a.sum())", "-s", "idea") == "109"
    assert result(talo, "int(a.sum())") == "10"


def test_rollback_after_the_worker_was_killed(talo):
    talo("run", "data = list(range(1000))")
    talo("snap", "safe")
    _, ls = run_json(talo, "ls")
    os.kill(ls["sessions"][0]["pid"], signal.SIGKILL)  # e.g. the OOM killer
    for _ in range(100):
        if run_json(talo, "ls")[1]["sessions"][0]["state"] == "dead":
            break
        time.sleep(0.05)
    session = run_json(talo, "ls")[1]["sessions"][0]
    assert session["state"] == "dead" and "SIGKILL" in session["death"]
    assert run_json(talo, "checkout", "safe")[0] == 0  # nothing to lose: the worker is gone
    assert result(talo, "len(data)") == "1000"


def test_death_of_a_forked_worker_is_reported(talo):
    talo("run", "y = 1")
    talo("snap", "s")
    run_json(talo, "checkout", "s", "--discard")
    pid = run_json(talo, "ls")[1]["sessions"][0]["pid"]
    os.kill(pid, signal.SIGKILL)
    for _ in range(100):
        session = run_json(talo, "ls")[1]["sessions"][0]
        if session["state"] == "dead":
            break
        time.sleep(0.05)
    assert "SIGKILL" in session["death"]


def test_snapshot_memory_is_accounted(talo):
    talo("run", "import numpy as np\nbig = np.ones(20_000_000)")  # 160 MB
    talo("snap", "s")
    _, snaps = run_json(talo, "snaps")
    assert snaps["snapshots"][0]["private_bytes"] < 20 << 20  # nothing modified yet: shared with the worker
    talo("run", "big += 1")  # rewrite every page: the snapshot now holds the old copy alone
    _, snaps = run_json(talo, "snaps")
    assert snaps["snapshots"][0]["private_bytes"] > 150 << 20


def test_memory_budget_refuses_snapshots(talo):
    assert talo("new", "small", "--memory-budget", "1M").returncode == 0
    talo("run", "-s", "small", "z = 1")
    rc, out = run_json(talo, "snap", "-s", "small")
    assert rc == 2 and "budget" in out["error"]["message"]


def test_strict_refuses_with_other_threads(talo):
    talo("run", "import threading, time\nthreading.Thread(target=time.sleep, args=(30,), daemon=True).start()")
    rc, out = run_json(talo, "snap", "--strict")
    assert rc == 2 and "other threads" in out["error"]["message"]
    rc, snap = run_json(talo, "snap")
    assert rc == 0 and snap["threads"]


def test_drop_and_shutdown_leave_no_processes(talo):
    talo("run", "w = 1")
    _, a = run_json(talo, "snap", "a")
    _, b = run_json(talo, "snap", "b")
    assert run_json(talo, "drop", "a")[0] == 0
    for _ in range(100):
        if not alive(a["pid"]):
            break
        time.sleep(0.05)
    assert not alive(a["pid"]) and alive(b["pid"])
    worker = run_json(talo, "ls")[1]["sessions"][0]["pid"]
    call("shutdown", autostart=False)
    for _ in range(200):
        if not alive(b["pid"]) and not alive(worker):
            break
        time.sleep(0.05)
    assert not alive(b["pid"]) and not alive(worker)


def test_unnamed_snapshots_are_numbered(talo):
    talo("run", "q = 1")
    _, first = run_json(talo, "snap")
    _, second = run_json(talo, "snap")
    assert (first["name"], second["name"]) == ("s1", "s2")


def test_autosnap_and_undo(talo):
    def x():  # show does not run a job, so it takes no autosnap
        return run_json(talo, "show", "x")[1]["text"]

    talo("run", "x = 0")
    talo("autosnap", "on", "--keep", "3")
    for i in range(1, 6):
        talo("run", f"x = {i}")
    rc, snaps = run_json(talo, "snaps")
    assert [s["name"] for s in snaps["snapshots"]] == ["@4", "@5", "@6"] and all(s["auto"] for s in snaps["snapshots"])
    # the baseline @1 (taken when autosnap was turned on) was dropped with the oldest
    rc, out = run_json(talo, "undo")
    assert rc == 0 and x() == "4"
    talo("run", "x = 'oops'")        # @6 again on another branch: the abandoned @6 is replaced
    talo("run", "y = undefined")     # a failed execution: undo goes back to before it
    rc, out = run_json(talo, "undo")
    assert x() == "'oops'"
    talo("checkout", "@4", "--discard")
    assert x() == "3"


def test_autosnap_baseline_makes_the_first_undo_work(talo):
    talo("run", "x = 'before'")
    rc, d = run_json(talo, "autosnap", "on")
    assert d["baseline"] == "default:s1"
    talo("run", "x = 'after'")
    talo("undo")
    assert run_json(talo, "show", "x")[1]["text"] == "'before'"
