"""M9: checkpoints, recover, autosave."""

import json
import os
import subprocess
import sys
import time

from talo.client import call


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def show(talo, expr, session="default"):
    return run_json(talo, "show", "-s", session, expr)[1]["text"]


SETUP = ("import numpy as np, pandas as pd\nbase = np.arange(1_000_000.0)\nview = base[10:20]\n"
         "df = pd.DataFrame({'a': np.arange(5), 's': list('abcde')})\nalias = df\nscale = 3\n"
         "def f(x):\n    return x * scale\ngen = (i for i in range(3))")


def test_save_and_restore_keep_values_aliasing_and_functions(talo):
    talo("run", SETUP)
    rc, out = run_json(talo, "save", "--name", "one")
    job = out["job"]
    assert rc == 0 and job["status"] == "ok" and job["result"]["checkpoint"] == "default/one"
    assert [s["name"] for s in job["result"]["skipped"]] == ["gen"]
    rc, out = run_json(talo, "save", "--name", "two")                     # nothing changed: nothing new written
    assert out["job"]["result"]["new_bytes"] == 0
    rc, out = run_json(talo, "restore", "default/one", "--as", "copy")
    assert rc == 0 and out["status"] == "ok"
    rc, r = run_json(talo, "run", "-s", "copy", "view[0] = -1\n(base[10], alias is df, f(2), base.flags.writeable)")
    assert r["job"]["result"]["data"]["text/plain"] == "(np.float64(-1.0), True, 6, True)"
    rc, c = run_json(talo, "checkpoints")
    assert {x["name"] for x in c["checkpoints"]} == {"one", "two"}
    assert "restored from checkpoint default/one" in talo("export", "-s", "copy").stdout


def test_a_corrupt_unit_is_refused(talo, talo_home):
    talo("run", "import numpy as np\nx = np.ones(100)")
    talo("save", "--name", "c")
    unit = next((talo_home / "store").iterdir())
    data = bytearray((unit / "buffers").read_bytes())
    data[0] ^= 0xFF
    (unit / "buffers").write_bytes(bytes(data))
    rc, out = run_json(talo, "restore", "default/c", "--as", "bad")
    assert rc == 1 and "corrupt" in out["error"]["evalue"]


def test_recover_after_the_supervisor_is_killed(talo, talo_home):
    talo("run", "import numpy as np\npanel = np.arange(500_000.0)\ntotal = float(panel.sum())")
    talo("save", "--name", "before")
    talo("run", "unsaved = 1")
    pid = call("ping")["pid"]
    for s in call("ls")["sessions"]:
        os.kill(s["pid"], 9)
    os.kill(pid, 9)                                                         # the supervisor dies
    time.sleep(0.5)
    rc, d = run_json(talo, "ls")                                            # a new supervisor starts
    assert d["sessions"] == [] and d["lost"][0]["name"] == "default"
    rc, d = run_json(talo, "recover", "--all")
    assert rc == 0 and d["recovered"][0]["checkpoint"] == "default/before"
    assert show(talo, "total") == "124999750000.0"
    rc, out = run_json(talo, "show", "unsaved")
    assert out["status"] == "error"                                         # only what was saved comes back


def test_autosave_and_gc(talo, talo_home, monkeypatch):
    talo("run", "x = 1")
    talo("stop")
    monkeypatch.setenv("TALO_AUTOSAVE_CHECK", "0.5")
    talo("new", "a")
    talo("run", "-s", "a", "x = 1")
    talo("autosave", "-s", "a", "on", "--every", "1s", "--keep", "2")
    for i in range(4):
        talo("run", "-s", "a", f"x = {i + 10}")
        time.sleep(1.6)
    rc, c = run_json(talo, "checkpoints", "-s", "a")
    autos = [x for x in c["checkpoints"] if x["auto"]]
    assert 1 <= len(autos) <= 2
    rc, g = run_json(talo, "store", "gc", "--dry-run")
    assert rc == 0
