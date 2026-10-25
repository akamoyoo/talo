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
    buf = next((talo_home / "store" / "buffers").iterdir())
    data = bytearray(buf.read_bytes())
    data[0] ^= 0xFF
    buf.write_bytes(bytes(data))
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


def test_restore_is_atomic_and_recover_uses_complete_checkpoints(talo, talo_home):
    talo("run", "import numpy as np\na = np.ones(10)\nb = np.zeros(10)")
    talo("save", "--name", "full")
    talo("save", "a", "--name", "only_a")                                   # a selection: not a recovery point
    rc, c = run_json(talo, "checkpoints")
    flags = {x["name"]: x for x in c["checkpoints"]}
    assert set(flags) == {"full", "only_a"}
    manifest = json.loads((talo_home / "checkpoints" / "default" / "full.json").read_text())
    assert manifest["complete"] and manifest["format"] == 2 and manifest["selection"] is None
    # corrupt the unit of b: restoring into a session that has a and b leaves it unchanged
    talo("run", "a = 'kept'\nb = 'kept'")
    unit_b = [u for u in manifest["units"] if "b" in u["names"]][0]["hash"]
    index = json.loads((talo_home / "store" / "units" / unit_b / "index.json").read_text())
    (talo_home / "store" / "buffers" / index["buffers"][0][0]).write_bytes(b"\0" * 79 + b"\1")
    rc, out = run_json(talo, "restore", "default/full", "-s", "default")
    assert rc == 1 and out["atomic"]["outcome"] == "restored"
    assert show(talo, "a") == "'kept'" and show(talo, "b") == "'kept'"
    assert json.loads(talo("ls", "--json").stdout)["lost"] == []


def test_a_malformed_manifest_is_refused(talo, talo_home):
    talo("run", "x = 1")
    talo("save", "--name", "m")
    path = talo_home / "checkpoints" / "default" / "m.json"
    m = json.loads(path.read_text())
    m["units"][0]["hash"] = "../../etc"
    path.write_text(json.dumps(m))
    rc, out = run_json(talo, "restore", "default/m", "--as", "z")
    assert rc == 2 and "malformed" in out["error"]["message"]


def test_only_changed_buffers_are_written_and_imports_come_back(talo):
    talo("run", "import numpy as np\nfrom collections import OrderedDict\nfrom types import SimpleNamespace\n"
                "box = SimpleNamespace(a=np.ones(1_000_000), b=np.zeros(1_000_000))\n"
                "def total():\n    return float(np.sum(box.a)) + len(OrderedDict(x=1))")
    rc, first = run_json(talo, "save", "--name", "one")
    assert first["job"]["result"]["new_bytes"] > 15_000_000
    talo("run", "box.b[0] = 5")                                   # one of the two arrays changes
    rc, second = run_json(talo, "save", "--name", "two")
    assert 7_000_000 < second["job"]["result"]["new_bytes"] < 9_000_000   # only b is written again
    rc, out = run_json(talo, "restore", "default/two", "--as", "r")
    assert rc == 0
    rc, out = run_json(talo, "run", "-s", "r", "total(), float(box.b[0]), np.__name__, OrderedDict.__name__")
    assert out["job"]["result"]["data"]["text/plain"] == "(1000001.0, 5.0, 'numpy', 'OrderedDict')"


def test_function_state_structured_views_and_damaged_buffers(talo, talo_home):
    talo("run", "import numpy as np\ndef f(acc=[]):\n    acc.append(1)\n    return len(acc)\nf(); f()\n"
                "rec = np.zeros(4, dtype=[('x', '<i4'), ('y', '<f8')])\nrec['y'] = [1, 2, 3, 4]\nhalf = rec[::2]")
    rc, out = run_json(talo, "save", "--name", "s1")
    assert out["job"]["status"] == "ok" and out["job"]["result"]["skipped"] == []
    rc, out = run_json(talo, "restore", "default/s1", "--as", "r")
    rc, out = run_json(talo, "run", "-s", "r", "f(), half.dtype.names, float(half['y'][1]), 'f' in globals()")
    assert out["job"]["result"]["data"]["text/plain"] == "(3, ('x', 'y'), 3.0, True)"
    # a damaged buffer in the store is repaired by the next save of the same content, not reused
    buf = max((talo_home / "store" / "buffers").iterdir(), key=lambda p: p.stat().st_size)
    buf.write_bytes(b"\0" * buf.stat().st_size)
    talo("save", "--name", "s2")
    rc, out = run_json(talo, "restore", "default/s2", "--as", "r2")
    assert rc == 0 and out["status"] == "ok"


def test_failed_imports_fail_the_restore_and_restore_returns_promptly(talo, talo_home):
    talo("run", "import json\nx = 1")
    talo("save", "--name", "imp")
    path = talo_home / "checkpoints" / "default" / "imp.json"
    m = json.loads(path.read_text())
    m["imports"]["json"] = {"module": "no_such_module_anywhere"}
    path.write_text(json.dumps(m))
    rc, out = run_json(talo, "restore", "default/imp", "-s", "default")
    assert rc == 1 and "no_such_module" in out["error"]["evalue"] and out["atomic"]["outcome"] == "restored"
    talo("run", "import time")
    talo("run", "time.sleep(3)", "-t", "0")             # the session is busy: the restore has to queue
    t0 = time.time()
    rc, out = run_json(talo, "restore", "default/imp", "--as", "fresh", "-t", "0")
    assert time.time() - t0 < 2.5


def test_recover_prefers_complete_checkpoints(talo, talo_home):
    talo("run", "x = 1")
    talo("save", "--name", "complete")
    talo("run", "gen = (i for i in range(2))")         # cannot be saved: later checkpoints are incomplete
    talo("save", "--name", "partial")
    pid = call("ping")["pid"]
    for s in call("ls")["sessions"]:
        os.kill(s["pid"], 9)
    os.kill(pid, 9)
    time.sleep(0.5)
    rc, d = run_json(talo, "ls")
    assert d["lost"][0]["last_checkpoint"] == "default/complete"
