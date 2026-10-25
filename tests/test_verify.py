"""M8: verify."""

import json


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def test_hot_and_cold_match(talo):
    talo("run", "import numpy as np\ndata = np.arange(100.0)")
    talo("snap", "loaded")
    talo("run", "total = float(data.sum())\nitems = [1, 2]\nitems.append(3)")
    rc, d = run_json(talo, "verify")
    assert rc == 0 and d["status"] == "match" and d["source"]["name"] == "loaded" and len(d["replayed"]) == 1
    rc, d = run_json(talo, "verify", "--cold")
    assert rc == 0 and d["status"] == "match" and len(d["replayed"]) == 2


def test_mismatch_from_hidden_state_and_randomness(talo):
    talo("run", "x = 1")
    talo("snap", "s")
    talo("run", "import random\nnoise = random.random()")
    talo("checkout", "s", "--discard")
    talo("run", "y = 2")
    rc, d = run_json(talo, "verify")
    assert d["status"] == "match"                      # the rolled-back job is not part of the lineage
    talo("run", "import random\nnoise = random.random()")
    rc, d = run_json(talo, "verify", "--cold")
    assert rc == 1 and d["status"] == "mismatch" and [c["name"] for c in d["comparison"]["changed"]] == ["noise"]


def test_replay_failure_hazards_and_no_snapshot(talo):
    talo("run", "import os\nx = 1")
    rc, d = run_json(talo, "verify")
    assert d["status"] == "inconclusive"               # nothing kept to replay from
    talo("run", "y = undefined")                        # a failed job in the lineage: a hazard
    talo("run", "open('flag', 'w').write('1')")
    talo("run", "z = int(open('flag').read())")
    os_remove = "os.remove('flag')"
    talo("run", os_remove)
    rc, d = run_json(talo, "verify", "--cold")
    assert d["hazards"] and d["status"] in ("match", "replay_failed")
    rc, d = run_json(talo, "run", "z")                  # verification did not change the session
    assert d["job"]["result"]["data"]["text/plain"] == "1"
