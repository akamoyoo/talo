"""M2: tree and diff."""

import json


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def test_tree_follows_branches_across_sessions(talo):
    talo("run", "x = 1")
    talo("snap", "loaded")
    talo("run", "x = 2")
    talo("snap", "clean")
    talo("new", "idea2", "--from", "loaded")
    talo("run", "-s", "idea2", "y = 1")
    rc, tree = run_json(talo, "tree")
    assert rc == 0
    snaps = {n["name"]: n for n in tree["nodes"] if n["kind"] == "snapshot"}
    workers = {n["session"]: n for n in tree["nodes"] if n["kind"] == "worker"}
    assert snaps["loaded"]["parent"] is None and snaps["clean"]["parent"] == snaps["loaded"]["ref"]
    assert workers["default"]["parent"] == snaps["clean"]["ref"] and workers["default"]["jobs_since_base"] == []
    assert workers["idea2"]["parent"] == snaps["loaded"]["ref"] and len(workers["idea2"]["jobs_since_base"]) == 1
    text = talo("tree").stdout
    assert "(empty start: default)" in text and "* idea2" in text and "└─" in text


def test_diff_reports_added_removed_changed_and_content(talo):
    talo("run", "import numpy as np, pandas as pd\n"
                "df = pd.DataFrame({'x': np.arange(10_000.0)})\nprices = np.arange(10_000.0)\ntmp = [1, 2]\n"
                "same = {'a': 1}\nbig = np.zeros(2_000_000)\nf = lambda: 1")
    talo("snap", "loaded")
    talo("run", "df = df[df.x > 10]\nprices[5] = -1\ndel tmp\nmask = prices > 0\nbig[0] = 1")
    rc, d = run_json(talo, "diff", "loaded", "--max-bytes", "1M")
    assert rc == 0 and d["a"] == "default:s1" and d["b"] == "default:@"
    assert [v["name"] for v in d["added"]] == ["mask"] and [v["name"] for v in d["removed"]] == ["tmp"]
    changed = {c["name"]: c["fields"] for c in d["changed"]}
    assert changed == {"df": ["shape"], "prices": ["content"]}
    assert d["unchanged"] == 1  # same
    reasons = {n["name"]: n["reason"] for n in d["not_compared"]}
    assert reasons["big"] == "size" and "pickled" in reasons["f"]
    # --content compares everything; snapshot against snapshot works the same way
    talo("snap", "after")
    rc, d = run_json(talo, "diff", "loaded", "after", "--content")
    assert rc == 0 and {c["name"] for c in d["changed"]} == {"df", "prices", "big"}


def test_diff_from_a_branch_finds_the_parent_snapshot(talo):
    talo("run", "x = 1")
    talo("snap", "base")
    talo("new", "b", "--from", "base")
    talo("run", "-s", "b", "x = 2")
    rc, d = run_json(talo, "diff", "-s", "b", "base")
    assert rc == 0 and d["b"] == "b:@" and [c["name"] for c in d["changed"]] == ["x"]
