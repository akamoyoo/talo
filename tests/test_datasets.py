"""M6: shared datasets."""

import json
import os


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def result(talo, code, *args):
    rc, out = run_json(talo, "run", *args, code)
    assert rc == 0, out
    return out["job"]["result"]["data"]["text/plain"]


def test_publish_get_versions_and_sharing(talo):
    talo("run", "import numpy as np, pandas as pd\n"
                "panel = pd.DataFrame({'a': np.arange(1_000_000.0), 'b': np.ones(1_000_000, dtype=np.int64)})")
    rc, out = run_json(talo, "publish", "panel")
    assert rc == 0 and out["job"]["result"]["version"] == 1 and out["job"]["result"]["new_bytes"] > 15_000_000
    talo("new", "reader")
    code = "import numpy as np\np = talo.get('panel')\n(float(p['a'].sum()), p['a'].to_numpy().flags.writeable)"
    assert result(talo, code, "-s", "reader") == "(499999500000.0, False)"
    rc, out = run_json(talo, "jobs", "-s", "reader", "--last", "1")
    assert out["jobs"][0]["datasets"] == [{"name": "panel", "version": 1, "action": "read", "copy": False}]
    # a new version with one changed column writes only that column; readers keep their version
    talo("run", "panel['b'] = np.full(1_000_000, 2, dtype=np.int64)")  # its own block (pandas groups by dtype)
    rc, out = run_json(talo, "publish", "panel")
    assert out["job"]["result"]["version"] == 2 and out["job"]["result"]["new_bytes"] < 9_000_000
    assert result(talo, "int(p['b'].sum())", "-s", "reader") == "1000000"
    assert result(talo, "int(talo.get('panel')['b'].sum())", "-s", "reader") == "2000000"
    assert result(talo, "int(talo.get('panel', version=1)['b'].sum())", "-s", "reader") == "1000000"
    # pandas copies on write, so editing a mapped table is fine and private; a mapped NumPy array is read-only
    assert result(talo, "q = talo.get('panel')\nq.loc[0, 'a'] = 7.0\nfloat(q['a'][0]), float(talo.get('panel')['a'][0])",
                  "-s", "reader") == "(7.0, 0.0)"
    talo("run", "import numpy as np\ntalo.publish('arr', np.arange(10.0))")
    rc, out = run_json(talo, "run", "-s", "reader", "a = talo.get('arr')\na[0] = 1")
    assert out["job"]["error"]["ename"] == "ValueError"
    assert result(talo, "c = talo.get('arr', copy=True)\nc[0] = 7\nfloat(c[0])", "-s", "reader") == "7.0"


def test_dropping_a_version_keeps_readers_working(talo):
    talo("run", "import numpy as np\nx = np.arange(100_000.0)\ntalo.publish('x', x)")
    talo("new", "r")
    talo("run", "-s", "r", "x = talo.get('x')")
    rc, out = run_json(talo, "dataset", "drop", "x")
    talo("dataset", "gc")
    assert result(talo, "float(x.sum())", "-s", "r") == "4999950000.0"     # still mapped
    rc, out = run_json(talo, "run", "-s", "r", "talo.get('x')")
    assert out["job"]["error"]["ename"] == "LookupError"


def test_map_cases_read_one_copy(talo):
    talo("run", "import numpy as np\nbig = np.ones(5_000_000)\ntalo.publish('big', big)\ndel big")
    talo("snap", "s")
    rc, d = run_json(talo, "map", "--from", "s", "--range", "4", "float(talo.get('big').sum()) + case")
    assert [r["result"] for r in d["map"]["rows"]] == [5e6, 5e6 + 1, 5e6 + 2, 5e6 + 3]
    assert all(r["datasets"][0]["version"] == 1 for r in d["map"]["rows"])
    rc, ds = run_json(talo, "datasets")
    assert ds["datasets"][0]["name"] == "big" and ds["stored_bytes"] >= 40_000_000


def test_publish_keys_byte_counts_and_gc_exclusion(talo):
    talo("run", "import numpy as np, pandas as pd\n"
                "t = pd.DataFrame({'x': np.arange(100_000.0), 's': [str(i) for i in range(100_000)]})")
    rc, out = run_json(talo, "publish", "t")
    r = out["job"]["result"]
    assert r["mapped_bytes"] >= 800_000 and r["private_bytes"] > 0
    assert result(talo, "talo.publish('k', np.ones(3), key='once'), talo.publish('k', np.ones(3), key='once')") == "(1, 1)"
    rc, d = run_json(talo, "datasets")
    assert {x["name"] for x in d["datasets"]} == {"t", "k"} and d["free_bytes"] > 0
    rc, out = run_json(talo, "dataset", "gc")
    assert rc == 0


def test_dataset_names_versions_never_reused(talo):
    talo("run", "import numpy as np\ntalo.publish('prices.v1-raw', np.ones(3))\ntalo.publish('prices.v1-raw', np.zeros(3))")
    talo("dataset", "drop", "prices.v1-raw", "--version", "2")
    assert result(talo, "talo.publish('prices.v1-raw', np.full(3, 7.0))") == "3"      # not 2 again
    assert result(talo, "float(talo.get('prices.v1-raw').sum())") == "21.0"
    assert result(talo, "float(talo.get('prices.v1-raw', version=1).sum())") == "3.0"


def test_what_is_shared_by_type(talo):
    """Arrow-backed strings and pyarrow tables are shared; object columns, polars frames and time zone-aware indexes
    are rebuilt by each reader, and publishing says so."""
    import pytest
    pytest.importorskip("pyarrow")
    pytest.importorskip("polars")
    talo("run", "import numpy as np, pandas as pd, pyarrow as pa, polars as pl\nn = 200_000\n"
                "strs = [str(i) for i in range(n)]\n"
                "arrowy = pd.DataFrame({'x': np.ones(n), 's': strs})\n"
                "objecty = pd.DataFrame({'x': np.ones(n), 's': pd.Series(strs, dtype=object)})\n"
                "tab = pa.table({'x': np.ones(n), 's': strs})\npolar = pl.DataFrame({'x': np.ones(n), 's': strs})\n"
                "tz = pd.Series(np.ones(n), index=pd.date_range('2024', periods=n, freq='s', tz='UTC'))")
    shares = {}
    for name in ("arrowy", "objecty", "tab", "polar", "tz"):
        rc, out = run_json(talo, "publish", name)
        r = out["job"]["result"]
        shares[name] = (r["private_bytes"] < 100_000, " ".join(r["hints"]))
    assert shares["arrowy"][0] and shares["tab"][0]
    assert not shares["objecty"][0] and "object columns s" in shares["objecty"][1]
    assert not shares["polar"][0] and "to_arrow" in shares["polar"][1]
    assert not shares["tz"][0] and "time zone" in shares["tz"][1]
    talo("new", "r")
    assert result(talo, "a = talo.get('arrowy')\nt = talo.get('tab')\n(a['s'].iloc[7], t.column('s')[9].as_py())",
                  "-s", "r") == "('7', '9')"
