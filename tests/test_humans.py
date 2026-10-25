"""M3: completion, rich outputs, export, attach."""

import json

import nbformat

from talo.client import call


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def test_complete_names_attributes_and_keys(talo):
    talo("run", "import collections\nvalue_one = 1\nvalue_two = 2\nd = {'alpha': 1, 'beta': 2}")
    names = call("complete", session="default", code="x = value_", cursor_pos=10)
    assert names["matches"] == ["value_one", "value_two"] and names["cursor_start"] == 4
    attrs = call("complete", session="default", code="collections.Ord")["matches"]
    assert attrs == ["collections.OrderedDict("]
    keys = call("complete", session="default", code="d['al")
    assert keys["matches"] == ["alpha"] and keys["cursor_start"] == 3


def test_images_and_display_are_files(talo):
    rc, out = run_json(talo, "run", "class Pic:\n    def _repr_png_(self):\n        return b'PNGDATA'\n"
                                    "display('shown', Pic())\nPic()")
    job = out["job"]
    assert rc == 0 and [o["mime"] for o in job["outputs"]] == ["image/png", "image/png"]
    assert all(open(o["path"], "rb").read() == b"PNGDATA" for o in job["outputs"])
    assert "shown" in out["output"]["text"]
    rc, out = run_json(talo, "run", "import matplotlib.pyplot as plt\nplt.plot([1, 2])\nplt.show()\n"
                                    "plt.figure()\nplt.plot([2, 1])\nNone")
    paths = [o["path"] for o in out["job"]["outputs"]]
    assert rc == 0 and len(paths) == 2 and all(open(p, "rb").read(4) == b"\x89PNG" for p in paths)
    rc, out = run_json(talo, "run", "plt.get_fignums()")
    assert out["job"]["result"]["data"]["text/plain"] == "[]"
    rc, out = run_json(talo, "vars")
    assert "display" not in {v["name"] for v in out["variables"]}


def test_export_follows_lineage(talo, tmp_path):
    talo("run", "x = 1\nprint('hello')")
    talo("snap", "a")
    talo("run", "x = 2")                    # rolled back below: not in the lineage
    talo("checkout", "a", "--discard")
    talo("run", "y = undefined_name")       # failed: kept as a comment
    talo("run", "z = x + 10\nz")
    script = talo("export").stdout
    assert "x = 1" in script and "x = 2" not in script and "# --- snapshot a" in script
    assert "# y = undefined_name" in script and "z = x + 10" in script
    assert "x = 2" in talo("export", "--all").stdout
    assert "z = x" not in talo("export", "--snap", "a").stdout
    path = tmp_path / "out.ipynb"
    rc, d = run_json(talo, "export", "-o", str(path))
    assert rc == 0 and d["format"] == "ipynb"
    nb = nbformat.read(path, 4)
    nbformat.validate(nb)
    code = [c for c in nb.cells if c.cell_type == "code"]
    assert code[0].outputs[0]["text"] == "hello\n" and code[-1].outputs[-1]["data"]["text/plain"] == "11"
    assert code[1].outputs[-1]["ename"] == "NameError"
    # the exported script reproduces the state
    ns = {}
    exec(compile(talo("export").stdout, "<export>", "exec"), ns)
    assert ns["z"] == 11


def test_attach_runs_blocks_and_magics_as_its_own_jobs(talo):
    talo("run", "x = 41")
    out = talo("attach", stdin="for i in range(2):\n    print('loop', i)\n\nx + 1\n%snap one\n1/0\n%bogus\n")
    assert out.returncode == 0, out.stderr
    text = out.stdout
    assert "loop 0\nloop 1" in text and "42" in text and "snapshot one" in text
    assert "ZeroDivisionError" in text and "unknown command %bogus" in text and "detached" in text
    assert "from another client" not in text
    rc, snaps = run_json(talo, "snaps")
    assert [s["name"] for s in snaps["snapshots"]] == ["one"]
