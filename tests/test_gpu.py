"""GPU sessions: once the worker has initialised CUDA, its state is never forked (see "GPU sessions" in
docs/design.md). The tests need no GPU: the worker runs as ``__main__``, and replacing its ``cuda_mappings`` makes it
see what the CUDA driver leaves in /proc/self/maps after cuInit."""

import json

from talo.client import call

CUDA = "import sys; sys.modules['__main__'].cuda_mappings = lambda: ['/dev/nvidia0', '/dev/nvidiactl']"


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def session(name="default"):
    return next(s for s in call("ls")["sessions"] if s["name"] == name)


def test_initialising_cuda_is_reported_once(talo):
    rc, out = run_json(talo, "run", "x = 1")
    assert rc == 0 and out["job"]["metrics"]["talo_cuda"] == [] and session()["cuda"] == []
    out = talo("run", CUDA)
    assert out.returncode == 0 and "this job initialised CUDA" in out.stderr
    rc, out = run_json(talo, "run", "y = 2")
    assert rc == 0 and out["job"]["metrics"]["talo_cuda"] == ["/dev/nvidia0", "/dev/nvidiactl"]
    assert "talo_cuda_new" not in out["job"]["metrics"]
    assert session()["cuda"] == ["/dev/nvidia0", "/dev/nvidiactl"]
    assert "CUDA in use" in talo("ls").stdout and "CUDA in use" in talo("brief").stdout


def test_every_fork_of_a_cuda_worker_is_refused(talo):
    talo("run", "x = 1")
    assert talo("snap", "before").returncode == 0
    talo("run", CUDA)

    out = talo("snap", "after")
    assert out.returncode != 0 and "initialised CUDA" in out.stdout + out.stderr
    rc, out = run_json(talo, "run", "--atomic", "z = 3")
    assert rc != 0 and out["job"]["atomic"]["outcome"] == "not_started" and "initialised CUDA" in out["job"]["atomic"]["error"]
    rc, out = run_json(talo, "run", "--trial", "t", "w = 4")
    assert rc != 0 and "initialised CUDA" in json.dumps(out)
    out = talo("map", "--range", "2", "case")
    assert out.returncode != 0 and "initialised CUDA" in out.stdout + out.stderr
    out = talo("save")
    assert out.returncode != 0 and "initialised CUDA" in out.stdout + out.stderr
    assert [s["name"] for s in call("snaps", session="default")["snapshots"]] == ["before"]

    # the code itself still runs, and the variables are still there
    rc, out = run_json(talo, "run", "x + 1")
    assert rc == 0 and out["job"]["result"]["data"]["text/plain"] == "2"
    assert "z" not in [v["name"] for v in call("vars", session="default")["variables"]]


def test_show_runs_in_the_worker_when_cuda_is_in_use(talo):
    talo("run", "x = [1, 2, 3]")
    talo("run", CUDA)
    rc, out = run_json(talo, "show", "x")
    assert rc == 0 and out["in_process"] is True and "[1, 2, 3]" in out["text"]
    assert "evaluated in the worker itself" in talo("show", "x").stderr


def test_autosnap_and_services_fail_without_failing_the_job(talo):
    talo("run", "x = 1")
    assert talo("autosnap", "on").returncode == 0
    rc, out = run_json(talo, "run", CUDA)
    assert rc == 0 and out["job"]["status"] == "ok" and "CudaInUse" in out["job"]["autosnap"]["error"]
    out = talo("run", "def f():\n    return 1\ntalo.expose(f)")
    assert out.returncode == 0 and "not published: CudaInUse" in out.stderr and "no autosnap: CudaInUse" in out.stderr
    assert call("services")["services"] == []


def test_snapshots_from_before_cuda_still_work(talo):
    talo("run", "x = 1")
    talo("snap", "before")
    talo("run", CUDA + "\nx = 2")
    assert talo("checkout", "before", "--discard").returncode == 0
    assert session()["cuda"] == []
    rc, out = run_json(talo, "run", "x")
    assert rc == 0 and out["job"]["result"]["data"]["text/plain"] == "1"
    assert talo("snap", "again").returncode == 0


def test_an_atomic_job_that_initialises_cuda_and_fails_is_restored(talo):
    talo("run", "x = 1")
    rc, out = run_json(talo, "run", "--atomic", CUDA + "\nx = 2\n1 / 0")
    assert rc != 0 and out["job"]["atomic"]["outcome"] == "restored"
    assert session()["cuda"] == []
    assert talo("snap", "fine").returncode == 0
