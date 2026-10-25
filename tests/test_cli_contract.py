"""The CLI contract agents rely on: bounded waits, --json everywhere, structured errors."""

import json


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def test_queued_snapshot_returns_the_job_when_it_does_not_finish_in_time(talo):
    talo("run", "import time", "-t", "0")
    talo("run", "time.sleep(3)", "-t", "0")
    rc, out = run_json(talo, "snap", "later", "-t", "0.2")
    assert rc == 3 and out["job"]["kind"] == "snap" and out["job"]["status"] == "queued"
    rc, out = run_json(talo, "wait", out["job"]["id"], "-t", "30")
    assert rc == 0 and out["job"]["result"]["name"] == "later"


def test_named_sessions_are_not_created_implicitly(talo):
    rc, out = run_json(talo, "run", "-s", "reseach", "x = 1")
    assert rc == 2 and out["error"]["type"] == "NoSession" and "talo new reseach" in out["error"]["message"]


def test_jobs_limit_and_preview(talo):
    for i in range(5):
        talo("run", f"x = {i}")
    rc, out = run_json(talo, "jobs", "--last", "2")
    assert rc == 0 and out["total"] == 5 and [j["code_preview"] for j in out["jobs"]] == ["x = 3", "x = 4"]


def test_json_for_export_stop_and_bad_arguments(talo):
    talo("run", "x = 1")
    rc, out = run_json(talo, "export")
    assert rc == 0 and out["format"] == "py" and "x = 1" in out["text"]
    rc, out = run_json(talo, "new", "b", "--memory-budget", "lots")
    assert rc == 2 and out["error"]["type"] == "BadArgument"
    rc, out = run_json(talo, "tail", "1", "-f")
    assert rc == 2 and out["error"]["type"] == "BadArgument"
    rc, out = run_json(talo, "stop")
    assert rc == 0 and out["stopped"] is True


def test_out_keeps_only_the_last_results(talo):
    for i in range(6):
        talo("run", f"[{i}]")
    rc, out = run_json(talo, "run", "sorted(Out), _")
    assert out["job"]["result"]["data"]["text/plain"] == "([4, 5, 6], [5])"
