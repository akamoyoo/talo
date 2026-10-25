"""M1: resident sessions driven from the CLI."""

import json
import os
import signal
import time

from talo.client import call


def run_json(talo, *args):
    out = talo(*args, "--json")
    return out.returncode, json.loads(out.stdout)


def test_state_persists_and_last_expression_is_the_result(talo):
    code, first = run_json(talo, "run", "x = 41")
    assert code == 0 and first["job"]["status"] == "ok" and first["job"]["result"] is None
    code, second = run_json(talo, "run", "x + 1")
    assert code == 0 and second["job"]["result"]["data"]["text/plain"] == "42"
    assert second["job"]["execution_count"] == 2


def test_output_is_captured_at_the_fd_level(talo):
    code, out = run_json(talo, "run", 'import os, sys\nprint("py")\nos.write(1, b"fd1\\n")\nos.write(2, b"fd2\\n")')
    assert code == 0
    assert out["output"]["text"].splitlines() == ["py", "fd1", "fd2"]


def test_errors_report_traceback_with_source_line(talo):
    code, out = run_json(talo, "run", "def f():\n    return 1 / 0\nf()")
    assert code == 1 and out["job"]["status"] == "error"
    assert out["job"]["error"]["ename"] == "ZeroDivisionError"
    assert any("return 1 / 0" in line for line in out["job"]["error"]["traceback"])
    assert not any("worker.py" in line for line in out["job"]["error"]["traceback"])


def test_timeout_returns_a_job_that_keeps_running(talo):
    code, out = run_json(talo, "run", 'import time\nfor i in range(3):\n    print(i, flush=True); time.sleep(0.5)\n"done"',
                         "-t", "0.2")
    assert code == 3 and out["job"]["status"] == "running"
    job = out["job"]["id"]
    code, done = run_json(talo, "wait", job, "-t", "30")
    assert code == 0 and done["job"]["result"]["data"]["text/plain"] == "'done'"
    assert done["output"]["text"].split() == ["0", "1", "2"]


def test_interrupt_a_running_job(talo):
    _, out = run_json(talo, "run", "import time\ntime.sleep(60)", "-t", "0.5")
    job = out["job"]["id"]
    assert talo("interrupt", job).returncode == 0
    code, done = run_json(talo, "wait", job, "-t", "30")
    assert code == 1 and done["job"]["status"] == "interrupted"
    code, after = run_json(talo, "run", "1 + 1")  # the session survives the interrupt
    assert code == 0 and after["job"]["result"]["data"]["text/plain"] == "2"


def test_jobs_queue_in_order(talo):
    _, a = run_json(talo, "run", "import time\ntime.sleep(1)\nseq = [1]", "-t", "0")
    _, b = run_json(talo, "run", "seq.append(2)\nseq", "-t", "0")
    assert b["job"]["status"] == "queued"
    code, done = run_json(talo, "wait", b["job"]["id"], "-t", "30")
    assert code == 0 and done["job"]["result"]["data"]["text/plain"] == "[1, 2]"


def test_vars_and_show(talo):
    talo("run", "import numpy as np, pandas as pd\n"
                "df = pd.DataFrame({'a': np.arange(5), 's': list('abcde')})\narr = np.zeros((3, 4))\nn = 7")
    _, rows = run_json(talo, "vars")
    by_name = {v["name"]: v for v in rows["variables"]}
    assert set(by_name) == {"df", "arr", "n"}  # modules are not listed
    assert by_name["df"]["shape"] == [5, 2] and by_name["df"]["columns"] == ["a", "s"]
    assert by_name["arr"]["dtype"] == "float64" and by_name["arr"]["nbytes"] == 96
    assert by_name["n"]["value"] == "7"
    _, shown = run_json(talo, "show", "df", "-r", "2")
    assert "dtypes: a=int64" in shown["text"] and len(shown["text"].splitlines()) == 5  # header, dtypes, 3 table lines
    _, missing = run_json(talo, "show", "nope")
    assert missing["status"] == "error" and missing["ename"] == "NameError"


def test_vars_is_refused_while_busy(talo):
    _, out = run_json(talo, "run", "import time\ntime.sleep(2)", "-t", "0.2")
    out2 = talo("vars", "--json")
    assert out2.returncode == 2 and "busy" in json.loads(out2.stdout)["error"]["message"]
    talo("wait", out["job"]["id"])


def test_worker_death_is_reported(talo):
    _, s = run_json(talo, "run", "import os\nos.getpid()")
    pid = int(s["job"]["result"]["data"]["text/plain"])
    _, out = run_json(talo, "run", "import time\ntime.sleep(60)", "-t", "0.5")
    os.kill(pid, signal.SIGKILL)
    code, done = run_json(talo, "wait", out["job"]["id"], "-t", "30")
    assert code == 1 and done["job"]["status"] == "lost"
    assert "SIGKILL" in done["job"]["error"]["evalue"]
    _, ls = run_json(talo, "ls")
    assert ls["sessions"][0]["state"] == "dead"


def test_sessions_are_independent_and_can_use_another_interpreter(talo, tmp_path):
    import sys
    assert talo("new", "other", "--python", sys.executable, "--cwd", str(tmp_path)).returncode == 0
    talo("run", "x = 'default'")
    talo("run", "-s", "other", "x = 'other'")
    _, a = run_json(talo, "run", "x")
    _, b = run_json(talo, "run", "-s", "other", "import os\n(x, os.getcwd())")
    assert a["job"]["result"]["data"]["text/plain"] == "'default'"
    assert b["job"]["result"]["data"]["text/plain"] == repr(("other", str(tmp_path)))
    assert talo("kill", "other").returncode == 0
    _, ls = run_json(talo, "ls")
    assert [s["name"] for s in ls["sessions"]] == ["default"]


def test_history_records_finished_jobs(talo):
    talo("run", "a = 1")
    talo("run", "1/0")
    _, h = run_json(talo, "history")
    assert [(x["code"], x["status"]) for x in h["history"]] == [("a = 1", "ok"), ("1/0", "error")]
    assert all(x["metrics"]["talo_rss_after"] > 0 for x in h["history"])


def test_user_sys_path_is_not_polluted(talo):
    _, out = run_json(talo, "run", "import sys\nsys.path[0], any(p.endswith('/talo') for p in sys.path)")
    assert out["job"]["result"]["data"]["text/plain"] == "('', False)"


def test_supervisor_survives_client_disconnects(talo_home, talo):
    talo("run", "keep = 123")
    pid = call("ping")["pid"]
    time.sleep(0.2)
    assert call("ping")["pid"] == pid
    _, out = run_json(talo, "run", "keep")
    assert out["job"]["result"]["data"]["text/plain"] == "123"


def test_long_talo_home_uses_a_short_socket_path(tmp_path, monkeypatch):
    from talo.paths import SOCKET_PATH_LIMIT, socket_path
    deep = tmp_path / ("d" * 60) / ("e" * 60)
    monkeypatch.setenv("TALO_HOME", str(deep))
    path = socket_path()
    assert len(str(path)) <= SOCKET_PATH_LIMIT and path.name.startswith("talo-")
    monkeypatch.setenv("TALO_HOME", str(tmp_path / "short"))
    assert socket_path() == tmp_path / "short" / "supervisor.sock"
