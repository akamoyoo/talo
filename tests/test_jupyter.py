"""M7: the Jupyter kernel bridge, driven by jupyter_client as a front end would."""

import json
import os
import subprocess
import sys

import pytest

pytest.importorskip("zmq")
from jupyter_client import BlockingKernelClient  # noqa: E402
from jupyter_client.connect import write_connection_file  # noqa: E402


@pytest.fixture
def kernel(talo_home):
    talo_home.mkdir(parents=True, exist_ok=True)
    started = []

    def start(session="nb"):
        cf, _ = write_connection_file(str(talo_home / f"kernel-{len(started)}.json"))
        proc = subprocess.Popen([sys.executable, "-m", "talo.cli", "kernel", "-s", session, "-f", cf])
        kc = BlockingKernelClient(connection_file=cf)
        kc.load_connection_file()
        kc.start_channels()
        kc.wait_for_ready(timeout=60)
        started.append((proc, kc))
        return kc
    yield start
    for proc, kc in started:
        kc.stop_channels()
        if proc.poll() is None:
            proc.kill()


def run(kc, code, timeout=60):
    msg_id = kc.execute(code)
    outs = []
    while True:
        m = kc.get_iopub_msg(timeout=timeout)
        if m["parent_header"].get("msg_id") != msg_id:
            continue
        if m["msg_type"] == "status" and m["content"]["execution_state"] == "idle":
            break
        outs.append((m["msg_type"], m["content"]))
    reply = kc.get_shell_msg(timeout=timeout)
    return reply["content"], outs


def text(outs, kind="stream"):
    return "".join(c.get("text", "") for t, c in outs if t == kind)


def test_execute_streams_results_errors_and_images(kernel):
    kc = kernel()
    reply, outs = run(kc, "x = 41\nfor i in range(2):\n    print('tick', i)\nx + 1")
    kinds = [t for t, _ in outs]
    assert reply["status"] == "ok" and reply["execution_count"] == 1
    assert kinds.index("execute_input") < kinds.index("execute_result")
    assert text(outs) == "tick 0\ntick 1\n"
    assert [c for t, c in outs if t == "execute_result"][0]["data"]["text/plain"] == "42"
    reply, outs = run(kc, "1/0")
    assert reply["status"] == "error" and reply["ename"] == "ZeroDivisionError"
    reply, outs = run(kc, "class P:\n    def _repr_png_(self):\n        return b'PNG'\ndisplay(P())")
    images = [c for t, c in outs if t == "display_data"]
    assert images and images[0]["data"]["image/png"] == "UE5H"   # base64 of b"PNG"


def test_displays_stay_in_order_with_printed_text(kernel):
    kc = kernel()
    code = ("class H:\n    def _repr_html_(self):\n        return '<b>hi</b>'\n    def __repr__(self):\n        return 'H'\n"
            "print('before')\ndisplay(H())\nprint('after')")
    reply, outs = run(kc, code)
    seq = [(t, c.get("text") or c.get("data", {}).get("text/html")) for t, c in outs if t in ("stream", "display_data")]
    assert seq == [("stream", "before\n"), ("display_data", "<b>hi</b>"), ("stream", "after\n")]


def test_magics_rollback_and_restart_keeps_the_session(kernel, talo):
    kc = kernel()
    run(kc, "x = 1")
    reply, outs = run(kc, "%snap a")
    assert reply["status"] == "ok" and "snapshot a" in text(outs)
    run(kc, "x = 2")
    reply, outs = run(kc, "%checkout a --discard")
    assert "state restored" in text(outs)
    reply, outs = run(kc, "x")
    assert [c for t, c in outs if t == "execute_result"][0]["data"]["text/plain"] == "1"
    reply, outs = run(kc, "%bogus")
    assert reply["status"] == "error"
    kc.shutdown()                                     # the front end shuts the kernel down ...
    kc2 = kernel()                                    # ... and starts it again
    reply, outs = run(kc2, "x")
    assert "Talo kept session nb" in text(outs)
    assert [c for t, c in outs if t == "execute_result"][0]["data"]["text/plain"] == "1"


def test_other_clients_completion_inspect_and_interrupt(kernel, talo):
    kc = kernel()
    run(kc, "value_one = 1\nimport time")
    talo("run", "-s", "nb", "value_two = 2", "--owner", "agent-b")
    reply, outs = run(kc, "value_one")
    assert "agent-b ran job" in text(outs)
    kc.complete("value_")
    assert kc.get_shell_msg(timeout=30)["content"]["matches"] == ["value_one", "value_two"]
    kc.inspect("value_two")
    assert kc.get_shell_msg(timeout=30)["content"]["data"]["text/plain"] == "2"
    msg_id = kc.execute("time.sleep(30)")
    import time
    time.sleep(1.5)
    kc.control_channel.send(kc.session.msg("interrupt_request", {}))
    while True:
        m = kc.get_shell_msg(timeout=30)
        if m["parent_header"].get("msg_id") == msg_id:
            break
    assert m["content"]["status"] == "error" and m["content"]["ename"] == "KeyboardInterrupt"


def test_kernelspec_install(talo_home, tmp_path):
    env = {**os.environ, "JUPYTER_DATA_DIR": str(tmp_path)}
    out = subprocess.run([sys.executable, "-m", "talo.cli", "kernelspec", "install", "-s", "research", "--json"],
                         capture_output=True, text=True, env=env, timeout=30)
    spec = json.loads((tmp_path / "kernels" / "talo-research" / "kernel.json").read_text())
    assert out.returncode == 0 and spec["display_name"] == "Talo: research (persistent)"
    assert spec["argv"][-4:] == ["--session", "research", "-f", "{connection_file}"]


def test_notebook_names_the_session_kernel(talo, tmp_path):
    import nbformat
    talo("run", "x = 1\nprint('hi')")
    env = {**os.environ, "JUPYTER_DATA_DIR": str(tmp_path / "jd")}
    path = tmp_path / "s.ipynb"
    out = subprocess.run([sys.executable, "-m", "talo.cli", "notebook", "-o", str(path), "--json"], capture_output=True,
                         text=True, env=env, timeout=60)
    assert out.returncode == 0, out.stderr
    nb = nbformat.read(path, 4)
    nbformat.validate(nb)
    assert nb.metadata.kernelspec.name == "talo-default"
    assert (tmp_path / "jd" / "kernels" / "talo-default" / "kernel.json").exists()
    assert any("x = 1" in c.source for c in nb.cells if c.cell_type == "code")


def test_a_notebook_runs_through_jupyters_kernel_manager(talo, tmp_path, monkeypatch):
    """The path JupyterLab and VS Code take: KernelManager starts the kernelspec, then the cells are executed."""
    nbclient = pytest.importorskip("nbclient")
    import nbformat
    monkeypatch.setenv("JUPYTER_DATA_DIR", str(tmp_path / "jd"))
    talo("run", "data = list(range(10))")
    path = tmp_path / "s.ipynb"
    assert subprocess.run([sys.executable, "-m", "talo.cli", "notebook", "-o", str(path)], timeout=60,
                          env={**os.environ}).returncode == 0
    nb = nbformat.read(path, 4)
    nb.cells.append(nbformat.v4.new_code_cell("sum(data)"))     # the data is already in memory
    nb.cells.append(nbformat.v4.new_code_cell("%snap from_notebook"))
    client = nbclient.NotebookClient(nb, timeout=60, kernel_name="talo-default")
    client.execute()
    assert nb.cells[-2].outputs[0]["data"]["text/plain"] == "45"
    assert "snapshot from_notebook" in nb.cells[-1].outputs[0]["text"]
    rc = subprocess.run([sys.executable, "-m", "talo.cli", "snaps", "--json"], capture_output=True, text=True)
    assert "from_notebook" in rc.stdout                            # the session itself, seen from the CLI


def test_wire_robustness_replays_and_control(kernel):
    kc = kernel()
    sock = kc.shell_channel.socket
    sock.send_multipart([b"garbage"])                                         # no delimiter
    sock.send_multipart([b"<IDS|MSG>", b"sig", b"{", b"{}", b"{}", b"{}"])    # bad signature / JSON
    reply, outs = run(kc, "1 + 1")
    assert reply["status"] == "ok"                                            # the channel survived
    msg = kc.session.msg("execute_request", {"code": "n = globals().get('n', 0) + 1", "silent": False,
                                             "store_history": True, "user_expressions": {}, "allow_stdin": False})
    kc.shell_channel.send(msg)
    kc.get_shell_msg(timeout=30)
    kc.session.send(kc.shell_channel.socket, msg)                             # the same signed message again
    reply, outs = run(kc, "n", timeout=30)
    assert [c for t, c in outs if t == "execute_result"][0]["data"]["text/plain"] == "1"   # the replay was dropped
    kc.control_channel.send(kc.session.msg("execute_request", {"code": "n = 100"}))       # not on control
    reply, outs = run(kc, "n")
    assert [c for t, c in outs if t == "execute_result"][0]["data"]["text/plain"] == "1"


def test_utf8_options_and_stop_on_error(kernel, talo):
    kc = kernel()
    reply, outs = run(kc, "import sys\nfor ch in 'héllo wörld ✓':\n    sys.stdout.write(ch)\n    sys.stdout.flush()\nprint()")
    assert text(outs) == "héllo wörld ✓\n"
    msg_id = kc.execute("y = 7", user_expressions={"double": "y * 2"})
    while True:
        m = kc.get_shell_msg(timeout=30)
        if m["parent_header"].get("msg_id") == msg_id:
            break
    assert m["content"]["user_expressions"]["double"]["data"]["text/plain"] == "14"
    first = kc.execute("raise ValueError('stop')")
    second = kc.execute("z = 1")                       # queued behind a failing cell: aborted
    replies = {}
    while len(replies) < 2:
        m = kc.get_shell_msg(timeout=30)
        replies[m["parent_header"]["msg_id"]] = m["content"]["status"]
    assert replies == {first: "error", second: "aborted"}
    rc = subprocess.run([sys.executable, "-m", "talo.cli", "run", "-s", "nb", "'z' in globals()", "--json"],
                        capture_output=True, text=True)
    assert json.loads(rc.stdout)["job"]["result"]["data"]["text/plain"] == "False"
    run(kc, "class H:\n    def _repr_html_(self):\n        return '<i>x</i>'\ndisplay(H())")
    jobs = json.loads(subprocess.run([sys.executable, "-m", "talo.cli", "jobs", "-s", "nb", "--last", "1", "--json"],
                                     capture_output=True, text=True).stdout)["jobs"]
    out = subprocess.run([sys.executable, "-m", "talo.cli", "tail", jobs[-1]["id"], "--all"], capture_output=True,
                         text=True).stdout
    assert "\x1b]" not in out and "<__main__.H object" in out  # the CLI never shows the display markers
