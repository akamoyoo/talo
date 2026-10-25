"""M7: the MCP server, driven over stdio as an MCP client would."""

import json
import subprocess
import sys
import time


def test_mcp_lifecycle_tools_and_concurrency(talo_home):
    proc = subprocess.Popen([sys.executable, "-m", "talo.cli", "mcp"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            text=True)
    answers = {}

    def send(msg):
        proc.stdin.write(json.dumps(msg) + "\n")
        proc.stdin.flush()

    def reply(mid, timeout=60):
        deadline = time.time() + timeout
        while mid not in answers and time.time() < deadline:
            line = proc.stdout.readline()
            msg = json.loads(line)                      # stdout carries protocol messages only
            answers[msg.get("id")] = msg
        return answers[mid]

    try:
        send({"jsonrpc": "2.0", "id": 0, "method": "tools/list"})          # before initialize: refused
        assert reply(0)["error"]["code"] == -32002
        send([1, 2])                                                        # not a request object
        assert reply(None)["error"]["code"] == -32600
        send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
              "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t"}}})
        init = reply(1)["result"]
        assert init["protocolVersion"] == "2025-06-18" and "tools" in init["capabilities"]
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        names = {t["name"] for t in reply(2)["result"]["tools"]}
        assert {"run", "wait", "interrupt", "brief", "snap", "checkout", "map", "call", "ledger"} <= names
        send({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
              "params": {"name": "run", "arguments": {"code": "import time\nx = 41\nprint('hi')\nx + 1"}}})
        r = reply(3)["result"]
        assert not r["isError"] and r["structuredContent"]["job"]["result"]["data"]["text/plain"] == "42"
        assert r["structuredContent"]["output"]["text"] == "hi\n" and json.loads(r["content"][0]["text"])["job"]
        send({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "run", "arguments": {"code": "1/0"}}})
        assert reply(4)["result"]["isError"]
        # a long job: wait is pending while interrupt is called on the same connection
        send({"jsonrpc": "2.0", "id": 5, "method": "tools/call",
              "params": {"name": "run", "arguments": {"code": "time.sleep(30)", "timeout": 0}}})
        jid = reply(5)["result"]["structuredContent"]["job"]["id"]
        send({"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {"name": "wait", "arguments": {"id": jid, "timeout": 60}}})
        time.sleep(1)
        send({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": "interrupt", "arguments": {"id": jid}}})
        assert reply(7)["result"]["structuredContent"]["result"] == "interrupting"
        waited = reply(6)["result"]
        assert waited["isError"] and waited["structuredContent"]["job"]["status"] == "interrupted"
        send({"jsonrpc": "2.0", "id": 8, "method": "tools/call", "params": {"name": "nope", "arguments": {}}})
        assert reply(8)["error"]["code"] == -32602
        send({"jsonrpc": "2.0", "id": 9, "method": "tools/call", "params": {"name": "show", "arguments": {"name": "x"}}})
        assert reply(9)["result"]["structuredContent"]["text"] == "41"
        send({"jsonrpc": "2.0", "id": 10, "method": "tools/call", "params": {"name": "checkout", "arguments": {"name": "nope"}}})
        assert reply(10)["result"]["isError"]
        send({"jsonrpc": "2.0", "id": 11, "method": "tools/call",
              "params": {"name": "run", "arguments": {"code": "'x' * 200_000"}}})
        big = reply(11)["result"]
        assert len(json.dumps(big["structuredContent"])) < 40_000      # bounded like the CLI (10 000 characters)
        assert len(big["structuredContent"]["job"]["result"]["data"]["text/plain"]) == 10_000
        assert json.loads(big["content"][0]["text"]) == big["structuredContent"]
        send({"jsonrpc": "2.0", "id": 12, "method": "tools/call",
              "params": {"name": "map", "arguments": {"code": "1 / case", "range": 2}}})
        assert reply(12)["result"]["isError"]                               # a map with a failed case
        send({"jsonrpc": "2.0", "id": 13, "method": "tools/call", "params": {"name": "run", "arguments": {"cod": "1"}}})
        assert reply(13)["error"]["code"] == -32602
    finally:
        proc.stdin.close()
        assert proc.wait(timeout=30) == 0   # end of input detaches; the session keeps running


def test_bound_keeps_valid_json_within_budget():
    from talo.mcp import bound
    data = {"rows": [{"text": "y" * 5000} for _ in range(100)], "note": "z" * 100_000}
    out = bound(data, [32_000])
    text = json.dumps(out)
    assert len(text) < 40_000 and json.loads(text)["rows"][-1].startswith("[") or len(out["rows"]) < 100
