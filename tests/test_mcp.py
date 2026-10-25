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
    finally:
        proc.stdin.close()
        assert proc.wait(timeout=30) == 0   # end of input detaches; the session keeps running
