"""Ephemeral workers: a worker forked from a snapshot that runs one piece of code and exits (see "Ephemeral workers"
in docs/design.md). The snapshot is unchanged; the session the snapshot belongs to keeps its own worker."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time

from talo import wire


class EphemeralFailed(RuntimeError):
    pass


async def run(socket_path: str, code: str, output: str, *, bind: dict | None = None, case: dict | None = None,
              json_limit: int = 65536, started=None, on_message=None) -> dict:
    """Fork a worker from the snapshot listening on ``socket_path``, run ``code`` in it and return
    ``{status, pid, result (JSON or None), text (repr preview), error, elapsed_s}``. ``started(pid)`` is called once
    the worker exists (for accounting and interrupts). Cancelling the task kills the worker."""
    reader, writer = await asyncio.open_unix_connection(socket_path, limit=wire.MAX_LINE)
    pid = None
    t0 = time.monotonic()
    try:
        writer.write(b'{"op": "spawn"}\n')
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), 30)
        reply = json.loads(line) if line else {}
        if not reply.get("ok"):
            raise EphemeralFailed("the snapshot did not spawn a worker")
        pid = reply["pid"]
        if started is not None:
            started(pid)
        msg = wire.message("execute_request", {"code": code, "silent": False, "store_history": True,
                                               "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
                           metadata={"talo_output": output, "talo_bind": bind or {}, "talo_case": case,
                                     "talo_json_result": json_limit})
        writer.write(wire.encode(msg))
        await writer.drain()
        out = {"status": None, "pid": pid, "result": None, "text": None, "error": None, "outputs": []}
        while True:
            line = await reader.readline()
            if not line:
                out["status"] = "lost"
                out["error"] = {"ename": "WorkerDied", "evalue": "the worker exited during the case", "traceback": []}
                break
            got = wire.decode(line)
            if got.get("parent_header", {}).get("msg_id") != msg["header"]["msg_id"]:
                continue
            kind = wire.msg_type(got)
            content = got["content"]
            if kind in ("talo_record", "talo_progress") and on_message is not None:
                on_message(kind, content)
            elif kind == "execute_result":
                meta = content.get("metadata", {})
                out["text"] = content["data"].get("text/plain", "")[:2000]
                j = meta.get("talo_json") or {}
                out["result"] = j.get("value")
                if "error" in j:
                    out["result_error"] = j["error"]
                out["outputs"] += [{"mime": m, "path": p} for m, p in meta.get("talo_files", {}).items()]
            elif kind == "display_data":
                out["outputs"] += [{"mime": m, "path": p}
                                   for m, p in content.get("metadata", {}).get("talo_files", {}).items()]
            elif kind == "error":
                out["error"] = {k: content.get(k) for k in ("ename", "evalue", "traceback")}
            elif kind == "execute_reply":
                out["status"] = content["status"]
                break
        try:
            writer.write(wire.encode(wire.message("shutdown_request", {"restart": False})))
            await writer.drain()
        except (ConnectionError, RuntimeError):
            pass
        out["elapsed_s"] = round(time.monotonic() - t0, 3)
        return out
    except BaseException:  # cancelled, or the connection failed: never leave the child running
        if pid:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        raise
    finally:
        writer.close()


class Channel:
    """A worker driven step by step (verify replays): forked from a snapshot, or a fresh interpreter."""

    def __init__(self, reader, writer, pid: int, proc=None):
        self.reader, self.writer, self.pid, self.proc = reader, writer, pid, proc

    @classmethod
    async def from_snapshot(cls, socket_path: str) -> "Channel":
        reader, writer = await asyncio.open_unix_connection(socket_path, limit=wire.MAX_LINE)
        writer.write(b'{"op": "spawn"}\n')
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), 30)
        reply = json.loads(line) if line else {}
        if not reply.get("ok"):
            writer.close()
            raise EphemeralFailed("the snapshot did not spawn a worker")
        return cls(reader, writer, reply["pid"])

    @classmethod
    async def fresh(cls, python: str, worker: str, cwd: str, env: dict) -> "Channel":
        import socket as _socket
        parent, child = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_STREAM)
        proc = await asyncio.create_subprocess_exec(
            python, worker, "--fd", str(child.fileno()), "--session", "verify", cwd=cwd, env=env,
            pass_fds=(child.fileno(),), stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL, start_new_session=True)
        child.close()
        reader, writer = await asyncio.open_unix_connection(sock=parent, limit=wire.MAX_LINE)
        channel = cls(reader, writer, proc.pid, proc)
        await channel.request("kernel_info_request", {})
        return channel

    async def request(self, msg_type: str, content: dict, *, metadata: dict | None = None,
                      timeout: float | None = None) -> tuple[dict, list]:
        """Send a request; return its reply and the other messages answering it (error, execute_result ...)."""
        msg = wire.message(msg_type, content, metadata=metadata)
        self.writer.write(wire.encode(msg))
        await self.writer.drain()
        reply_type, others = msg_type.replace("_request", "_reply"), []

        async def read() -> dict:
            while True:
                line = await self.reader.readline()
                if not line:
                    raise EphemeralFailed("the worker exited")
                got = wire.decode(line)
                if got.get("parent_header", {}).get("msg_id") != msg["header"]["msg_id"]:
                    continue
                if wire.msg_type(got) == reply_type:
                    return got
                others.append(got)
        return await asyncio.wait_for(read(), timeout), others

    async def execute(self, code: str, output: str) -> dict:
        reply, others = await self.request(
            "execute_request", {"code": code, "silent": False, "store_history": True, "user_expressions": {},
                                "allow_stdin": False, "stop_on_error": False}, metadata={"talo_output": output})
        error = next((m["content"] for m in others if wire.msg_type(m) == "error"), None)
        return {"status": reply["content"]["status"], "error": error}

    async def close(self) -> None:
        try:
            self.writer.write(wire.encode(wire.message("shutdown_request", {"restart": False})))
            await self.writer.drain()
        except (ConnectionError, RuntimeError):
            pass
        self.writer.close()
        try:
            os.kill(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if self.proc is not None:
            await self.proc.wait()
