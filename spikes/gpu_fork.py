"""Measurement: after how much CUDA use does fork() stop working, and what can a stdlib-only worker see?

For each stage below, a fresh process runs the steps up to that stage, records what it can observe without importing
or initialising anything (mapped libraries, open /dev/nvidia* files, threads, torch's own flag if torch is already
imported, nvidia-smi), then forks. The child reports, one line per test: plain Python, a torch CPU matmul, a new CUDA
tensor, and (when the parent has one) reading the parent's CUDA tensor. A test that does not answer is a hang.

Stages (torch):
  0 nothing           1 import torch          2 torch.cuda.is_available()   3 torch.cuda.device_count()
  4 get_device_name   5 first CUDA tensor     6 small model forward on GPU  7 6, then del + empty_cache()
Stages without torch's own initialisation (each alone, not cumulative):
  8 import torch; is_available() with PYTORCH_NVML_BASED_CUDA_CHECK=1      9 nvmlInit() through ctypes only
  10 cuInit(0) through ctypes only (any native extension)                   11 cupy: allocate an array
The child also tries cuInit + cuCtxCreate through ctypes: the driver's own answer, independent of any framework.

Usage: CUDA_VISIBLE_DEVICES=1 python gpu_fork.py            (all stages; one process per stage)
       python gpu_fork.py --stage N                          (one stage)
"""

from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import threading
import time

STAGES = ["nothing", "import torch", "cuda.is_available()", "cuda.device_count()", "get_device_name(0)",
          "first CUDA tensor", "model forward on GPU", "forward, then del + empty_cache()",
          "NVML-based is_available()", "nvmlInit() via ctypes", "cuInit() via ctypes", "cupy array"]
CHILD_TIMEOUT = 30.0


def run_stage(n: int) -> dict:
    keep = {}
    if n == 8:
        os.environ["PYTORCH_NVML_BASED_CUDA_CHECK"] = "1"
        import torch
        keep["available"] = torch.cuda.is_available()
        return keep
    if n == 9:
        import ctypes
        keep["nvml"] = ctypes.CDLL("libnvidia-ml.so.1").nvmlInit_v2()
        return keep
    if n == 10:
        keep["cuInit"] = cuda_driver().cuInit(0)
        return keep
    if n == 11:
        import cupy
        keep["c"] = cupy.ones(4)
        return keep
    if n >= 1:
        import torch
    if n >= 2:
        keep["available"] = torch.cuda.is_available()
    if n >= 3:
        keep["count"] = torch.cuda.device_count()
    if n >= 4:
        keep["name"] = torch.cuda.get_device_name(0)
    if n >= 5:
        keep["t"] = torch.ones(4, device="cuda")
    if n >= 6:
        model = torch.nn.Sequential(torch.nn.Linear(1024, 1024), torch.nn.ReLU(), torch.nn.Linear(1024, 10)).cuda()
        with torch.no_grad():
            keep["y"] = model(torch.randn(64, 1024, device="cuda"))
        torch.cuda.synchronize()
        keep["model"] = model
    if n >= 7:
        keep.clear()
        del model
        torch.cuda.empty_cache()
    return keep


def cuda_driver():
    import ctypes
    return ctypes.CDLL("libcuda.so.1")


def driver_context() -> str:
    """cuInit + cuCtxCreate in this process, through the driver API only."""
    import ctypes
    cu = cuda_driver()
    rc = cu.cuInit(0)
    if rc:
        raise RuntimeError(f"cuInit returned {rc}")
    dev, ctx = ctypes.c_int(), ctypes.c_void_p()
    rc = cu.cuDeviceGet(ctypes.byref(dev), 0) or cu.cuCtxCreate_v2(ctypes.byref(ctx), 0, dev)
    if rc:
        raise RuntimeError(f"cuDeviceGet/cuCtxCreate returned {rc}")
    return "context created"


def observe() -> dict:
    """What a stdlib-only worker can see, without importing or initialising anything."""
    with open("/proc/self/maps") as f:
        maps = f.read()
    libs = sorted({os.path.basename(line.split()[-1]) for line in maps.splitlines()
                   if any(k in line for k in ("libcuda.so", "libcudart", "libnvidia-ml", "libcublas", "libnccl"))})
    dev_maps = sorted({line.split()[-1] for line in maps.splitlines() if " /dev/nvidia" in line})
    devs = set()
    for fd in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:
            continue
        if target.startswith("/dev/nvidia"):
            devs.add(target)
    me = str(threading.get_native_id())
    threads = []
    for tid in os.listdir("/proc/self/task"):
        if tid != me:
            with open(f"/proc/self/task/{tid}/comm") as f:
                threads.append(f.read().strip())
    torch = sys.modules.get("torch")
    flags = {}
    if torch is not None:
        flags = {"is_initialized": torch.cuda.is_initialized(),
                 "_is_in_bad_fork": bool(getattr(torch.cuda, "_is_in_bad_fork", lambda: None)())}
    return {"libs": libs, "dev_fds": sorted(devs), "dev_maps": dev_maps, "threads": sorted(threads), "torch": flags,
            "smi_pid_mib": smi_used_by(os.getpid())}


def smi_used_by(pid: int):
    try:
        out = subprocess.run(["nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits"],
                             capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"unavailable: {error}"
    for line in out.splitlines():
        p, _, mib = line.partition(",")
        if p.strip() == str(pid):
            return int(mib)
    return None


def child_tests(keep: dict, w: int) -> None:
    def say(name, fn):
        try:
            result = fn()
            line = f"{name}: ok {result if result is not None else ''}".rstrip()
        except BaseException as error:  # noqa: BLE001
            line = f"{name}: error {type(error).__name__}: {str(error).splitlines()[0][:160] if str(error) else ''}"
        os.write(w, (line + "\n").encode())

    say("python", lambda: sum(range(1000)))
    say("driver cuInit + cuCtxCreate", driver_context)
    torch = sys.modules.get("torch")
    if torch is None:
        import torch  # stage 0: the child imports torch itself, as a fresh worker would
    say("torch cpu matmul", lambda: float((torch.ones(256, 256) @ torch.ones(256, 256))[0, 0]))
    say("new CUDA tensor", lambda: float(torch.ones(4, device="cuda").sum().item()))
    if "t" in keep:
        say("read parent's CUDA tensor", lambda: float(keep["t"].cpu().sum()))
    os.write(w, b"done\n")


def one_stage(n: int) -> dict:
    t0 = time.perf_counter()
    keep = run_stage(n)
    stage_s = time.perf_counter() - t0
    seen = observe()
    r, w = os.pipe()
    t0 = time.perf_counter()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        try:
            child_tests(keep, w)
        finally:
            os._exit(0)
    fork_ms = (time.perf_counter() - t0) * 1000
    os.close(w)
    buf, deadline = b"", time.time() + CHILD_TIMEOUT
    while time.time() < deadline and not buf.endswith(b"done\n"):
        if select.select([r], [], [], 0.5)[0]:
            chunk = os.read(r, 4096)
            if not chunk:
                break
            buf += chunk
    lines = buf.decode().splitlines()
    if "done" not in lines:
        os.kill(pid, signal.SIGKILL)
        lines.append(f"(child gave no further answer within {CHILD_TIMEOUT:.0f}s: hang, killed)")
    os.waitpid(pid, 0)
    return {"stage": n, "name": STAGES[n], "stage_s": round(stage_s, 2), "fork_ms": round(fork_ms, 1),
            "seen": seen, "child": [x for x in lines if x != "done"]}


def main() -> None:
    if "--stage" in sys.argv:
        print(json.dumps(one_stage(int(sys.argv[sys.argv.index("--stage") + 1]))), flush=True)
        return
    print(f"python {sys.version.split()[0]}, CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES')}")
    for n in range(len(STAGES)):
        proc = subprocess.run([sys.executable, __file__, "--stage", str(n)], capture_output=True, text=True,
                              timeout=CHILD_TIMEOUT * 6)
        try:
            res = json.loads(proc.stdout.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            print(f"\n## stage {n}: {STAGES[n]}: no result\n{proc.stdout}\n{proc.stderr[-2000:]}")
            continue
        s = res["seen"]
        print(f"\n## stage {n}: {res['name']}  (stage {res['stage_s']}s, fork {res['fork_ms']} ms)")
        print(f"   libs:    {', '.join(s['libs']) or '-'}")
        print(f"   dev fds: {', '.join(s['dev_fds']) or '-'}")
        print(f"   dev mmaps: {', '.join(s['dev_maps']) or '-'}")
        print(f"   threads: {', '.join(s['threads']) or '-'}")
        print(f"   torch:   {s['torch'] or '-'}    nvidia-smi MiB for pid: {s['smi_pid_mib']}")
        for line in res["child"]:
            print(f"   child  {line}")


if __name__ == "__main__":
    main()
