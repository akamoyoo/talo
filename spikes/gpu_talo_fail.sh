#!/usr/bin/env bash
# Measurement: failures in a CUDA session with today's Talo. Interrupting a long GPU job, CUDA out of memory, checking
# out a snapshot taken after CUDA, a worker killed by a signal and a supervisor killed by a signal: is the session
# still usable, and is VRAM released? Also: how long reading /proc/self/maps and nvidia-smi take.
#
# Usage: CUDA_VISIBLE_DEVICES=1 PY=/path/to/python bash gpu_talo_fail.sh       (PY needs torch; run from the repo root)
set -u
PY=${PY:-python3}
export TALO_HOME=${TALO_HOME:-$(mktemp -d /tmp/talo-gpu-XXXX)}
export PYTHONPATH=$PWD
T() { timeout "${TO:-90}" "$PY" -c 'import sys; from talo.cli import main; sys.exit(main(sys.argv[1:]))' "$@"; }
step() { echo; echo "\$ talo $*"; local t0=$SECONDS; T "$@" 2>&1 | tail -n "${LINES_SHOWN:-5}"; echo "  -> exit ${PIPESTATUS[0]}, $((SECONDS - t0))s"; }
vram() { echo "  [vram of talo processes: $(nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader \
          | while IFS=, read -r p m; do ps -o args= -p "$p" 2>/dev/null | grep -q talo && echo -n "$p:$m "; done)]"; }
LOAD='import torch; model = torch.nn.Sequential(*[torch.nn.Linear(4096, 4096) for _ in range(64)]).cuda().eval(); xs = torch.randn(256, 4096)'

step new f --python "$PY"
step run -s f 'import torch; n = 1'
step snap -s f cpu0
step run -s f "$LOAD"
step snap -s f after_cuda
vram

echo; echo "### cost of the checks inside a CUDA worker"
step run -s f 'import time, subprocess
t = time.perf_counter(); maps = open("/proc/self/maps").read(); hit = " /dev/nvidia" in maps
print(f"maps: {len(maps.splitlines())} lines, nvidia mapped={hit}, {1000 * (time.perf_counter() - t):.2f} ms")
t = time.perf_counter(); subprocess.run(["nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader"], capture_output=True)
print(f"nvidia-smi --query-compute-apps: {1000 * (time.perf_counter() - t):.0f} ms")'

echo; echo "### interrupt a long GPU loop"
JOB=$(T run -s f -t 1 'with torch.no_grad():
    for i in range(10**6): y = model(xs.cuda())' 2>&1 | grep -o 'job [0-9]*' | head -1 | cut -d' ' -f2)
sleep 2
step interrupt "$JOB"
step wait "$JOB"
step run -s f 'with torch.no_grad(): print(model(xs.cuda()).sum().item())'

echo; echo "### CUDA out of memory"
step run -s f 'big = torch.empty(200 * 2**30, dtype=torch.uint8, device="cuda")'
step run -s f 'with torch.no_grad(): print(model(xs.cuda()).sum().item())'
vram

echo; echo "### checkout of a snapshot taken after CUDA"
step checkout -s f after_cuda --discard
step run -s f 'print(n, xs.shape)'
step run -s f 'with torch.no_grad(): print(model(xs.cuda()).sum().item())'
vram

echo; echo "### worker killed by a signal"
step checkout -s f cpu0 --discard
step run -s f "$LOAD"
vram
W=$(T ls 2>/dev/null | awk '$1 == "f" {print}' ); echo "  ls: $W"
WPID=$(nvidia-smi --query-compute-apps=pid --format=csv,noheader | while read -r p; do ps -o args= -p "$p" 2>/dev/null | grep -q talo && echo "$p"; done | head -1)
echo "  kill -9 worker $WPID"; kill -9 "$WPID"; sleep 3
vram
step ls
step checkout -s f cpu0 --discard
step run -s f 'print(torch.ones(1, device="cuda").item())'

echo; echo "### supervisor killed by a signal"
step run -s f "$LOAD"
vram
SPID=$(pgrep -u "$USER" -f "talo.*supervisor|talo.cli.*daemon|supervisor.py" | head -1)
echo "  supervisor candidates: $(pgrep -u "$USER" -af talo | grep -v -e gpu_talo -e pgrep | grep -i -e daemon -e supervisor | head -3)"
echo "  kill -9 supervisor $SPID"; kill -9 "$SPID"; sleep 5
vram
echo "  talo processes left: $(pgrep -u "$USER" -af 'talo' | grep -v -e gpu_talo -e pgrep | wc -l)"
pkill -9 -u "$USER" -f "$TALO_HOME" 2>/dev/null
step stop
