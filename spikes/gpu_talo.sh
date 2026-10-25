#!/usr/bin/env bash
# Measurement: what today's Talo does when a session's worker uses CUDA.
# A session prepares CPU state (snapshot cpu0), loads a model onto the GPU, runs inference repeatedly, then tries every
# command that forks the worker. Then: checkout of the pre-CUDA snapshot, a map from it, and whether VRAM is released.
#
# Usage: CUDA_VISIBLE_DEVICES=1 PY=/path/to/python bash gpu_talo.sh       (PY needs torch; run from the repo root)
set -u
PY=${PY:-python3}
export TALO_HOME=${TALO_HOME:-$(mktemp -d /tmp/talo-gpu-XXXX)}
export PYTHONPATH=$PWD
T() { timeout "${TO:-90}" "$PY" -c 'import sys; from talo.cli import main; sys.exit(main(sys.argv[1:]))' "$@"; }
step() { echo; echo "\$ talo $*"; local t0=$SECONDS; T "$@" 2>&1 | tail -n "${LINES_SHOWN:-6}"; echo "  -> exit ${PIPESTATUS[0]}, ${SECONDS}s total, $((SECONDS - t0))s"; }
vram() { echo "  [vram of talo processes: $(nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader \
          | while IFS=, read -r p m; do ps -o comm=,args= -p "$p" 2>/dev/null | grep -q talo && echo -n "$p:$m "; done)]"; }
echo "TALO_HOME=$TALO_HOME python=$PY CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-}"

step new g --python "$PY"
step run -s g 'import torch; xs = torch.randn(256, 4096); n = 1'
step snap -s g cpu0
step run -s g 'model = torch.nn.Sequential(*[torch.nn.Linear(4096, 4096) for _ in range(64)]).cuda().eval(); torch.cuda.synchronize(); print(sum(p.numel() for p in model.parameters()) * 4 / 2**30, "GiB")'
step run -s g 'import time; t = time.perf_counter()
with torch.no_grad(): y = model(xs.cuda()).sum().item()
print(y, f"{time.perf_counter() - t:.3f}s")'
step run -s g 'import time; t = time.perf_counter()
with torch.no_grad(): y = model(xs.cuda()).sum().item()
print(y, f"{time.perf_counter() - t:.3f}s")'
vram
step vars -s g
step show -s g n
step show -s g 'model[0].weight.sum().item()'
step snap -s g after_cuda
step run -s g --atomic 'z = model(xs.cuda()).sum().item(); print(z)'
step run -s g --trial t1 'w = model(xs.cuda()).sum().item(); print(w)'
TO=60 step trials -s g
step map -s g --range 2 'print(case, torch.ones(1, device="cuda").item())'
step map -s g --from cpu0 --range 2 'print(case, torch.ones(1, device="cuda").item())'
step save -s g
step verify -s g
step run -s g 'def infer(k: int = 0) -> float: return float(model(xs.cuda())[k].sum())'
step expose -s g infer
step call g.infer '{"k": 1}'
step memory -s g
vram
step checkout -s g cpu0 --discard
step run -s g 'print(torch.cuda.is_available(), torch.ones(1, device="cuda").item())'
vram
step kill g
sleep 3; vram
step stop
