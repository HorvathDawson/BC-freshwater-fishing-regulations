#!/usr/bin/env bash
# Regenerate the reference harness's golden outputs from the CURRENT export (read-only).
#   pipeline/deliver/answers/reference/regenerate.sh OUT [EXPORT_DIR]
# OUT gets: build28/ (the page's 28 display waters), sample_waters.json, buildS/ (28 + the sample),
# checks*.json (the page's Checks tab), golden/ (JSON lines, gzipped, + manifests). Run from the repo root.
# No credits: nothing here calls the `claude` CLI.
set -euo pipefail
OUT=${1:?usage: regenerate.sh OUT [EXPORT_DIR]}
EXPORT=${2:-/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/regs}
PY=${PY:-.venv/bin/python}
[ -x "$PY" ] || PY=/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/.venv/bin/python
HERE=pipeline/deliver/answers/reference
JOBS=${JOBS:-4}
mkdir -p "$OUT/golden"

"$PY" -m pipeline.deliver.answers.reference.convert --export-dir "$EXPORT" --out "$OUT/build28"
"$PY" -m pipeline.deliver.answers.reference.sample --export-dir "$EXPORT" --out "$OUT/sample_waters.json"
"$PY" -m pipeline.deliver.answers.reference.convert --export-dir "$EXPORT" --waters "$OUT/sample_waters.json" --out "$OUT/buildS"
node "$HERE/checks.js" "$OUT/build28" --json "$OUT/checks.json" | tee "$OUT/checks.txt"

# golden: the sample build (the 28 come first in it), in JOBS slices run side by side
N=$(node -e "console.log(JSON.parse(require('fs').readFileSync('$OUT/buildS/data.json','utf8')).waters.length)")
# the 28 page waters carry the most parts: slice by parts, not by waters
BOUNDS=$(node -e "
const W=JSON.parse(require('fs').readFileSync('$OUT/buildS/data.json','utf8')).waters, J=$JOBS;
const tot=W.reduce((a,w)=>a+w.parts.length,0); const b=[0]; let acc=0;
W.forEach((w,i)=>{ acc+=w.parts.length; if (acc>=tot*b.length/J && b.length<J) b.push(i+1); }); b.push(W.length);
console.log([...new Set(b)].join(' '));")
set -- $BOUNDS
PIDS=()
while [ $# -gt 1 ]; do
  node "$HERE/golden.js" --build "$OUT/buildS" --out "$OUT/golden" --from "$1" --to "$2" & PIDS+=($!)
  shift
done
for p in "${PIDS[@]}"; do wait "$p"; done
node "$HERE/manifest.js" "$OUT/golden" "$N"
