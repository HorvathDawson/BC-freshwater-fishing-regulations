#!/usr/bin/env bash
# (T810 V-py, trout table contract 2.8) THE TROUT FIXTURE REFRESH: the whole chain, in order, one command. Nobody runs
# the steps by hand: a builder change reaches the committed trout fixture, its vectors and the goldens only through here.
#
#   scripts/heavy.sh trout-refresh 50 -- scripts/trout-refresh.sh                 # build the side release, then 2-5
#   scripts/heavy.sh trout-refresh 50 -- scripts/trout-refresh.sh --release DIR   # 2-5 on a side release already built
#
#   1. side release  the live bundle's answers (`pipeline.deliver answers`: the trout section is in every build since the
#                    cutover, T810 L), the card gate (the
#                    trout proof J runs in it), the regs store and the release, into $SIDE (default
#                    data/generated/side/trout-refresh/; never promoted: LATEST does not move). Inputs are the live
#                    ones: the bundle, the export pair, the status index, the tiles. Heavy: run it through heavy.sh.
#   2. regs fixture  python -m pipeline.deliver.release.regs_fixture --release <side> --trout-cases
#                    -> app/fixtures/trout/regs-release/ + section-lines.json (the waters of trout_vectors.CASES)
#   3. vectors       python -m pipeline.tools.emit.trout_vectors -> app/fixtures/trout/trout-vectors.generated.json
#   4. golden sync   node app/tools/regs-recut/sync.mjs --release <side>: ONLY at the cutover
#                    (`trout` in KNOWN_VERSIONS of core/src/regs/versions.ts: ticket L; not at M0, see step 4). Before that the goldens stay the normal release's
#                    and this step says so (a golden cut from a side release names a section version the app's
#                    conformance does not hold, so `pnpm check` would fail on a fixture, not on a fault)
#   5. checks        every generator `--check` (`python -m pipeline.tools.emit --check`, the trout vectors among them),
#                    and the fixture cut AGAIN into a scratch root must be byte for byte the committed one (run twice =
#                    no change). Any stale file fails the script.
#
# Inputs: DATA_ROOT (default <repo>/data) holds generated/ (bundle, regs export, tiles) and release/ (the released tile
# folders the side release names). PY (default <repo>/.venv/bin/python).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA="${DATA_ROOT:-$REPO/data}"
PY="${PY:-$( [ -x "$REPO/.venv/bin/python" ] && echo "$REPO/.venv/bin/python" || echo python3 )}"
SIDE="$DATA/generated/side/trout-refresh"
RELEASE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --release) RELEASE="${2:?--release needs a side release folder}"; shift 2 ;;
    --side-dir) SIDE="${2:?--side-dir needs a folder}"; shift 2 ;;
    -h|--help) sed -n '2,27p' "$0"; exit 0 ;;
    *) echo "trout-refresh: unknown argument $1" >&2; exit 2 ;;
  esac
done
cd "$REPO"
export PYTHONPATH="$REPO${PYTHONPATH:+:$PYTHONPATH}"
step() { echo; echo "trout-refresh $1/5: $2"; }

if [ -z "$RELEASE" ]; then
  step 1 "the side release into $SIDE"
  B="$DATA/generated/bundle/bundle.sqlite"
  for f in "$B" "$DATA/generated/regs/ui-rules-export.json" "$DATA/generated/regs/ui-rules-guide.json" \
           "$DATA/generated/bundle/status_index.bin" "$DATA/generated/tiles/atlas.meta.json"; do
    [ -e "$f" ] || { echo "trout-refresh: missing input $f (python -m pipeline.deliver all makes it)" >&2; exit 1; }
  done
  rm -rf "$SIDE"; mkdir -p "$SIDE/regs"
  cp "$DATA/generated/regs/ui-rules-export.json" "$DATA/generated/regs/ui-rules-guide.json" "$SIDE/regs/"
  "$PY" -m pipeline.deliver answers --bundle "$B" --export-dir "$SIDE/regs"
  "$PY" -m pipeline.deliver card_check --bundle "$B" --export-dir "$SIDE/regs"
  "$PY" -m pipeline.deliver store --bundle "$B" --export-dir "$SIDE/regs" --out "$SIDE/regs.sqlite"
  # the release step builds the folder, then runs the size gates. A side release built before the cutover was OVER THE
  # FIRST-TAP GATE by design (the section beside the old trout groups); since ticket L the groups are dropped and the
  # release command gates the real candidate. Here, a fixture's source: those two gates are said and passed over; any
  # other refusal stops the script
  set +e
  "$PY" -m pipeline.deliver release --bundle "$B" --store "$SIDE/regs.sqlite" \
    --status-index "$DATA/generated/bundle/status_index.bin" --export-dir "$SIDE/regs" --out-root "$SIDE/release" \
    --tiles "$DATA/generated/tiles" --tiles-release-root "$DATA/release" --source "$DATA/source" \
    --dfo-rules "$DATA/generated/regs/dfo_salmon/rules" 2> "$SIDE/release.err"
  rc=$?; set -e
  cat "$SIDE/release.err" >&2
  if [ $rc -ne 0 ]; then
    if grep -v -E '^SIZE GATE: web first_tap(_low)?: |^pipeline\.deliver: the release was refused \(a size gate\)' \
         "$SIDE/release.err" | grep -q . || ! grep -q '^SIZE GATE: ' "$SIDE/release.err"; then
      echo "trout-refresh: the release step failed (above)" >&2; exit 1
    fi
    echo "trout-refresh: the side release is over the first-tap size gate only (the release command gates the" \
         "real candidate; A1 measures it); it is a test fixture's source, never promoted"
  fi
  rm -f "$SIDE/regs.sqlite" "$SIDE/release.err"
  RELEASE="$(ls -d "$SIDE"/release/r-* | head -n 1)"
else
  step 1 "the side release: $RELEASE (given)"
fi
[ -f "$RELEASE/manifest.json" ] || { echo "trout-refresh: no release at $RELEASE" >&2; exit 1; }
"$PY" - "$RELEASE" <<'PYEOF'
import json, sys
v = (json.load(open(sys.argv[1] + "/manifest.json")).get("regulations") or {}).get("versions") or {}
if "trout" not in v:
    sys.exit(f"trout-refresh: {sys.argv[1]} holds no trout section: a release cut before the trout cutover (T810 L)")
PYEOF

step 2 "the trout fixture (app/fixtures/trout/regs-release + section-lines.json)"
"$PY" -m pipeline.deliver.release.regs_fixture --release "$RELEASE" --trout-cases >/dev/null

step 3 "the trout vectors"
"$PY" -m pipeline.tools.emit.trout_vectors

step 4 "the golden sync"
# 2026-11-17: only at the cutover (L, `trout` in KNOWN_VERSIONS), not at M0. The goldens are also what the app's
# local release tests (regs/blob/reads/store.release.test.ts) hold the LIVE release to, so they must stay the live
# release's until the live release carries the section; and reads.trout.test.ts needs the normal regs fixture without it.
if grep -Eq 'export const KNOWN_VERSIONS = Object\.freeze\(\{[^}]*\btrout:' \
     app/packages/core/src/regs/versions.ts; then
  node app/tools/regs-recut/sync.mjs --release "$RELEASE"
else
  echo "trout-refresh: golden sync NOT run: \`trout\` is not in KNOWN_VERSIONS (the cutover, ticket L); the goldens" \
       "stay the live release's."
fi

step 5 "every generated file fresh, and the fixture cut again is the same bytes"
"$PY" -m pipeline.tools.emit --check
AGAIN="$(mktemp -d "${TMPDIR:-/tmp}/trout-refresh-again.XXXXXX")"
trap 'rm -rf "$AGAIN"' EXIT
"$PY" -m pipeline.deliver.release.regs_fixture --release "$RELEASE" --trout-cases --out "$AGAIN/regs-release" >/dev/null
if ! diff -r "$AGAIN/regs-release" app/fixtures/trout/regs-release >/dev/null \
   || ! cmp -s "$AGAIN/section-lines.json" app/fixtures/trout/section-lines.json; then
  echo "trout-refresh: the trout fixture cut twice from $RELEASE differs: the cut is not deterministic" >&2
  exit 1
fi
echo
echo "trout-refresh: done ($(basename "$RELEASE")). Commit app/fixtures/trout/ with the side release's id in the message."
