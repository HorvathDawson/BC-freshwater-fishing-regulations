#!/bin/bash
# T246 — THE RESTORE TEST: restore from the backups only, into a scratch bucket prefix and a scratch dir, and prove every
# byte by hash. STAGING ONLY (bucket names hardcoded). Needs the rclone remote `r2:`.  Exit 1 on any mismatch.
#   deploy/staging/restore-test.sh [YYYY-MM-DD]     (default: the newest daily/ snapshot)
#
# 1. R2 state   daily/<day>/ → r2 backup restore-test/<stamp>/ (server-side; 1-day lifecycle) → local scratch dir;
#               every object's sha256 == daily/<day>/_manifest.json; then how many still equal the LIVE bucket
#               (the feeds keep writing, so a later change is expected and only reported)
# 2. curated    the newest curated/*.tar.gz → its .sha256, then every file inside against its MANIFEST.sha256; and how
#               it differs from data/curated on disk now (reported)
# 3. release    every object of the live release/ is in the backup's release/ with the same size + MD5 (rclone check)
# A restore of the whole staging bucket = 1 + 3 copied back (rclone copy … r2:bc-fishing-regulations-staging/); 3 is the
# live release byte for byte and 1 the state as of the snapshot, so the restored bucket serves what the smoke test
# (app/tools/smoke-staging.mjs) checks today.
set -euo pipefail
cd "$(dirname "$0")/../.."
SRC=r2:bc-fishing-regulations-staging
BK=r2:bc-fishing-regulations-staging-backup
F=(--s3-no-check-bucket)
fails=0
fail() { echo "FAIL $*"; fails=$((fails + 1)); }
day=${1:-$(rclone lsf "$BK/daily/" --dirs-only "${F[@]}" | sort | tail -1 | tr -d /)}
[[ -n "$day" ]] || { echo "no daily/ snapshot in $BK"; exit 1; }
stamp=$(date -u +%Y%m%dT%H%M%SZ)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/restore-test-XXXX"); trap 'rm -rf "$tmp"' EXIT

echo "== 1. R2 state: daily/$day → restore-test/$stamp/ → $tmp/state"
rclone copy "$BK/daily/$day" "$BK/restore-test/$stamp" "${F[@]}" --transfers 16
rclone copy "$BK/restore-test/$stamp" "$tmp/state" "${F[@]}" --transfers 16
python3 -I - "$tmp/state" <<'EOF' || fails=$((fails + 1))
import hashlib, json, os, sys
root = sys.argv[1]
man = json.load(open(os.path.join(root, "_manifest.json")))["objects"]
bad = [k for k, v in man.items() if not os.path.exists(os.path.join(root, k))
       or hashlib.sha256(open(os.path.join(root, k), "rb").read()).hexdigest() != v["sha256"]]
extra = [os.path.relpath(os.path.join(d, f), root) for d, _, fs in os.walk(root) for f in fs]
extra = [k for k in extra if k != "_manifest.json" and k not in man]
print(f"{'PASS' if not bad and not extra else 'FAIL'} {len(man)} objects restored, sha256 = manifest"
      + (f"; MISMATCH {bad[:5]}" if bad else "") + (f"; NOT IN MANIFEST {extra[:5]}" if extra else ""))
sys.exit(1 if bad or extra else 0)
EOF
same=$(rclone check "$tmp/state" "$SRC" --one-way --exclude _manifest.json "${F[@]}" 2>&1 | grep -oE '[0-9]+ matching files' || true)
diff=$(rclone check "$tmp/state" "$SRC" --one-way --exclude _manifest.json "${F[@]}" 2>&1 | grep -oE '[0-9]+ differences found' || echo "0 differences found")
echo "info vs the live bucket now: ${same:-? matching files}, $diff (feeds written since the snapshot)"

echo "== 2. curated archive"
arc=$(rclone lsf "$BK/curated/" --include '*.tar.gz' "${F[@]}" | sort | tail -1)
if [[ -z "$arc" ]]; then fail "no curated/*.tar.gz in $BK"; else
  rclone copy "$BK/curated/$arc" "$tmp/c" "${F[@]}"; rclone copy "$BK/curated/$arc.sha256" "$tmp/c" "${F[@]}"
  if ( cd "$tmp/c" && shasum -a 256 -c "$arc.sha256" >/dev/null ); then echo "PASS $arc matches its .sha256"; else fail "$arc ≠ its .sha256"; fi
  tar -C "$tmp/c" -xzf "$tmp/c/$arc"
  d="$tmp/c/${arc%.tar.gz}"
  n=$(wc -l < "$d/MANIFEST.sha256" | tr -d ' ')
  if ( cd "$d" && shasum -a 256 -c MANIFEST.sha256 >/dev/null ); then echo "PASS $n files inside match MANIFEST.sha256 ($(head -1 "$d/GIT.txt"))"; else fail "files inside $arc do not match its MANIFEST.sha256"; fi
  ch=$(diff -rq "$d/data/curated" data/curated -x __pycache__ 2>/dev/null | wc -l | tr -d ' ' || true)
  echo "info vs data/curated on disk now: $ch file(s) differ"
fi

echo "== 3. release"
if out=$(rclone check "$SRC/release" "$BK/release" --one-way "${F[@]}" --checkers 32 2>&1); then
  echo "PASS $(echo "$out" | grep -oE '[0-9]+ matching files' | head -1) — the live release/ is in the backup, size + MD5"
else fail "release backup: $(echo "$out" | grep -E 'differences|ERROR' | head -3 | tr '\n' ' ')"; fi

echo; [[ $fails == 0 ]] && echo "restore test ok (daily/$day, scratch prefix restore-test/$stamp/)" || { echo "RESTORE TEST FAILED: $fails"; exit 1; }
