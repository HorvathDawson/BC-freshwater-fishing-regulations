#!/bin/bash
# T246 — the Mac's half of the backups (the feeds Worker's daily cron does the R2 state: app/workers/feeds/src/backup.ts).
# STAGING ONLY: the source and backup bucket names are hardcoded.  Needs the rclone remote `r2:`.
#
#   deploy/staging/backup.sh curated   data/curated/** (incl. the review marks AS THEY ARE ON DISK — marks.jsonl is often
#                                      uncommitted, so git alone does not cover it) → one dated tar.gz with a sha256 of
#                                      every file inside (MANIFEST.sha256) and the git commit it sits on; kept in
#                                      $BACKUP_DIR (default ~/canifishthis-backups, outside the repo) AND uploaded to
#                                      r2:bc-fishing-regulations-staging-backup/curated/ (365-day lifecycle)
#   deploy/staging/backup.sh release   r2 staging release/ → backup release/ (server-side copy; content-named, immutable:
#                                      only what is new is copied; no expiry — prune by hand with old releases)
#   deploy/staging/backup.sh all       both
# Committed data is in git (nothing under data/curated is git-ignored — checked by `curated` below).
set -euo pipefail
cd "$(dirname "$0")/../.."
SRC=r2:bc-fishing-regulations-staging
BK=r2:bc-fishing-regulations-staging-backup
DIR=${BACKUP_DIR:-$HOME/canifishthis-backups}
what=${1:-all}

curated() {
  ign=$(git ls-files --others --ignored --exclude-standard data/curated | head -5)
  [[ -z "$ign" ]] || echo "WARN: git-ignored files under data/curated (not in git, only in this backup): $ign"
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  tmp=$(mktemp -d); trap 'rm -rf "$tmp"' RETURN
  mkdir -p "$DIR" "$tmp/curated-$stamp"
  rsync -a --exclude '__pycache__' data/curated/ "$tmp/curated-$stamp/data/curated/"
  ( cd "$tmp/curated-$stamp" && find data -type f -print0 | LC_ALL=C sort -z | xargs -0 shasum -a 256 > MANIFEST.sha256 )
  { echo "commit $(git rev-parse HEAD)"; echo "branch $(git rev-parse --abbrev-ref HEAD)"; echo "at $stamp";
    echo "uncommitted:"; git status --porcelain -- data/curated | sed 's/^/  /'; } > "$tmp/curated-$stamp/GIT.txt"
  tar -C "$tmp" -czf "$DIR/curated-$stamp.tar.gz" "curated-$stamp"
  ( cd "$DIR" && shasum -a 256 "curated-$stamp.tar.gz" > "curated-$stamp.tar.gz.sha256" )
  rclone copy "$DIR/curated-$stamp.tar.gz" "$BK/curated/" --s3-no-check-bucket
  rclone copy "$DIR/curated-$stamp.tar.gz.sha256" "$BK/curated/" --s3-no-check-bucket
  echo "curated: $DIR/curated-$stamp.tar.gz ($(du -h "$DIR/curated-$stamp.tar.gz" | cut -f1), $(wc -l < "$tmp/curated-$stamp/MANIFEST.sha256" | tr -d ' ') files) + $BK/curated/"
}
release() {
  rclone copy "$SRC/release" "$BK/release" --s3-no-check-bucket --transfers 16 --checkers 32
  echo "release: $(rclone size "$BK/release" --s3-no-check-bucket | tr '\n' ' ')"
}
case "$what" in
  curated) curated ;;
  release) release ;;
  all) curated; release ;;
  *) echo "usage: $0 curated|release|all" >&2; exit 2 ;;
esac
