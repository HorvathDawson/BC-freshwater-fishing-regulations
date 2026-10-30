#!/usr/bin/env bash
#
# hydat-fetch.sh — (T301) ONE HYDAT release, downloaded RESUMABLY and CHECKED, then extracted.
#
#   scripts/hydat-fetch.sh <release YYYYMMDD> <work dir> <dest sqlite3>
#
#   <work dir>/Hydat_sqlite3_<release>.zip     the download; a rerun resumes it (curl -C -), never starts over
#   <work dir>/Hydat_sqlite3_<release>.sha256  sha256 of the zip (ECCC publishes no checksum: this is OUR record of
#                                              the bytes the products were built from)
#   <dest sqlite3> + <dest>.release            the extracted archive and its release marker (what every consumer reads)
#
# CHECKS (any failure exits non-zero and leaves <dest> untouched): the zip's size equals the listing's Content-Length,
# every member's CRC passes (`unzip -t`), and SQLite's `PRAGMA quick_check` says ok on the extract before it replaces
# <dest>. Used by .github/workflows/feeds-hydat.yml; runs the same on the Mac.
set -euo pipefail
[ $# -eq 3 ] || { echo "usage: scripts/hydat-fetch.sh <release YYYYMMDD> <work dir> <dest sqlite3>" >&2; exit 2; }
REL=$1; WORK=$2; DEST=$3
[[ $REL =~ ^[0-9]{8}$ ]] || { echo "hydat-fetch: release must be YYYYMMDD (got $REL)" >&2; exit 2; }
URL="https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/Hydat_sqlite3_${REL}.zip"
UA="BC-FishRegs-DataFetch/1.0"
ZIP="$WORK/Hydat_sqlite3_${REL}.zip"
mkdir -p "$WORK" "$(dirname "$DEST")"

want=$(curl -sSfIL -A "$UA" "$URL" | tr -d '\r' | awk 'tolower($1)=="content-length:" {n=$2} END {print n}')
[ -n "$want" ] || { echo "hydat-fetch: no Content-Length for $URL" >&2; exit 1; }
size() { [ -f "$1" ] && wc -c < "$1" | tr -d ' ' || echo 0; }

for i in 1 2 3 4 5 6; do
  [ "$(size "$ZIP")" = "$want" ] && break
  echo "hydat-fetch: $(size "$ZIP") of $want bytes; downloading (try $i)"
  curl -sS -fL -C - --retry 5 --retry-delay 10 -A "$UA" -o "$ZIP" "$URL" || true
done
[ "$(size "$ZIP")" = "$want" ] || { echo "hydat-fetch: $ZIP is $(size "$ZIP") bytes, the listing says $want" >&2; exit 1; }

unzip -tq "$ZIP" >/dev/null || { echo "hydat-fetch: $ZIP fails its CRC check; delete it and rerun" >&2; exit 1; }
sha=$( (command -v sha256sum >/dev/null && sha256sum "$ZIP" || shasum -a 256 "$ZIP") | awk '{print $1}')
echo "$sha  $(basename "$ZIP")" > "$WORK/Hydat_sqlite3_${REL}.sha256"

inner=$(unzip -Z1 "$ZIP" | grep -i '\.sqlite3$' | head -1)
[ -n "$inner" ] || { echo "hydat-fetch: no .sqlite3 inside $ZIP" >&2; exit 1; }
unzip -p "$ZIP" "$inner" > "$DEST.part"
[ "$(sqlite3 "$DEST.part" 'PRAGMA quick_check;')" = ok ] || { rm -f "$DEST.part"; echo "hydat-fetch: extract fails quick_check" >&2; exit 1; }
mv "$DEST.part" "$DEST"
printf %s "$REL" > "${DEST%.*}.release"
echo "hydat-fetch: HYDAT $REL -> $DEST ($(size "$DEST") bytes; zip sha256 $sha)"
