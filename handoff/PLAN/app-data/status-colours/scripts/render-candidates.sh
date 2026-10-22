#!/usr/bin/env bash
# T47: render each candidate on the same views (shots.mjs) and its regulations card (card-shots.mjs; CARDS=0 skips it),
# one Expo web server per candidate, then put today's palette back. VIEWS=<id,...> / ONLY=<ground,...> narrow shots.mjs.
#
#   TILE_PORT=39481 APP_PORT=39482 bash handoff/PLAN/app-data/status-colours/scripts/render-candidates.sh A B C
#
# Needs the data server already up on TILE_PORT (`TILE_PORT=39481 node app/tools/serve-tiles.mjs`, read-only). For each
# candidate: apply-palette.mjs -> `node tools/build-style.mjs` -> start Expo web on APP_PORT (this worktree, CI mode, the
# palette baked in) -> shots.mjs into candidates/<id>/ -> stop THAT Expo process (its own PID only) -> restore.
# Never use the shared development ports (39217/39218).
set -u
ROOT="$(cd "$(dirname "$0")/../../../../.." && pwd)"
S="$ROOT/handoff/PLAN/app-data/status-colours/scripts"
OUT="$ROOT/handoff/PLAN/app-data/status-colours/candidates"
TILE_PORT="${TILE_PORT:-39481}"; APP_PORT="${APP_PORT:-39482}"
case "$APP_PORT$TILE_PORT" in *39217*|*39218*) echo "refusing the shared ports"; exit 2;; esac
EXPO="$ROOT/app/node_modules/expo/bin/cli"
PID=""
stop() { [ -n "$PID" ] && kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""; }
finish() { stop; node "$S/apply-palette.mjs" --restore >/dev/null; (cd "$ROOT/app" && node tools/build-style.mjs >/dev/null 2>&1); }
trap finish EXIT
for id in "$@"; do
  echo "== $id"
  node "$S/apply-palette.mjs" "$id" >/dev/null || exit 1   # baseline too: the theme files hold D since part 2
  (cd "$ROOT/app" && node tools/build-style.mjs 2>&1 | tail -1)
  (cd "$ROOT/app/apps/mobile" && EXPO_PUBLIC_DATA_BASE="http://localhost:$TILE_PORT" CI=1 exec node "$EXPO" start --web --port "$APP_PORT" \
     > "${TMPDIR:-/tmp}/t47-expo-$id.log" 2>&1) &
  PID=$!
  for _ in $(seq 1 120); do curl -s -o /dev/null "http://localhost:$APP_PORT/" && break; sleep 2; done
  node "$S/shots.mjs" "$id" "$OUT/$id" "http://localhost:$APP_PORT/"
  [ -n "${CARDS:-1}" ] && [ "${CARDS:-1}" != 0 ] && node "$S/card-shots.mjs" "$id" "$OUT/$id" "http://localhost:$APP_PORT/"
  stop
  node "$S/apply-palette.mjs" --restore >/dev/null
done
