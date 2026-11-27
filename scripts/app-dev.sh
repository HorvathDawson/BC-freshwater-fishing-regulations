#!/usr/bin/env bash
# `make app` (T1193): the app in a browser with local tiles — `pnpm -C app tiles` and `pnpm -C app dev:web` together.
#
#   bash scripts/app-dev.sh          then open http://localhost:39218 (Ctrl-C stops both)
#
# Never half-working: it refuses to start when something already listens on the tile port (TILE_PORT, default 39217)
# or the web port (39218), naming the PID — it never stops a process it did not start — and when either server it
# started stops, it stops the other. Both run in the background of this script, so Expo's single-key commands (r, w)
# are not read here; run `pnpm -C app dev:web` alone for them.
set -uo pipefail
cd "$(dirname "$0")/.."
TILE_PORT=${TILE_PORT:-39217}
WEB_PORT=39218                      # app/apps/mobile/package.json `web`: expo start --web --port 39218
taken() {  # $1 port, $2 what to say. Taken = something accepts a connection there (bash's /dev/tcp: no tool needed);
  # the PID comes from lsof when the machine has it
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null || return 1
  local pid who="PID unknown: lsof is not installed"
  if command -v lsof >/dev/null 2>&1; then
    pid=$(lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | head -1)
    [[ -n $pid ]] && who="PID $pid ($(ps -o command= -p "$pid" 2>/dev/null | cut -c1-80))"
  fi
  echo "make app: port $1 is taken by $who: $2" >&2
}
taken "$TILE_PORT" "your own tile server is already up: run \`pnpm -C app dev:web\` alone, or stop it" && exit 1
taken "$WEB_PORT" "a web server is already up there: stop it first" && exit 1

tiles="" web=""
stop() { for p in $web $tiles; do kill "$p" 2>/dev/null; done; }   # only the two PIDs started below
trap stop EXIT
trap 'exit 130' INT TERM
pnpm -C app tiles & tiles=$!
pnpm -C app dev:web & web=$!
while kill -0 "$tiles" 2>/dev/null && kill -0 "$web" 2>/dev/null; do sleep 1; done
if ! kill -0 "$tiles" 2>/dev/null; then
  wait "$tiles"; echo "make app: the tile server stopped (rc=$?): stopping the web server too" >&2; exit 1
fi
wait "$web"; rc=$?
echo "make app: the web server stopped (rc=$rc): stopping the tile server too" >&2
exit "$rc"
