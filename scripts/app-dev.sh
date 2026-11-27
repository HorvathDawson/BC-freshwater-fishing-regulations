#!/usr/bin/env bash
# `make app` (T1193): the app in a browser with local tiles — `pnpm -C app tiles` and `pnpm -C app dev:web` together.
#
#   bash scripts/app-dev.sh          then open http://localhost:39218 (Ctrl-C stops both)
#
# Never half-working: it refuses to start when something already listens on the tile port (TILE_PORT, default 39217)
# or the web port (39218), naming the PID — it never stops a process it did not start — and when either server it
# started stops, it stops the other. Both run in the background of this script, so Expo's single-key commands (r, w)
# are not read here; run `pnpm -C app dev:web` alone for them.
#
# Each server runs as its own process group (`set -m`), and the clean-up stops those two groups and nothing else: the
# `pnpm` wrappers have the real servers as children, so stopping only the wrapper PIDs left them running. Ctrl-C, TERM
# and a hang-up (a closed window) each run the clean-up once.
#
# It also refuses (after the port checks), before starting anything, when the tiles the tile server serves are not built (the same four files as
# app/tools/serve-tiles.mjs ARCHIVES): on a fresh clone run `make build-first` (about 1h15, 24 GB of memory).
#
# Test seam, used only by pipeline/tests/test_makefile.py: APP_DEV_TILES_CMD / APP_DEV_WEB_CMD replace the two pnpm
# commands (and APP_DEV_WEB_PORT the web port); with APP_DEV_TILES_CMD set the built-tiles check is skipped.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
TILE_PORT=${TILE_PORT:-39217}
case $TILE_PORT in ''|*[!0-9]*) echo "make app: TILE_PORT must be a port number, got '$TILE_PORT'" >&2; exit 2;; esac
export TILE_PORT
WEB_PORT=${APP_DEV_WEB_PORT:-39218}   # app/apps/mobile/package.json `web`: expo start --web --port 39218
TILES_CMD=${APP_DEV_TILES_CMD:-pnpm -C app tiles}
WEB_CMD=${APP_DEV_WEB_CMD:-pnpm -C app dev:web}
taken() {  # $1 port, $2 what to say. Taken = something accepts a connection there, on IPv4 or IPv6 (bash's /dev/tcp: no
  # tool needed); the PID comes from lsof when the machine has it
  local h up=""
  for h in 127.0.0.1 ::1; do
    if (exec 3<>"/dev/tcp/$h/$1") 2>/dev/null; then up=1; break; fi
  done
  [[ -n $up ]] || return 1
  local pid who="PID unknown: lsof is not installed"
  if command -v lsof >/dev/null 2>&1; then
    pid=$(lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | head -1)
    [[ -n $pid ]] && who="PID $pid ($(ps -o command= -p "$pid" 2>/dev/null | cut -c1-80))"
  fi
  echo "make app: port $1 is taken by $who: $2" >&2
}
taken "$TILE_PORT" "your own tile server is already up: run \`pnpm -C app dev:web\` alone, or stop it" && exit 1
taken "$WEB_PORT" "a web server is already up there: stop it first" && exit 1

if [[ -z ${APP_DEV_TILES_CMD:-} ]]; then
  for f in data/generated/tiles/atlas.pmtiles data/generated/tiles/atlas.meta.json data/generated/tiles/basemap.pmtiles data/source/bc_outside.geojson; do
    if [[ ! -e $f ]]; then
      echo "make app: $f is not built, so the map would 404: run \`make build-first\` first (a fresh clone: about 1h15 and 24 GB of memory)" >&2
      exit 1
    fi
  done
fi

tiles="" web=""
# Stop the two process groups started below (a group's id is its first process's PID, `set -m`); never anything else.
stop() { local p; for p in $web $tiles; do kill -TERM -- -"$p" 2>/dev/null; done; }
trap stop EXIT
trap 'exit 130' INT TERM
trap 'exit 129' HUP
set -m
bash -c "$TILES_CMD" & tiles=$!
bash -c "$WEB_CMD" & web=$!
while kill -0 "$tiles" 2>/dev/null && kill -0 "$web" 2>/dev/null; do sleep 1; done
if ! kill -0 "$tiles" 2>/dev/null; then
  wait "$tiles"; echo "make app: the tile server stopped (rc=$?): stopping the web server too" >&2; exit 1
fi
wait "$web"; rc=$?
echo "make app: the web server stopped (rc=$rc): stopping the tile server too" >&2
exit "$rc"
