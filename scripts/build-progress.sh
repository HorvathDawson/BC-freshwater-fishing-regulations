#!/usr/bin/env sh
# Where the province rebuild has got to.
#   sh scripts/build-progress.sh          one look
#   sh scripts/build-progress.sh -f       follow it live
#   BUILD_LOG=... sh scripts/build-progress.sh
#
# The default used to be a hard-coded scratchpad path from whichever session wrote it, which
# is dead the moment that session ends. It now takes the newest build log it can find.
LOG="${BUILD_LOG:-$(ls -t /tmp/build*.log ./build*.log ./output/v2/*.log 2>/dev/null | head -1)}"
[ -n "$LOG" ] && [ -f "$LOG" ] || { echo "no build log; set BUILD_LOG=<path>"; exit 1; }
echo "log: $LOG"

if [ "$1" = "-f" ]; then exec tail -f "$LOG"; fi

printf '\n== split sources (all four must appear) ==\n'
grep -E "curated splits:|gauge cuts:|added streams:|curated lake polygon|area '" "$LOG" | tail -8
printf '\n== stages, slowest last ==\n'
grep -E "^\s+\[.*: [0-9.]+s\]" "$LOG" | tail -12
printf '\n== last line ==\n'
tail -1 "$LOG"
printf '\n== running? ==\n'
if pgrep -f "pipeline.atlas.build|pipeline.deliver.tiles" >/dev/null 2>&1; then
  ps -o pid,etime,%cpu,rss,command -p "$(pgrep -f 'pipeline.atlas.build|pipeline.deliver.tiles' | head -1)" \
    | sed 's/\(.\{140\}\).*/\1…/'
else
  echo "not running"
  ls -lh output/tiles/atlas.pmtiles 2>/dev/null && echo "(tiles present)"
fi
printf '\n'
