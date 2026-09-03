#!/usr/bin/env sh
# Where the province rebuild + tile build has got to.
#   sh scripts/build-progress.sh          one look
#   sh scripts/build-progress.sh -f       follow it live
LOG="${BUILD_LOG:-/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/e5ec6ad4-f0aa-4fd2-8c45-a6666319c6c8/scratchpad/province2.log}"
[ -f "$LOG" ] || { echo "no log at $LOG"; exit 1; }

if [ "$1" = "-f" ]; then exec tail -f "$LOG"; fi

printf '\n== stage ==\n'
grep -E "^\s{2}(minted|area '|area membership|management units|wrote|loading|stream |lake |wetland |under_lake |place |park |mu )" "$LOG" | tail -14
printf '\n== timings so far ==\n'
grep -E "^\s{2}\S.*[0-9]+\.[0-9]s$" "$LOG" | tail -8
printf '\n== last line ==\n'
tail -1 "$LOG"
printf '\n== running? ==\n'
if pgrep -f "pipeline.atlas.build --full|pipeline.deliver.tiles" >/dev/null 2>&1; then
  ps -o pid,etime,%cpu,rss,command -p "$(pgrep -f 'pipeline.atlas.build --full|pipeline.deliver.tiles' | head -1)" \
    | sed 's/\(.\{140\}\).*/\1…/'
else
  echo "not running"
  ls -lh output/tiles/atlas.pmtiles 2>/dev/null && echo "(tiles present)"
fi
printf '\n'
