# shellcheck shell=bash
# SOURCED by deploy/staging/sync-inputs.sh and deploy/prod/sync-inputs.sh: one copy, so the two cannot drift.
#
# (T597 item 2) AN INPUT IN A FORMAT THE DEPLOYED WORKER DOES NOT LIST IS REFUSED, NEVER PUT. On 2026-11-09 matches.json
# moved to inseason-links/3 before the worker that reads it was deployed, and the in-season job failed until the deploy.
# The feeds worker serves the formats it reads on /status.json `inputs` (app/workers/feeds/src/status.ts INPUT_FORMATS;
# on its 503 too). A worker that cannot be asked (unreachable, or one from before the list) refuses every input that
# names a format; an input with no format (the DFO entry files, the roster) is not checked.
#
#   input_formats_filter <manifest.tsv> <staged dir> <put.tsv>
#
# reads `<key>\t<cache-control|->` lines, writes the ones that may be put to <put.tsv>, and calls `refuse "<key>: why"`
# for each of the others. The caller sets:
#   FEEDS_URL      the worker that reads the bucket being written (empty: none named, every formatted input refused)
#   DEPLOY_HINT    the command that deploys it (said in a refusal)
#   refuse()       records the refusal (the caller exits non-zero after putting the rest)
#   NO_WORKER_YET  "1": the caller's explicit "no feeds worker is deployed for this bucket yet" (production's first
#                  seed, before its worker exists: there is no reader to break). Honoured ONLY when FEEDS_URL gives no
#                  HTTP answer at all, or the production PLACEHOLDER answers (deploy/prod/feeds-placeholder/worker.js:
#                  every request "feeds (production) is not live yet"). A real worker that answers is asked, whatever
#                  the flag says. Staging never sets it.
PLACEHOLDER_SAYS="feeds (production) is not live yet"
input_formats_filter() {
  local manifest=$1 out=$2 put=$3 key cache fmt accept="" asked=0 absent=0
  : > "$put"
  while IFS=$'\t' read -r key cache; do
    fmt=$(jq -r 'if type == "object" then (.format // "") else "" end' "$out/$key" 2>/dev/null || true)
    if [[ -n "$fmt" ]]; then
      if [[ -z "${FEEDS_URL:-}" ]]; then
        refuse "$key: no feeds worker was named for this bucket (set FEEDS_URL to the worker that reads it): $fmt not put"
        continue
      fi
      if [[ $asked == 0 ]]; then
        asked=1
        accept=$(curl -sS --max-time 20 "$FEEDS_URL/status.json" 2>/dev/null | jq -c '.inputs // empty' 2>/dev/null || true)
        if [[ -z "$accept" && "${NO_WORKER_YET:-}" == 1 ]]; then
          # no HTTP answer of any kind (no such host, refused, timed out: curl fails), or the placeholder's one line.
          # Any other answer means a worker is deployed there, and the claim does not apply.
          local said prc=0
          said=$(curl -sS --max-time 20 "$FEEDS_URL/status.json" 2>/dev/null) || prc=$?
          if [[ $prc != 0 || "$said" == "$PLACEHOLDER_SAYS" ]]; then
            absent=1
            echo "input formats: NOT CHECKED — no feeds worker reads this bucket yet ($FEEDS_URL: $([[ $prc != 0 ]] && echo "no answer" || echo "the placeholder")). Deploy it next ($DEPLOY_HINT): it refuses a format it does not read and says so on its /status" >&2
          else
            echo "input formats: \"no worker yet\" was claimed, but $FEEDS_URL answers and is not the placeholder: a worker is deployed there, so its list decides" >&2
          fi
        fi
      fi
      if [[ $absent == 1 ]]; then
        printf '%s\t%s\n' "$key" "$cache" >> "$put"
        continue
      fi
      if [[ -z "$accept" ]]; then
        refuse "$key: $FEEDS_URL/status.json does not say which input formats the worker reads (unreachable, or deployed before T597): $fmt not put. Deploy the worker ($DEPLOY_HINT), then sync again"
        continue
      fi
      if ! jq -e --arg k "$key" --arg f "$fmt" '(.[$k] // []) | index($f) != null' <<<"$accept" >/dev/null; then
        refuse "$key: $fmt, but the deployed worker reads $(jq -r --arg k "$key" '(.[$k] // ["no file at this key"]) | join(", ")' <<<"$accept"): not put. Deploy the worker that reads $fmt first ($DEPLOY_HINT), then sync again"
        continue
      fi
    fi
    printf '%s\t%s\n' "$key" "$cache" >> "$put"
  done < "$manifest"
}
