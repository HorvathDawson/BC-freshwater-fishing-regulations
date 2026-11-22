#!/bin/bash
# STAGING ONLY: deploy the feeds Worker (bc-fishing-feeds-staging; hardcoded), then RUN EVERY FEED JOB at once, so a
# release's new output is in the feeds now, not at the next cron (owner 2026-11-05: the forecast detail stayed old after a
# worker release). The deployed version is in every job's reuse key (src/io.ts `rev`), so these runs recompute everything.
#   deploy/staging/feeds.sh
# Exit 1 when a job fails (its JSON is printed). Production: the same with its own worker, deploy/prod/feeds.sh (the
# owner's step since 2026-11-22; see its header).
set -euo pipefail
cd "$(dirname "$0")/../.."
URL=https://bc-fishing-feeds-staging.horvath-dawson.workers.dev
# (T431) the worker's own tests first (DFO + gauge parity with the Python, the endpoints, the crons): a red suite deploys
# nothing. The same command as app-ci.yml's "Feeds worker tests".
(cd app/workers/feeds && npx vitest run --root .)
# (T994 #19) the secret list is read ONCE, its exit checked: `2>/dev/null | grep -q` read an expired login as "not set"
# (and `grep -q` could SIGPIPE wrangler under pipefail). Then each required secret is looked for in what it said.
SECRETS=$(npx --prefix app wrangler secret list -c app/workers/feeds/wrangler.toml) || {
  echo "feeds.sh: could not list the staging feeds worker's secrets (wrangler, above: logged in? npx --prefix app wrangler login)" >&2; exit 1; }
# (T431) REPORT_SALT is a required secret (src/reports.ts `noSalt`: without it every report and crash is answered 503)
if ! grep -q 'REPORT_SALT' <<<"$SECRETS"; then
  echo "REPORT_SALT is not set on the staging feeds worker. Set it once (runbooks/secrets.md), then re-run:" >&2
  echo "  openssl rand -hex 32 | npx --prefix app wrangler secret put REPORT_SALT -c app/workers/feeds/wrangler.toml" >&2
  exit 1
fi
# (T558) NOTIFY_EMAIL_TO is a required secret too: without it every owner message is only logged (src/events.ts send)
if ! grep -q 'NOTIFY_EMAIL_TO' <<<"$SECRETS"; then
  echo "NOTIFY_EMAIL_TO is not set on the staging feeds worker: nobody would be told of a failing feed. Set it once:" >&2
  echo "  npx --prefix app wrangler secret put NOTIFY_EMAIL_TO -c app/workers/feeds/wrangler.toml" >&2
  exit 1
fi
C=app/workers/feeds/wrangler.toml
# (T559) the backups keep the privacy page's dates only while the backup bucket's daily/ copies expire within 30 days
# (app/workers/feeds/src/backup.ts DAILY_KEEP_DAYS; personal records are never copied to monthly/)
BK=$(sed -n 's/^BACKUP_BUCKET_NAME = "\(.*\)"/\1/p' "$C")
LC=$(npx --prefix app wrangler r2 bucket lifecycle list "$BK")
DAILY_DAYS=$(printf '%s\n' "$LC" | awk '/^prefix:/ {p=$2} /^action:/ && p=="daily/" && /Expire objects after/ {print $(NF-1); exit}')
[[ -n "$DAILY_DAYS" && "$DAILY_DAYS" -le 30 ]] || {
  echo "the backup bucket $BK has no daily/ expiry of 30 days or less (found: '${DAILY_DAYS:-none}'): the privacy page's 180/90-day" >&2
  echo "promises would not hold in the backups. Set it: npx --prefix app wrangler r2 bucket lifecycle add $BK daily-30d daily/ --expire-days 30" >&2
  exit 1; }
# (T757 item 7) the mirror of upload.sh's pieces gate: a worker whose code does not serve dfo.pieces is refused while
# the LIVE release binds a closed-area piece (it would make the app drop the live feed). A worker that serves the shape passes
# without the release being read (a first deploy has none yet); for one that does not, an unreadable release = refused;
# an unreadable worker shape = refused always
# (T994 #19, kept on purpose) a failed read is NOT hidden: rclone / node say why on stderr, and the empty value it leaves
# is the gate's "could not be read", which refuses unless the worker serves the shape (a first deploy has no release)
LIVE_M=$(mktemp); DEPLOY_LOG=$(mktemp); trap 'rm -f "$LIVE_M" "$DEPLOY_LOG"' EXIT
rclone cat "r2:bc-fishing-regulations-staging/release/manifest.json" --s3-no-check-bucket > "$LIVE_M" || : > "$LIVE_M"
WSHAPE=$(node deploy/lib/worker-shape.mjs) || WSHAPE=
.venv/bin/python -m pipeline.deliver.release.pieces_gate --worker-deploy --live-manifest "$LIVE_M" \
  --releases "${RELEASES_DIR:-data/release}" --worker-shape "$WSHAPE"
# (T994 #19) a failed deploy shows ALL of wrangler's words (only its last 4 lines were shown); a good one, its last 4
npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.toml > "$DEPLOY_LOG" 2>&1 \
  || { awk '!/npm warn/' "$DEPLOY_LOG" >&2; echo "feeds.sh: wrangler deploy failed (above)" >&2; exit 1; }
awk '!/npm warn/' "$DEPLOY_LOG" | tail -4
T=$(tr -d '\n' < app/workers/feeds/.run-token)
sleep 10   # the new version reaches every location
fail=0
for job in gauges dfo inseason; do
  out=$(curl -s -w '\n%{http_code}' -X POST -H "Authorization: Bearer $T" "$URL/run/$job")
  code=${out##*$'\n'}; body=${out%$'\n'*}
  echo "$job: HTTP $code $(printf '%s' "$body" | head -c 300)"
  [[ "$code" == 200 ]] || fail=1
done
exit $fail
