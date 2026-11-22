#!/bin/bash
# PRODUCTION. WHO RUNS IT: THE OWNER (owner 2026-11-22 on CONFIRM_PROD: "only me unless i say otherwise that time").
# Claude prepares it (the tests, `--dry-run`) and hands over the command; it runs it only when the owner says so, that
# one time. A release step after the production data and the matching website (before the data only when the release
# binds closed-area pieces the production worker does not serve yet).
# The twin of deploy/staging/feeds.sh: the worker's
# tests, then deploy app/workers/feeds with wrangler.prod.toml (bc-fishing-feeds: prod bucket, backup bucket, crons,
# FEEDS_ENV=production) OVER the placeholder (its secrets stay), then run every feed job once.
#   CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh
#   deploy/prod/feeds.sh --dry-run      (tests + `wrangler deploy --dry-run`: builds the bundle locally, deploys nothing,
#                                        needs no confirmation)
# The run token is the PRODUCTION one (deploy/prod/feeds-placeholder.sh asked for it), kept in app/workers/feeds/.run-token.prod
# (gitignored); never staging's.
set -euo pipefail
cd "$(dirname "$0")/../.."
NAME=bc-fishing-feeds
URL=https://feeds.canifishthis.ca        # the custom domain (Q171); workers.dev is off
C=app/workers/feeds/wrangler.prod.toml
DRY=; [[ " $* " == *" --dry-run "* ]] && DRY=1
[[ -n $DRY || "${CONFIRM_PROD:-}" == "$NAME" ]] || { echo "feeds.sh (prod): set CONFIRM_PROD=$NAME to deploy production" >&2; exit 2; }
grep -q "^name = \"$NAME\"$" "$C" || { echo "feeds.sh (prod): $C does not name $NAME" >&2; exit 1; }
# the worker's own tests first (they include test/config.test.ts: this file is staging's with production's names)
(cd app/workers/feeds && npx vitest run --root .)
if [[ -n $DRY ]]; then
  OUT=$(mktemp -d)
  npx --prefix app wrangler deploy -c "$C" --dry-run --outdir "$OUT"
  echo "dry run: bundle built in $OUT, nothing deployed; a real run then POSTs /run/gauges, /run/dfo, /run/inseason on $URL"
  exit 0
fi
# (T994 #19) the secret list is read ONCE, its exit checked: an expired login read as "not set" (and `grep -q` could
# SIGPIPE wrangler under pipefail). Then each required secret is looked for in what it said.
SECRETS=$(npx --prefix app wrangler secret list -c "$C") || {
  echo "feeds.sh (prod): could not list $NAME's secrets (wrangler, above: logged in? npx --prefix app wrangler login)" >&2; exit 1; }
# REPORT_SALT is required (no salt = every report and crash answered 503); the placeholder script set it
grep -q 'REPORT_SALT' <<<"$SECRETS" || {
  echo "REPORT_SALT is not set on $NAME: CONFIRM_PROD=$NAME deploy/prod/feeds-placeholder.sh (or wrangler secret put REPORT_SALT -c $C)" >&2; exit 1; }
# (T558) NOTIFY_EMAIL_TO is required: without it production keeps every owner message unsent (src/events.ts send)
grep -q 'NOTIFY_EMAIL_TO' <<<"$SECRETS" || {
  echo "NOTIFY_EMAIL_TO is not set on $NAME: npx --prefix app wrangler secret put NOTIFY_EMAIL_TO -c $C" >&2; exit 1; }
# (T559) the backups keep the privacy page's dates only while the backup bucket's daily/ copies expire within 30 days
# (app/workers/feeds/src/backup.ts DAILY_KEEP_DAYS; personal records are never copied to monthly/)
BK=$(sed -n 's/^BACKUP_BUCKET_NAME = "\(.*\)"/\1/p' "$C")
LC=$(npx --prefix app wrangler r2 bucket lifecycle list "$BK")
DAILY_DAYS=$(printf '%s\n' "$LC" | awk '/^prefix:/ {p=$2} /^action:/ && p=="daily/" && /Expire objects after/ {print $(NF-1); exit}')
[[ -n "$DAILY_DAYS" && "$DAILY_DAYS" -le 30 ]] || {
  echo "the backup bucket $BK has no daily/ expiry of 30 days or less (found: '${DAILY_DAYS:-none}'): the privacy page's 180/90-day" >&2
  echo "promises would not hold in the backups. Set it: npx --prefix app wrangler r2 bucket lifecycle add $BK daily-30d daily/ --expire-days 30" >&2
  exit 1; }
TOKF=app/workers/feeds/.run-token.prod
[[ -s $TOKF ]] || { echo "no $TOKF: write the production RUN_TOKEN there (the value given to feeds-placeholder.sh)" >&2; exit 1; }
# (T757 item 7) the mirror of upload.sh's pieces gate: a worker whose code does not serve dfo.pieces is refused while
# production's LIVE release binds a closed-area piece (it would make the app drop the live feed). A worker that serves the shape passes
# without the release being read (a first deploy has none yet); for one that does not, an unreadable release = refused;
# an unreadable worker shape = refused always
# (T994 #19, kept on purpose) a failed read is NOT hidden: rclone / node say why on stderr, and the empty value it leaves
# is the gate's "could not be read", which refuses unless the worker serves the shape (a first deploy has no release)
LIVE_M=$(mktemp); trap 'rm -f "$LIVE_M"' EXIT
rclone cat "r2:bc-fishing-regulations/release/manifest.json" --s3-no-check-bucket > "$LIVE_M" || : > "$LIVE_M"
WSHAPE=$(node deploy/lib/worker-shape.mjs) || WSHAPE=
.venv/bin/python -m pipeline.deliver.release.pieces_gate --worker-deploy --live-manifest "$LIVE_M" \
  --releases "${RELEASES_DIR:-data/release}" --worker-shape "$WSHAPE"
npx --prefix app wrangler deploy -c "$C"
T=$(tr -d '\n' < "$TOKF")
sleep 10   # the new version reaches every location
fail=0
for job in gauges dfo inseason; do
  out=$(curl -s -w '\n%{http_code}' -X POST -H "Authorization: Bearer $T" "$URL/run/$job")
  code=${out##*$'\n'}; body=${out%$'\n'*}
  echo "$job: HTTP $code $(printf '%s' "$body" | head -c 300)"
  [[ "$code" == 200 ]] || fail=1
done
echo "status: $URL/status"
exit $fail
