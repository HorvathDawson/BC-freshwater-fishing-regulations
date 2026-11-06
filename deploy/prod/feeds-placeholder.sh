#!/bin/bash
# PRODUCTION — THE OWNER RUNS THIS. Creates the prod feeds worker `bc-fishing-feeds` as a 503 placeholder and sets the
# secrets the real worker will need, so T404's switch is a code deploy only. Each `secret put` prompts for its value.
#   CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds-placeholder.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
[[ "${CONFIRM_PROD:-}" == bc-fishing-feeds ]] || { echo "set CONFIRM_PROD=bc-fishing-feeds to create the prod placeholder" >&2; exit 2; }
W=(npx --prefix app wrangler)
C=deploy/prod/feeds-placeholder.wrangler.toml
"${W[@]}" deploy -c "$C"
# the staging worker's secret names (wrangler secret list -c app/workers/feeds/wrangler.toml); prod gets its OWN values:
#   GITHUB_DISPATCH_TOKEN  the same fine-grained token is fine (repo-only, Actions write), or a second one
#   RUN_TOKEN              a NEW random value (openssl rand -hex 24), never staging's
#   REPORT_SALT            a NEW random value (openssl rand -hex 32): report ids must not match staging's
#   NOTIFY_EMAIL_TO        your verified Email Routing address
for s in GITHUB_DISPATCH_TOKEN RUN_TOKEN REPORT_SALT NOTIFY_EMAIL_TO; do
  echo "== $s"; "${W[@]}" secret put "$s" -c "$C"
done
"${W[@]}" secret list -c "$C"
