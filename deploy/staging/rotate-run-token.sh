#!/bin/bash
# T249 — ROTATE the feeds Worker's RUN_TOKEN (STAGING ONLY: bc-fishing-feeds-staging; the worker name is hardcoded).
#   deploy/staging/rotate-run-token.sh
# 1. a fresh 48-hex-character token (openssl rand -hex 24)
# 2. `wrangler secret put RUN_TOKEN` on the staging worker (a new version goes live with it; nothing else changes)
# 3. app/workers/feeds/.run-token (git-ignored, mode 600) gets the new value — the one place local tools read it
#    (curation-review/backend/reports.py for GET /reports, and the owner's curl for POST /run/*)
# 4. VERIFY on the live worker: the new token is accepted (POST /notify/preview → 200: reads state, sends nothing) and
#    the old one is refused (403). Exit 1 if either fails — the old token is kept in .run-token.prev until it passes.
# Nothing else holds the token (no GitHub secret, no other worker). Production: the same steps with its own worker
# name, by the owner (T240).
set -euo pipefail
cd "$(dirname "$0")/../.."
WORKER=bc-fishing-feeds-staging
URL=https://bc-fishing-feeds-staging.horvath-dawson.workers.dev
F=app/workers/feeds/.run-token
OLD=$(tr -d '\n' < "$F" 2>/dev/null || true)
NEW=$(openssl rand -hex 24)
umask 077
[[ -n "$OLD" ]] && printf '%s\n' "$OLD" > "$F.prev"
printf '%s' "$NEW" | npx --prefix app wrangler secret put RUN_TOKEN -c app/workers/feeds/wrangler.toml --name "$WORKER" 2>&1 \
  | grep -v "npm warn" | tail -2
printf '%s\n' "$NEW" > "$F"
code() { curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $1" "$URL/notify/preview"; }
# the secret takes a few seconds to reach every location; RL_AUTH allows 10 token calls a minute per IP, so poll gently
ok=""
for i in 1 2 3 4 5 6; do
  c=$(code "$NEW"); if [[ "$c" == 200 ]]; then ok=1; break; fi
  echo "new token: HTTP $c (try $i), waiting"; sleep 8
done
[[ -n "$ok" ]] || { echo "FAIL: the new token is not accepted (HTTP $c); old token kept in $F.prev"; exit 1; }
echo "new token accepted (HTTP 200)"
if [[ -n "$OLD" ]]; then
  c=$(code "$OLD")
  [[ "$c" == 403 ]] || { echo "FAIL: the old token answered HTTP $c (want 403)"; exit 1; }
  echo "old token refused (HTTP 403)"
fi
rm -f "$F.prev"
echo "rotated RUN_TOKEN on $WORKER; value in $F"
