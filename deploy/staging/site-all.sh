#!/bin/bash
# STAGING ONLY. THE ONE COMMAND that redeploys the staging site from a clean tree (T544):
#
#   deploy/staging/site-all.sh [--relink] [--dry-run]
#
#   1. cheap checks first (seconds): LATEST names a release folder with a manifest, the /changes page fits its budget
#      (build-seo --check-changes), the deploy scripts parse. Any failure stops here, before the 15 minutes.
#   2. web export (`export:web:staging`), under scripts/heavy.sh.
#   3. SEO pages on LATEST (`build-seo.mjs --app app/dist/web-staging`), under scripts/heavy.sh; or with --relink (only the
#      app changed, the SEO pages were built on LATEST already) the stored pages are pointed at the new export's assets.
#   4. deploy/staging/site.sh (merge, noindex, stamp /site-release.json, deploy, then the staging smoke, which prints the
#      site's release and the manifest's). --dry-run goes through to wrangler: nothing is deployed, no smoke runs.
#
# HEAVY_LOCK=<path> picks the heavy-job lane (default: heavy.sh's own). Nothing here touches production.
set -euo pipefail
cd "$(dirname "$0")/../.."
RELINK=0; PASS=()
for a in "$@"; do case "$a" in --relink) RELINK=1 ;; *) PASS+=("$a") ;; esac; done
R=$(tr -d '[:space:]' < data/release/LATEST)
D=data/release/$R
W=app/dist/web-staging; S=app/dist/seo

echo "site-all: release $R (LATEST)"
# 1. cheap checks ------------------------------------------------------------------------------------------------------
[[ -f $D/manifest.json ]] || { echo "site-all: $D/manifest.json is missing (LATEST names a release that is not here)" >&2; exit 1; }
for f in deploy/staging/site.sh deploy/staging/site-all.sh; do bash -n "$f"; done
node app/tools/build-seo.mjs --release-dir "$D" --check-changes
if [[ $RELINK == 1 ]]; then
  [[ -f $S/snapshot.json ]] || { echo "site-all: --relink needs the SEO pages in $S (run without --relink)" >&2; exit 1; }
  built=$(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).release)' "$S/snapshot.json")
  [[ $built == "$R" ]] || { echo "site-all: --relink: $S was built on $built, LATEST is $R: run without --relink" >&2; exit 1; }
  OLD=$(mktemp -d); cp app/dist/staging-site/index.html "$OLD/index.html"   # the last deployed export's asset names
fi

# 2. export -------------------------------------------------------------------------------------------------------------
scripts/heavy.sh site-export 50 -- pnpm -C app/apps/mobile export:web:staging
# (T535) that heavy.sh ran fast-fail for this HEAD (or HEAVY_NO_FAST_FAIL says why not): the SEO step's need not again
FAST_FAIL_DONE=${FAST_FAIL_DONE:-$(git rev-parse HEAD)}; export FAST_FAIL_DONE

# 3. SEO pages ----------------------------------------------------------------------------------------------------------
if [[ $RELINK == 1 ]]; then
  node deploy/staging/relink-seo.mjs "$S" "$OLD/index.html" "$W/index.html"
else
  scripts/heavy.sh site-seo 50 -- node app/tools/build-seo.mjs --release-dir "$D" --app "$W" --threads 4
fi

# 4. merge, stamp, deploy, smoke ----------------------------------------------------------------------------------------
deploy/staging/site.sh ${PASS[@]+"${PASS[@]}"}
