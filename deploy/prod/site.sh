#!/bin/bash
# PRODUCTION — THE OWNER RUNS THIS (agents never deploy prod). Assemble app/dist/web-prod (the production web export) and
# app/dist/seo-prod (build-seo.mjs on the SAME release, `--app app/dist/web-prod --out app/dist/seo-prod`) into
# app/dist/prod-site and deploy it to canifishthis.ca. Unlike deploy/staging/site.sh there are NO staging edits: tools/seo's
# robots.txt stays (Allow + sitemap), no X-Robots-Tag, links stay absolute to https://canifishthis.ca.
#   CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh [--dry-run]
set -euo pipefail
cd "$(dirname "$0")/../.."
[[ "${CONFIRM_PROD:-}" == canifishthis.ca ]] || { echo "site.sh (prod): set CONFIRM_PROD=canifishthis.ca to deploy production" >&2; exit 2; }
W=app/dist/web-prod; S=app/dist/seo-prod; O=app/dist/prod-site; K=app/dist/prod-site-worker; H=app/dist/prod-host-worker
js=$(grep -o '/_expo/static/js/web/index-[0-9a-f]*\.js' "$W/index.html")
one=$(find "$S/water" -name index.html.gz -print -quit)
gunzip -c "$one" | grep -q "$js" || { echo "site.sh: $S's water pages were built against another app export (not $js): rebuild with --app $W" >&2; exit 1; }
# (T304) the app focuses the region a water page names; a page without `cift:region` is from an older SEO build (every
# landing would read the whole files AND report it): rebuild the pages, not just relink them
gunzip -c "$one" | grep -q '<meta name="cift:region"' || { echo "site.sh: $S's water pages carry no cift:region (built before T304): run build-seo again" >&2; exit 1; }
grep -q 'X-Robots-Tag' "$S/_headers" && { echo "site.sh: $S/_headers carries X-Robots-Tag: not a production build" >&2; exit 1; }
grep -q '^Sitemap: https://canifishthis.ca/sitemap.xml' "$S/robots.txt" || { echo "site.sh: $S/robots.txt names no production sitemap" >&2; exit 1; }
rm -rf "$O" "$K" "$H"; mkdir -p "$O" "$K" "$H"
cp -R "$W/." "$O/"
X=(--exclude /_worker.js --exclude /host-worker.js --exclude /budget.json --exclude /lighthouserc.json --exclude /snapshot.json
   --exclude /slug-map.json --exclude /legacy-redirects.json --exclude /redirects.json)
comm -12 <(cd "$W" && find . -type f | sort) <(cd "$S" && find . -type f | sort) | sed 's/^/clash (app kept): /'
rsync -a --ignore-existing "${X[@]}" "$S/" "$O/"
cp "$S/_worker.js" "$K/worker.js"
cp "$S/host-worker.js" "$H/worker.js"
echo "files: $(find "$O" -type f | wc -l)"
npx --prefix app wrangler deploy -c deploy/prod/site.wrangler.toml "$@"
echo "deployed. Optional host redirect (see deploy/prod/host.wrangler.toml): npx --prefix app wrangler deploy -c deploy/prod/host.wrangler.toml"
