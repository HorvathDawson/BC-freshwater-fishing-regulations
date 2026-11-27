#!/bin/bash
# PRODUCTION. WHO RUNS IT: THE OWNER (owner 2026-11-22 on CONFIRM_PROD: "only me unless i say otherwise that time").
# Claude prepares it (the full CI tier, the staging smoke, the two builds below, `--dry-run`) and hands over the command;
# it runs it only when the owner says so, that one time. Deployed with each app version so the two match.
# Assemble app/dist/web-prod (the production web export) and
# app/dist/seo-prod (build-seo.mjs on the SAME release, `--app app/dist/web-prod --out app/dist/seo-prod`) into
# app/dist/prod-site and deploy it to canifishthis.ca. Unlike deploy/staging/site.sh there are NO staging edits: tools/seo's
# robots.txt stays (Allow + sitemap), no X-Robots-Tag, links stay absolute to https://canifishthis.ca.
#   CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh
#   deploy/prod/site.sh --dry-run     (T404: assembles app/dist/prod-site and runs `wrangler deploy --dry-run`, which
#                                      uploads nothing — no confirmation needed)
set -euo pipefail
cd "$(dirname "$0")/../.."
DRY=; [[ " $* " == *" --dry-run "* ]] && DRY=1
[[ -n $DRY || "${CONFIRM_PROD:-}" == canifishthis.ca ]] || { echo "site.sh (prod): set CONFIRM_PROD=canifishthis.ca to deploy production" >&2; exit 2; }
W=app/dist/web-prod; S=app/dist/seo-prod; O=app/dist/prod-site; K=app/dist/prod-site-worker; H=app/dist/prod-host-worker
# (2026-11-14) PRODUCTION IS BUILT FROM A COMMIT: run this, and the two builds before it (docs/runbooks/release-by-hand.md Step 7 item 5), in a tree
# whose app/ has no uncommitted edits (a clean worktree: `git worktree add --detach <dir> <sha>`). A dirty app/ is refused
# unless SITE_DIRTY_OK=<reason>, printed. (T788 #4) The commit stamped in /site-release.json is the EXPORT's (read from its
# bundle), not HEAD at deploy time; the dirty flag is app/ now. One copy with staging: deploy/lib/site-stamp.sh.
source deploy/lib/site-stamp.sh
SITE_DIRTY=   # never taken from the shell here: production looks itself
js=$(site_bundle "site.sh (prod)" "$W")   # (T910) named three times: its preload and the after-paint loader
site_stamp "site.sh (prod)" "$W" "$js"
one=$(find "$S/water" -name index.html.gz -print -quit)
[[ -n $one ]] || { echo "site.sh (prod): $S/water holds no water page (index.html.gz): run build-seo (release-by-hand.md Step 7 item 5)" >&2; exit 1; }
gunzip -c "$one" | grep -c "$js" >/dev/null || { echo "site.sh: $S's water pages were built against another app export (not $js): rebuild with --app $W" >&2; exit 1; }
# (T304) the app focuses the region a water page names; a page without `cift:region` is from an older SEO build (every
# landing would read the whole files AND report it): rebuild the pages, not just relink them
# grep -c, not -q: -q stops reading at the first hit, gunzip then dies on the closed pipe and `pipefail` turned a page that
# HAS the tag into this failure at random (staging deploy 2026-11-14). -c reads to the end; the test is the same.
gunzip -c "$one" | grep -c '<meta name="cift:region"' >/dev/null || { echo "site.sh: $S's water pages carry no cift:region (built before T304): run build-seo again" >&2; exit 1; }
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
node -e 'const fs = require("fs"), [S, O, C, D] = process.argv.slice(1);
const release = JSON.parse(fs.readFileSync(S + "/snapshot.json", "utf8")).release ?? null;
fs.writeFileSync(O + "/site-release.json", JSON.stringify({ release, commit: C, dirty: D === "true" }) + "\n");
console.log("site-release.json: " + release + ", commit " + C + (D === "true" ? " DIRTY" : " clean"));' "$S" "$O" "$SITE_COMMIT" "$SITE_DIRTY"
echo "files: $(find "$O" -type f | wc -l)"
npx --prefix app wrangler deploy -c deploy/prod/site.wrangler.toml "$@"
echo "deployed. Optional host redirect (see deploy/prod/host.wrangler.toml): npx --prefix app wrangler deploy -c deploy/prod/host.wrangler.toml"
