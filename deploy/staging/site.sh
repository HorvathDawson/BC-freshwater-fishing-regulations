#!/bin/bash
# STAGING ONLY: merge app/dist/web-staging (`pnpm -C app/apps/mobile export:web:staging`: EXPO_PUBLIC_APP_ENV=staging, T232) and
# app/dist/seo (build-seo.mjs on the same release, `--app app/dist/web-staging`: T269) into app/dist/staging-site, make it
# unindexable, deploy.
#   deploy/staging/site-all.sh [--relink] [--dry-run]   THE ONE COMMAND (T544): checks, export, SEO build, this script
#   deploy/staging/site.sh [--dry-run]        (T241: the staging smoke test runs after a real deploy; SMOKE=0 skips it)
set -euo pipefail
cd "$(dirname "$0")/../.."
W=app/dist/web-staging; S=app/dist/seo; O=app/dist/staging-site; K=app/dist/staging-site-worker
source deploy/lib/site-stamp.sh
# (T269) a water page is the app's own index.html with its pane prerendered: it must be built against THIS export
# (`build-seo.mjs --app app/dist/web-staging`), or its script tag names a bundle the site does not have. (T910) named
# three times: its preload and the after-paint loader. (T994 #19) none named is said, not a bare exit 1.
js=$(site_bundle site.sh "$W")
# (T788 #4) app/ is checked HERE too, not only by site-all.sh; the export's own commit is what gets stamped
site_stamp site.sh "$W" "$js"
# (T279: a water page is stored compressed only — index.html.gz / .br — and served by the worker)
one=$(find "$S/water" -name index.html.gz -print -quit)
[[ -n $one ]] || { echo "site.sh: $S/water holds no water page (index.html.gz): run build-seo (deploy/staging/site-all.sh)" >&2; exit 1; }
gunzip -c "$one" | grep -c "$js" >/dev/null || { echo "site.sh: $S's water pages were built against another app export (not $js): rebuild with --app $W" >&2; exit 1; }
# (T304) the app focuses the region a water page names; a page without `cift:region` is from an older SEO build (every
# landing would read the whole files AND report it): rebuild the pages, not just relink them
# grep -c, not -q: -q stops reading at the first hit, gunzip then dies on the closed pipe and `pipefail` turned a page that
# HAS the tag into this failure at random (staging deploy 2026-11-14). -c reads to the end; the test is the same.
gunzip -c "$one" | grep -c '<meta name="cift:region"' >/dev/null || { echo "site.sh: $S's water pages carry no cift:region (built before T304): run build-seo again" >&2; exit 1; }
rm -rf "$O" "$K"; mkdir -p "$O" "$K"
cp -R "$W/." "$O/"
# SEO files never overwrite the app (the app owns /index.html); report any clash
X=(--exclude /_worker.js --exclude /budget.json --exclude /lighthouserc.json --exclude /snapshot.json --exclude /slug-map.json
   --exclude /legacy-redirects.json --exclude /redirects.json --exclude /host-worker.js)
comm -12 <(cd "$W" && find . -type f | sort) <(cd "$S" && find . -type f | sort) | sed 's/^/clash (app kept): /'
rsync -a --ignore-existing "${X[@]}" "$S/" "$O/"
# The hubs' "Open the app" link is absolute to production; on staging it must open the staging app (canonicals stay on
# the .ca). (T269: a water page IS the app — its links are already relative.) (T994 #19) a failed rewrite stops (it was
# `|| true`), and nothing names production's app afterwards (checked). (T1010) the link carries the region's box
# (`/?fit=w,s,e,n`): the origin goes, the query stays
HUB_LINK='<a class="app" href="https://canifishthis.ca/'
if grep -rlqF --include=index.html "$HUB_LINK" "$O"; then
  grep -rlF --null --include=index.html "$HUB_LINK" "$O" \
    | xargs -0 perl -pi -e 's#<a class="app" href="https://canifishthis\.ca/#<a class="app" href="/#g'
fi
if grep -rlqF --include=index.html "$HUB_LINK" "$O"; then
  echo "site.sh: a hub page still opens production's app after the staging rewrite:" \
       "$(grep -rlF --include=index.html "$HUB_LINK" "$O" | head -3)" >&2; exit 1
fi
# (T1010) a region hub's water search carries the same crash endpoint and env in its form's `data-cfg`, HTML-escaped
# (&quot;env&quot;:&quot;production&quot;): rewritten the same way, and NO staged page may still name production's crash endpoint
if [[ -d "$O/region" ]]; then
  find "$O/region" -name index.html -print0 | xargs -0 perl -pi -e 's#(&quot;|")https://feeds\.canifishthis\.ca/crash(&quot;|")#$1https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/crash$2#g; s#(&quot;|")env(&quot;|"):(&quot;|")production(&quot;|")#$1env$2:$3staging$4#g'
fi
# (T487) a gauge page's live half names production's data host and crash endpoint (tools/seo gauges.ts LIVE): on staging it
# reads staging's feed (the cards the staging feeds worker draws) and reports to staging's worker
if [[ -d "$O/gauge" ]]; then
  find "$O/gauge" -name index.html -print0 | xargs -0 perl -pi -e 's#"https://data\.canifishthis\.ca/#"https://data-staging.canifishthis.ca/#g; s#"https://feeds\.canifishthis\.ca/crash"#"https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/crash"#g; s#"env":"production"#"env":"staging"#g'
  if grep -rlq --include=index.html -e 'data\.canifishthis\.ca/feeds' -e 'feeds\.canifishthis\.ca' -e '"env":"production"' "$O/gauge"; then
    echo "site.sh: a gauge page still names a production host after the staging rewrite" >&2; exit 1
  fi
  # (T1010) not only /gauge and /region: ANY staged page naming production's crash endpoint, or a production env in a page's config
  if grep -rlq --include=index.html -e 'feeds\.canifishthis\.ca/crash' -e '&quot;env&quot;:&quot;production&quot;' "$O/gauge" "$O/region"; then
    echo "site.sh: a staged page still names production's crash endpoint or env after the staging rewrite:" \
         "$(grep -rlE --include=index.html -e 'feeds\.canifishthis\.ca/crash' -e '&quot;env&quot;:&quot;production&quot;' "$O/gauge" "$O/region" | head -3)" >&2; exit 1
  fi
  [[ -f "$O/gauge/stations.json" && -f "$O/share/gauge.png" ]] || { echo "site.sh: $S has gauge pages but no gauge/stations.json or share/gauge.png" >&2; exit 1; }
else
  echo "site.sh: $S has no gauge pages (built before T487): rebuild with build-seo" >&2; exit 1
fi
cp "$S/_worker.js" "$K/worker.js"
# (T544) THE SITE NAMES THE RELEASE IT WAS BUILT ON: /site-release.json = the SEO build's release (snapshot.json) and that
# release's min app version. The staging smoke compares it with staging's manifest. A build on anything but LATEST is
# refused (SITE_RELEASE_ANY=<reason> allows it, printing the reason): the site goes up on the release about to be uploaded.
node -e '
const fs = require("fs"), [S, O, LATEST_F, ANY, COMMIT, DIRTY] = process.argv.slice(1);
const rel = JSON.parse(fs.readFileSync(S + "/snapshot.json", "utf8")).release;
if (!rel) { console.error("site.sh: " + S + "/snapshot.json names no release"); process.exit(1); }
const latest = fs.readFileSync(LATEST_F, "utf8").trim();
if (rel !== latest) {
  if (!ANY) { console.error("site.sh: " + S + " was built on " + rel + ", but LATEST is " + latest + ": rebuild the SEO pages (deploy/staging/site-all.sh), or SITE_RELEASE_ANY=<reason>"); process.exit(1); }
  console.log("site.sh: SITE_RELEASE_ANY: built on " + rel + ", LATEST is " + latest + " - " + ANY);
}
const mf = "data/release/" + rel + "/manifest.json";
if (!fs.existsSync(mf)) { console.error("site.sh: " + mf + " is missing: the site cannot name its release\x27s min app version"); process.exit(1); }
const min = JSON.parse(fs.readFileSync(mf, "utf8")).minAppVersion ?? null;
const commit = COMMIT || null, dirty = DIRTY === "true" ? true : DIRTY === "false" ? false : null;
fs.writeFileSync(O + "/site-release.json", JSON.stringify({ release: rel, minAppVersion: min, commit, dirty }) + "\n");
console.log("site-release.json: " + rel + " (min app " + min + "), commit " + commit + (dirty ? " DIRTY" : dirty === false ? " clean" : " (dirty unknown)"));
' "$S" "$O" data/release/LATEST "${SITE_RELEASE_ANY:-}" "$SITE_COMMIT" "$SITE_DIRTY"   # site_stamp (above)
# staging must never be indexed: robots disallows everything, every response says noindex
printf 'User-agent: *\nDisallow: /\n# STAGING (staging.canifishthis.ca): not for indexing. Production robots.txt is built by tools/seo.\n' > "$O/robots.txt"
printf '\n/*\n  X-Robots-Tag: noindex, nofollow\n' >> "$O/_headers"
echo "files: $(find "$O" -type f | wc -l)"
npx --prefix app wrangler deploy -c deploy/staging/site.wrangler.toml "$@"
# (T241) the smoke test on the live staging site: exit 1 (and screenshots) when any check fails
if [[ "${SMOKE:-1}" != 0 && " $* " != *" --dry-run "* ]]; then
  node app/tools/smoke-staging.mjs --shots "${SMOKE_SHOTS:-app/dist/smoke-staging}"
fi
