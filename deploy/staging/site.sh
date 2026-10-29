#!/bin/bash
# STAGING ONLY: merge app/dist/web-staging (`pnpm -C app/apps/mobile export:web:staging`: EXPO_PUBLIC_APP_ENV=staging, T232) and
# app/dist/seo (build-seo.mjs on the same release, `--app app/dist/web-staging`: T269) into app/dist/staging-site, make it
# unindexable, deploy.
#   deploy/staging/site.sh [--dry-run]        (T241: the staging smoke test runs after a real deploy; SMOKE=0 skips it)
set -euo pipefail
cd "$(dirname "$0")/../.."
W=app/dist/web-staging; S=app/dist/seo; O=app/dist/staging-site; K=app/dist/staging-site-worker
# (T269) a water page is the app's own index.html with its pane prerendered: it must be built against THIS export
# (`build-seo.mjs --app app/dist/web-staging`), or its script tag names a bundle the site does not have
js=$(grep -o '/_expo/static/js/web/index-[0-9a-f]*\.js' "$W/index.html")
# (T279: a water page is stored compressed only — index.html.gz / .br — and served by the worker)
one=$(find "$S/water" -name index.html.gz -print -quit)
gunzip -c "$one" | grep -q "$js" || { echo "site.sh: $S's water pages were built against another app export (not $js): rebuild with --app $W" >&2; exit 1; }
rm -rf "$O" "$K"; mkdir -p "$O" "$K"
cp -R "$W/." "$O/"
# SEO files never overwrite the app (the app owns /index.html); report any clash
X=(--exclude /_worker.js --exclude /budget.json --exclude /lighthouserc.json --exclude /snapshot.json --exclude /slug-map.json
   --exclude /legacy-redirects.json --exclude /redirects.json)
comm -12 <(cd "$W" && find . -type f | sort) <(cd "$S" && find . -type f | sort) | sed 's/^/clash (app kept): /'
rsync -a --ignore-existing "${X[@]}" "$S/" "$O/"
# The hubs' "Open the app" link is absolute to production; on staging it must open the staging app (canonicals stay on
# the .ca). (T269: a water page IS the app — its links are already relative.)
grep -rl --include=index.html '<a class="app" href="https://canifishthis.ca/">' "$O" | xargs perl -pi -e 's#<a class="app" href="https://canifishthis\.ca/">#<a class="app" href="/">#g' || true
cp "$S/_worker.js" "$K/worker.js"
# staging must never be indexed: robots disallows everything, every response says noindex
printf 'User-agent: *\nDisallow: /\n# STAGING (staging.canifishthis.ca): not for indexing. Production robots.txt is built by tools/seo.\n' > "$O/robots.txt"
printf '\n/*\n  X-Robots-Tag: noindex, nofollow\n' >> "$O/_headers"
echo "files: $(find "$O" -type f | wc -l)"
npx --prefix app wrangler deploy -c deploy/staging/site.wrangler.toml "$@"
# (T241) the smoke test on the live staging site: exit 1 (and screenshots) when any check fails
if [[ "${SMOKE:-1}" != 0 && " $* " != *" --dry-run "* ]]; then
  node app/tools/smoke-staging.mjs --shots "${SMOKE_SHOTS:-app/dist/smoke-staging}"
fi
