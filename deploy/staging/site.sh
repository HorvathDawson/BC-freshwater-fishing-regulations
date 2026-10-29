#!/bin/bash
# STAGING ONLY: merge app/dist/web-staging (`pnpm -C app/apps/mobile export:web:staging`: EXPO_PUBLIC_APP_ENV=staging, T232) and
# app/dist/seo (build-seo.mjs on the same release) into app/dist/staging-site, make it unindexable, deploy.
#   deploy/staging/site.sh [--dry-run]
set -euo pipefail
cd "$(dirname "$0")/../.."
W=app/dist/web-staging; S=app/dist/seo; O=app/dist/staging-site; K=app/dist/staging-site-worker
rm -rf "$O" "$K"; mkdir -p "$O" "$K"
cp -R "$W/." "$O/"
# SEO files never overwrite the app (the app owns /index.html); report any clash
X=(--exclude /_worker.js --exclude /budget.json --exclude /lighthouserc.json --exclude /snapshot.json --exclude /slug-map.json
   --exclude /legacy-redirects.json --exclude /redirects.json)
comm -12 <(cd "$W" && find . -type f | sort) <(cd "$S" && find . -type f | sort) | sed 's/^/clash (app kept): /'
rsync -a --ignore-existing "${X[@]}" "$S/" "$O/"
# "Open in the app" is absolute to production; on staging it must open the staging app (canonicals stay on the .ca)
grep -rl --include=index.html 'href="https://canifishthis.ca/?regs=' "$O" | xargs perl -pi -e 's#href="https://canifishthis\.ca/\?regs=#href="/?regs=#g'
# (T232) and the bare "Open the app" link (no ?regs=) too
grep -rl --include=index.html '<a class="app" href="https://canifishthis.ca/">' "$O" | xargs perl -pi -e 's#<a class="app" href="https://canifishthis\.ca/">#<a class="app" href="/">#g' || true
cp "$S/_worker.js" "$K/worker.js"
# staging must never be indexed: robots disallows everything, every response says noindex
printf 'User-agent: *\nDisallow: /\n# STAGING (staging.canifishthis.ca): not for indexing. Production robots.txt is built by tools/seo.\n' > "$O/robots.txt"
printf '\n/*\n  X-Robots-Tag: noindex, nofollow\n' >> "$O/_headers"
echo "files: $(find "$O" -type f | wc -l)"
npx --prefix app wrangler deploy -c deploy/staging/site.wrangler.toml "$@"
