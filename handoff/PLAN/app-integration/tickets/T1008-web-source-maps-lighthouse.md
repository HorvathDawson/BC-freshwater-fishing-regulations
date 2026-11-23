# T1008 — Website: Lighthouse "Missing source maps for large first-party JavaScript"

Status: DONE — 8cd3341a8: the web's maps are served, checked before they ship (lane L2, 2026-11-22).

Source: owner 2026-11-22 (Lighthouse on https://canifishthis.ca, unscored): "Large JavaScript file is missing a source
map" for `…web/index-76f27ad….js`. (The same report's two chrome-extension rows are React DevTools', not ours.)

## Today: deliberate
`app/tools/sourcemaps.mjs keep-web` (T352) moves every `.map` out of the web export into `data/sourcemaps/<key>/web/`
(git-ignored) and drops the `sourceMappingURL` comment: maps are "kept per build and never served", used only to
symbolicate crash reports (`sourcemaps.mjs symbolicate`). So Lighthouse sees none.

## The choice
- **Serve them (recommended):** the repository is public, so a served map shows nothing that is not already on GitHub.
  Production debugging in any browser gets real file and line names, and Lighthouse stops flagging it. Maps are
  fetched only when DevTools is open, so visitors download nothing more. Keep the private copies for crash
  symbolication as now.
- **Keep them hidden:** nothing changes; the Lighthouse item stays (it is unscored).

## Work (if served)
1. `keep-web` keeps a copy and leaves the `.map` files plus the `sourceMappingURL` comment in the export; the site
   upload includes them (staging and production), with the same cache rules as the bundle.
2. A check after the web export: every served `.js` over 100 KB has its map, and each map's `file` matches.
3. The same check refuses a map whose `sources` name a file that is not in the repository or `node_modules` (a map
   must show nothing GitHub does not).
4. `release-by-hand.md` and `sourcemaps.mjs`'s header say maps are served.

Depends: none. Size: S. Owner: decided (serve).

## Done 2026-11-22 (lane L2, 8cd3341a8)
`sourcemaps.mjs keep-web` copies the maps to `data/sourcemaps/<key>/web/` (symbolicate as before) and leaves them and the
`sourceMappingURL` lines in the export; the site deploy copies the export whole, so `/_expo/static/js/web/*.js.map` is served
under the bundle's `/_expo/static/*` cache rule. First `checkServedMaps` (`sourcemaps.test.ts`): every .js over 100 KB names
its map, the map exists, parses (v3, indexed maps too) and carries the bundle's debug id (the indexed Metro map has no
`file`; where one is present it must be the bundle's name), and names only files git tracks under app/ or node_modules
holds (Metro's `__prelude__` / `\0polyfill:` modules allowed). Run on the 0.8.5 b42 export with its kept maps: clean.
`release-by-hand.md` and `app/MAP.md` say maps are served. NOT checked: a real `export:web` + deploy (Lighthouse after the
next release). Size: the main map is ~14 MB (fetched only with DevTools open).
