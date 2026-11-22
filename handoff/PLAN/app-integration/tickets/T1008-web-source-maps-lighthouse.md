# T1008 — Website: Lighthouse "Missing source maps for large first-party JavaScript"

Status: OPEN — the owner picks when it is built (default below).

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
3. `release-by-hand.md` and `sourcemaps.mjs`'s header say maps are served.

Depends: none. Size: S. Owner: picks serve or hidden (default: serve).
