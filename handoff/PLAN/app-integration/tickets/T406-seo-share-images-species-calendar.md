# T406 — SEO: raster share images and a species-by-region season calendar

Status: PARTIAL — 08837f197 (part 1, a proposal for review); part 2 (species calendar) not started: needs a decision.

Source: T195 leftovers (2026-11-05). Share cards are SVG (`/share/<slug>.svg`); Facebook, X and most chat apps do not
render an SVG `og:image`, so a shared water link shows no picture. The species season-start calendar needs the
per-species year strips, not the water strip.
## Work
1. A PNG (1200x630) per water rendered at build time from the same card (a well-established rasteriser added with a
   line in app/deps.md; self-hosted, no service), `og:image` pointing at it; size counted in the upload.
2. `/region/<n>/season-calendar`: opening/closing dates by species from the per-species strips, linked from the hub.
## Acceptance
A water link pasted into a Facebook/X preview tool shows its card; the calendar's dates equal the app's species strips
(test on the fixture release); the budget gate still passes.

Depends: T195. Size: M. Owner: no. Default: PNG via a rasteriser package; one card per water.

## Outcome (2026-11-05)
- **Part 1 (08837f197, a proposal):** every water's card drawn at build time as a 1200 x 630 PNG (`share/<slug>.png`,
  `tools/seo/share.ts`, @resvg/resvg-js — already a devDependency — in the app's Archivo files, no system font, 8 ms a card);
  `og:image` names it with `og:image:type/width/height`; the SVG is no longer shipped. Full build: 19,635 cards, 531 MB,
  largest 49.9 KB (budget.json `share`; a card over 100 KB fails the build). On staging (ffb4f71f): e.g.
  https://staging.canifishthis.ca/share/vedder-river.png. Not yet checked in a Facebook/X preview tool (staging is noindex and
  the canonical/og URLs point at canifishthis.ca, which does not serve them yet): the owner checks it at the prod transition.
  Upload: the first deploy took 292 s for the new files; later deploys upload only changed cards.
- **Part 2 not done — needs a decision:** the app has no per-species year strips (only the water's game-fish strip and the
  salmon strip, `core/regs/strip.ts`, `core/salmon.ts`), so "dates by species" needs a source: build one from the rules'
  per-species seasons (new core logic) or limit the calendar to game fish vs salmon per water.
