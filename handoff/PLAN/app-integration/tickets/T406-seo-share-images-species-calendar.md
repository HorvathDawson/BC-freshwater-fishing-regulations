# T406 — SEO: raster share images and a species-by-region season calendar

Status: DONE 2026-11-05 (08837f197, 7b20ec0da; owner 11-05: region bar and map button accepted as they are — bar stays on the first screen, the button opens the whole map, no region start link).

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

## Part 2, the cheap version (owner 2026-11-05; 7b20ec0da, a PROPOSAL; staging fc215b52)
- Every region hub (`/region/<n>`) has two new cards (app tokens, square corners): **"For a more visual experience, open
  the map"** with an "Open the map" button, and **"When waters are open in Region N"**: one bar per kind (streams; lakes
  with wetlands), each week's column = the share of that kind's parts open (lower, `--open`) vs closed (upper, `--closed`),
  folded from the water pages' own game-fish strips (`PartModel.year`, `tools/seo/regionYear.ts`; no new season logic),
  plus a sentence per row ("Streams (1546): 71% open at most; fewest open in the week of Apr 30, 60%.").
- The map button opens `/` (the app's map). The app takes no region or bbox in a link (`startLink.ts` ROUTE_KEYS: water
  place keys, `browse`, `spot`, `date`), so it is not framed on the region; framing it needs a new start-link key (owner).
- **Salmon strip left out:** it is the live DFO feed per water (rules turn over ~50 %/yr), so a region roll-up baked at build
  time would be stale; only the game-fish strip is folded.
- **Load gate (Lighthouse mobile, staging):** before /region/2 99 / LCP 1.68 s, /region/4 99 / 1.69 s (one run each);
  after, 4 runs: /region/2 97, 100, 98, 99 / LCP 2.43, 1.23, 2.15, 1.69 s; /region/4 98, 100, 100, 99 / LCP 2.24, 1.24,
  1.25, 1.55 s; CLS 0, TBT 0 throughout; HTML +1.3 KB transfer. Perf stays >= 95 every run, but the LCP element is now the
  year card's caption (`p.small`, it is in the first phone screen) and the runs spread 1.2-2.4 s: not provably unchanged.
  Owner: keep, move the card below the waters list (out of the first screen), or drop it — one line in `site.ts`.
