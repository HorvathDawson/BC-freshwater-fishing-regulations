# T406 — SEO: raster share images and a species-by-region season calendar

Status: OPEN.

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
