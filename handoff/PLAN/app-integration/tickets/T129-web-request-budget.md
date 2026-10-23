# T129 — Web request budget: gate what a web visitor fetches, not only the release total

Status: OPEN — owner 2026-10-22: "as long as it isn't too much of a raise. we can be smart on the webapp on how things
are requested." (The release total gate went 14 → 15 MB, Q55.)

## Why
The release total is what a PHONE holds offline. The web app fetches lazily (MINIMAL-PACKAGING): paint first, then
core/water, a region's regs file only on a tap in that region, conditions/near on demand. Nothing gates those web
paths, so a growing file (regs-3 swung +130 KB on a dictionary change today) can quietly slow first paint or a tap.

## Work
- Measure, on the live release, the bytes a web visitor fetches for: first paint (map coloured), first search, first
  tap on a water (per region), opening Conditions, the Change horizon, a spot. Record each in a table.
- Gates (named constants, in the release build and an app test): first-paint bytes, per-tap bytes (largest region),
  Conditions open. Set them from the measured numbers with headroom; fail the build when crossed.
- Look for easy wins: is anything fetched before it is needed (e.g. `water` 2.7 MB before the first tap? `access` on
  the Map tab?), can a region file be split (regs-3 is the largest), HTTP caching headers on R2 (immutable, sha-named
  files), prefetch the likely region after first paint.
- A Playwright/network test on the web build records the request list for the walk above and pins it.

## Acceptance
The table, the gates in the build and the app, the pinned request walk; any lazy-load fixes with before/after bytes.
