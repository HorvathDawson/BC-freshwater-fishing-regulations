# T630 — One hydrograph presenter for the app and the public gauge page

Status: OPEN

Source: T564 report 2026-11-11. The public gauge page still draws its own SVG; the app's presenter lives in `ui`,
which the site build cannot import. T564's parity test compares shared `core` words, not the live app panel.

## Work
1. Move the hydrograph presenter (scales, ticks, marks, range band, words) into `core`; `ui` and `tools/seo` both draw from it.
2. The parity test compares the page's chart model with the app panel's for the same gauge and day.

## Acceptance
- One presenter; a change to it changes both. The parity test fails when either side adds its own mark or word.

Depends: T564. Size: M.
