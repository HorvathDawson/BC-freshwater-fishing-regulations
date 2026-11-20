# T937 — Salmon map: no-access water drawn grey like Trout, with its key entry

Status: OPEN.

Source: owner 2026-11-20 ~01:30 (main chat), verbatim: "Also salmon should have the no access land set to grey like trout
with proper item in legend. Use same look as trout".

## Work
In Trout & more, water with no public access is drawn in the no-access look (grey, dotted: `color.status.no_access`,
`stream-noaccess`) and the key has a "No access" entry. In Salmon mode, draw the same water with the SAME look and add the
same key entry, with the same words and swatch. Find where the two modes differ (the salmon paint in
`app/packages/map/src/salmon-paint.ts`, the style `app/packages/map/style/layers.source.json` (AGENTS 28: edit the source,
regenerate), the key in `app/packages/ui-native/src/shell/MapLegend.tsx` / core legend words). One look, shared, not a copy.
Same on the app and the website.
## Acceptance
Pictures: one no-access water in Trout and in Salmon (map + key), light and dark, 390; owner approves.
Depends: none. Size: S. Owner: yes (pictures).
