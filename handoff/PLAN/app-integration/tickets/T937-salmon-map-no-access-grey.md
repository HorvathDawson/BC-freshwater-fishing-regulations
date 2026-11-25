# T937 — Salmon map: no-access water drawn grey like Trout, with its key entry

Status: DONE — 81bb25ee5, b9ff4163a, 627303b49, 802f9a5ef; Salmon draws no-access water like Trout; merged via a4ced3eff; owner looks answered 11-20.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T937 | Salmon map: no-access water drawn grey like Trout, with its key entry (owner) | DONE | 81bb25ee5, b9ff4163a, 627303b49, 802f9a5ef | owner 2026-11-20 "salmon should have the no access land set to grey like trout" |

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

## Owner answer (owner 2026-11-20, dialog after the round-2 pictures)
Asked whether the Salmon key may take a third row for the No access entry, the owner answered verbatim: "Remove checked dste"
(= remove the checked date). Done: the "Checked <date>" line is gone from the Salmon map key (`MapLegend.tsx`, with
`salmonChecked` / `salmonCheckedLine` / `MAP_KEY.checked` in core and their tests), so the key is two rows at 390 dp again.
The date still shows in the Salmon water card ("Checked with DFO …", `salmonFeedLine`) and in the update/feed rows in More.
The one case that still takes a third row: the issue entry ("Check rule", T759), present only when the feed has such a water.
