# T904 — Desktop Conditions: the map fills the window behind; no white bars

Status: IN PROGRESS (lane app/t904-conditions-desktop; left (Claude): owner sees pictures, then integrate).

Owner 2026-11-19 ~06:40 (main chat), on a desktop staging shot of Conditions (Harrison River watershed: the "Flow vs.
past years" key and the day strip sit on a full-width white bar under the map, and a white strip runs down the right
edge), verbatim: "also could we make the conditions menu have map in backgroudn and remove big white bars on dekstop?"

## Work
- Desktop layout class only (phones and narrow web unchanged): on Conditions the map fills the whole window under the
  side bar, edge to edge; the key + watershed day strip become a card floating over the map at the bottom (square
  corners, map UI layout rule: top = search + day; the legend and its control in the bottom bar as a card), like the
  other modes' bottom card. No white bar at the bottom or the right.
- Same in light and dark; the card never covers the map's own controls or attribution.
- Render test at 1280 / 1279 px; pictures (1440 light + dark, before/after) to the owner before it lands.

## Built (lane app/t904-conditions-desktop)
- `LegendStrip` (Chrome.tsx) takes `card`: with `full`, a centred, square-cornered, fully bordered card (one ramp wide + 14 px
  padding, 12 px above the window's bottom edge) instead of the full-width top-bordered bar. `MapLegend` passes it for the
  Conditions key when `tabsAsRail(useLayoutClass())` (desktop, >= 1280 px). The foot was already an overlay on the map (T275),
  so the map already ran behind it; only the bar's opaque full-width background was the "white bar". The foot's measured height
  now includes the 12 px gap, so the map's scale bar, attribution and the gauge-only card stay above the key card.
- Phones and narrow web: unchanged (390 px before/after differ 0.24% of pixels, map noise).
- Scope: Conditions only. Map and Runs keep their bars (no desktop floating bottom card existed to reuse).
- Right-edge white strip: NOT reproduced (headless Chrome at 1440x900 and 2000x877: canvas is the full window, no overflow
  anywhere). Most likely the owner's browser showing a classic scrollbar gutter / window edge; nothing in the layout leaves 17 px.
- Test: `packages/ui-native/src/T904.legend-card.test.tsx` (card at 1280, bar at 1279).
- Pictures: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/1db4ea05-d81d-4b07-a925-fc0b9cdbe701/scratchpad/t904-shots/ before-1440-light|dark.png, after-1440-light|dark.png, before/after-390-light.png, contact-light.png,
  contact-dark.png, contact-phone.png (before | after).
