# T887 — Salmon map, light mode: keep is a bright green, clearly apart from release, closed and check

Status: OPEN — built; lands after the owner sees the pictures (app chat).

Owner 2026-11-18 ~22:33, verbatim: "Light mode keep salmon vs closed or release has very little contrast. Should be
bright green for keep in light I think too?"

Today: light keep (`color.salmon.open`) is the light `status.open`, a deep teal-blue; release, closed and check are a
dark purple, a dark red and an amber. Far apart in hue, but three of the four are dark, so on a thin map line keep,
release and closed read alike. Dark mode's keep is already a bright mint green.

## Work
1. Light `color.salmon.open` gets its own green (no longer the `status.open` alias), as bright as the light basemap
   allows: it still holds 3:1 on every basemap ground (tools/map-contrast.test.ts, WCAG 1.4.11). Dark and the
   colour-blind theme unchanged (cvd keeps its blue keep).
2. The water page's salmon parts (the Salmon card's chips, the salmon year bar) read the salmon tokens, so they match
   the map; the Salmon legend and the Help key already do.
3. Pictures (Salmon map at 390: light before/after, dark for comparison; the salmon year bar in light) before it lands.

## Built (branch app/t887-salmon-light-green; not merged)
Light keep `#016890` (deep teal-blue) -> `#007E33` (a saturated green on dark keep's own hue, 146). Dark and cvd unchanged.

| light, keep vs | dE2000 before | after | luminance ratio before | after |
|---|---|---|---|---|
| release `#421652` | 35.7 | 50.9 | 2.30 | 2.75 |
| closed `#8F0006` | 49.6 | 64.4 | 1.56 | 1.86 |
| check `#D08404` | 53.4 | 44.2 | 2.06 | 1.73 |

- On the light basemap the green is as light as it can go: worst ground 3.05:1 (scrub; land 3.92, water 3.36), WCAG 1.4.11
  still met, no new exception. A paler "bright" green would sink into the basemap's greens.
- Colour-blind readers: in LIGHT the green sits 9.3 from closed under deuteranopia (was 41) — the red/green pair; that
  is what the colour-blind theme is for, and it keeps its blue keep (worst reader, Vienot + Machado: 20.2 from release,
  45.0 from closed, 49.2 from check).
- The Salmon card's chips and the salmon year bar now read the salmon tokens (theme.ts `salmon`); the Salmon legend
  and Help key already did. The game-fish bar's "Open" stays `status.open` (teal-blue in light), so a water page shows a
  green salmon bar over a blue game-fish bar in light.
- Pinned: tools/map-contrast.test.ts "the Salmon view's keep (T887)", mutation-checked (the old alias fails it).

Pictures (scratchpad): t887-compare-cowichan.png (keep / release / closed, Jun 15 2027 — release exists only on the
Cowichan in DFO's data), t887-compare-kamloops.png (keep / check / closed, Sep 15 2027), t887-compare-yearbar.png.
No place carries all four at once at a readable zoom.

## Option B (built after A's pictures: A's #007E33 sinks into the light map's green land)
Light keep `#22C55E` (bright, hue 147 — dark mint's family) in a thin ink casing (`color.salmon.casing` = the gear ink
#0A1116, 1.25 px each side, under the line; runtime layer `stream-salmon-casing`, Salmon view, the day's keep labels).
- dE2000 vs release 69.0, closed 79.8, check 45.4; luminance vs closed 4.25:1.
- Casing on the light grounds 11.1-15.4:1; the green on its casing 8.3:1. Deuteranopia vs closed 32.0 (Vienot) / 35.3
  (Machado) — far better than A's 9.3, because B's green is light and closed is dark. Weakest pair: keep vs check under
  deuteranopia 10.3 (both light).
- Cards: a bright green cannot carry white words (2.3:1), so the Salmon card's chips, the year bar and its status word
  use `color.ui.salmon-keep` (A's #007E33 in light; the map's keep elsewhere). Legend and Help keys draw the cased line.
- THE SWITCH: light.json `color.salmon.open` + `width.salmon.casing` — A: #007E33 + 0; B: #22C55E + 1.25. Dark and cvd 0.
  Every test passes in both. The branch is on B.
- Lakes: a keep lake's fill and shoreline are the bright green with no casing (not built for B).
