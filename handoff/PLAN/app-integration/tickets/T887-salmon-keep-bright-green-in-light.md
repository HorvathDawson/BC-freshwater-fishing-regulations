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
