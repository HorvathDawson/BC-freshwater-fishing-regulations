# `design/` — the two documents this app is built to

| file | settles |
|---|---|
| `riffle.html` | what it looks like and how it behaves |
| `data-contract.html` | what the data is, and in what format |

`data-contract.html` is "One Interface, Two Storages", saved out of
<https://claude.ai/code/artifact/c672479b-fe44-4ddb-9046-6eb2300aeccf>. It settles the
four artifacts (tiles / bundle / feeds / precomputes), the two version domains, the
byte budget, and — §10 — that the bundle is **SQLite, range-read**, one file serving a
resident mobile copy and a range-read web copy. When an implementation disagrees with it,
it is right and the implementation is wrong.

---

# `riffle.html` — the design this app is being built to

The interactive prototype, saved out of
<https://claude.ai/code/artifact/49731737-c331-4eac-a949-36ed79801825> so it survives
outside claude.ai and can be diffed. Open it in a browser; it runs on real build output.

The host's `frame-runtime` script is stripped — that is claude.ai's injection, not ours.
Everything else is byte-identical to the published artifact.

**This is the target, not a mood board.** When a screen in `packages/ui-native` disagrees
with this file, this file is right.

## What it settles

| | |
|---|---|
| navigation | four tabs — **Map · Search · Conditions · Spots** — with the map as home, not a page you navigate to |
| type | **Bricolage Grotesque** for display, **Archivo** for text, **JetBrains Mono** for figures |
| colour | outcome carries the colour, provenance carries the line weight |
| the map | generic OSM-flavoured base, our water drawn over it, four colour layers (rules / flow / depth / stocking) |
| the verbatim panel | the synopsis paragraph, quoted exactly, with the parsed rule located inside it |
| minimaps | search results and "the water above you" reuse the same map source with less chrome |

## Where the app currently stands against it

**Already carried over.** The palette — `packages/ui-native/src/theme.ts` is Riffle's
tokens almost to the digit (`ink #15181C`, `closed #D81E1E`, `open #0A8552`,
`accent #5F26E0`), plus the dark set and the colour-blind variant. The status vocabulary,
the gauge-trust rule, the hydrograph's shaded normal band.

**Also carried over now.** The four-tab shell with the map as home; the three typefaces;
the search screen; the water sheet redrawn against this file; the floating date pill, zoom
stack, Layers button and legend strip.

**The map renders for real** on mobile web: `data/bc.pmtiles` (Protomaps OSM basemap for BC)
under `data/generated/tiles/atlas.pmtiles` (our streams, lakes, wetlands and administrative areas),
both read by byte range. `pnpm tiles` serves them in development.

**Not carried over yet.**

- **Colour by rule.** The map draws our water in its no-data colour because nothing feeds
  per-feature status into it. The mechanism exists (`setData` -> feature-state); what is
  missing is a viewport-to-sections query.
- **Search rows carry no answer.** Riffle's rows show CLOSED / a live reading / stocked.
  `NameHit` carries only name, alias and piece count, so a row cannot say what it is yet.
- The verbatim synopsis panel, the layer switcher's contents, the date sheet, Spots.
- **The native map.** `Map.native.tsx` says so on screen rather than rendering nothing.
  It needs `@maplibre/maplibre-react-native` and a development build.
