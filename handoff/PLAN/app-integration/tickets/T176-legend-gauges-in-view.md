# T176 — The Conditions map legend's "N gauges in view" counts sections, not gauges

Status: DONE — lane-T176 work on lane-T175, e8999343 (with T175); this ticket update follows it. App only.

Source: T168 review (2026-10-24): the "13 gauges in view" the owner saw on Nicomen Slough is the Conditions map legend
(`app/packages/ui-native/src/Chrome.tsx` `LegendRamp`), which counts the VISIBLE SECTIONS that have a standing — not
gauges. Count distinct stations in view (split as T168 does: on this water / estimating / not reporting) or relabel it to
what it counts ("13 sections coloured"). One function in core for the count; whole-value test; shots. Ships: app. Size: S.

## As built — relabelled to what it counts
- **Why not a station count.** The legend's histogram IS of the reaches on screen with a standing (`useConditionsColour`
  `marks`: one per visible section with a value; the -1 "no baseline" sentinel left out) — a station count beside it
  would describe something the bars are not. The map reports no station set for what is in view (its dots are a GeoJSON
  layer it never queries), and T168's split (`gaugeSide`) is relative to a section: the browse map has no "this water".
- **The one function** — core `legendCount(n)` (`core/src/conditions.ts`): "13 reaches coloured" / "1 reach coloured";
  nothing for 0 or an unknown count (rule 32). `legendTickLabel(c, where)` names each histogram tick the same way
  ("2 reaches near the 6th percentile", Change: "1 reach near 4× today's flow") — the ticks said "gauges" too.
  `LegendRamp` draws both; no word is spelled in the component. Flow, Level and Change legends alike.

## Tests
- `core/src/legendCount.test.ts` — whole strings: plural, singular, none / null / NaN / negative → nothing; tick labels.
- `ui-native/src/LegendRamp.test.tsx` — whole legend text ("Flow vs. past years, this date4 reaches colouredLowNormalHigh",
  the Change legend), tick aria labels, colours.
- Mutations: core back to "gauges in view" (M8) and a literal in `Chrome.tsx` (M9) — both fail.
- `pnpm -C app check` (with T175): green, 8,156 passed / 24 skipped.

## Shots (own Expo port 8231) — `handoff/PLAN/app-integration/T176-shots/`
The Conditions map over Nicomen Slough / the Fraser at Chilliwack: `nicomen-legend-desktop-light.png` and
`nicomen-legend-desktop-dark.png` ("17 reaches coloured"), `nicomen-legend-phone-light.png`, `nicomen-legend-phone-dark.png`.

## Adversarial review
- The count says "reaches", and lakes with a standing are counted among them (a lake section is a reach of water here).
- On the web the count appears once the map has reported what is on screen (it did on every shot after a settle).
- No count is invented: an empty view draws no count line (test).

## Owner questions (defaults built)
1. **Word** — default "N reaches coloured". Alternatives: "N reaches in view" (but reaches with no reading are in view
   too), "N waters" (wrong: a water has many reaches).
2. **A real station count** — default: not built. Doing it needs the map to report the gauge dots on screen (a new
   `onVisible` for the `gauges` layer) and, for a T168-style split, a selected water on the browse map; say if wanted.
