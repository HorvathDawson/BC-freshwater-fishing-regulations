# T181 — The gauge pill keeps the theme the map was built with

Status: DONE 7c43151a (see Outcome). Found by the T62 app lane (3.5 split of Map.web.tsx), not fixed there (not part of a no-behaviour split).

`addPill` (app/packages/map/src/web/) draws the gauge pill with the theme captured when the map was created; switching
light/dark/cvd afterwards may leave the pill in the old theme.

Work: confirm in the browser (switch theme with a gauge pill showing); if confirmed, read the current theme when the
pill draws (or redraw it on theme change), and pin with a test that a theme switch re-colours it.
Size: S. Owner: no.

## Outcome

CONFIRMED and FIXED 2026-10-24 (7c43151a). Inside the effect that builds the map, `useMapInstance` set
`pill.current = () => addImages(m, theme)`. That effect is keyed on the archives only, so the closure kept the theme
the map was built with. `useLook` does call `pill.current()` on every theme change, so the pill was re-added, but in
the old theme. The route's "you are here" pin and the hatches go through the same `addImages` and had the same bug.

The test came first: `app/packages/map/src/pill-theme.render.test.tsx` runs Map.web over a recording maplibre-gl. It
builds a map in light, switches it to dark and then to cvd, and compares the `gauge-pill` and here-pin pixels with
`pillImage` / `hereImage` in each theme. It also checks that a `styleimagemissing` after a switch re-adds the pill in
the current theme. Before the fix it failed: after light → dark the pill still matched the light pixels. The fix makes
the re-adder read the theme from a ref (`themeNow`) that is updated on every render; all three tests then pass.
Mutation: put the closure back over `theme` and the switch test fails again. This was confirmed in the test harness,
not in a browser.
