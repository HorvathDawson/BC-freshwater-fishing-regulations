# T181 — The gauge pill keeps the theme the map was built with

Status: OPEN. Found by the T62 app lane (3.5 split of Map.web.tsx), not fixed there (not part of a no-behaviour split).

`addPill` (app/packages/map/src/web/) draws the gauge pill with the theme captured when the map was created; switching
light/dark/cvd afterwards may leave the pill in the old theme.

Work: confirm in the browser (switch theme with a gauge pill showing); if confirmed, read the current theme when the
pill draws (or redraw it on theme change), and pin with a test that a theme switch re-colours it.
Size: S. Owner: no.
