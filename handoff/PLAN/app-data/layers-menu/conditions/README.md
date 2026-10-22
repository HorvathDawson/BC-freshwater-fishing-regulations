# Level as its own view, in the Conditions map's Layers (2026-10-22)

These are web shots (react-native-web in headless Chromium). The Expo dev server ran from worktree wf_e0887ad5-3e6-1 on
port 39952. The main checkout's `serve-tiles.mjs` ran on port 39951 and served the newest release with the live feed.

| shot | what |
|---|---|
| `*-01-flow-closed.png` | The Conditions map as it opens: Flow. The rail now has Layers too. The legend strip has no Flow / Level / Temp switch, only the ramp ("low flow for the date"). The horizon chips stay on the map. |
| `*-02-sheet-flow.png` | The sheet on Conditions: Show (Flow* · Level · Temperature), Map (ground), On the map (Gauges, Flow arrows, both on), and the always-shown line. Colour the water by, private land and contours are not listed here. |
| `*-03-sheet-level.png` | Level is chosen. The map behind it is recoloured from level gauges only, and the rail badge reads 1. |
| `*-04-level-closed.png` | The closed map in Level. The ramp reads "low stage for the date", and the badge stays at 1. |

The shots come in three sets: `390-light`, `390-dark` (390x844) and `1280-light` (desktop, 1280x800).

Not shot: native (Android/iOS), which needs an emulator.
