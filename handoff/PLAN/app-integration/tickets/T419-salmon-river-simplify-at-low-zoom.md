# T419 — Salmon River (Region 2) is not simplified enough when zoomed out

Status: OPEN (owner 2026-11-05: "salmon river in region 2 is an example of a river i would expect to have gotten more
simplified at higher zooms. it looks really bad because of it." — check with the UI work).

## Work
1. Look at the Salmon River (Region 2, Fraser Valley / Langley) on the map at each zoom from z8 up to z13 (emulator +
   web). Screenshot the bad zooms. Find why its line keeps full detail when zoomed out: tippecanoe simplification on
   the stream layer (`pipeline/deliver/tiles`), a per-feature detail/`-pS`-style setting, the part line, or how many
   sections it is split into (many short pieces simplify badly).
2. Check whether it is one water or a class (meandering lowland streams with many short sections), and list others.
3. Fix with a measured before/after: screenshots at the same zooms, tile sizes. A tiles change needs a tile build
   (heavy, one at a time) and ships with a data release; tiles and bundle must share a vintage.
Size: S to look, M if the fix is a tiles change. Owner: phone/web look after the fix.
