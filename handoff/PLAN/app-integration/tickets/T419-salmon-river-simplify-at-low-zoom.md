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

## Look (2026-11-06, chat 3, staging r-5a5999c94588; shots in T419-shots/)
- **Not many short pieces:** the Langley Salmon River is 10 features; the main stem is ONE order-4 line of 420 points,
  drawn from z9. Only 17 of its points are shared with other streams (tributary mouths), so
  `--no-simplification-of-shared-nodes` is not what keeps it detailed.
- **The tiles do simplify it:** points of "Salmon River" in the shipped tiles — z9 33, z10 108, z11 198, z12 316,
  z13 786. At z10 that is a point every ~230 m, and the tolerance (`SIMPLIFICATION` 5 units of the 2,048 low grid) is
  ~95 m, about 0.6 px. So every meander bend bigger than a pixel survives, and a tightly meandering lowland river
  draws as a sawtooth at z9–z11 (`t419-out2.png`), most of all under the thick highlight / closed hatch.
- **A class, not one water:** the angular look shows on other meandering streams at the same zoom
  (`t419-z10-plain.png`, the closed streams east of Chilliwack).
- **Fix to try (tiles change → tile build, ships with a release):** a larger tolerance BELOW z12 only (e.g. 5 → 10–15
  units at z≤11; z12–13 unchanged), measured before/after on the same views and the tile bytes. Optionally a round
  line-join on the stream layers. Owner looks at the shots before it ships.
