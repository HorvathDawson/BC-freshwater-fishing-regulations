# T891 — braided rivers drawn simpler: side channels thinner, braids simplified in the tile build

Status: OPEN (main chat: the tile build; app chat: the map style). After production; candidate for 0.8.2 if small.

Owner 2026-11-18 ~22:02 (main chat), staging map shot of the Lillooet River braids near Pemberton, MU 2-9
(/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/8b47b3ee-image.png), verbatim: "Isn't
braiding like this supposed to be simplified more ?". Asked how (dialog: main bold + braids thin / fill the river
shape / hide braids zoomed out / leave), his words: "We could simplify these even more in tile building? Yes side
channels should be less bold too."

Today: the braid work so far was about RULES (which part a braid piece belongs to); nothing changes how braids are
DRAWN. Every FWA side channel is a full-width line. T699 (trim tiles; simplification per zoom) was deferred 11-13.

## Work
1. Tiles (main chat): mark each stream line that is a side channel of its river (same watershed code as the main
   channel, not the main stem) as a tile attribute, and simplify braids harder at low and middle zooms in the tile
   build (merge or drop the smallest braid pieces below a zoom; keep every piece that carries its own rules
   tappable at high zoom).
2. Style (app chat): side channels draw thinner than the main channel, same colour (one water); the main channel keeps
   its width.
3. Pictures to the owner (Lillooet at Pemberton, the lower Fraser, the Squamish, the Skeena) before it lands.
Tiles and bundle share a vintage: this needs a tiles build with the next data release.

## Tile attribute (main chat, 2026-11-19, for the app chat's style)
Stream features carry `sc: 1` on side-channel pieces only (absent = the main channel). Test = `is_side_channel_piece`
(pipeline/deliver/tiles/export.py: FWA secondary flow, not a canal, never a river's own line); add `sc` to layers.py's
prop list. Style filters on `["has","sc"]`. Ships with the next tiles build.
