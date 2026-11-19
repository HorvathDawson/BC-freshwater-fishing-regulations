# T891 — braided rivers drawn simpler: side channels thinner, braids simplified in the tile build

Status: PARTIAL — part one on staging (tiles `sc` + thinner style, r-379e72fc9524; 2b5a1748a, 07ee36ad5); owner: OWNER-TODO "Test 0.8.2 on staging (2026-11-19 morning)"; part two (braid simplification) waits for him.

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

## Part one built (lane data/t891-side-channels, 2026-11-19 ~03:10)
- Tiles: `export.side_channel_flag(node)` stamps `sc: 1` on the stream LINE writer for `is_side_channel_piece` pieces
  only; absent elsewhere (a stream polygon's route and a lake link never carry it). `sc` in layers.py's stream attrs,
  tile-contract.json regenerated. Pinned: `test_tiles.py::test_a_side_channel_carries_sc_1_and_the_main_channel_carries_nothing`
  (writer + tile cut), the inventories in test_tiles / test_one_data_flow.
- Style: `width.sideChannel` on the stream layer (layers.source.json) — the linear width times token `width.stream.side`
  (0.5, all three themes) where `["has","sc"]`, INSIDE each stop of the one curve (`paint.ts linearStops`). So `stream`,
  `stream-heavy`, the selection casing/glow, gear edges, closed stripe, runs and issue marks all follow it; no layer
  added (T848/T876/T903). Same colour. A selected / lit side channel still takes the highlight floor
  (`width.stream.highlight` / `.water`) under its casing. Pinned: `app/tools/side-channel.test.ts` (every theme x every
  stream mode: plain, standing, profile, salmon, runs; evaluated with MapLibre's evaluator). midzoom stays green (the
  main channel's width is unchanged).
- Measured on the live 11-18 atlas build (read-only): 40,797 of 1,253,165 stream features (3.3%) would carry `sc`.
  By FWA edge type: ~31k are braid lines (1100 / 1350 / 1150, often with 1450); ~9.8k are 1400/1410 (~9.2k), 2000 (523),
  1425 (28), 6010 (6) — mostly order 1-3 and unnamed; they also draw thinner under today's `is_side_channel_piece`.
  Tile size: a scratch archive over three boxes (13,281 stream features, 388 `sc`) grew 5,861,299 -> 5,866,235 bytes
  (+0.08%).
- Pictures (scratch atlas over the boxes, plain map; before | after pairs, 390 px light + dark, plus 1280x800):
  `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/1db4ea05-d81d-4b07-a925-fc0b9cdbe701/scratchpad/sc-lane/shots/`
  (`pair-*.png`, `sc-desktop/`). The owner's spot is the Lillooet at Tipella (head of Harrison Lake), not Pemberton;
  `label-shots.mjs` gained places `lillooet-tipella`, `lillooet-upper`, `lillooet-pemberton`, `fraser-herrling`.
- Release: needs a tiles build (the live tiles carry no `sc`; with them the new style draws exactly as today). The
  style + tiles ship together (tiles and bundle share a vintage).


## Owner 2026-11-19 ~03:25: the side-channel style approved
Owner, verbatim (main chat), on the night's pictures: "T900 looks good you can add side channels style too". Part one (tiles `sc` + thinner style) ships in 0.8.2 with tonight's tiles build. Still open for him: whether the ~9.8k small non-braid lines that pass `is_side_channel_piece` (FWA types 1400/1410/2000/1425/6010) should also draw thinner, and part two.


## Owner 2026-11-19 ~06:40: the small non-braid lines stay thinner
On the ~9,800 small lines that also draw thinner, verbatim: "probably okay". Kept as built.
