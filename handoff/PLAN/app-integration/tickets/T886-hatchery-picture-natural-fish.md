# T886 — "How to tell a hatchery fish": a natural fish, not a smiling one

Status: DONE (4a9cc54da, 3f5497585; in 0.8.2 on app/integrate-082, ledger brought up to date 2026-11-19). Was: IN PROGRESS (app chat lane app/t884-t886-clip-hatchery-art). Pictures to the owner before it lands.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T886 | "How to tell a hatchery fish": a natural fish (the loader's coho), not a smiling one (owner) | DONE | 4a9cc54da, 3f5497585 | owner 2026-11-18 ~21:30 |

Owner 2026-11-18 ~21:30, verbatim: "Also the how to tell hatchery fish is very smile creepy we want to make it more
realistic I think. Better shape more natural smile. Could use the salmon general shape from loading maybe?"

## Do
- Redraw the two pictures ("Wild · adipose fin there", "Hatchery · clipped") on the loading screen's coho
  (`tools/fish-loader/fish_loader.py`, the owner's drawing): its shape, with the adipose fin on the wild fish and a
  healed scar where it was on the clipped one, the spot ringed on both, so the comparison still teaches.
- Colours only from tokens (check-colours); right in light, dark and the colour-blind theme.
- Before / after pictures at 390 wide, dark and light.

## Done (lane app/t884-t886-clip-hatchery-art, 4a9cc54da)
- The two drawings are the loader's coho (`tools/fish-loader/adipose_art.py` over the owner's `fish_loader.build()`), at
  rest, its mouth CLOSED (the loader's open kype with three teeth read as a grin in a still picture); wild: the adipose
  fin; clipped: no fin, a healed scar (a low mound in the red block, a carved line across it); both ringed with a leader.
- Colours are tokens: the fish in `color.ui.coho.ink` + `.red`, the ring in `color.ui.accent`, PRINTED per theme as the
  loader's sheet is (`tools/build-adipose.mjs`, `pnpm adipose` in check:fast reads the committed files back against
  the tokens). Light and the colour-blind theme share a pair; dark has its own (cream fish on the dark card).
- `AdiposeArt` picks the pair by palette (as `cohoSheet`); the old `ADIPOSE_IMAGES` plumbing, `emit-adipose.mjs` and
  `fixtures/fishart.js` are gone. `T38-shots/` re-taken.

## Round 2 (review, 2026-11-18): the difference read at phone size, the face plainer
- The wild fish's adipose fin is drawn bigger (a fleshy fin with no rays, its base carved) on the back between the dorsal
  fin and the tail; the clipped fish's back runs on bare with a short red seam (the healed scar).
- Each drawing is 2:1 with a 2.5x close-up circle of the spot over the back, joined to the ring on the fish.
- The face: the loader's jaw tab, gill swirl, plate, brow and cheek lines replaced by a plain eye, a straight closed mouth
  line and one gill arc.
