# T778 — A river side channel that enters the bounding lake on its own "straddles the cut" and stops the reach run (Shuswap at Mabel Lake)

Status: IN PROGRESS (left (Claude): diagnose and fix in a lane, binding diff, then the app data build; blocks tonight's release).

Source: `pipeline build --hold-latest`, 2026-11-15 23:26 (log `data/generated/build-logs/2026-11-15T232613/reach.log`):
the reach run REFUSED (`UnplacedGateError`, T632).

## What happened
The owner's saves for the two Middle Shuswap DFO places (`8:shuswap-river:middle-between-shuswap-falls-and-m` and its
older wording `~2`) landed tonight once the new cut `shuswap_river__shuswap_falls_wilsey_dam` (blk 356363194,
m 136,497.1) was built. Their extent is the stretch between that cut and `shuswap_river__mabel_lake` (the lake-end
alias; two candidates 93,618.86 and 113,657.25; the run uses 113,657.25, the inlet end, correctly).

The run then withheld one piece from both places: `355998688:691` (`straddles_the_cut`). It is a 122 m piece of a
Shuswap River side channel (blk 355998688, m 691.8 to 813.7) at the river's delta: it flows INTO Mabel Lake
(lake:329663955) on its own, and above it runs through a small unnamed lake (lake:329664272) to the channel's upper
piece `355998688:895`, which leaves the mainstem piece `356363194:113657` (inside the place). So the piece is river
water above Mabel Lake and belongs inside the place. The classifier (`pipeline/atlas/reach/extent.py`) sees it touch
nothing of the water directly (both neighbours are lakes) and cannot place it by its own line or its flow.

A provincial piece that straddles a cut is never accepted in `unplaced_piece_calls.json` (T67 / T632), and accepting
it would be wrong anyway.

## What to build
- The cause, shown on the built atlas (which branch of the classifier leaves it unplaced, and why the side check
  before landing the marks, `apply dfo --side-checked`, did not see it).
- The smallest correct fix. Expected: in a window whose end is a lake-end cut, a piece of the water whose flow enters
  THAT lake without passing another section of the water is on the window's side of the cut. `POLICY_VERSION` bump
  with its changelog line if the reach policy changes.
- Proof: the piece binds to both places; a binding diff of the whole reach run before and after, every difference
  attributed (expected: these two places gain one section each and nothing else moves); a test on built data.
