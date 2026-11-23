# T779 — The side check used when a DFO mark is landed does not report a withheld piece; only the full reach run refuses it

Status: DONE — c77eeb601 (2026-11-22). Not yet seen on a real straddling place in the browser; tested on the gate rule and the backend call (stubbed binding).

Source: T778, 2026-11-15 night. The two Middle Shuswap saves were landed with `apply dfo --side-checked` ("0 -> 17
sections, bound"), and the full build then stopped 14 minutes into its reach run on a piece those places withheld.

## What is wrong
For a DFO place the side check is the in-memory `build_reach` binding (`proposals.side_commands`: "the in-memory
check above IS its binding"). It reports the sections and the outcome only. The unplaced-piece diagnostic
(`unclassified`, not included) that the T632 gate reads is produced by the same call and thrown away. The review
app's preview has the same blind spot, so the owner cannot see it when he saves.

## What to build
- The side check and the review app's preview list every piece the place withholds, with its reason, and the check
  FAILS (apply refuses to land the mark as side-checked) when a withheld piece is not in `unplaced_piece_calls.json`
  `accepted`: the same rule as the reach gate, at save time.
- A test on built data: a place with a straddling piece is refused by the side check.
