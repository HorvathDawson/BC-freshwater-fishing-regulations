# T779 — The side check used when a DFO mark is landed does not report a withheld piece; only the full reach run refuses it

Status: DONE — c77eeb601 (2026-11-22), dd942ce76, ff7cece62, 4916c3cf0 (2026-11-23, review fixes). Tested on built data (`test_review_withheld_built.py`, slow): the Middle Shuswap places with T778's fix off are refused, with it on they land. Not yet seen in the browser.

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

State 2026-11-23: review fixes dd942ce76, ff7cece62, 4916c3cf0 on branch fix/T775-review-app-leftovers (PR #48); origin/main merged.

## Review fixes 2026-11-23 (PR #48 independent review, dd942ce76)
- A check that cannot run refuses (W46): a DFO place on a cut-changing chain whose check says `cheap: False` (a new or
  just-landed cut the live atlas has not got, an unbound split, an error) is refused with the reason, never landed
  unchecked. Every side DFO place is checked, not only `--side-checked` ones (a ready side atlas skipped it too).
  Landing order for such a place: the cut, an atlas rebuild, then the place.
- Stale acceptances: `withheld.check` reports this place's `accepted` rows that name no piece now; the rule is the gate's
  own (`build.unplaced_verdict`, shared by `unplaced_gate`).
- The run's limits: every save-time binding of a place (preview, size, landing check, dead pieces) takes
  `dfo.as_the_run_binds` = `resolve_all`'s `regional=False` and tidal limit (`keep_tidal`). On the served atlas 328 of 328
  places now equal the reach run's rows; with the old arguments 27 differed, all by tidal water (e.g. Chemainus River
  24 vs 8 sections); the region limit moved none (the Fraser places at the Region 2 / 3 line at Hope are pinned).
- Tests: the gate inside `run()` (side-checked, side atlas ready, no cut change), the all-accepted case, stale, parity;
  each pinned by a mutation.
