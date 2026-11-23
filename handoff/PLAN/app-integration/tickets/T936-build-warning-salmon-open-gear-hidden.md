# T936 — Build warning: a water whose salmon are open on a game-fish-closed day that carries a B.C. gear rule

Status: DONE — cff5478e9, 0f8a4143d; shipped in 0.8.4 (cut e769f710a, data r-fec3fab4c5aa; 0.9.0 audit 2026-11-22, every hash is an ancestor of origin/main and of e769f710a). (Earlier status: IN PROGRESS — built on lane/t936-t931 (cff5478e9); left (Claude): main chat review + merge.)

Source: owner 2026-11-20 ~01:03 (main chat), after T934. Asked whether a B.C. closure closes salmon: yes, a FULL closure
does (rule book p.77; T796 A5 puts it on the salmon chains). The leftover case: the game-fish strip reads closed on a day
by species closures that together close every game fish (no single full closure), DFO has salmon open or release that
day, and the B.C. gear answer holds a bait ban / fly fishing only / artificial flies only. The strip drops gear bits on
a closed day (`display/strip.py` `strip_code`: CLOSED alone), so the salmon bar shows no gear mark there (T934; owner:
"closed is only closed"). Verbatim: "Maybe add a build warning if there is a Waterboys [waterbody] like that".

## Work
In the release build (beside `bundle_problems` in `pipeline/deliver/store/build.py`, or where the salmon chains and the
display / gear frames are both at hand): list each water + section + day span where all three hold: game-fish display
status closed, not by a book full closure (T796); a DFO salmon verdict open / release; the gear answer at the same part
key and segment carries `bait_ban` or `fly`. Print it as a WARNING with a count and the first N rows (never fails the
build), and write the full list beside the release's other build reports. A test pins one synthetic case.
## Acceptance
The build prints the warning (or "none"); the count is reported to the owner with the next release.
Depends: none. Size: S. Owner: no (see the count with the release). Data change: rides the next data release.

## Built (lane/t936-t931, cff5478e9; 0f8a4143d: deliver all passes `--reports` before the tile flags)
`pipeline/deliver/release/salmon_gear.py`, called by `release.build` right after the T796 book closures join the salmon
chains (`with_book_closures`): per section and day, every moment's display frame closed with NO rule closing every game
fish at every moment (the frame's `closing`, the same test as `salmon.book_closures`), the chain answering `open` /
`release` for some salmon species, and some moment's gear frame holding `bait_ban` or `fly`. Rows grouped per water part
(or unnamed stretch) x day span x species x gear. Prints `WARNING (T936): N stretch(es) … First 10:` or
`T936 salmon-open, gear-hidden days: none`; never a stop. The whole list goes to
`<reports>/<release id>/salmon_gear_hidden.{json,txt}` (`release.build(reports=)`, `--reports`; `deliver all` passes
`data/generated/release/checks`, beside `gone_ids` / `live_counts`). For a shipped release:
`python -m pipeline.deliver.release.salmon_gear [--release DIR] [--out DIR]`. Test: `test_t936_salmon_gear.py` (the case,
a full closure, no gear, a moment open, unnamed + fly).
COUNT ON THE LIVE RELEASE r-6258ffded676: **none.** Every one of its 906 closed display frames has a single rule closing
every game fish (a full closure), so no day is closed by species closures alone (all 1,911 closed cells carrying a gear
rule are full closures, where the salmon verdict is closed too).
