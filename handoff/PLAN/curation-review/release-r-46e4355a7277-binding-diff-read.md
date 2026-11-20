# Release r-46e4355a7277: the binding diff against the live release r-f749475d0962, as read (2026-11-16 02:25)

`data/generated/release/checks/r-46e4355a7277/binding_diff.json`: 407 rules, 0 unresolved, 32 licensing, 194 DFO
places, 1 tidal section (Pallant Creek 360874940:0 now tidal, from the owner's tidal mark), 3 diagnostics (the Mabel
Lake end picked for three Shuswap places).

Split (scratch `release-diff/before-tonight.json`, `tonight.json`; `binding_diff` run on the kept reach runs):
- EARLIER WORK, live release -> `reaches/full.pre_ri3` (chat 13's promoted atlas and reach reruns, each read at its own
  promote: `curation-review/side-rebuild-2026-11-15.md`, `promote-2026-11-15.md`, `rebuild-finish-2026-11-15.md`):
  379 rules, 29 licensing, 164 DFO places, 1 tidal. NOT re-read by me; relied on those records.
- TONIGHT, `full.pre_ri3` -> `reaches/full`: 187 rules, 6 licensing, 56 DFO places, 2 diagnostics. Rules and licensing
  are IDENTICAL (same keys, same gained / lost / rescoped) to side build 3's diff, read and attributed before the
  promote (13 new cuts, 2 length cuts re-spaced, Puntledge +27 m, Sproat -50 m). DFO: 47 equal to that read; 9
  differ, all landings after it: Kasiks x3 (no_extent -> bound: 193, 233, 425), Middle Shuswap x2 (+18 each, T778
  piece included), Skeena sockeye pair (rest-of with no sections of its own, -53, reads "unresolved" in the table;
  answers verified by the cascade review part 2), Nitinat below Parker Creek (-3, rest-of), Stamp "Except…" (+2).
- Every key of the release diff is in one or both sets (rules 0 outside, licensing 0, DFO 0); one DFO place changed
  earlier and changed back tonight.
- 50 section ids exist only in the new atlas and 18 only in the old one (the cuts); zone-wide rules show +50 -19
  (the 18 plus Pallant's tidal section).
