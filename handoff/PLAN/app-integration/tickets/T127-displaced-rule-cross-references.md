# T127 — A displaced rule still shows a "counts toward this one" line, and the card gate bounds a kind by it

Status: DONE — dbc7fd2f (lane wf_754f5fcd-d71-2; found 2026-10-22 by the Perry Creek card-gate fix, 3a6d20c3, T80).
`rows.counts_toward` (governs, or `contains`) decides xref / group kinds / outer total. Answers diff (scratch, full set):
5 cards lose a cross-reference (St. Mary r2 EB on keys 65 + 326 incl. Perry Creek; Beattie Lake r2 DV on key 2056 ×3),
Perry Creek's own row becomes a 20-a-day group (9 new strings); card gate 0 problems on 7,180 cards. Left: ships with the
next deliver (answers rebuild).

- On Perry Creek (gnis:14741) the St. Mary River's "trout/char 1 on all tributaries" row (r2) still lists brook trout
  as "counts toward this one (1)", though the reader DISPLACED r2 for brook trout (reason `own_row`: Perry Creek's own
  "brook trout 20" governs). Source: a displaced ("replaced") role still produces a cross-reference item
  (pipeline/deliver/answers/rows.py ~736). The reader is right; the card's words are wrong.
- `card._kind_n` takes the smallest number over every item holding a kind, including such cross-references, so the
  card gate can be misled by them too.
- Fix (one decision): a displaced role yields no cross-reference, decided by the same "who governs this kind here"
  answer the reader gives (no second copy); the gate's `_kind_n` then needs no special case. Whole-value test on
  Perry Creek; answers diff listing every card whose "counts toward" lines change. Needs deliver.
