# T127 — A displaced rule still shows a "counts toward this one" line, and the card gate bounds a kind by it

Status: OPEN — found 2026-10-22 by the Perry Creek card-gate fix (f9256346 on the agent branch; T80).

- On Perry Creek (gnis:14741) the St. Mary River's "trout/char 1 on all tributaries" row (r2) still lists brook trout
  as "counts toward this one (1)", though the reader DISPLACED r2 for brook trout (reason `own_row`: Perry Creek's own
  "brook trout 20" governs). Source: a displaced ("replaced") role still produces a cross-reference item
  (pipeline/deliver/answers/rows.py ~736). The reader is right; the card's words are wrong.
- `card._kind_n` takes the smallest number over every item holding a kind, including such cross-references, so the
  card gate can be misled by them too.
- Fix (one decision): a displaced role yields no cross-reference, decided by the same "who governs this kind here"
  answer the reader gives (no second copy); the gate's `_kind_n` then needs no special case. Whole-value test on
  Perry Creek; answers diff listing every card whose "counts toward" lines change. Needs deliver.
