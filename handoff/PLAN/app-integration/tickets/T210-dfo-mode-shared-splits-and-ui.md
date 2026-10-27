# T210 — DFO review mode: proposed splits shared along the river, Place "y" without a new split, simpler new-split UI

Status: DONE (0805e479)

Source: owner 2026-10-26, using the DFO extents mode (T164).

1. A split proposed on one DFO location (a `new_splits` entry in a mark) must show on EVERY other DFO location / entry
   on the same river (same item / blk), drawn as "proposed" and pickable as a point — today it disappears when you move
   on, so it gets entered several times. Read proposed splits from all marks; dedupe by anchor (same blk + measure
   within a few metres, or same confluence/lake anchor).
2. When the extent is right with the existing cuts, the reviewer must be able to answer Place = y; find and remove what
   pushes the answer to ? / n when no new split is added.
3. New-split UI: click the river → pick an anchor type (confluence · lake inlet/outlet · bridge · dam · sign · here) →
   confirm; name and side pre-filled from the anchor and the extent; edit only if needed. No long form.
Mark stays the only write (T185 / AGENTS 66–67).

## Outcome (2026-10-26)

1. `marks.proposals()` reads `new_splits` from ALL marks (marks.jsonl untouched), dedupes by anchor (same
   tributary/lake, same blk within 10 m, or coords within 10 m), keeps the first name/id and the marks that
   proposed it. Today: 3 proposals in the store = 2 distinct cuts, 1 duplicate (Quinsam confluence on two
   Campbell locations). `GET /api/dfo/locations/<id>` carries `proposed` for every location on the river
   (amber on the map, listed, pickable as a point in fix-extent); progress counts distinct cuts. A mark that
   re-proposes a cut reuses the id (response `reused`, UI "already proposed — reused"); T185 tickets say
   "ALREADY PROPOSED as `nX`".
2. What forced Place to ? / n: an extent fix of existing cuts was only allowed on a wrong/unsure mark. A DFO
   mark with no new split and only existing-split fix points may now be verified (P y) and still exports as a
   ticket; any new/proposed cut still needs P ? or n.
3. New-split UI: click the river, pick confluence / lake inlet / lake outlet / bridge / dam / sign / here,
   confirm; name and side pre-filled (notice hints, op). Long form removed.
Scope: DFO mode only (synopsis-entry fix editor unchanged).

## Outcome 2026-10-27: the saved extent shows again
Owner: "didn't save the ones I did before". The 4 `extent_fix` marks WERE in marks.jsonl; the screen never read them back.
- Reopening a DFO location now shows "your extent (saved <date>)" (op, points by name, items, tributaries), draws it
  teal on the map (distinct from magenta "binds now"), and the fix-extent editor opens PRE-FILLED with it, so a
  re-mark carries the fix on instead of dropping it. Latest mark wins.
- The editor is open on every location (`defaultOpen`), with or without cuts or a new split; a "Save extent" button
  records a mark with it (Place ? when nothing was answered).
- A save says "saved ✓ ..." (also in the top notice after the screen moves on); a refusal shows "NOT SAVED" with the
  server's reason, at the top of the location, role=alert.
- Tests: `test_review_dfo.py` (round trip + latest wins; screen source pins), `test_curation_review_screen.py`
  (POST then GET shows the fix; refusal has a reason). One-write pins unchanged and green.
