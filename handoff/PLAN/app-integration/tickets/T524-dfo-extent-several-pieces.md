# T524 — A DFO place's extent can be several pieces on different waters

Status: DONE (2026-11-09; the owner restarts the review app, then marks the proposals)

Owner 2026-11-09 (review app, DFO region 2): "from a line between 2 fishing boundary signs on either side of the
Chilliwack River 100 m from the confluence of the Chilliwack River and Slesse Creek downstream including that portion
of the Sumas River from the Barrow Town Pump Station downstream to fishing boundary signs near the confluence with the
Fraser River" cannot be marked: "it needs downstream of pump station and downstream of slesse on 2 different rivers. so
either needs 2 extents combined or a different representation".

## Today
- A mark holds one `extent_fix` per scope (`pipeline/regs/review/marks.py` "one extent fix per scope"); a DFO mark has
  one scope (entry), and a `between`/`downstream` fix sits on one water. So a two-river place has no representation.
- Same text on several region-2 locations (places/region-2.json locations 12, 48, 54, 56 at least).

## What (recommended: combine pieces)
- A DFO place's extent is a list of PIECES, each one of the existing ops on its own water(s) (e.g. Chilliwack-Vedder
  from the Slesse cut downstream; Sumas River from the Barrow Town Pump Station boundary downstream). The place binds
  the UNION. One piece = today's format, unchanged (no migration of existing marks).
- Review app (ExtentFixEditor / DfoReview): "add another piece" on a DFO mark; preview shows the union.
- Pipeline: proposals → curated place extent → reach binding read every piece; a piece that binds nothing fails the
  build (no silent drop). Tests: two-river union, one-piece parity with today.
- The owner's running review app is not restarted by an agent; the owner restarts it when told.

## Done (2026-11-09)
- **Model:** a DFO mark's `extent_fix` list is the place's PIECES (`marks.pieces_ok`: all on scope entry, each once, no
  `rest` beside another, one tributary call). Every other mark keeps "one extent fix per scope". One piece = the mark as
  it always was (no migration; `line()` unchanged).
- **Proposals:** each piece lands in order in the place's `binding.extents` (the union the builder already reads —
  `classify` unions extents); a piece's new point / new split is its own `new_split` proposal the extent depends on.
- **Build gate:** `reach.dfo_salmon.dead_pieces` resolves each piece of a several-piece place alone (no walk);
  `resolve_all` STOPS naming any piece that binds nothing. Dry run on the live atlas: every existing multi-piece place
  passes except **6:e:section piece 2** ("within MU 6-12/6-13 outside MU 6-12/6-13" — an area minus itself; binding-neutral
  to remove; owner decision in OWNER-TODO / fix proposal) — **the next build stops until it is resolved.**
- **Review app:** one ExtentFixEditor per piece, "+ add another piece"; "preview on map" on any piece previews the
  UNION, with what each piece binds alone and a dead piece called out (`/extent-preview?fix=[…]`); saved extent shows
  every piece. Also (owner 11-09): a new fix/piece starts on the place's tributary call (own binding, else the DFO
  title's), labelled; the reach line says "with tributaries — not drawn; tick Show tributaries" instead of "(without
  tributaries)"; a banner says when the mark's fix REPLACES the curated extent (old struck through, magenta, vs the fix,
  teal), "same as current", or "fix applied", and the queue row carries a "replaces extent" / "same as current" /
  "fix applied" tag (`dfo.fix_vs_current`).
- **FWA facts:** the Sumas "Barrow Town Pump Station" auto boundary (`sumas_river__sumas_river`) no longer exists — the
  Sumas lake polygon was absorbed into the Sumas River item; a lake-outlet anchor is refused, so the Sumas piece starts
  at a NEW point cut 50 m below that polygon's outlet (`check_anchor`: blk 356352298 m 5579.5). At the Forks, FWA's
  water below the confluence is the BULKLEY (blk 360873822, wsc 400-431358); the Morice (blk 360885316) ends there.
- **Proposals for the owner** (`data/generated/review/dfo_fix_proposals.json`, "use this fix"): region-2 locations 56,
  12 (2 pieces + the new Barrow Town cut), 48 (same + FLAG: the "with exception of … closure" carve-out), 54 (FLAG only:
  self-contradictory "upstream … downstream"); region-6 Forks 128 + 129 (2 Bulkley pieces, tributaries excluded; 129
  replaces the Morice arm). Sweep, not proposed (need new cuts / owner reading): r1 San Juan + Fairy Lake tributary,
  r1 Cayeghle incl. Colonial (one `whole` on both items), r2 Alouette above 216th incl. North Alouette, r3 South Thompson
  incl. Little Shuswap Lake.
- Tests: `test_review_proposals.py` (two-river union, Forks two pieces, one-piece parity, dead piece, replaces/same/
  applied), `test_dfo_suggest.py` (pieces pre-fill, tributary default + wording), `test_review_dfo.py` (screen pins).
