# T524 — A DFO place's extent can be several pieces on different waters

Status: OPEN

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
