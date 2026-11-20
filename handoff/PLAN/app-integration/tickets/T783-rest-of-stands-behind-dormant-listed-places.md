# T783 — A rest-of place stands behind its LIVE listed places only; a retired (dormant) listed place it now covers can be answered after it

Status: DONE — 3f90138cf. A rest-of place stands behind its dormant listed places too (Stamp chains 55, 56); merged 02aea5be3.

Closed by the 2026-11-20 audit: built and tested on lane/salmon-rules-1116 (3f90138cf, test_t783_rest_behind_dormant.py, chain diff = 3 Stamp sections). Merged via 02aea5be3 after the owner's 2026-11-16 go-ahead ("Go on all (Recommended)", which names T783). Shipped from the 0.8.0 data release on.

Source: general audit 2026-11-16, code checker finding 1 (`handoff/PLAN/audit-2026-11-16/code.md`).

## What is wrong
T761 made a dormant listed place "take nothing", so the rest-of place binds that place's sections. `rest_after` moves a
rest behind its LIVE listed places only; against a dormant one plain size order decides. In r-46e4355a7277 this is the
Stamp River, chains 20, 55 and 56: the rest (2 sections) sorts BEFORE the dormant "upstream of signs at Girl Guide"
(7 sections). If DFO prints that wording again, the live feed gives it rules and the rest's opening (chinook and coho,
129 days) would answer first there until the next release (the chain order was read from the release; the feed step
by reading).

## What to build
- A rest-of place stands behind every place it lists or leaves out, live or dormant.
- A test on built chains: a dormant listed place that gains rules answers before the rest.
- Chain diff of the whole release before and after: expected, only those Stamp chains reorder.

## Lane result (lane/salmon-rules-1116, 2026-11-16)
- `deliver.salmon.dormant_listed`: each rest's DORMANT listed places (`entries.rest_listing`), merged into `after` in
  `build`, so `rest_after` puts the rest behind them as behind its live ones. No reach re-run needed; no worker change
  (the live feed reads the chain order from the release).
- Test: `pipeline/tests/test_t783_rest_behind_dormant.py` (built chains; mutation: the merge a no-op -> fails).
- Whole-release chain diff (salmon step rebuilt from the built inputs of r-46e4355a7277, the baseline rebuild
  byte-equal to the release): 3 sections change, all Stamp: chain 55 (2 sections) and chain 56 (1 section) now put
  "upstream of signs located at Girl Guide" before the rest. No verdict, gear or map run changes today (the dormant
  place has no rules). Chain 20 already had the rest behind its two live listed places; nothing to move there.
