# T783 — A rest-of place stands behind its LIVE listed places only; a retired (dormant) listed place it now covers can be answered after it

Status: OPEN (pipeline `pipeline/deliver/salmon.py` `rests_behind` / `rest_after`; latent: no answer is wrong today).

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
