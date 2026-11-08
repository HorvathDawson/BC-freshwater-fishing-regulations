# T491 — test_curation_review_screen's tidal test reads live curated data

Status: OPEN — found 2026-11-07; the owner's 11-06 Nanaimo mark decided it.

(File restored by the T483 lane from the ledger row: 5c76a2892 added the row but not this file, which broke the ticket
ledger check. The finder fills in the details.)

## Do
- Give the tidal test in `pipeline/tests/test_curation_review_screen.py` its own fixture instead of the live curated
  files, so an owner mark cannot decide its outcome.
