# T1084 — FWA `LEFT_RIGHT_TRIBUTARY` is UNKNOWN for 188 streams, so a one-bank rule cannot tell which side they join

Status: OPEN.

Source: the 0.9.1 chat, 2026-11-24 (T1029 ticket, draft PR #112): T1029 carries the field into the atlas; a one-bank place then reports `tributary_side_undetermined` for these streams.

## Work
1. List the 188 and how many sit on a water with a one-bank rule today (only those matter now).
2. For those: work the side out from geometry (which side of the mainstem's direction the mouth joins), as a proposal the owner reviews, never pre-accepted.

## Acceptance
the count that matters is in the ticket; each has a proposed side or a written reason; none is silently treated as either bank.

Severity: low. Release: 0.9.2, or with T1029 if its build needs one of them. Model: Opus. Size: S–M. Owner: yes if any proposed side needs review.
