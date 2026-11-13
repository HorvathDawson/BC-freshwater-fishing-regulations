# T713 — Trace why trout_quota.r5b, trout_char_quota.r6b and r9b bind nowhere

Status: OPEN.

Source: boundary-name pack (PACK.md ~line 218): trout_quota.r5b (Region 1 "All wild steelhead"), trout_char_quota.r6b (Region 5 "ALL STEELHEAD"), trout_char_quota.r9b (Region 6 "all ...") were already unresolved in the old release; cause not traced. Owner 2026-11-13: needs tracing.
## Work
1. Trace each rule from catalogue to bundle: what scope it names, why placement finds no water.
2. Fix if it is ours; otherwise carry the rule on its water (unplaced-extent rule: never widen) and say why.
## Acceptance
Cause written for each of the three; each either binds to the right water(s) (binding diff shown) or is recorded as deliberately unplaced; unresolved count in the tests updated.
Depends: none. Size: S-M. Owner: no. Default: carry, never widen.
