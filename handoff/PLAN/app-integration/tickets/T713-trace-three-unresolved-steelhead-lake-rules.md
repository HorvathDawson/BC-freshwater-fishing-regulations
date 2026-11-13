# T713 — Trace why trout_quota.r5b, trout_char_quota.r6b and r9b bind nowhere

Status: DONE (655d3000a; traced: by design, no change).

Source: boundary-name pack (PACK.md ~line 218): trout_quota.r5b (Region 1 "All wild steelhead"), trout_char_quota.r6b (Region 5 "ALL STEELHEAD"), trout_char_quota.r9b (Region 6 "all ...") were already unresolved in the old release; cause not traced. Owner 2026-11-13: needs tracing.
## Work
1. Trace each rule from catalogue to bundle: what scope it names, why placement finds no water.
2. Fix if it is ours; otherwise carry the rule on its water (unplaced-extent rule: never widen) and say why.
## Acceptance
Cause written for each of the three; each either binds to the right water(s) (binding diff shown) or is recorded as deliberately unplaced; unresolved count in the tests updated.
Depends: none. Size: S-M. Owner: no. Default: carry, never widen.

## Trace (2026-11-13, pipeline lane; live release r-adc4e248a163, reach run `data/generated/reaches/full`)

All three are the LAKE twins of a zone's wild-steelhead release (AGENTS 54): `trout_quota.r5b` (z1, sibling r5),
`trout_char_quota.r6b` (z5, sibling r6), `trout_char_quota.r9b` (z6, sibling r9). Each has one extent
`{op: steelhead_waters, siblings: [<base>], area_id: area:region:N, feature_types: [lake, wetland]}`: the book-known
steelhead waters of its region that its stream sibling does not already bind. The base binds every STREAM of the region.

Why they bind nothing (reach diagnostic `steelhead_waters`, all three `in_area: 0, kept: 0` of 73,929 known
sections): a lake is book-known ONLY as a steelhead row's own water (ruling 2026-10-06, `steelhead.LAKES_ONLY_BY_OWN_ROW`),
and the province has exactly two — Lois Lake and Khartoum Lake, both Region 2 (`steelhead_presence.jsonl`, kind lake,
`known`), which `z2` `trout_char_quota.r7b` binds (`in_area: 2`). Regions 1, 5 and 6 have no steelhead row written for a
lake or wetland, and the unnamed widenings that used to be lake water were folded into their rivers by T678 D1, where
the stream sibling binds them. The same holds for `z1:hg_quota` r6b and `z3` r5b.

Not ours to fix and not a defect: the rules are right to bind nothing until a steelhead row names a lake in those
regions. They are already on the T89 allow-list as BY DESIGN (`pipeline/tests/test_t89_rules_that_bind_no_water.py`,
with this reason; the bundle test `test_only_the_allowed_rules_bind_no_water` pins the set both ways). No change, no
count change.
