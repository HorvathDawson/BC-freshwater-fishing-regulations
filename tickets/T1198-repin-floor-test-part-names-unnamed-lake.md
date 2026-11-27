# T1198 — Re-pin the floor test `test_part_names_unnamed_lake::test_province_wide_no_part_ends_at_a_lake_or_a_count` (474 vs 475)

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1198 | Re-pin the floor test `test_part_names_unnamed_lake::test_province_wide_no_part_ends_at_a_lake_or_a_count` (474 vs 475) | OPEN | — | change the pinned floor on `main` and say why |

Source: #312's round-4 author, 2026-11-26: the test's pinned number (474) predates the PR; T1146 (#264) folded four
waters, so the true count on `main` is 475. Found by the planning chat's review of #312.

## Work
1. On `main`, re-pin the count in `pipeline/tests/test_part_names_unnamed_lake.py` (the file may sit in a stage
   sub-folder after the test sort: use its path on `main`). A one-line change with a comment: "T1146 folded four
   waters, 474 -> 475".
2. Run only that test, then the pre-push tier.

## Acceptance
The test passes on `main` with the new number and a comment saying where it came from.

Severity: low. Model: Sonnet. Size: XS. Owner: no.
