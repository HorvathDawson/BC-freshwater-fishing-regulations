# T190 — a two-region piece's "How this was decided" ladder heads both regions' rules with one region

Status: DONE (5a6cb255)

Source: T183 test re-pin lane, 2026-10-24 (commit 4e2bd4ca).

## Problem
On a piece in two regions (T183), the ladder's region step is headed after the FIRST rule's region only
(`steps()` in app/packages/core/src/regs/decided.ts): the Fraser's part 14 (Regions 3 and 5) lists Region 5's
rules under "Region 3". T183 asked for the page to show the two regions clearly. No parity test covers this page
since x15p14 became PART_GONE in taps.py.

## Work
Group the region step by region (one heading per region, its rules under it) when a part carries two regions; pin
it with a fixture test on a real straddler (Fraser part 14, West Road part 3) — the golden cannot, it predates T183.

## Outcome
Done 2026-10-24 (5a6cb255). `steps()` in `app/packages/core/src/regs/decided.ts` groups the region level (rank 3) by the rule's
region (`prov.entry_name`): one heading per region, its own rules under it; a one-region part keeps one step. Pinned:
`decided.test.ts` (two regions → two steps, one → one) and `app/conformance/src/regs/twoRegions.fixture.test.ts` on the
committed regs fixture release — the Fraser part 14's trout-and-char ladder reads "Region 3" then "Region 5", each with
its own possession line (was both under "Region 3"); no region heading repeats in any question of any row of the
Fraser part 14 or the West Road part 3 (its Region 6 rules sit under "Zone A"). Mutation: grouping by rank alone fails
3 of them. Golden parity unchanged (127/127 keep, plain rows, sheets).
