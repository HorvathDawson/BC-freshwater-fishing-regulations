# T190 — a two-region piece's "How this was decided" ladder heads both regions' rules with one region

Status: OPEN

Source: T183 test re-pin lane, 2026-10-24 (commit 4e2bd4ca).

## Problem
On a piece in two regions (T183), the ladder's region step is headed after the FIRST rule's region only
(`steps()` in app/packages/core/src/regs/decided.ts): the Fraser's part 14 (Regions 3 and 5) lists Region 5's
rules under "Region 3". T183 asked for the page to show the two regions clearly. No parity test covers this page
since x15p14 became PART_GONE in taps.py.

## Work
Group the region step by region (one heading per region, its rules under it) when a part carries two regions; pin
it with a fixture test on a real straddler (Fraser part 14, West Road part 3) — the golden cannot, it predates T183.
