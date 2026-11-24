# T1091 — A fresh Standard map fetches only the salmon head: anything drawn from the full salmon facts is missing until the Salmon map has been opened

Status: OPEN.

Source: the 0.9.1 chat, 2026-11-24 (T1029 ticket and draft PR #112, CDP proof on section 1234098): on a fresh
Standard map only the salmon head is fetched; the full facts (shards) arrive only after the reader visits the Salmon
map, and labels computed from them then persist when switching back. T1029's one-bank cue on Standard did not draw
until Salmon had been opened (fixed there by labelling from the head). Not verified beyond the T1029 case.

## Work
1. Check first: list everything the Standard map draws or says from the full salmon facts, starting with T796's
   finfish closures. For each, open the Standard map cold (web and phone) and say whether it is missing.
2. If a closure can be missing on first open: that is the defect to fix first. Either the Standard map reads only
   the head (and the head carries what it needs), or it loads the facts it depends on; one load path, the same map
   whatever the way in (`one-load-path-per-screen`), and no silent absence (`no-silent-fallbacks`).
3. A test that renders the Standard map with no prior Salmon visit and asserts each such layer and label.

## Acceptance
The list from step 1 with a verdict per item; the cold-open test fails before the fix; the Standard map is the same
before and after a visit to the Salmon map.

Depends: none. Severity: medium until step 1 is done; HIGH if a closure is missing from the Standard map on first
open. Release: 0.9.1 if step 1 shows a missing closure (broken), else 0.9.2. Model: Opus. Size: S for the check, S–M
for the fix. Owner: no.
