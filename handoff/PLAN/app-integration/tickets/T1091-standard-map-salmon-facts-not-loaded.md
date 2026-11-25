# T1091 — A fresh Standard map fetches only the salmon head: anything drawn from the full salmon facts is missing until the Salmon map has been opened

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1091 | A fresh Standard map fetches only the salmon head: what is drawn from the full salmon facts is missing until the Salmon map was opened | OPEN — 0.9.2 (code reading 2026-11-24 found no missing closure; the cold-open test and a live check are left) | | 0.9.1 chat 2026-11-24 |

Source: the 0.9.1 chat, 2026-11-24 (T1029 ticket and draft PR #112, CDP proof on section 1234098): on a fresh
Standard map only the salmon head is fetched; the full facts (shards) arrive only after the reader visits the Salmon
map, and labels computed from them then persist when switching back. T1029's one-bank cue on Standard did not draw
until Salmon had been opened (fixed there by labelling from the head). Not verified beyond the T1029 case.

Step 1 verdict, 2026-11-24 (T1029's helper, by reading the code, NOT a device check): nothing else fails to draw on
a cold Standard map. Finfish closures (T796) do come from the full facts (`packages/map/src/colouring.ts:238`,
`core/src/salmon.ts:565` and `:584`), but the full read is triggered when needed: on a cold web Standard map
`useMapColouring.ts:177-180` asks for it when `useFinfishCue` (`ui/src/salmonColour.ts:79-84`) finds a finfish closure
on the map's day in the live DFO feed; with no feed capture there is no cue, by design. The issue stripe and the
one-bank cue read the full facts too but are drawn in the Salmon view only. T1029's own case is fixed in its branch
(1ba481823, labelled from the head). Left: step 3's cold-open test, and one live check on a finfish closure day.

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

Depends: none. Severity: low to medium (no missing closure found by code reading; unproven on a device). Release:
0.9.2. Model: Opus. Size: S. Owner: no.
