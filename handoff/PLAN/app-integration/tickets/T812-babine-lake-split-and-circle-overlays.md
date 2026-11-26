# T812 — Babine Lake: split on the Gullwing Creek line; creek-mouth closures drawn as circles, not lake sections

Status: IN PROGRESS — the split landed (a9475700d, merge 8f6dd09cf; ef6480005, aa69b89d6; 2026-11-17 rebuild a5638c5e7); the circles are built (65035f248, b7df430dc, 7cb64faa1; PR #50). left (Claude): the independent review of PR #50, then the 0.9.0 data release ships the circles (`ca` in the salmon head).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T812 | Babine Lake: split on the Gullwing Creek line; creek-mouth closures drawn as circles, not lake sections | IN PROGRESS — left (Claude): independent review of PR #50, then the 0.9.0 data release ships the circles | a9475700d, 8f6dd09cf, ef6480005, aa69b89d6, 65035f248, b7df430dc, 7cb64faa1 | owner 2026-11-16; look picked 2026-11-22 "Yes, as shown" |

Source: owner 2026-11-16, typed: "We want to split babine lake on the line like our other split lakes. Then we want the
circles to just be circles on the map not separate sections of the lake. Otherwise it will be overboard." Before it:
the four missing creek mouths found and accepted with name variants ("That sounds good can we add name variants? With
source?"), the Gullwing line "Due south" (worker branch `lane/babine-mouths`, 6b1aa369d; old owner to-do 18:54).

## What
Split Babine Lake on the due-south Gullwing Creek line (Gullwing Creek mouth 54.48497,-125.36625 to the south shore
54.45490,-125.36625); the east part binds DFO's "Also closed east of a line from Gullwing Creek to south shore of Babine
Lake". The 12 creek-mouth circles (7 confirmed 13:28 + 4 accepted 18:54 + Tsezakwa on Nilkitkwa as it is) are NOT cut
out of the lake: they ship as circle geometry (centre + 400 m) the app draws as an overlay; no geometry computed in the
app. The circles' look, owner: "Salmon map only (Recommended)"; "Follow the day (Recommended)"; tap: "Opens the lake's
card (Recommended)". Owner pick 2026-11-22 23:35 (dialog with the Babine circle pictures): "Yes, as shown (Recommended)"
— finish T812 (its two failing tests), review, ship with the 0.9.0 data.

Done: the split (a9475700d, merge 8f6dd09cf; ef6480005, aa69b89d6; rebuild a5638c5e7). The circles (65035f248,
b7df430dc, 7cb64faa1; PR #50): pipeline ships each placed closed-area circle (`ca` in the salmon head,
`deliver.salmon.closed_area_circles`; 11 circles, Tsezakwa unconfirmed so words only); core `salmonCirclesOn` + feed
filter; map overlay `salmon-circles.ts` (web + native, Salmon view only, follows the day, tap goes through to the lake
card — checked on web); fake web maps take run-time layers (b7df430dc); review fixes (7cb64faa1): a style with no
`stream` layer is reported and draws no circles, the decode test holds a not-closed four-point ring. `pnpm -C app
check` passes (779 files, 8,235 tests); `pytest pipeline/tests/test_dfo_closure_build.py` 42 passed. Pictures:
T812-shots/. Lanes: L12 (branch feat/T754-one-bank-places), L18 (PR #50).

## Work (left)
The independent review of PR #50; an emulator look at the circles; the data release that ships `ca`. (`ca` is what
grew the salmon head past its gate: T905.) T754 in this PR stays stopped (lane L20 builds it on its own branch).

## Owner

- [x] 2026-11-16 — Babine Lake → "We want to split babine lake on the line like our other split lakes. Then we want the circles to just be circles on the map not separate sections of the lake. Otherwise it will be overboard." (2026-11-16 18:56); the Gullwing Creek line → "Due south"; the four missing circles → "That sounds good can we add name variants? With source?" (18:54).
