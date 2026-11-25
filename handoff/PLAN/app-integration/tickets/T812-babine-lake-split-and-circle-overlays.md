# T812 — Babine Lake: split on the Gullwing Creek line; creek-mouth closures drawn as circles, not lake sections

Status: IN PROGRESS — the split landed (a9475700d, merge 8f6dd09cf; ef6480005, aa69b89d6; 2026-11-17 rebuild a5638c5e7); the circles are built (65035f248, b7df430dc, 7cb64faa1; PR #50). left (Claude): the independent review of PR #50, then the 0.9.0 data release ships the circles (`ca` in the salmon head).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T812 | Babine Lake: split on the Gullwing Creek line; creek-mouth closures drawn as circles, not lake sections | IN PROGRESS — left (Claude): independent review of PR #50, then the 0.9.0 data release ships the circles | a9475700d, 8f6dd09cf, ef6480005, aa69b89d6, 65035f248, b7df430dc, 7cb64faa1 | owner 2026-11-16; look picked 2026-11-22 "Yes, as shown" |

Source: owner 2026-11-16, typed: "We want to split babine lake on the line like our other split lakes. Then we want the
circles to just be circles on the map not separate sections of the lake. Otherwise it will be overboard." Before it:
the four missing creek mouths found and accepted with name variants ("That sounds good can we add name variants? With
source?"), the Gullwing line "Due south" (worker branch `lane/babine-mouths`, 6b1aa369d; old owner to-do 18:54).

## Data (main chat)
- Split Babine Lake on the due-south Gullwing Creek line (Gullwing Creek mouth 54.48497,-125.36625 to the south shore
  54.45490,-125.36625) the same way other lakes are split today (find the mechanism the existing split lakes use);
  the east part binds DFO's "Also closed east of a line from Gullwing Creek to south shore of Babine Lake".
- The 12 creek-mouth circles (7 confirmed 13:28 + 4 accepted 18:54 + Tsezakwa on Nilkitkwa as it is) are NOT cut out
  of the lake. They ship as circle geometry (centre + 400 m) the app draws as an overlay, with the card's note for the
  days they close; no extra sections.
- Rebuild: whatever the split needs (atlas or reach + bundle), via heavy.sh; changed answers to the owner first.

## App (app chat)
- Draw a closure circle as a circle overlay on the map (style per the palette), from data the pipeline ships; no
  geometry computed in the app.

## Owner, the circles' look
"Salmon map only (Recommended)"; "Follow the day (Recommended)"; tap: "Opens the lake's card (Recommended)". So the
data ships each circle's closure days (from its DFO piece) with the centre, radius and piece id; the app draws closed
on those days only, Salmon map only, and a tap opens Babine Lake's card.

State 2026-11-22 (stopped for credits, lane L12, branch feat/T754-one-bank-places): done: pipeline ships each placed closed-area circle (`ca` in the salmon head, `deliver.salmon.closed_area_circles`; side run on live inputs: sections + chains identical to the live bundle, only `ca` added, 11 circles, Tsezakwa unconfirmed so words only); core `salmonCirclesOn` + feed filter; map overlay `salmon-circles.ts` (web + native, Salmon view only, follows the day, tap goes through to the lake card — checked on web). Pictures: T812-shots/ (proposal look). Left: `pill-theme.render.test.tsx` is red (its fake map has no addLayer: guard or extend the fake), full app check, native/emulator look, independent review, data release.

## Owner pick 2026-11-22 23:35 (dialog with the Babine circle pictures)
"Yes, as shown (Recommended)" — finish T812 (its two failing tests), review, ship with the 0.9.0 data.

State 2026-11-23 (lane L18, finishing on PR #50): the owner's pick above is the look as shown (T812-shots/). The two red
tests were four fake web maps with no `addLayer` (pill-theme failed; one-engine, water-copy and firstSeen threw unhandled
errors from the circles hook): the fakes now take run-time layers (b7df430dc), no app code changed. `pnpm -C app check`
passes (779 files, 8,235 tests); `pytest pipeline/tests/test_dfo_closure_build.py` 42 passed. Not done here: an emulator
look at the circles, and the data release that ships `ca`. T754 in this PR stays stopped (lane L20 builds it on its own
branch after the owner's round 3 answer).

Review fixes 2026-11-23 (#50, lane L18, 7cb64faa1): a style with no `stream` layer is reported and draws no circles (never on
top); the decode test holds a four-point ring that is not closed, so the closure check can fail (both mutation-proved).

## Owner

- [x] 2026-11-16 — Babine Lake → "We want to split babine lake on the line like our other split lakes. Then we want the circles to just be circles on the map not separate sections of the lake. Otherwise it will be overboard." (2026-11-16 18:56); the Gullwing Creek line → "Due south"; the four missing circles → "That sounds good can we add name variants? With source?" (18:54).
