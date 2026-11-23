# T812 — Babine Lake: split on the Gullwing Creek line; creek-mouth closures drawn as circles, not lake sections

Status: PARTIAL — the split landed: Babine Lake split on the Gullwing Creek line (a9475700d, merge 8f6dd09cf; ef6480005, aa69b89d6), shipped with the 2026-11-17 rebuild (a5638c5e7). left (Claude): the 12 creek-mouth circles as circle geometry the pipeline ships and the app draws (Salmon map only, follows the day, tap opens Babine Lake's card); the same circles are T885 item 2.

Source: owner 2026-11-16, typed: "We want to split babine lake on the line like our other split lakes. Then we want the
circles to just be circles on the map not separate sections of the lake. Otherwise it will be overboard." Before it:
the four missing creek mouths found and accepted with name variants ("That sounds good can we add name variants? With
source?"), the Gullwing line "Due south" (worker branch `lane/babine-mouths`, 6b1aa369d; OWNER-TODO 18:54).

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

## Owner pick 2026-11-22 23:35 (dialog with the Babine circle pictures)
"Yes, as shown (Recommended)" — finish T812 (its two failing tests), review, ship with the 0.9.0 data.
