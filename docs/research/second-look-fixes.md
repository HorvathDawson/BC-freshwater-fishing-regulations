# DFO extents: what was held back at the second look

Conclusions only. Open tickets T773 and T774 depend on these. The second look's recommended fixes were applied as
review marks saved "unsure" for the owner to re-tick (Region 1: Somass, Stamp and Little Qualicum "except ..." places to
whole river; Cowichan and Nanaimo stretches to tributaries off; Regions 2 to 8 similar). Their counts live in the marks
and the build, not here.

## Why "whole river" is right for an "except in those areas and times" place

Every place that binds a section is in that section's chain (`covering`, `pipeline/deliver/salmon.py`), sorted by
`chain_order` (precedence, own region, fewer sections first, riders, id). `verdict_on` answers at the first place with a
limit held for that fish that day and moves on when a place has no rule for that fish on that day. So a narrower listed
stretch answers on its own dates and the whole-river place on every other day: DFO's "except in those areas and times".

## Held or not done

- **Kitsault tidal boundary name (owner note on mark 2682):** the row is a mark-written `mouth` row and every mouth row
  gets the fixed label "tidal boundary"; the owner's name needs a code change or a new cut (open ticket T773).
- **Tatshenshini "downstream of the BC/Yukon border" (~2):** the only other place matching the tight side-stream rule,
  held back: turning side streams off would take it from 6,261 to 44 sections, the owner verified it (mark 2719), and its
  older wording said "including tributaries".
- **Kitimat west bank:** the 750 m all-year closure also covers 11.9 km of side streams and closes the whole width (the
  one-bank places work, T754, is not built). It is held undrawn with a card note in DFO's words and tributaries off
  (fallback mark 2786); a place cannot yet be saved as "not drawn" (open ticket T774).
- **Maria Slough needs no mark:** all six Mission-area Fraser places already bind it (8 sections) and the gazetted
  Jeperson line (`reach/channels.py`). Strawberry, Nicomen and Nelson sloughs stay out (not named by the owner). Little
  Campbell needs no change.

## Code finds (not data)

1. **Kitsumkalum Lake "including tributaries" takes 16 side-channel pieces (14.6 km) of the Kitsumkalum River above the
   lake:** the through-river rule drops only the river's main line. The lake must not take its through-river's own side
   channels.
2. The Nass-near-Meziadin lower end is the confluence, not DFO's signs; the Sumas part runs to the mouth, not to the
   signs near the Fraser.

Everything else checked: two-part places bind both parts, nothing lands outside BC, cut ends and lengths match the
words.
