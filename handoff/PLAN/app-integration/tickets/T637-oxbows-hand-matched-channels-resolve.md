# T637 — Okanagan River Oxbows: the owner's hand-matched side channels must bind again

Status: OPEN

Source: owner 2026-11-11, on T632 item 3: "what you mean they never had an item if they are unnamed side channels? i hand matched them?"

## Facts (measured 2026-11-11)
- The override `OKANAGAN RIVER OXBOWS` (data/curated/regulations/book_name_links.json) holds 40 waterbody keys and 27 watershed codes. Its note says "32 watershed codes, 40 waterbody keys, and 1 linear feature ID": 5 codes and the linear feature id are already gone from the file.
- The 40 polygons resolve. Of the 27 codes, none is in `section_handles.txt`; one is Park Rill's own code (300-432687-516449; the owner kept Park Rill separate 2026-11-03).
- The T632 agent's reading: the v2 registry gives an item only to named water, so a code of an unnamed line has nothing to point at. Not confirmed that the 26 lines exist as unnamed graph sections.
- Since T632 (8fdcb79d4) a dead override id stops the reach run and fast-fail, so builds stop here until this is fixed.

## Work
1. Find when the 5 codes and the linear feature id left the override (git history) and whether that was the owner's edit. Report; restore nothing without the owner.
2. Confirm in the graph that each of the 26 codes is a real unnamed line beside the Okanagan River, and its length.
3. Make a hand-matched watershed code of an unnamed line bind (registry items for these lines, or an override that binds sections by code). The owner's matches are kept, never deleted to make the check pass.
4. Park Rill's code: propose its removal from this override to the owner (it contradicts the 2026-11-03 decision), with a map.
5. Binding diff before and after; map of the 40 polygons and the channel lines for the owner.

Depends: T632. Size: M. Model: Opus. Owner: one tick (Park Rill code), one look at the map.

## Owner 2026-11-11 (later)
- "they should have name variants too of the oxbow name?": the owner expects the side channels to carry a curated name variant with the oxbow name. Measured: `data/curated/waters/name_variants.json` has no oxbow variant today (its one hit is "OXBOW LAKE", another water). Work item 6: search git history of the name-variant files (and any older variant store) for oxbow variants on these lines; report when and why they left. If they existed, restoring them is likely the fix (a variant names the line, so it gets an item) and goes to the owner as a proposal.
- Park Rill's code: the owner says it can be dropped from this override. Item 4 needs no further tick; drop it with the fix.
