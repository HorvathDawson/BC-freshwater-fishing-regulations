# T664 — Okanagan River Oxbows: one water, not 31 items

Status: OPEN

Source: owner 2026-11-11: "the oxbows should be combined as one item. Right now a shit load come up in the search etc..
when they are the same with same rules".

Since T637 each oxbow channel is its own registry item (31 `wsc:` items, all named "Okanagan River Oxbows", same rule).
Search and the map list each one.

Want: one "Okanagan River Oxbows" item holding every channel (one search hit, one card, one name on the map), bound by
the one rule as today.

Notes for whoever takes it:
- The registry groups by watershed code; a curated group across codes is needed (look for an existing mechanism before
  adding one: `name_variants.json`, `book_name_links.json`, the merge report).
- Needs an atlas rebuild, a restamp of `r8:okanagan_river_oxbows@8-1`, a reach run and binding diff (the rule must bind
  the same sections as build 27: 89).
- Ask before merging any other same-name group on the same pattern; this ticket is the Oxbows only.
- Not in build 27 (owner did not hold the release for it); rides the next atlas rebuild, with the nine waiting DFO cuts.
