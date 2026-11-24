# T807 — A DFO place's side streams are derived from the exact heading its row sits under and its own words; a saved value is only a stated exception (ten places go river-only; a named creek keeps its side streams; the Kitimat outfall strip takes them)

Status: DONE — 235408838, fa098f1f4, 3593c2c9b, cb008158f, da515cfeb. a DFO place's side streams are derived from its own words, else the exact heading, else river only; stored values only as stated exceptions; review app shows the deciding words; 20 marks re-saved for the owner.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T807 | A DFO place's side streams are derived from the exact heading its row sits under and its own words; a saved value is only a stated exception (ten places go river-only; a named creek keeps its side streams; the Kitimat outfall strip takes them) | DONE | 235408838, fa098f1f4, 3593c2c9b, cb008158f, da515cfeb | — |

Closed by the 2026-11-20 audit: built fa098f1f4 (side_streams.derive, review app, check), owner 17:01 rulings applied 3593c2c9b, marks re-saved by Claude on the owner's word cb008158f, merged with da515cfeb; owner OK'd the changes (e59d7146f); shipped from 0.8.0.

Source: owner 2026-11-16 (`handoff/PLAN/OWNER-TODO.md`, "Answers by batch" and "Re-check of saves"): "this depends if the
name has tributraries included in it. if it doesnt then no tribs otrherwise yes tribs"; "The heading counts"; "the
locations should be matched only to their exact naming then"; the ten disagreeing places: "Yes, river only for all ten
(Recommended)"; named creeks: "A named creek keeps its side streams"; the Kitimat outfall strip: "Heading counts here
too". The audit: `handoff/PLAN/curation-review/dfo-side-streams-against-exact-headings-2026-11-16.md` (225 of 239 live
places agree; the 14 that do not; how the pipeline decides today; the design note).

## What to build
1. Keep the exact heading text on every row and place, with what it says about side streams; the name merge
   (`untangle.split_name`, `untangle()`) no longer carries a side-stream setting; a check fails when one cleaned name
   has the same place words under two different headings.
2. Derived setting: the place's own words if they speak ("including / all / excluding tributaries", "mainstem");
   otherwise the exact heading; otherwise off. "All tributaries of / to" = side streams only. A row that NAMES a
   creek (no "tributaries") takes that creek's own side streams (his exception; Tatshenshini: Kwatini, Stanley and
   Goat creeks; the Blanchard).
3. The water-level `tributaries` setting on the water records (ON today for 49 waters whose heading has no tributary
   words) leaves the decision; `entries.to_reach_input` reads derived-unless-exception; a stored setting on a place
   is an exception with a reason and its save line.
4. Apply: river only for the Tatshenshini whole-river row (6,217 side pieces of coho 2 a day), Kitseguecla, Sustut,
   Cranberry, Kiteen, Meziadin (the bait ban leaves 733 / 4,243 / 2,579 / 1,191 / 57 side pieces), three Nanaimo
   places, Little Campbell, the Morice Forks; the Kitimat outfall strip takes its side streams again (save 2786
   reversed, about 12 pieces).
5. Review app: show the derived answer and the words that decided it; a matching save stores nothing.
6. A test: every live place is derived or a listed exception; a change in a heading or in the words is flagged.
7. Reach rerun; the list of every answer that changes (water, fish, days) to the owner before the release.

2026-11-17 ~17:01, owner: land it (verbatim in OWNER-TODO 17:01); Tatshenshini named-tributary places bind only the named tributary; dormant three follow DFO words; Claude re-saves the 12 changed marks.
