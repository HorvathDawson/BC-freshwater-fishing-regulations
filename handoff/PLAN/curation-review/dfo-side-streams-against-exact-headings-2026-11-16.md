# DFO salmon places: side streams against the exact heading each row sits under (2026-11-16)

Asked by the owner's answers of 2026-11-16: "this depends if the name has tributraries included in it. if it doesnt
then no tribs otrherwise yes tribs"; "The heading counts"; "the locations should be matched only to their exact
naming then". A read-only audit (Opus), saved by the main chat from its hand-back. Full per-place table (352 places)
and the script: session scratch folder `heading-audit/` (`places.tsv`, `audit.py`). Nothing was changed.

**The rule audited:** a place takes side streams if and only if DFO's words say so: the place's own line when it
speaks ("including / all / excluding tributaries", "mainstem"), otherwise the EXACT water heading its row sits under
on DFO's page (read from the cached pages of 2026-11-06 with the project's table parser), otherwise off.

## Counts [ran]
- 352 curated places; 239 live named-water places with rows on DFO's page today (20 defaults / closures not judged;
  93 dormant, 78 of them still in shipped chains with no rules).
- **AGREE 225; DISAGREE 14**: 2 open a fish on side streams the words do not cover; 6 put a closure or bait ban on
  side streams the words do not cover; 1 is missing side streams; 5 have the wrong setting but ship nothing different.

## Specific answers [ran]
- **Morice:** "from the confluence of the Bulkley and Morice Rivers upstream to the Bymac Bridge on Walcott Road" sits
  under **"Morice River (including tributaries)"** (a heading spanning 13 rows: the blank whole-river row, lake outlet
  to Gosnell, Gosnell to Lamprey, 100 m below Gosnell to Lamprey, below Lamprey, upstream of the Bymac Bridge): all
  ON, all agree. Only the Forks row ("Bulkley River and Morice River waters within the four white triangular fishing
  boundary signs…") sits under a separate plain **"Morice River"** heading: rule OFF, saved ON (line 2544, verified),
  0 side pieces ship. Morice Lake: its own words say "including tributaries": ON, agrees.
- **Atnarko:** both rows under "Atnarko River including tributaries" (whole river; below the Tweedsmuir Park Lodge
  sign, line 2584): ON, agree. **Bella Coola:** both rows under "Bella Coola River including tributaries (not
  including Atnarko River)": agree, the Atnarko subtracted from both.
- **Kitsumkalum** (four rows, heading "Kitsumkalum River (including tpinributaries) Note: The mouth…", DFO's typo read
  as "tributaries"), **Lakelse** (four rows, "Lakelse River (including tributaries)"), **Kispiox** (two rows,
  "Kispiox River (including tributaries)"): all ON, all agree.
- **Bulkley** (four rows under plain "Bulkley River"): all agree by their own words ("all tributaries … other than…"
  = side streams only; "excluding tributaries" = OFF; the Forks OFF).
- **Fraser, Regions 2 and 3:** every live and dormant row is "mainstem" / OFF and agrees.
- **Vancouver Island** (Cowichan, Nanaimo, Stamp, Somass, Nitinat, Puntledge, Little Qualicum): headings carry no
  tributary words; every row OFF and agrees, except where the place's own words say otherwise (Nanaimo "including
  tributaries, except in the area described below", line 2664; Stamp "From the confluence of the Ash River upstream
  to the Great Central Lake Dam, including tributaries", line 2459): ON, agree. Stave: "including its tributary"
  belongs to the "except" part about the Northrop channel: OFF is right.

## The 14 that disagree, most serious first
**Side streams open a fish the words do not cover**
1. **Tatshenshini** (Region 6), heading "Tatshenshini River (downstream of the BC/Yukon border)", own words blank.
   Not saved (last save line 2719): inherits ON from the water-level setting; ships 6,261 sections, 6,217 of them
   side streams, coho 2 per day. Rule: OFF (about 6,217 pieces out).
2. **Tatshenshini upstream, Blanchard**, heading "Tatshenshini River (upstream of the BC/ Yukon border – along the
   Haines Hwy.)", words "Blanchard River". Saved ON (line 2779); ships 287 sections, 272 of them the Blanchard's side
   streams. Rule: OFF. [inferred] no answer changes: those streams would fall to "mainstem and all other
   tributaries", whose rules are identical.

**A closure or bait ban on side streams the words do not cover**
3. **The three creeks** (same heading; "Kwatini Creek, Stanley Creek and Goat Creek"): not set (line 2780), inherits
   ON, 192 side pieces under a year-round closure. Rule: OFF; they would go to "all other tributaries" (coho 2 a
   day). OWNER QUESTION: do named side creeks take their own side streams?
4-8. **Kitseguecla, Sustut, Cranberry, Kiteen, Meziadin River** (Region 6): plain heading, blank whole-river row whose
   only rule is "No natural bait allowed"; not set (lines 2636, 2642, 2632, 2634, 2638), inherit ON; the bait ban
   ships on 733, 4,243, 2,579, 1,191 and 57 side pieces. Rule: OFF. (Each river's sibling row "including
   tributaries", the chinook closure, is correctly ON.)

**Missing side streams**
9. **Kitimat outfall** (heading "Kitimat River (including tributaries)"; "on west bank between signs at Kitimat
   hatchery outfall."): saved OFF (line 2786, written by Claude 11-15, unsure; it cut 13 sections to 1). Rule: ON
   (about 12 pieces back). The rule read literally contradicts that fix: OWNER QUESTION.

**Wrong setting, nothing ships differently today**
10-12. **Nanaimo** (heading "Nanaimo River"): "from the Cedar Road Bridge crossing upstream to the Highway 19 Bridge
   crossing" and the two "in Subarea 17-14 (tidal portions…)" places inherit ON (lines 2444, 2473, 2474), 0 or 1
   sections each.
13. **Little Campbell** (Region 2; "downstream of a line between 2 fishing boundary signs…"): inherits ON (line 2538).
14. **The Morice Forks** under plain "Morice River": saved ON (line 2544, verified).

## How the pipeline decides today [read]
- `untangle.split_name` strips "(including tributaries)" from the heading and `untangle()` groups rows by the cleaned
  name, so "Morice River (including tributaries)" and "Morice River" become ONE water whose setting is the first
  heading's (`pipeline/regs/dfo_salmon/untangle.py` about lines 440 to 490).
- A place's setting is its own words, else that merged water setting; the fingerprint (section + cleaned name + place
  words) cannot see the heading.
- The water-level setting lives on the water record in `data/curated/dfo_salmon/places/region-*.json`, filled once by
  `entries._ensure_water` and never re-read: it is ON for 49 waters whose heading has no tributary words (Cowichan,
  Nanaimo, Kitseguecla, Sustut…), copied from a sibling place's "including tributaries". A blank place inherits it
  (`entries.to_reach_input`, lines 926 to 933): that is how all nine blank places above went ON.
- `handoff/PLAN/curation-review/water-heading-tributaries-2026-11-15.md` labelled our merged water setting as "DFO
  flag" ("Cowichan River · yes"): it was not DFO's heading.

## Design note: derive the setting from the words, save only exceptions
1. Keep the exact heading text on every row and place, and what it says about side streams (in / out / silent); the
   merge no longer carries a side-stream setting; the fingerprint stays (saves still match), with a check that fails
   when one cleaned name has the same place words under two different headings.
2. Derived setting: the place's own words if they speak; otherwise the exact heading; otherwise off. "All tributaries
   of / to" = side streams only.
3. The place's stored setting becomes a stated exception (value, reason, save line); the water-level setting leaves
   the decision; `to_reach_input` reads derived-unless-exception.
4. Review app: show the derived answer and the words that decided it; a save that matches stores nothing, one that
   differs becomes an exception.
5. Migration and guard: saves that match the derived answer are dropped, the rest (today the list above) become
   exceptions for the owner; a test asserts every live place is derived or a listed exception and flags any change
   in the heading or the words.

## Not checked
Piece counts are of what ships today (no new reach run); the 93 dormant places; "opens a fish" against "closes" was
read from the rule text; whether the 11-15 night saves (lines 2721 to 2792) were ever ticked by the owner.
