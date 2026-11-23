# T973 — Classified Waters: does a unit carry up through a tributary whose own row prints no unit? (ask a CO)

Status: DONE — 9475cfdb0, e04d6d87b, bf28dbc76, 28eaf73e9, 71403fec6, bbce17168; shipped in 0.8.4 (cut e769f710a, data r-fec3fab4c5aa; 0.9.0 audit 2026-11-22, every hash is an ancestor of origin/main and of e769f710a). (Earlier status: IN PROGRESS — left (Claude): main chat review + merge (built on `lane/l10-cw-through`: carried unit with)
`class`/`source`/`through`, optional release file `cw-through/1`, app 0.8.4 "may need" note; the CO question stays open,
T974 cleans up after the answer).

Source: owner 2026-11-20 11:44 (dialog, on T944's changes): "We should maybe just make this a ticket and a gotcha that
needs to be asked to a CO. Give info in the ticket. Doesn't hold release (keep what we have but on ticket reset we can
measure it better etc and make a decision)".

## The question (a gotcha)
The book prints "[Includes Tributaries]" on a Classified Water. One of those tributaries has its OWN row in the book,
and that row prints no Classified Waters unit. Is that tributary, and everything above it, inside the classified water
or outside it?

## How it works today (the owner's 2026-10-03 rulings, `pipeline/atlas/reach/licensing.py first_classified_downstream`)
- **RU-13 (one unit per section).** A section walked by two designations takes the unit of the FIRST classified water it
  flows into. Two units always conflict: an angler can't need two different day licences for one spot. T944's moves
  are this rule:
  - three unnamed Morice tributaries go from the Bulkley to the Morice;
  - Brownie Creek goes from the Elk to the Fording.
- **LI-2 (a rowed tributary with no unit is not classified).** If the walk down from a section first meets a stream
  that has its own row in the book and no unit, the section is DROPPED, and so is everything above it. This is the
  2026-09-30 confluence ruling ("whose own row prints no CW — the Iltasyuko — does not inherit") applied to every walked
  tributary. Examples:
  - the Endako, under the Stellako's "[Includes Tributaries]";
  - Gosnell Creek, under the Morice's;
  - Harmer Creek, above Grave Creek's row (Elk);
  - the unnamed Horsefly creek, above Little Horsefly's row;
  - the unnamed Stellako creek 60818, above the Endako's row.
  T944 only made the dead ends of those streams agree with the rest of their water. It changed no policy.

## Why it's a question
The owner's general tributary rule (2026-11-20): all downstream tributary rules carry up. Where two conflict, the
nearest wins; where they are compatible, they stack. Read that way, a plain row with no unit doesn't CONFLICT with a
unit. Under it, Harmer Creek would keep the Elk's unit and Gosnell Creek would take the Morice's. LI-2 instead reads the
row as the book's word on that water, which says nothing about a licence, so the water is not classified. Only a CO
(or the Ministry) can say which reading is enforced.

## What to ask the CO
The question is in `pipeline/docs/GOTCHAS.md`, "Questions the owner will put to BC Fish and Wildlife" → Classified
Waters licence (one example: the Stellako and the Endako). Longer draft:
"A river is listed as Classified Waters with '[Includes Tributaries]'. One of its tributaries has its own line in the
synopsis that does not mention Classified Waters (e.g. Grave Creek under the Elk, Gosnell Creek under the Morice, the
Endako under the Stellako). Does a non-resident fishing that tributary, or a creek above it, need the river's
Classified Waters licence?"

## Owner's plan (2026-11-20 12:10): carry the unit through, with where it came from
Verbatim: "Maybe we should propogate classified waters through the separate listed streams still just make it a tuple
or something with what class, where the source of that classified waters is and anither item saying if it passed
through anither listed stream without classified water title etc. still have nearest classified water level with so
downstream cwi is overruled by a more upstream cwii etc. make sense? Then we can at least have the info throughout".

What that means:
- **The walk no longer stops** at a tributary with its own no-unit row (LI-2's stop). The unit carries on up.
- **Each placed unit carries a record:**
  - `class`: the unit's class, e.g. Class I or Class II;
  - `source`: the classified water it comes from, e.g. the Stellako;
  - `through`: the rowed no-unit waters it passed on the way up, e.g. [the Endako]. It is empty when it passed none.
- **RU-13 stays:** one unit per section, and the nearest classified water wins. An upstream Class II overrules a
  downstream Class I for the sections above it.
- **The data is kept everywhere.** Whether a unit with a non-empty `through` counts as "you need this licence" or as
  "you may need it, ask" is a display choice; the CO's answer settles it later without a rebuild.

## Owner answers (2026-11-20 12:11, dialog)
- Display until a CO answers: **"\"May need\" note (Recommended)"**. A unit that passed through a rowed no-unit water
  shows with a short note, e.g. "Stellako River Class II may apply here: the Endako has its own line without Classified
  Waters. Check with a CO." A unit that passed through no such line shows as today.
- When: **"In this release"** (not after 0.8.4). The release waits for this lane and its review.

## Work (this release)
1. Reach: carry the unit through rowed no-unit waters with `class`, `source`, `through`; RU-13's nearest-unit rule
   stays. A unit with an empty `through` is placed exactly as today (byte-identical where nothing passes a rowed water).
2. Bundle + release: the carried units with a non-empty `through` go in an OPTIONAL new table/file, so apps before 0.8.4
   show nothing new (as today) and never show them as a plain requirement.
3. App 0.8.4: the licence card shows the "may need" note from that table.
4. Measure and list: sections and waters that gain a "may need" note, the biggest first.

## Work (when the CO answers)
1. Measure: every section LI-2 drops today (switch `own_row_not_classified` off vs on, in memory through heavy.sh, as
   T944 did), counted by water, with the biggest waters listed and rules digest unchanged.
2. If the CO says "carries through": stop treating a rowed no-unit tributary as a stop in `first_classified_downstream`
   (RU-13's nearest-unit rule stays), update the docstring, AGENTS and the rulings, run reach + bundle, diff by place.
3. If the CO says "not classified": record the answer beside LI-2; no code change.

## Acceptance
The CO's answer is quoted here with the date. The measurement is in this ticket. The code matches the answer, with a
mutation pin on the stop.

Depends: a CO's answer (owner). Size: M. Owner: yes (ask a CO). Default: keep LI-2, as shipped.

## Owner 12:24 (2026-11-20): nearest wins both ways; only a tributary-applying unit claims what is above it
Verbatim: "this also happens invereted (Nearest still wins: a stream gets only one unit, and an upstream Class II overrules a downstream Class I above it.) for example a upstrem class i overrules a downstream class ii too. (assumign that the classified water sis applied to tributaries) usually it says it?"
- Nearest wins whatever the class: an upstream Class I overrules a downstream Class II above it (RU-13 already does).
- A classified water claims the sections above it only when its row applies to tributaries ("[Includes Tributaries]",
  or a tributaries-only row). Today a walked section is re-homed to ANY designation whose own reach it meets. Default
  built (L10, to confirm with the owner): a classified water whose unit does not reach its tributaries is a
  pass-through like a rowed no-unit water: it goes in `through`, and the card shows the "may need" note. L10 counts the
  cases and names up to 5.


## Owner 12:54 (2026-11-20): the 5 Classified Waters units that stop at their own water are right
Checked against the book by the owner: Thompson River (R3), Skeena River sections 2 and 4 (R6, mainstem only), West Road
(Blackwater) River (R5, "Mainstem only"), Deena Creek (R1, no tributary mark on its Class II line). The other 60
waters' units reach their tributaries. Owner verbatim: "yes i missed those both are correct too." (after confirming
Thompson and Skeena from the book: "so only a few classified waters streams in regulations dont apply to the
tributaries").

## Owner 13:22 (2026-11-20): show it as Classified Waters, the why behind a button
- Owner 13:22, on the Endako shot (note above "You need" with a "?"), verbatim: "in the photo you sent there is a rouge ? in the line and the line is under the header Canadian, other province, 167 and older?". Then (dialog), verbatim: "we should just say it is classified waters then give a button that expands the note on why we say that maybe? ".
  Built as: a carried unit shows as an ordinary Classified Waters licence row, in the same place and for the same
  anglers as a real unit. An expand control on the row opens the why ("Stellako River's Class II covers its
  tributaries; Endako River has its own line in the rule book without Classified Waters, so it may not apply here.
  Check with a CO."). The "?" and the note above "You need" go.

## Built (lane L10, `lane/l10-cw-through`, 2026-11-20)
Merged first: `lane/lc-reach-leftovers` (T944, POLICY_VERSION 48) and `lane/l7-stages`; this change is 49.

**Where the stop is applied (every place).**
1. `pipeline/atlas/reach/licensing.py first_classified_downstream`, the rowed-water branch: the walk down from a walked
   section first meets a stream of a water with its own row and no designation → the section and everything above it
   leave the placement (`own_row_not_classified`). This is LI-2 for every walked tributary, and the only place it is
   applied to a walk. **Changed here** (the carried unit below).
2. `pipeline/atlas/reach/build.py confluence_excludes` step 3 (the 2026-09-30 ruling T9, "whose own row prints no CW —
   the Iltasyuko — does not inherit"): a joining water WITH a row of its own at a rule's or a record's confluence CUT is
   kept out of the stretch, it and everything above it (`tributaries.CONFLUENCE_WATER_WITHOUT_A_ROW_GOES_WITH_THE_CUT`).
   A separate path: it decides which side of a cut a joining water is on, for rules and licensing records alike.
   **Not changed** (default: it is a boundary rule, not a tributary walk; changing it moves rules too).
3. `pipeline/deliver/bundle/licensing.py _own_row_not_classified`: a water's own row that SAYS "NOT a Classified
   Water" (Coal Creek's `not_classified` record) beats a designation reaching it by a walk. A printed "not classified",
   not a row that is silent. **Not changed.**

**Reach.** `first_classified_downstream(…, carried=[…], claims={…})`: a section dropped at a rowed no-unit water is
still dropped from the placement; the walk then goes on down past that water (and any further one) to the first stop
that decides — the walker's own water, another designation's own reach, a rowed water with a designation — and the
section gets a `CarriedUnit(section, entry_id, record_id, through)`: the designation (its `class` and unit), the
classified water it comes from (`source` = that designation's water), and `through` = `[(item_id, name), …]`, the rowed
no-unit waters passed, in order UP from the classified water. Nothing below (sea, braid loop): the walker's unit, as
an unpassed section stays the walker's (default). `carried_units`: one per section, none where a final designation
holds the section. Run table `licensing_carried.jsonl` + `licensing_carried_digest`; `licensing_digest` covers the
placements only.

**Owner 12:24 addition, built (default, to confirm).** `claims` = the designations whose row reaches its tributaries
(the record's `includes_tributaries`, else its entry's; or `tributaries_only`): 69 of 74. A walked section whose walk
meets a non-claiming designation's own reach first is passed through it (`unit_not_on_tributaries`, that water in
`through`); a section on that designation's own reach is always its own. Switch
`licensing.ONLY_A_UNIT_ON_ITS_TRIBUTARIES_CLAIMS_THEM`. The five that do not reach their tributaries: Deena Creek,
Thompson River (downstream of the Kamloops Lake outlet signs), West Road (Blackwater) River, Skeena River 2 and Skeena
River Section 4 (mainstem only). **Measured on the live atlas: 0 sections** — no walked section meets one of them first
(each walker's own water comes first), so nothing moves and there are no examples to name. Nearest wins both ways:
pinned by `test_the_nearest_classified_water_wins_whatever_its_class` (II under I, I under II).

**Bundle + release.** Bundle `designation_carried (sid, entry_id, designation_id, through)` — never a `licensing_set`
row, so no answer reads it. Release `cw-through/1` (sqlite: `cw_note (ix, unit_name, classified, dates, through)`,
`section_cw (sid, ix)`), in `contract.OPTIONAL`, written whatever `packed.SHIP_PACKED_FORMATS` says (off this release).
An app before 0.8.4 has no `cw-through` in its contract, so the app gate asks nothing of it and it reads nothing new; it
never shows a carried unit as a requirement. `app_reads.json` 0.8.4 row replaced (`--replace`, as T679 #8 did).

**App 0.8.4 (as rebuilt after the owner's 13:22 answer).** Python resolves the whole answer: `cw-through` carries, per
carried section, its licence YEAR as if the unit held (`answers.licence.carried_year` = `key_year` at the section with
`carried_holds`: the section's own answer plus the designation and what holds because of it at the unit's own water —
the `on_designation` rows, the Classified Waters Licence and the classified steelhead stamp, and the stamp waiver it
prints — on the days the unit is in force there). The regs store reads it (held to water's atlas), `loadRegsInputs` picks the tap day's value, and the
licence card draws that answer exactly as a real unit's (the class box, the Classified Waters Licence row with its
price, for the same anglers and on the same days), the part's own closures to the non-resident alien kept. The only
difference is a fold ("Why this may not apply here", the card's existing `Fold`) on the Classified Waters Licence row —
on the unit's class box for an angler who needs no such licence — that opens the why (`LIC.MAY_WHY`): "Stellako River’s
Class II covers its tributaries. Endako River has its own line in the rule book without Classified Waters, so it may not
apply here. Check with a CO." Two waters: "Thautil River and Gosnell Creek have their own lines …"; a part only partly
carried starts "On part of this water: ". The 12:11 note and its "?" are gone. No carried unit: the card exactly as
before. `cw-through/1` measured 76,015 B gz (7,080 sections, 21 notes, 15 licence years; gate 120,000).

**Proof: byte-identical where nothing passes a rowed water** (reach step in memory on the live atlas through
`scripts/heavy.sh`, one run, `first_classified_downstream` called three ways on the same inputs): with the carry asked
and without (= switch off), the placements and the diagnostics are EQUAL; with the tributary-claim rule on too, 0
sections move. `licensing_digest` with every T973 switch off (the same run's placements carried through the pass's
later steps) = with both on = **72f10f9269d699f8**, which is also T944's switch-on result on the same atlas (pre-T973
code; a second run reproduced all three); rules digest 6ca9a300dad3d95e unchanged. So every licensing table is
byte-identical; only the new `licensing_carried` table is added.

**Measured (live atlas): 7,080 sections gain a "may need" note** (raw 10,991 walk-drops, one per section; 0 placed
elsewhere; 1,908 sections reached by two walks name two rows, and in all of them the same unit and class
(`disagree_unit` 0: the Stellako's Region 6 and 7 rows, the Elk's three), on 97 named waters + 6,454 unnamed sections; 14 units, 21 rowed no-unit waters. Biggest first, by
section count (carried unit | through | named waters among them):

| sections | carried unit | through (up from the classified water) | named waters |
|---:|---|---|---:|
| 1,824 | Stellako River Class II | Endako River (gnis:17338) | 25 |
| 847 | Morice River Class II | Thautil River (gnis:31345) | 7 |
| 806 | Morice River Class II | Thautil River > Gosnell Creek (gnis:18182) | 2 |
| 485 | Horsefly River Class II | Little Horsefly River (gnis:10857) | 8 |
| 476 | Sustut River Class I | Bear River (gnis:15535) | 6 |
| 386 | Horsefly River Class II | Moffat Creek (gnis:14045) | 3 |
| 375 | Gitnadoix River Class I | Magar Creek (gnis:36404) | 1 |
| 326 | Horsefly River Class II | McKinley Creek (gnis:25591) | 8 |
| 294 | Babine River Class I | Nichyeskwa Creek (gnis:21084) | 1 |
| 215 | White River Class II | East White River (gnis:10163) | 6 |
| 175 | Elk River Class II | Line Creek (gnis:10373) | 4 |
| 175 | Wakeman River Class II | Wahpeeto Creek (gnis:31813) | 1 |
| 117 | Kitsumkalum River Class II | Lean-to Creek (gnis:3723) | 1 |
| 111 | White River Class II | Whiteswan Lake outlet stream | 4 |
| 110 | Bulkley River Class II | Glacier Creek (wsc:400-431358-…) | 1 |
| 84 | Elk River Class II | Grave Creek (gnis:19452) (Harmer Creek) | 2 |
| 75 | Skookumchuck Creek Class II | Sandown Creek (gnis:20588) | 1 |
| 67 | Kitsumkalum River Class II | Deep Creek (gnis:3983) | 1 |
| 66 | Kispiox River Class II | Hevenor Creek (gnis:37312) | 1 |
| 44 | Bulkley River Class II | Station Creek (gnis:2404) | 2 |
| 22 | Bulkley River Class II | Two Mile Creek (gnis:30327) | 1 |

Named waters with the most noted sections: Shovel Creek 33, Tatin Creek 32, Endako River 29, Decker Creek 29, Magar
Creek 28, Moffat Creek 24, Niquidet Creek 24, Gosnell Creek 20, Dillabough Creek 19, Sandown Creek 18.
**By name:** Stellako → Endako: 1,824 sections (`through` = Endako River). Morice → Gosnell Creek: 806 sections, and the
`through` is **Thautil River > Gosnell Creek** (Gosnell joins the Thautil, which has its own no-unit row, 222 m above the
Morice); 847 more are the Thautil's own drainage.
**RU-13 check, independent of the pass** (the main-flow path from each carried section down to its unit's own reach):
4,141 reach it with no other unit's reach first; 77 meet another reach first — all 77 are "Elk River's tributaries"
carried units whose path crosses the Elk upstream-of-Elko-Dam reach, the SAME unit and class (Elk River, II): none
where an upstream classified water would give a different unit. The other 2,862 end on the walker's own water beyond
its reach (no own reach on the main-flow path), as placed sections do.

**Side release (the lane's code end to end).** Reach run (above) → `pipeline.deliver all --out-dir
data/generated/side/l10-cw/out` → release `r-247dc010285b` (vintage check passes; rebuilt after 13:22): bundle
`designation_carried` 7,080 rows; `cw-through/1` 75,898 B gz (gate 120,000), 7,080 sections, 21 notes, 15 licence years. The DFO salmon placements were
copied from the live run (the reach CLI writes them, the in-memory run does not; same atlas, tidal and outside tables
byte-identical). The app gate on it: `cw-through/1` asks nothing of any app (needs 0.0.0); the release's oldest reader is
0.8.0 with or without the file. The mini release was re-cut (it now carries `cw-through` and the `flow-own` the merged switch writes).
**Pictures** (web, 390 px, side release `r-247dc010285b` = the lane's code after the owner's 13:22 answer, Jul 15,
Canadian from another province): `T973-shots/endako-licence-collapsed.png` — the Endako River's Licence card: the
Stellako's class box, then "You need": the basic licence and the Classified Waters Licence ($22.86 a day, the book's
terms) exactly as on the Stellako, with the fold "Why this may not apply here" closed under it, and no steelhead stamp
(the Stellako's waiver, as its box says); `T973-shots/endako-licence-expanded.png` — the fold open: "Stellako River’s
Class II covers its tributaries. Endako River has its own line in the rule book without Classified Waters, so it may not
apply here. Check with a CO." No "?" and no note above "You need". (The 12:11 picture, `endako-licence-may-need.png`,
is removed.)

**Defaults chosen (tell the owner):** (a) the confluence-cut path (T9, item 2 above) is not changed; (b) nothing below a
rowed water → the walker's unit; (c) two walks carrying different rows to one section keep the first by (entry, record)
(`disagree` 1,908, all the same unit); (d) a part partly carried says "On part of this water:"; (e) REPLACED (owner 13:22): the
carried unit follows the real unit's answer for every profile; (e2) the carried answer adds the source's `on_designation`
rows AND the stamp waiver the designation prints (the Stellako's "No Steelhead Stamp required": the carried Endako's
list drops the steelhead stamp, as its class box says and as the Stellako's own card does — first built without the
waiver, the box and the list disagreed on the shot); (e3) one licence year per
(licensing set, unit), read at the set's first carried section; (f) the owner's 12:24 rule is ON (0 sections affected today).

Tests: `test_licensing_first_classified.py` (+9: carried with path, two waters in order, upstream unit's own reach
wins, both-ways nearest, a unit on its own water only is passed, one per section, mutation pins for both switches),
`test_bundle_licensing.py` (+7: the table, four refusals, a run without the table refused, run file + digests),
`test_release_packed.py` (+2: optional, unswitched, one note and one licence year per section, once per (set, unit),
refusal; `carried_holds` adds only the unit's on_designation rows, and nothing off its days), `test_reach_builder.py`
(policy pin 49); app `licence.test.ts` (+3, owner 13:22: for every angler the blocks equal a real unit's but for the one
why — on the Classified Waters Licence row with its price, on the class box for a resident —, no "?", no note above
"You need"; two waters in order and the part prefix; none = as before), `inputs.test.ts` (+2: the tap day's value of the
year), `methods-licence.render.test.tsx` (+1: the fold closed until pressed, then the why).

## Review leftovers (lane LF, 2026-11-20, 5b07930fc)
- **One licence year per full licence key.** `release.build.cw_through` computed one carried licence year per
  (licensing set, unit), but a section's licence answer is per `LicenceKey` (set, province exceptions, tidal, water kind,
  keyed rule set): a carried lake, wetland or tidal section beside a stream would take the stream's year. It now groups by
  `answers.licence.section_licence_keys` + unit. Test: `test_t973_review_carried_sections_share_a_year_only_under_one_full_licence_key`
  (a stream, a lake and a tidal section of one set; fails when grouped by set alone). Measured on the l10-cw side
  bundle (heavy.sh): 7,080 carried sections; groups 15 -> 25 (5 groups held 2-4 keys each, all differing only in the
  keyed rule set, all streams, none tidal); the cw-through tables (`section_cw`, `cw_year`, `cw_note`) are identical
  before and after — the split groups' years are the same.
- **Only the carried unit's own stamp waiver.** `carried_holds` copied the source section's whole `waived` list. `holds`
  now keeps (internally, `_waived_by`) which designation waives each requirement, and only a waiver the carried
  designation prints comes off. Test: the carried-answer test gains a source with two designations, the waiver printed
  by the other (the section keeps its steelhead stamp; fails with the old copy). Measured on the same side bundle: the
  cw-through tables are identical with the old copy and the new.
