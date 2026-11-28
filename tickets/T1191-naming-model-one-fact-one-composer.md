# T1191 — The naming model: one `part_place` fact, one composer, names and chips for every part

Status: OPEN — design done; the owner picked P3 and the full first stage for the 0.10.0 re-cut; his naming answers of 2026-11-27 are recorded (Q4 settled 2026-11-27, below); build tickets are numbered by the planning chat from the plan.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1191 | The naming model: one `part_place` fact, one composer, names and chips for every part | OPEN — design done; build in the 0.10.0 re-cut | — | the 7 build tickets (planning chat numbers them); a Python facts step and one TypeScript composer |

Source: the owner's naming rulings, 2026-11-26 (planning chat), verbatim; the notes `naming-pathway-audit` (the audit),
`naming-model-design` (README rounds 1-3 and the sheets P1..P3-6). Related: #312 and T1172 (the words already built),
T1181, T1196, T1176 (superseded), T1180, T1240, T1199.

## What the owner ruled
- **The audit's trigger (about the final table):** "could we look at what this would change now? we want the most
  universal well labelled items that can not confuse a user on ui and things in closed areas like eco reserves should
  same they are in that area on the card in ui. need to audit entire naming pathway it feelsa bad right now and fragile".
  The audit found 430 parts wholly inside an area whose rule binds them that say nothing about it (393 closed all
  year), 2,517 parts wholly inside any park, WMA, eco reserve or national park, and 291 parts whose name and subtitle
  resolve their ends differently.
- **The model:** "i cant find write up in chat but option 3 shoulds good. we just need to make sure that it handles the
  section combining in the front end for neighbouring sections thta have the same rules. each of those use the
  downstream end and upstream to get the scope. all of this happens in front end to allow stuff like salmon regs to
  change from feeds and update cleanly. we just need to make sure the clean updates like this are still possible while
  having consistent better names." (One fact, one composer; front-end joins by ends stay; live feed changes still update.)
- **Which parts:** "i think any where the area catalog binds should be an option then if one of the areas applies a rule
  that could be used as a name especially if it closes it like eco reserves. but also other items where rules change
  for a section that is in an area could be handled this way to but we want the cleanest implementation witht he
  conditions i mention before of feeds changing stuff and wanting consistent naming that iks easy to interpret."
- **Other lands:** "definitely for military. but then for indigenous land we currently have it just as a banner in the
  front end as a notice that is passes through that land. we could rework this a little bit if you think it should be
  mentioned better? but miltitary (or closed areas like watersheds) noa ccess i mean cutt the streams and change the
  ability to fish those areas so they are way more important as items to manage. think about it".
- **Timing:** "Inside the 0.10.0 re-cut (Recommended)".
- **Chips (the design review):** "yes i like the tag option (3) the best so far" (P3: area names plus ranked chips), "also
  the facts as colour chips might be good because then we could have a chip for being partly in indigenous land or no
  access areas etc?". Scope: "The full first stage (Recommended by the design)". Areas shown: "Wholly inside any area,
  plus 'partly in' when the area's rule binds (Recommended)". Eco reserves, verbatim: "how can skeen river inside skeena river eco reseve be 72% inside. we cut on eco reserves so it
  should be 1005 always for eco reserves other areas sure might have partial but not thing like eco reserves.... ... the
  skeena river p4 72% inside eco reserve one is weird. should be impossible.  eco reserve for oxbows doesnt have name in
  tag." (The cut-area check found the 72% real but safe; the cut rule that answers it is T1199; the Oxbows chip carries
  the reserve's name.)
- **Side channels:** "yes to all 3 but it begs the question why the side channel has different regs and is not combined
  with neighbours if it is not getting differen regs from an area or something else that could be in the naming?":
  plain "Side channel" / "N side channels" / "Main channel"; Thelwood loses the park name; and **every part says why it
  is separate** (a chip naming the rule that differs), or it folds (T1240). The Snehumption label: "Plain \"2 side
  channels\" (Recommended)" (the rule: if not every channel in the part has a named end, plain count); Klinaklini and
  MacMillan: "Plain \"Side channel\" / \"3 side channels\" (Recommended)" (T1196).
- **Short form:** "Shorten Ecological Reserve to \"Eco Reserve\"", then "it should be \"[name] eco reserve\" everywhere"
  (names, ends and subtitles alike).
- **Lakes partly in a cut area:** "this similar to the naming etc it could be a tag saying partly in eco reserve? then
  later can also be in the cut rule options in the later releases?" (one chip, "Closed all year · partly in <Area>", 84
  lakes; cutting lakes is a later option).
- **Return batches 5a and 5b, 2026-11-27 (verbatim; the answers are also on `docs/OWNER-RULINGS.md`; "Reading:" is the planning chat's plain reading):**
  - No access on the card (Q4): "i dont understand the cottoinwood river example but i like the layout in p3-5 access. i
    dont understand the salmon stretch tag either. also the tag for noons creek is weird too. i think for noon creek as
    a side note since fwa and city have 2 different streams in same area same name they should both get noons creek regs
    for safety". Reading: he likes the chip layout as on P3-5; the status-pill version was not chosen; the three
    examples are still to be explained to him. NEW ruling: the FWA stream and the city stream of the same name in the same
    area both get the Noons Creek regulations, for safety.
  - Which land (Q5): "all no access in that layer. also we seperatiely want a tag for tags that is like indegenous except
    \"crosses private land\" as another tag". Reading: ALL closed land in the layer counts as "No public access" (no
    filtering); a NEW, separate tag "Crosses private land", like the Indigenous one.
  - Indigenous chips (Q6): "chip replaces the banner and no when wholyl yes and make it if the water goes through
    indigenous at all it will get part tag"; confirmed with "Yes, that is right". Reading: the chip replaces the banner;
    a chip when wholly on reserve land; a "crosses" chip on ANY touch (no 5% floor).
  - The 14 side parts (Q-R2): "Fold by the rule, in the re-cut (Recommended)".
  - Klinaklini / MacMillan DFO salmon side channels (Q-R1): "why do tey not get reached? that seems like an issue not
    something that needs a tag". Reading: no tag; a defect (T1240 carries the fix: the DFO salmon stretch must reach the side channels).
  - Words (Q7 + Q10): "options one except if it is the only section it doesnt need a locator at all". Reading:
    "Cowichan 1 reserve" and "Main channel"; BUT a water with only one section or part gets no locator line at all.


## The design (summary; the notes hold the numbers and pictures)
One `PlaceFact` row per part, written by Python (`part_place`, with a legacy projection so the display version stays 20
and 0.9.x keeps working); one TypeScript composer in `app/packages/core/src/place/` that builds the name, subtitle,
chips, joins of neighbouring same-rule parts (by their downstream and upstream ends) and the closed banner for every
surface. About 4,200 subtitles and about 330 names change.

## Work (the plan; the planning chat numbers the tickets)
0.10.0 re-cut slice: (1) the bundle `part_area` + rule-to-area link; (2) `PlaceFact` and `display.areas`; (3) the `core/place` composer and grammar tests; (4) every surface on the composer (`mergeNav`, map list and bar, header, banner,
search part rows, in-season "Where", SEO); (5) the legacy projection and a release-wide test over both feed extremes;
(6) delete the hand lists (`REVIEWED_INSIDE`, `AREA_SHORT`, `MOUTH_WORDS`, "Within", the side grammars); (7) the minimal
no-access state. With chips: about one more lane-day. 3-4 lane-days in all with review. 0.11.0: cut short words, salmon
card place, search cut rows, no-access kinds, Indigenous land words, lake pieces as parts of their lake (with T1180).
If the re-cut cannot take it all, the smallest safe slice is 1 + 2 + 5.

### What the first stage builds, after his rulings of 2026-11-27
- **No access:** ALL closed land in the layer counts as "No public access" (no filtering to military and watersheds). The
  card layout is P3-5 (Q4 settled 2026-11-27: "Yes, settled (Recommended)").
- **Indigenous land** (placed in the first stage by the planning chat; the Work plan lists "Indigenous land words" under
  0.11.0): the chip replaces the banner; a chip when wholly on reserve land; a "crosses" chip on any touch (no 5% floor).
- **Words:** "Cowichan 1 reserve"; "Main channel" for a mouth-to-source main line beside side-channel parts; a water with
  only one section or part gets no locator line at all.
- **Side parts:** the 14 with no nameable reason fold by the rule in the re-cut (T1240). The DFO salmon side channels
  (Klinaklini, MacMillan) are a defect to fix in T1240, with no chip.
- Ruled, timing not given (the planning chat places them):
  - NEW scope, a separate tag "Crosses private land", like the Indigenous one. Its data source is NOT chosen yet (the
    planning chat numbers the build ticket once it is).
  - Noons Creek: the FWA stream and the city stream of the same name in the same area both get the Noons Creek regs.

## Acceptance
Every part with an area that binds it says so in its name or chip on every surface; no surface throws on a missing
fact; a feed change still re-joins neighbouring sections cleanly (the live-join sheet's states pass as tests).

Depends: T1196 (first), T1199 (an eco-reserve part is wholly inside). Severity: medium. Model: Opus. Size: L.

## Owner

- [x] 2026-11-27 — No-access on the card (Q4): his words were "i like the layout in p3-5 access" (the chip layout; the status-pill version for 0.11.0 was not chosen), but he did not follow the Cottonwood River example, the salmon stretch tag or the Noons Creek tag. The planning chat owes him a plain explanation of the three; then confirm (a): the "No public access" chip ranks first and the day answer is unchanged, for 0.10.0.
  → with the plain sheet (night notes `T1191-Q4/`): "Yes, settled (Recommended)" (2026-11-27). Reading: the layout as P3-5; the three tags are NOT built (Cottonwood "Build STOP" was a build fold; Klinaklini's salmon tag is the T1240 defect; Noons: both streams get the Noons regs).
- [x] 2026-11-27 — Should Turner Creek take the Noons row "No Fishing upstream of railway bridge" (open → closed, about 2.65 km)? → "No, leave Turner as today (Recommended)" (2026-11-27). The city's Noons / West Noons lines already get the Noons row: no clash with T1246.
