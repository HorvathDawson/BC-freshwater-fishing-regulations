# T1191 — The naming model: one `part_place` fact, one composer, names and chips for every part

Status: OPEN — design done; the owner picked P3 and the full first stage for the 0.10.0 re-cut; open picks below; build tickets are numbered by the planning chat from the plan.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1191 | The naming model: one `part_place` fact, one composer, names and chips for every part | OPEN — design done; build in the 0.10.0 re-cut; owner: see ## Owner | — | the open picks; the 7 build tickets (planning chat numbers them); a Python facts step and one TypeScript composer |

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

## Acceptance
Every part with an area that binds it says so in its name or chip on every surface; no surface throws on a missing
fact; a feed change still re-joins neighbouring sections cleanly (the live-join sheet's states pass as tests).

Depends: T1196 (first), T1199 (an eco-reserve part is wholly inside). Severity: medium. Model: Opus. Size: L.

## Owner

- [ ] 2026-11-26 — No-access on the card (Q4): (a) the "No public access" chip ranks first and the day answer is unchanged, for 0.10.0 (recommended); (b) the status pill says it and the rules fold under "If you have permission" (0.11.0).
- [ ] 2026-11-26 — Which no-access land counts (Q5): (a) all OSM "closed" land, as today; (b) military land and watersheds only, the rest becomes "Access may be restricted" (recommended; this filters content, so it needs his OK); (c) one by one.
- [ ] 2026-11-26 — Indigenous land chips (Q6): (a) "On" when wholly, "Crosses" at 5% or more (recommended); (b) only at 50% or more; (c) banner only.
- [ ] 2026-11-26 — Reserve words (Q7): (a) "Cowichan 1 reserve" (recommended); (b) "Indian reserve (Cowichan 1)"; (c) the name alone.
- [ ] 2026-11-26 — "Main channel" (Q10) for a mouth-to-source main line beside side-channel parts: (a) yes (recommended); (b) "Mouth → Source"; (c) the water's name.
- [ ] 2026-11-26 — The 14 side-channel parts with no nameable reason (Q-R2): (a) fold them by the approved rule "channels take the river's rules" (recommended; this is T1240); (b) fix each binding by hand first, then fold. His "Hold the re-cut for all of them" covers the timing.
