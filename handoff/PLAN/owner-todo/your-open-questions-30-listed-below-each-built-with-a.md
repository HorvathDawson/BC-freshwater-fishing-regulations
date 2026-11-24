order: 160

## Your open questions (30 listed below — each built with a default; the table in tickets/README.md has the wording)

Triaged 2026-10-23: 64 smaller calls were decided by Claude and 12 were already answered or obsolete — all recorded in
decisions.md "CLAUDE 2026-10-23 — owner-question triage" (read and reverse any). What is left needs you:

- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **T387 point-bound facts (2026-11-04, default: left per tap):** every other fact on a Regulations page is now its
  PART's. Two still follow the tap: the sun line (sunrise / sunset at the tapped point — or the part's midpoint?) and the
  scroll memory (reopens where you left it per tapped reach — or per part?). Ticket T387 has the why.
- **Readings of the book (a wrong call misleads anglers):** ANSWERED 2026-11-12: Q124 a river's rules on unnamed widenings (456 polygons): keep as built now (the polygons take the river's rules); later, fold more small widenings into the stream and draw the stream line instead of the polygon where it is really the stream, real lakes stay lakes (owner's words in RULINGS E27, ticket T695; DEFERRED 2026-11-13 with stocked lakes and bathy map, no longer "after closed testing") ·
  (Q87, Q140, Q62, Q63 answered 2026-10-28.)
- ANSWERED 2026-11-12 ("Leave gaugeless"; "Show 08GC005 above the canal"; built, RULINGS P54): **T696: diversion gauges — no gaugeless section is a diversion gauge's.** The 28 stay gaugeless; 08GC005 now leads the Theodosia above its canal, labelled "measures a diversion" (shows on the next deliver + app build).
- ANSWERED 2026-11-12 ("Leave it": no intake cut; the ~160 m below the intake reads 08GC005 with the river above, as built): **T696: a cut at the Theodosia intake?** (2026-11-12). The canal leaves the river at the intake dam about 160 m ABOVE where the river's section begins (the 08GC006/08GC007 bypass gauge cut). So that 160 m stretch — where the bypass gauges stand — reads 08GC005 with the river above. Add a cut at the intake (rides the next atlas build, which the T690/T691 cuts already need) so it stays 08GC006's? Pictures: ~/bcfish-session-backup-2026-11-12/theodosia/theodosia_after_closeup.png (intake and section end marked), theodosia_before_closeup.png, theodosia_after_overview.png. Ticket T696.
- DONE (answered 2026-11-13, released r-f749475d0962): **T691: five silent gauge stations need a match call before they can be cut** (2026-11-12). Q113 is built (32 cuts, ride the atlas build, RULINGS P53). Not cut: 08FE002 Kemano Powerhouse and 08ND025 Revelstoke Project Outflow (never matched); 08JA013 Skins Lake Spillway, 08NE126 Arrow Reservoir Outflow, 08NJ158 Kootenay Lake Outflow (matches marked wrong, no water). Say which river line each measures, or leave them uncut. Also worth a look: 08JB013 "North Beach Creek above Allin Creek" is matched to "Beach Creek". (Final atlas build 2026-11-12: 08JB013 and 08PA004 Skagit made no cut — 520 m and 1,015 m from their line, over the 400 m limit.) Ticket T691. Claude's proposed answer per station (2026-11-13, NOT applied: a row each, in plain words, in the ticket's "Phase 2 proposals" table; pictures ~/bcfish-session-backup-2026-11-12/gauges/*.png): accept or change each row.
- ANSWERED 2026-11-12: **Which gauge speaks:** Q113 "silent doesn't mean retired so we should add cuts Incase they start transmitting again?" (built, T691) · Q103 "Canal gauge also serves river", then "Leave gaugeless" and "Show 08GC005 above the canal" (built, T696). Still open: Q31 the gauge rule.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **How it looks (taste):** Q130 each model's line under Change · Q132 day chips may still use ELF · Q79 "river" vs "stream" on the page ·
  Q91 "no gauge close enough" wording · Q123 how the Map view marks the selected part (built: dim the others; thin halo in
  `app-integration/T151-shots/alt-halo--*`) ·
  Q2 / Q139 / Q47 no-access and tidal notes on Conditions.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **"Today" is the phone's day, not BC's (T368 Q6, 2026-11-04):** (B.C. time is the settled reading; see the permanent daylight time note.) your answer assumed the app counts days in BC time; it
  counts them in the PHONE's zone (`localDate`): an Alberta phone at 00:30 MT is already on the next day while BC is at
- ANSWERED 2026-11-12 ("It should be in the vessel restriction section but it should say the locator text there"; built c63427bbd, RULINGS P51, shows on the next app build): **Vessel rules (T209):** the 10 close-wording undrawn parts kept ("various locations", "swimming areas") in
  on-parts.md: whole water too, or leave as notes? (`max_power_kw` 7.5 is stored on the 162 electric-only rules, 10-28.)
- ANSWERED 2026-11-12 ("Yes, book's full words"; built 81598325e, RULINGS P51 follow-up, shows on the next deliver + app build): **Harrison River vessel line (T209):** reads "Speed restriction or no powered boats — in small bays along the river as signed"; the other 21 undrawn-part vessel rules checked against the printed book: nothing else dropped.
- ANSWERED 2026-11-12 ("Keep them as two entries"; built 2841fb605, RULINGS P51 second follow-up, shows on the next deliver + app build): **"or" vessel lines as one joined entry? (T209)** No: speed restriction and no vessels are listed separately, each with the book's locator — Harrison "No powered boats — in small bays along the river as signed" and "Speed restriction — in small bays along the river as signed" (Claude's reading: the Harrison too); Okanagan, Kalamalka, Skaha "No vessels — various locations (as buoyed and signed)" and "Speed restriction — various locations (as buoyed and signed)".
- ANSWERED 2026-11-12 ("Yes, read the real boundary"; supersedes the earlier "'Unnamed lake (km 2.3)'"; built 040b79d16, RULINGS P55 follow-up, shows on the next release): **T697: part ends at an unnamed lake** read the real boundary that stands there, never the lake — Slack Creek "Region 8 boundary", Jones Creek "B.C. border"; a cut aliased onto a lake edge keeps its own name (also Duncan "Duncan Dam", Mitchell, Babine "Juvenile counting weir"). One end still has no real boundary: Gagnon Creek at km 3.66 (next row).
- ANSWERED 2026-11-13 ("Mouth is still Williston"; RULINGS P57; built in the T678 junctions candidate atlas `full_next`, shows on the next release): the mouth is where the creek enters Williston, the signs ARE the mouth (cut `gagnon_creek__fishing_boundary_signs_at_the_mouth`, the 400 m cut removed), the whole creek keeps the spring closure, the unnamed lake stays a lake of its own (`waters/pond_calls.json`). Was: **Gagnon Creek: is the unnamed lake really part of Williston?** (2026-11-12). You said: "Maybe we could put signs near the mouth? Let's just assume the mouth is the signs and add it as a variant name. Would this fix it? Seems it is in the wrong spot??", then "Where it enters Williston so it captures the whole stream. Also unnamed lake is part of Williston I think". NOT BUILT: the map data and imagery point the other way — the 58 ha lake (FWA wbk 329465442, coded "lake", Williston is coded "reservoir") sits behind what looks like a rail/road embankment, joined to Williston only by the creek's 74 m channel under it. Picture: ~/bcfish-session-backup-2026-11-12/lake-ends/gagnon_creek.jpg. Build it as you said anyway (the lake is Williston water, the signs at the creek's mouth at km 3.66, the whole flowing creek keeps the spring closure), or keep the lake as its own water and put the signs at the creek's mouth into that lake (km 3.66)? Ticket T697.
- **Salmon (T164), refreshed 11-05:** 179 DFO places still need an extent; each has a fresh proposal (52 on existing
  cuts, 47 whole water, 65 new splits, 15 with a reason). Review app → DFO salmon extents → accept suggestion → Save
  extent. Includes Skeena B(i) (T229 Q3) and the 3 items T326 waits on. Claude rebuilds and measures after.
- ANSWERED 2026-11-12: POSTPONED (owner: close as answered). **User accounts (T223, new 2026-10-28):** one account for web + phone. Questions Q-T223a–f in the ticket: auth
  provider, what syncs (spots / photos / pins / settings), privacy of spot locations, offline merge rule, optional or
  required, before or after first release. Nothing is built until you answer.
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **Gauge gaps on main rivers (T174, shipped r-bda3fea28838), including Q154 (small ponds):** Q153 (side piece adopts the largest ANSWERED river section) and
  Q155 (a water's own lines between alike pieces) shipped: +407 Flow / +327 Level sections, 0 answers changed — veto =
  one switch each in panels.py. **Q154 is yours:** the "double-line polygons" are small FWA lakes; walking them as the
  river moves 96,590 sections (+95k Flow) — kept off. Numbers in tickets/T174. Your "how small?": examples +
  a proposed pool-only cut (< 1 ha and ≤ 2× the river's width: 10,650 of 136,080, 11,423 sections) in
  curation-review/q154-examples.md. **See them in the review app:** Sloughs tab → "Small ponds (Q154)"
  (`#sloughs/q154/`): the 20 md examples + a 60-pond sample (caught / just missed / clearly not, all regions), each
  on the satellite map with its outline and river; mark each pool / real lake / unsure (keys p / l / ?) and the tally
  shows how your calls line up with the cut — then say yes / no / another cut.
- **T89 six waters (updated 2026-11-13, T708):** Hidden, Secret and Square (= Whitton) Lake are DECIDED (owner "add the 3 high confidence lakes"; name records, applied by the next lane). Still unmatched: Frog (5-6), Redfern (5-15), Squirrel (6-1). Owner: "will look harder for frog and redfern later" (Owner To-Do row `t708-frog-redfern-squirrel`; evidence in T708 Work 8).
- **Run timing (T199, built 10-28 on staging; Q144 answered with the defaults):** (1) run
  `gh workflow run feeds-runtiming.yml --ref redesign/stream-sections -f upload=true` once (uses the repo's R2 secrets;
  staging only) and check its report; (2) after every atlas promote, rebuild the binding base on the Mac:
  `python -m pipeline.runtiming.feed.base --upload --yes`, then run the workflow; (3) when happy, uncomment its
  monthly schedule; (4) look at Run timing on the phone (T199-shots/).
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **T182 caps (built with defaults, veto if wrong):** offline total 20 MB (was 15); cold-load gate 3.35 MB (salmon on
  the first Salmon press, as you decided 10-28).
- ANSWERED 2026-11-12: **Scope and big rebuilds:** Q10 imagery corridor (offline corridor + pools go/no-go; online Esri is settled): no, offline imagery is not for the first release.
- **Things only you can do or know:** Q4 slough marks — review app **Sloughs** tab (`#sloughs/`): 811 confirmed so far; still to call: the 135 wide-marsh pre-marks and 314 on your list (113 intermittent) · Q126 were you
  on Level? · Q9 phone testing · Q26 hosting/app name ·
  Q27 sign-off walk.
