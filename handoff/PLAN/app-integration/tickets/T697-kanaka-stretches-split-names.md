# T697 — Kanaka Creek's stretches: named by their split names, never "stretch 1 of 2" (T204 follow-up)

Status: DONE — 77e856247, 434449f3d (follow-up: an unnamed lake at a water's end is its Source; rides the next release; RULINGS P55)

Owner 2026-11-12, asked about T204's old phone-check line ("its lower part is two separately selectable stretches
('… · stretch 1 of 2'); say if the stretch wording should differ"): "No it has to use split names like we said before ??"

## The earlier decision
- T509, owner 2026-11-08 **"Ends only"**: "a stretch named by both ends carries no '· stretch k of n' (it confused)".
- T517, owner 2026-11-09 ("Names from ends"): a part ships its two end tokens; one app function (`spanName`) names every
  part "lo → hi" in the split's own words, for the header, map list, Parts sheet, share/SEO and in-season "Where"; no
  "(n)", no "Within", no two parts of a water with one name. Owner 2026-11-09 on build 24: "fine".
- Rule: a part is named by the names of the splits at its two ends ("Vedder Crossing → Tamihi"), never by a count.

## Found (release r-0a9930fb8826, read-only)
- The question was stale: written 2026-10-27 (r-8b43fb267199, before T509/T517). No code in the app or the pipeline makes
  "stretch N of M" any more (T517 deleted `stretchEnds`); the label layer is the app (`@app/core` `partName` →
  `spanName` over `part.lo_s` / `part.hi_s` and `display.ends`); the pipeline ships tokens only.
- Kanaka Creek (gnis:11318) is 3 parts in river order, each with both end tokens, 3 marks:
  `kanaka_creek__cpr_bridge_tidal_boundary` → `kanaka_creek__112_avenue_bridge` (part 1), → `kanaka_creek__cliff_park_falls`
  (part 2, closed all year), → `lake_outlet` (part 3). The map reads "CPR Bridge (tidal boundary) → 112 Avenue bridge",
  "112 Avenue bridge → Cliff Park Falls", "Cliff Park Falls → A lake outlet" (pinned in `tools/stretch-names.test.ts`).
- Province-wide (562 waters of several entries, every entry through `loadWaterGroup` + `partName`): 0 named by a count,
  every keyed entry equals `spanName` of its ends, no duplicate names. (Fulton River's "Downstream end of the stretch
  opposite the spawning channels" is its split's own wording, not a count.)
- Still says a count, not a name: the picker's choice text "Closed all year · N stretches" (45 waters, e.g. Kootenay,
  Peace, Morice, Dean, Bulkley); it shows only as the Parts sheet's second line, and that sheet has no button since T150
  (reachable only by a start link `&sheet=parts`). The map bar's kicker "Part 1 of 3 · Kanaka Creek" is a position beside
  the split name, not a name.

## Done
- 77e856247 `app/tools/stretch-names.test.ts`: a province-wide case (every water of several entries: no count in a name, each keyed
  entry is `spanName` of its own ends, no duplicates). Mutation-checked: `partName` appending "· stretch k of n" fails it.

## For the owner (not done)
- "A lake outlet" (an unnamed lake's outlet, Kanaka's top end) is the one end with no name of its own. Proposed rule:
  an unnamed end is said by the nearest named feature, as T517 already does for crossings ("lake outlet near X").
- The Parts sheet's "Closed all year · N stretches" second line (45 waters, start link only): owner 2026-11-12 "Remove the
  screen" — removed, T698.

## Follow-up 2026-11-12: "Cliff Park Falls → A lake outlet" (RULINGS P55)
Owner, verbatim: "Why would this cause a cut? All cuts have a name? So then it is mouth to cut the cut to head? But yes
unnamed lakes should be unnamed lake near x... But I still don't know why that is needed here...".

**Why.** No cut stands there. Kanaka Creek's FWA blue line (blk 356352191) ends at 24.77 km: its last section (295116,
24,725–24,767 m) runs from the creek's own polygon to an unnamed lake, and no section of the creek (or of that blue line)
lies above it. The creek BEGINS at that lake. The atlas writes a lake edge as an end token (`bundle.spans.end_token`: a
section whose upper end touches a lake is `lake_outlet`, `:<item>` dropped for a lake that is not a named water) instead
of `source`, and the display said it "A lake outlet". T517's own end words are "Mouth" / "Source" (`part._end_short`); T517
had no rule for an unnamed lake at a water's end (its crossing rule, "said near what stands there", fired only where two
names clashed).

**Counts (r-0a9930fb8826 / the live export).** Bare unnamed-lake tokens at a part end: 1,390 (1,387 tops, 3 bottoms). On
waters whose parts are named (two or more): 56 on the release, 67 on the live export (62 tops, 5 mid-water). Of those, the
water's OWN end: 62 (all tops); a real part boundary at an unnamed lake: 5 ends on 3 waters (Slack Creek: rules change
across a lake at 2.3–2.4 km; Gagnon Creek 1.8 / 3.7 km; Jones Creek above its outside-B.C. piece). None of the 5 has a
named feature within 2 km (no `crossing_near` row).

**Done (434449f3d, pipeline answers display — rides the next release).** An unnamed lake at the water's own top / bottom
(the highest `km_from` / lowest `km_to` of all its parts' main-stem runs) is `source` / `mouth`. Anywhere else it is always
said at its place: "Unnamed lake near X" (`crossing_near`), else "Unnamed lake (km N)". Kanaka will read "CPR Bridge (tidal
boundary) → 112 Avenue bridge", "112 Avenue bridge → Cliff Park Falls", "Cliff Park Falls → Source". Tests:
`pipeline/tests/test_part_names_unnamed_lake.py` (4 of 5 synthetic cases and the live-export sweep fail on the old code).
Not changed: the regulation's long "runs" text ("from the Cliff Park Falls up to a lake outlet"), which no screen shows
now that the Parts sheet is gone (T698), and the 1,333 single-part waters, which are said by their water's name.

**For the owner.** The 5 mid-water ends read "Unnamed lake (km 2.3)" (T517's km fallback: nothing named within 2 km). Say if
those should name something farther away instead.

## Follow-up 2026-11-12: "Yes, read the real boundary" (RULINGS P55 follow-up; supersedes the km answer above)
Owner, dialog option verbatim: "Yes, read the real boundary" — a part end always reads the real boundary standing there,
never the lake. Cause: the atlas cuts a line at a lake edge once, as the lake. Built in the bundle (`bundle.spans`): a cut
aliased onto another water's lake edge keeps its name (`end_token`, and its `split` row names the cut); where the pieces
either side of an unnamed lake differ in region or in/out of B.C., both edges read that line (`lines_at_lake_edges`, from
`_water_places` and `outside_bc`). Side bundle (live atlas `full`, reach run `full`, corpus c9488453c): 10 span ends
change, 6 multi-part waters' names: Slack "Mouth → Region 8 boundary" / "Region 8 boundary → Source"; Jones "B.C. border
→ Source"; Gagnon's outlet end the signs cut ("Mouth → The mouth (400 m above Williston Lake)" — the display's sign-word
trim); Duncan "… → Duncan Dam", Mitchell "… → 100 m above the Mitchell Lake outlet weir", Babine "… → Juvenile counting
weir" (cuts aliased onto a named lake). Still open: Gagnon's part above the lake (inlet km 3.66) has no line or cut —
"Unnamed lake near the fishing boundary signs …" (owner's Williston question); pinned as the one last-resort end in
`test_part_names_unnamed_lake.py`. Rides the next bundle; that test fails on the current live export until then.
