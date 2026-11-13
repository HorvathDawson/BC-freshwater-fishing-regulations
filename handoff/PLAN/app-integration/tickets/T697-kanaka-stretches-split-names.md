# T697 — Kanaka Creek's stretches: named by their split names, never "stretch 1 of 2" (T204 follow-up)

Status: DONE — 77e856247 (nothing to rebuild: the live release and app already do it; a province-wide guard added)

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
- The Parts sheet's "Closed all year · N stretches" second line (45 waters, start link only): drop the line, or the sheet.
