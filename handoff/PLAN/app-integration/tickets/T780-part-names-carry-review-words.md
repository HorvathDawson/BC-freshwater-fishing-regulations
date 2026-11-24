# T780 — Part names show a cut's review words: "…, approximate location (medium confidence)"

Status: DONE — 72efb1a10, 1daa94efe, 6b23aa2ca; shipped in 0.8.4 (cut e769f710a, data r-fec3fab4c5aa; 0.9.0 audit 2026-11-22, every hash is an ancestor of origin/main and of e769f710a). (Earlier status: IN PROGRESS — 72efb1a10, 1daa94efe, 6b23aa2ca; part names say the place alone; merged via 6720c8eb7; pictures approved 11-20; left (Claude): ships with the next data release (and the Sheldens Creek tidal label with the next atlas build). (Earlier note: IN REVIEW (built on lane/l2-parts 72efb1a10, 2026-11-20: part names and end words say the place's name alone; side deliver r-6db99b676653: binding diff EMPTY (reach digest 6ca9a300dad3d95e unchanged), answers diff 0 of 1,957,010 sections changed, only part-name wording. owner: OWNER-TODO "T780 part names drop the suffix" (picked 11-20); left (Claude): main chat review + merge; ships with the next data release).))

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T780 | Part names show a cut's review words ("…, approximate location (medium confidence)": Hope bridge, Alexandra Bridge, Skutz Falls, Sandy Pool sign, the Zymoetz Hwy 16 bridge); move them out of the label, add a check | DONE | 72efb1a10, 1daa94efe, 6b23aa2ca | — |

Source: reading the release r-46e4355a7277 fixtures, 2026-11-16. The live release before it already showed one (the
Cowichan's Sandy Pool sign).

## What is wrong
A part's name is built from its end cuts' stored labels. Several cuts made from the owner's cut answers carry the
proposal's review words in the label itself, and an angler reads them:
- Fraser: "From the Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope), approximate location …", "the Alexandra
  Bridge (Hwy 1), approximate location (medium confidence)".
- Cowichan: "the Skutz Falls, approximate location (medium confidence)", "the white triangle boundary sign located in
  Sandy Pool Regional Park, approximate location (medium confidence)".
- Zymoetz: "the Hwy 16 bridge over the Zymoetz (Copper), approximate location (medium confidence)".

## What to build
- List every cut whose label holds "approximate location", "confidence", "CHECK" or a bracketed review note; move
  those words to the cut's `note` / `confidence` fields (they exist) and leave the label as the place's name.
- A check in `check_curated --waters`: a cut label may not contain those words.
- The app chat left two pins that name the Sandy Pool label as it is today (`waterGroup.fixture.test.ts`,
  `waterMap.test.ts`, each says so in a comment): change them with the labels.
- Labels only: no section moves; one deliver run, binding diff empty.

## Owner answer 2026-11-20

Owner 2026-11-20 ("Couldn't we just assume most of these are done already?"): take the default, part names do not carry the review words (the ticket's "What is wrong" and "What to build"). The change is NOT built yet; the next step is Claude's.

Status line before this answer: OPEN (the words are not in `data/curated/waters/cuts.json`: the labels are clean (e.g. "Skutz Falls", confidence "medium"); deliver appends "approximate location (… confidence)" from the cut's `confidence` field by the T207 rule (`pipeline/deliver/answers/display/part.py`, `confidence_words`). Needs a call on whether part names keep that suffix, then one deliver run and a release)

## Built (lane L2, branch `lane/l2-parts`, 2026-11-20)
Commits 72efb1a10 (code) and 1daa94efe (the mini release re-cut, which the change moves). `display.part.end_name` no longer appends `confidence_words`: `display.ends` (a part's name, a map
mark, an in-season "Where") and the bundle's part_mark labels say the place's name alone. The cut's `confidence` still
ships (export `splits`, bundle `part_mark`) and the review app still says "approximate location (… confidence)"
beside a cut's label. The cut labels in `cuts.json` were already clean (0 of 505 carry review words).
- Guard: `check_curated --waters` refuses a cut label in `cuts.json` with review words ("approximate location",
  "confidence", CHECK, TODO, "??", "[review …]"); a TIDAL-LIMIT label with them was a warning, refused since the review fix below.
- Guide `confidence` sentence: a review fact, never added to the cut's name where an angler reads it.
- water_map tool: "≈" from the mark's own `confidence` (an older release's clause still folded).
- Pins: `test_t207_split_confidence.py` (end_name), `test_water_map.py`, `test_check_waters.py` (2 mutations, the tidal
  warning, the "Checkerboard" near miss); app `waterNav.test.ts` (toy ends: a mark's confidence is never appended),
  `waterMap.test.ts` + `waterGroup.fixture.test.ts` read the committed (pre-T780) fixture through `plain` until the
  release re-cuts it; `waterNav.merge.test.ts` needed nothing (toy ends without the words).
- Proof, side build of this branch (`pipeline build --side … --no-tiles` via heavy.sh; release r-6db99b676653):
  reach run identical (digest 6ca9a300dad3d95e = live), `binding_diff` live vs side: **no binding changed**;
  `answers_diff` r-6258ffded676 → r-6db99b676653: **0 of 1,957,010 sections changed**, 17 waters' part-name wording only
  (16 lose the suffix, the Skeena gains T801's "Mainstem only"); `display.ends`: 58 end tokens lost the suffix, 0 of
  10,124 carry "approximate location" now.
- Pictures (web app at 390 px, light; before = live release, after = side release): `T780-shots/fraser-hope-before.png`
  / `-after.png` (part 4 and its boundary mark), `T780-shots/cowichan-before.png` / `-after.png` (the parts list and the
  Sandy Pool mark). The Zymoetz has no part ending at the Hwy 16 bridge in the live release (that end is no part's end
  since r-46e4355a7277), so its names show no change and no shot was taken.

The one tidal limit label that still shipped review words: Sheldens Creek, `tidal_limits.json` "Spur 19 Bridge site
(road bridge near the mouth, low confidence)". OWNER 2026-11-20 ~06:34 (dialog), verbatim pick: "Sheldens Creek label"
— DONE on this branch: the label reads "Spur 19 Bridge site (road bridge near the mouth)" (one line of the file changed,
diff-proved; the row's `why` keeps "LOW CONFIDENCE"). It is an ATLAS INPUT: it ships with the next atlas build (until
then release preflight lists `tidal_limits` as newer than the atlas). Its cut id follows the label:
`sheldens_creek__spur_19_bridge_site_road_bridge_near_the_mouth_low` -> `…_near_the_mouth` (no curated rule, test or
fixture names it; the end token in `display.ends` moves with the atlas).

REVIEW FIX (independent review 2026-11-20): the guard matched with `re.I`, so "Check dam" or "Confidence Creek
confluence" would have failed the build's first step. Now CHECK / TODO match in capitals only, and confidence only as a
note — "(low|medium|high confidence)" or ", low|medium|high confidence" — besides "approximate location", "??" and
"[review …]". With the Sheldens label reworded, a tidal limit label with review words is REFUSED too (no longer a
warning). Tests: `test_check_waters.py` (7 refused tidal labels, 6 place names that pass in both files).

PICTURES APPROVED by the owner 2026-11-20 ~06:34 (dialog): the Fraser, Cowichan and Skeena before/after shots above.
