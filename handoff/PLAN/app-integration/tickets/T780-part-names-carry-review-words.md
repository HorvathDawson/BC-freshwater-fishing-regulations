# T780 — Part names show a cut's review words: "…, approximate location (medium confidence)"

Status: OPEN (data: `data/curated/waters/cuts.json` labels; then one deliver run and a release).

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
