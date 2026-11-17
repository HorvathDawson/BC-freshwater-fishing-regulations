# T803 — Label follow-ups from the owner's answers of 2026-11-16: Cowichan tributaries mark off, Middle Shuswap main river only, Campbell and Fraser Region 3 re-saved, Mara Bridge cut name, Babine areas confirmed, Gitnadoix powerline at the mouth, a side channel renamed, gotchas, Stamp Falls cut 60 m down

Status: OPEN (curated data and saves; some need the next map rebuild).

Source: the owner's answers of 2026-11-16 by question dialog (`handoff/PLAN/OWNER-TODO.md`, "Answers by batch",
batches 2 to 9). His words are quoted there; the work, one item each:

1. **Cowichan**: the water's "including tributaries" mark OFF ("Turn it off"). List the changed answers for him first.
2. **Middle Shuswap** (Shuswap Falls to Mabel Lake): "Main river only".
3. **Atnarko below the Tweedsmuir Park Lodge sign**: stays with its side streams. His rule, for every DFO place: side
   streams follow DFO's words; "The heading counts" (a heading "including tributaries" covers the stretches under it).
   Write it into `pipeline/docs/RULINGS.md`; check no other stretch under such a heading has side streams off against it.
4. **Fraser Region 3 mainstem**: "it is right if it is spuzzum to the 3/5 boundary now. ON THE FRASER". It is
   (Spuzzum km 196.97 to the Region 3 / Region 5 line at Fraser km 452.70): re-save as right with his words; fix the
   label lookup that showed "Region 3 – Region 8 boundary" (it keeps the last row per split id; T776).
5. **Kanaka Creek** (112 Avenue bridge for DFO's "112th Street bridge") and **Kispiox** (the Kispiox River Lodge at
   13.7 km for "Kispiox River Resort"): keep; add both to `pipeline/docs/GOTCHAS.md`.
6. **Shuswap closed stretch**: "just add to gotchas that is all. might be 1km as bird flies too": the two printed
   points are 1,738 m apart in a straight line and 2,075 m along the river; neither is "one km". Gotcha entry only.
7. **Chemainus tidal boundary**: checked against DFO's written boundary he supplied ("a boundary sign on the Chemainus
   River near the Bald Eagle Campground"): our point (2,091 m up) is 0 to 135 m from the campground along the river.
   It stays; record DFO's sentence as its source (`data/curated/waters/tidal_split_authority.json` says
   `dfo_written: false`).
8. **Campbell River near the mouth** (Quinsam confluence, Maple Street sign, cement block sign): "They are right".
9. **Lower Shuswap, Mara Bridge**: "at the bridge but make sure the cut name has the fact signs are nearby".
10. **Babine Lake's 13 closed areas**: "i think you can confirm them. it is just confluences and unless the names dont
    match they are easy to auto confirm": confirm each circle whose creek name matches DFO's words and the Gullwing
    Creek line; any mismatch goes to him.
11. **Gitnadoix**: "the powerline is. the mouth essentially so maybe you could add the cut for powerline to the mouth
    then it is all good?": the powerline is the mouth for this place (the mouth carries the name); the whole river
    stays open for coho; no new cut.
12. **Skeena side channel name**: reword from the lower end up (check which end is lower first).
13. **Stamp Falls lower cut**: "Move it 60 m down (Recommended)" (225 m below Deer Creek; next map rebuild; picture
    to him before it ships). **Sproat**: his 50 m fix is in; re-save as right.
14. **Kitimat east bank**: both banks stay closed until one-bank places are built (no change).
15. **Nicomekl and Serpentine** "No angling (from a powered boat) — upstream of dyke gates" binds 27 side streams too
    (the app chat's note, 11:32): check the book's words; the two rivers only, or their side streams.
16. **Eulachon**: "general rule is (Reminder: There is no fishing for eulachon in Region 6. ) why would bait remove
    eulachon rule?": the line shows on every Region 6 water; a bait rule never takes its place (with T770).

## Lane result, item 15 (2026-11-16, branch `lane/book-rule-fixes-1116`): no change
Book p.25 (row images page_027_row_002 / _031): NICOMEKL RIVER and SERPENTINE RIVER each carry the global ✱
(includes tributaries) beside the NAME; their last line reads "No angling from powered boats upstream of dyke gates".
RULINGS T2: a row whose name carries the global ✱ includes tributaries for EVERY rule. So the 27 side streams are bound
by the book's words; the binding stays. Item 3's rule ("The heading counts") is RULINGS T16 (865cbac37).

## Where the items stand (lane/bugs-080 check, 2026-11-17)
Done elsewhere: 1, 2, 9, 11 (a5990dd7b, 5debac3ef; carries 02f0fedca), 3 (RULINGS T16, 865cbac37), 5 and 6 (GOTCHAS,
4bd6e5333), 15 (no change), 16 (with T770), 14 (no change). Left:
- 4, 8, the Sproat re-save, 10: saves through the review app (marks), not a file edit; 4's label lookup is T776.
- 7 (Chemainus DFO sentence in `waters/tidal_split_authority.json`) and 13 (Stamp Falls cut 60 m down, `cuts.json`): both
  files are ATLAS INPUTS (`pipeline/atlas/inputs.py`); an edit makes the live atlas stale and the release preflight
  stops. They go with the next atlas rebuild, not the 0.8.0 reach-only build (13 also wants a picture to the owner).
- 12 (Skeena side channel name): not traced here.
