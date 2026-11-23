# T997 — Water map: two boundary labels at almost the same place draw on top of each other (Nicomekl River)

Status: DONE — round 2 (owner rule 2026-11-22; whole-river rule answered 2026-11-23): 4cdf3245f 5644e8ef1 (round 1, #37: 92ebd9c5d dcc18bb8f, superseded).

Source: owner 2026-11-21, phone screenshot (Android, dark mode), "Add ticket for this ui bug too for next chat".
Shot: `handoff/PLAN/app-integration/T997-shots/nicomekl-labels-overlap.png`.

## What is seen
Nicomekl River → Map tab, Part 2 of 3 "Dyke gates → 208th Street". At the part's downstream end two boundary label
boxes overlap and the text is unreadable: "23 m below the Nicomekl floodgates dam" and "Dyke gates" are drawn on top of
each other, both above the end marker (near Highway 99A).

## Likely cause (to confirm)
Two cuts (or one cut and its alias/neighbour) about 23 m apart each get their own label at the same spot; the label
layer has no collision handling between them (`text-allow-overlap` / `icon-allow-overlap`, or two separate layers that
do not collide with each other). Related: T929 (boundary labels lined up with their place, DONE) and T780 (part names
show a cut's review words).

## Work
1. Reproduce on the emulator and the web (Nicomekl River, Part 2) and find which labels/cuts these are.
2. Labels never overlap each other: collide them (one layer or shared collision), and when two boundaries sit within a
   few metres show ONE box naming both (or the end the part uses) rather than two.
3. Check every water for boundary pairs closer than the label size (a list from the release), not just this one.
4. A test that two near-coincident boundaries produce one readable label; shots before/after on both phones.

## Acceptance
Nicomekl Part 2 shows readable end label(s) on Android, iPhone and web; no overlapping boundary labels in the sweep.

Depends: —. Size: S-M. Owner: looks at the after shots.

## Result (2026-11-22, lane L3)
Cause: the marks' label layer drew every label (allow-overlap on, no collision), and the VM kept one label per END TOKEN
only: "23 m below the Nicomekl flood dam" and "Dyke gates" are different ends 23 m apart, so both drew. Fix (review round
2026-11-22): `mergeNearLabels` (waterMap.ts) merges labelled marks, THE MOUTH'S TOO, whose boxes would meet at the part's
own framing (box 180 x 44 px, metres per px from the part's bounds, `framingMetresPerPx`; under 50 m always) into one box
naming both ("A · B"), each mark joining the NEAREST kept box, so no two kept boxes meet; rings/a11y/taps stay per mark. The
label layer (`marksLayers`) also collides its own labels (open mark first, `symbol-sort-key`) for what a pan or zoom brings
together. Same file for web and native. A road/place name under a boundary box gives way to it.
Sweep (r-741815c6090d, `part_mark` + `part_geo` + `water_exit`, default part framing): 901 part frames with 2+ labelled marks;
73 frames in 47 waters merge a box (30 with the mouth: bc_border 0 m, Nimpkish reserve 5 m ...); 0 kept boxes still meet.
The first version (fixed 100 m, no mouth) left 39 frames colliding. Whole-water framing not swept. No pipeline/data change.
Shots: web dark before/after, web light after, Android after (taken on the first version; the web look is unchanged by round 2).
Not shown: an Android "before" (the owner's shot is the before) and the iPhone simulator.

## Owner answer, round 2 (2026-11-22 23:35, verbatim, on the round 1 after shots)
"Looks better but why does it even need to show both? Like dyke gates is different then 23m below? Only need boundaries
for highlighted section."

## Round 2 (2026-11-23, lane L18, branch fix/T997-selected-part-labels; supersedes round 1's merge, #37)
- **Rule:** the water map labels ONLY the lit (selected) part's own two ends, in the words its name says them
  (`@app/core` `partEnds`: the placed mark carrying the part's `lo` token that it sits above, and the one carrying its `hi`
  token that it sits below; where two carry one token, the one toward the next part in the line). The two labels are
  the two halves of the bar's name ("Dyke gates" / "208th Street" for "Dyke gates → 208th Street"). Every other mark is
  a ring alone: its tap, its pop (which says its words) and its words for a screen reader are unchanged; the open mark
  is no longer labelled on the map (the pop names it). The mouth part's lower end is the mouth's "Flows into …" (T985)
  where no boundary carries its lower end.
- **Whole water (⤢):** the same rule. The part stays lit on the whole-water frame, so its two ends are the only labels;
  no other boundary is named. (Default taken; the alternative, no labels at all under ⤢, is one line.)
- **Never covered, never hidden:** of the (at most) two labels, the one lower on the screen hangs BELOW its ring and the
  other above (`@app/map` `hangBelow`, measured where the camera settles; by latitude before that), so one box lies
  wholly above the higher ring and the other wholly below the lower one: they cannot meet at any zoom, a 23 m part
  zoomed out included. The layer allows overlap (a label is never collided away: the #51 review's "a colliding label
  vanishes with nothing naming it" cannot happen) and is still placed, so a road or place name under a boundary's
  words gives way to them (#37). Web and native share it (`point-marks.ts`).
- **Superseded:** round 1's `mergeNearLabels` ("A · B" boxes) and label collision, T443's one-label-per-place merge and
  T465's framing estimate (`framingMetresPerPx`, PR #51, which under-counted metres/px up to 8.4x): with two labels
  hung apart none of them is needed. T465 closes with this.
- **Tests (fail on main, pass here):** `waterMap.test.ts` (every part of the Chilliwack, Fraser, Skeena and Cowichan,
  part and whole frame: exactly the part's own ends labelled and `"<lo label> → <hi label>"` IS the bar's name; a mark
  23 m from the lower end with other words is a ring; the Landstrom zone; the Skeena's twin marks), `point-marks.test.ts`
  (every pair of ring positions on a 390 x 300 map, coincident included: the two boxes never meet; 0 of 13,000+),
  `marks.test.ts` (`hangBelow`, `top` anchors). Mutation: on main's code the new tests fail (4 in waterMap, 5 in map).
- **Shots** (T997-shots/round2-*): web dark before (main before #37, the owner's state) / after for Nicomekl Part 2,
  Skeena part 2 (the Ecological Reserve pair), the Fraser's top part and the Fraser whole (⤢); web light after for
  Nicomekl. Android emulator (own AVD, test-bed APK of this code + T467, before = the kept test-bed APK 80e981bc): dark
  before / after for Nicomekl Part 2, Skeena part 2 and the Fraser's top part (`round2-*-android-dark-*`).
- **Seen on the emulator, not changed here:** a `regs=` link opened while another water's page was open kept the old
  camera on the new water's first part (Nicomekl then Skeena; Upstream reframed it), and `theme=dark` on a regs link did
  not switch the theme. Both are reported for a ticket by the lane, not fixed in this PR.

## Owner answer, whole river (2026-11-23 ~10:30, verbatim)
"Keep the two labels (Recommended)" — under ⤢ the lit part's two ends stay the only labels, as built.
Review fix (#65): a labelled ring the map did not place sends both labels to the latitude side (5644e8ef1), so the two
cannot both hang above.
