# T997 — Water map: two boundary labels at almost the same place draw on top of each other (Nicomekl River)

Status: IN REVIEW — owner: OWNER-TODO "Look (you): T997 Nicomekl labels" (the after shots, T997-shots/); code 92ebd9c5d dcc18bb8f, PR open.

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
only: "23 m below the Nicomekl flood dam" and "Dyke gates" are different ends 23 m apart, so both drew. Fix: `mergeNearLabels`
(waterMap.ts) — labelled marks under `MARK_LABEL_MERGE_M` = 100 m are one box naming both ("A · B"), rings/a11y/taps per mark;
and `marksLayers` collides its own labels (open mark first, `symbol-sort-key` rank) so farther pairs never overlap either (side
effect: a road/place name under a boundary box gives way to it). Same file for web and native (point-marks.ts).
Sweep (release r-741815c6090d, `part_mark`, pairs sharing a part end): 28 pairs of DIFFERENT ends under 100 m in 22 waters
(Nicomekl, Serpentine 12 m; Pinkut 12 m; Little Qualicum 17 m; ...), now one box each; 38 same-end pairs were already one.
Shots: web dark before/after, web light after, Android (test-bed emulator, local-data APK) after. No pipeline/data change needed.
Not shown: an Android "before" (the owner's own shot is the before) and the iPhone simulator.
