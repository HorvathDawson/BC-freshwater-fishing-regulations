# T997 — Water map: two boundary labels at almost the same place draw on top of each other (Nicomekl River)

Status: OPEN (owner 2026-11-21: next chat).

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
