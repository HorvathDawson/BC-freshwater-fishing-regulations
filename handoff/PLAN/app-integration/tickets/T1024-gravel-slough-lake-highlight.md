# T1024 — "Weird highlighting on the lake" (Gravel Slough)

Status: NOT REPRODUCED on web (staging, 412 px, whole water + NE loop at z14: a clean part line, no stray fill; shots in `T1024-shots/`). The report is from the Android build at 49.202,-121.904 z14.3; an emulator pass was not done. Stays open for a device repro.

Source: STAGING report `3444007425e9d3bd`, 2026-11-23 05:32Z, Android 0.8.5 (staging build), water · map, Gravel Slough
(`gnis:8009`) part 0, zoom 14.3 at 49.202, -121.904. Verbatim: "Weird highlighting on the lake here".

## Work
1. Reproduce on the emulator and web (Gravel Slough, Map tab, part 0, zoom ~14): shoot what highlights. A slough is a
   stream by rule 55; check whether a lake polygon nearby (or the slough's own polygon) takes the part's selection
   highlight, or the T467 shoreline / T1000 out-of-B.C. drawing is involved.
2. Fix where it comes from; before/after shots.

Depends: none. Size: S (until reproduced). Owner: no.
