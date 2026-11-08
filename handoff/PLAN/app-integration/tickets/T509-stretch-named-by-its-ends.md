# T509 — The Fraser's lowest part reads "2 stretches · stretch 1 of 2": name a stretch by its own ends

Status: IN REVIEW — 48710b7cb; owner sees it on the next APK (shot: `shots/T509-after.png`). Part of T495 gate 1.

Owner 2026-11-08 (r-6dce77a24e6a, shot `shots/T499-before.png` left): the Fraser's Map view, lowest part, reads
"PART 1 OF 19 · FRASER RIVER / 2 stretches · stretch 1 of 2". Weird for the lowest part.

## Found
- **What a stretch is.** One regulation part the release splits into pieces that do not touch (T204, `part_stretch` in
  the geo file; another part lies between). The water map lists each piece on its own. Its name was the part's short name
  plus "· stretch k of n" (`@app/core` `partName`).
- **The Fraser's part 1 is the regulation "Upstream of the CPR Bridge at Mission"** (release part 2), up to Spuzzum Creek.
  The 138 m **Landstrom Bar sign zone** (part 6, its own rules) sits inside it on the mainstem (blk 356364114,
  156,379–156,517 m), so the part is two pieces:
  - stretch 1: CPR Bridge at Mission (76.5 km) → Landstrom Bar zone (156.4 km): 79.9 km of mainstem + 24 side-channel
    lines (28 sections);
  - stretch 2: Landstrom Bar zone (156.5 km) → Spuzzum Creek confluence (197.0 km): 40.4 km + 3 side channels (6 sections).
- **Not T497.** r-25a89ce9f8e6 had the same two stretches; T497 only took two side channels out of stretch 1 (blk
  355991891 to Nicomen Slough, 355992357 to Maria Slough: 30 → 28 sections). The Fraser has 17 regulation parts (16 listed
  + the tidal piece below Mission); the list counts 19 because parts 2 and 3 are listed per stretch. "The Fraser is in two
  parts split at Landstrom Bar" was not right: its lowest part is in two stretches either side of that zone.
- **Why the words said nothing.** The pipeline's short name (`deliver/answers/display/part.py` `short_label`) names a
  part's ends only when no run ends on an area; the Landstrom zone is an area, so the short is the bare "2 stretches".
  The app then added "stretch 1 of 2". Live release: 459 stretch entries; 68 parts have a bare "N stretches" short.
- The data is right; the name was the defect.

## Done (app, 48710b7cb)
- `partName` names a stretch by **its own ends** where both are known, from words the release already writes: the one
  named mark below it (else the named boundary below a part the map does not list — the Fraser's tidal piece, kept as
  `lowEnd` — else, on the part's lowest stretch, the short name's first end) and the one named mark above it (a mark
  to side channels only is mid-stretch, not an end; else, on the highest stretch, the short name's last end). Otherwise
  the old words. "· stretch k of n" stays (it says the piece is one of a part with the same rules).
- Fraser now: "The CPR Bridge at Mission → Landstrom Bar sign zone boundary · stretch 1 of 2" and "Landstrom Bar sign
  zone boundary → Spuzzum Creek confluence · stretch 2 of 2". One load path: header, parts list and screen reader all read
  `partName`, after `mergeNav`, whatever way the part was opened.
- Live release: 155 of 459 stretch names now carry their own ends (e.g. Skeena "Tidal boundary → Skeena River Ecological
  Reserve boundary · stretch 1 of 3", Kanaka "CPR Bridge (tidal boundary) → 112 Avenue bridge · stretch 1 of 2"); 114
  still read a bare "N stretches · stretch k of n" (Fort Nelson, Mackin Creek, Blaney Creek …): their ends are a mouth,
  a source or a mark-less junction the release does not name per stretch.
- Tests: `waterNav.test.ts` (Kanaka by its marks; a Fraser-shaped part whose lower end is a mark off a tidal part),
  `tools/stretch-names.test.ts` (live release: the Fraser's two names, no bare count left on the Fraser); Skeena
  expectations in `waterMap.test.ts` / `WaterMapView.test.tsx` updated to the new names.

## Options left for the owner (not done)
1. **The 114 still-bare names** need the pipeline to write each stretch's own short name (a `short` on `part_stretch`,
   from the runs' ends in `short_label`). Bundle + release rebuild, no atlas. Recommend: do it with the next release.
2. **Is the Landstrom Bar zone right at 138 m of mainstem?** The hand-drawn ring (`added_areas.geojson`) mostly covers
   the bar; the regulation's line follows the bank from a sign at the bar's eastern end. If the owner expects a longer
   zone, the ring is the thing to redraw (atlas rebuild). Not checked against the synopsis here.
