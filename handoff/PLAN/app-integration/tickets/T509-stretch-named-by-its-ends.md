# T509 — The Fraser's lowest part reads "2 stretches · stretch 1 of 2": name a stretch by its own ends

Status: IN REVIEW — 48710b7cb, ed962781d, b569fb5e2 + T517, in build 22 (3d807829d); owner: OWNER-TODO "Phone check with build 22" (Fraser parts named by their ends).
Earlier: IN REVIEW — 48710b7cb, ed962781d (owner "Ends only"); the 114 bare names are done by T517 (one part per run: 0 bare names on its side release r-d62af06225a2), which replaces `stretchEnds`; owner sees it with T517.

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
  the old words.
- **Owner 2026-11-08 "Ends only"**: a stretch named by both ends carries no "· stretch k of n" (it confused); the count
  stays only on a name with no ends ("2 stretches · stretch 1 of 2"). Each stretch is already its own entry ("Part 1 of
  19" counts entries), so the map shows the pieces as separate parts in river order.
- Fraser now: "The CPR Bridge at Mission → Landstrom Bar sign zone boundary" and "Landstrom Bar sign zone boundary →
  Spuzzum Creek confluence". One load path: header, parts list and screen reader all read
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
2. **Landstrom Bar zone at 138 m: plausible, closed (owner 2026-11-08: the sign splits are right).** The curated ring's
   corners match the text, and its "S end of Croft Island" (49.3761, −121.4573) is the island's south edge in OSM
   (49.3762). The ring sits mostly over the side-channel mouth on the north side. The FWA mainstem runs along the south
   bank and crosses only the ring's downstream corner: 156,379–156,517 m, 138 m.

## Parts as contiguous runs in the pipeline: tried, reverted (2026-11-08)
Owner: "we combine sections after if the regulations are the same for neighbouring items, so this should naturally happen
from the extent structure" — then "make sure … the painting feeds … stuff gets combined properly". Tried: `derived.part_rows`
cut each same-rules group into its contiguous runs (`topology.stretches`). Dry run on the live bundle: 20,708 → 20,933
parts, 160 waters renumbered; the Fraser 17 → 20 (Mission → Landstrom, the zone, Landstrom → Spuzzum). **Reverted, not
shipped**: the bundle refuses it (`part` is UNIQUE per (water, rule set, licensing, …), schema.sql:401 — one part per rule
set is a built-in invariant), and the consumer map below shows behaviour changes beyond the split. Kept instead: the
grouping as it is, the stretches presented as separate entries named by their own ends (above). Nothing renumbers.

### Consumers of parts / `part_ix` (static map, 2026-11-08)
- **Unaffected (keyed by section, not part):** tiles (no part property), `paint.py`, `salmon-*.bin`, app feature-state /
  `litKey` (section ids), status_index / verdicts, feeds worker + DFO/gauge feeds (no part reference), share links
  (blk + metre), `carry` / `place`, `guideClosed`.
- **Would change if parts were split** (why the split was reverted):
  - SEO `/changes`: `rulesKey` holds `partIx` per part (`app/tools/seo/model.ts:232`), so ~160 waters would list "Rules
    changed" with no rule changed.
  - In-season `matches.json` `places` (`pipeline/regs/inseason/publish.py`): part indexes reach the device. Guarded by the
    release name (`core/src/inseason.ts:151`), but every water falls back to `extents` until it is re-published.
  - Picker / Parts sheet / closed-page merge: one choice per piece, longer lists. `targetFor`'s akey fallback picks one of
    the sibling pieces. `part_label`'s duplicate pass stamps "(same rules, a separate stretch)" on siblings.
  - Golden taps (`regs-recut/taps.py`): moved indexes need PART_MOVED / PART_GROWN / PART_GONE declarations.
  - Problem reports keep `part N` beside the release name: old reports read against a new release point elsewhere.
- **Per release, rebuilt each load (fine either way):** answers `parts`, export `waters.parts`, store decode, regs reads,
  `waterGroup` / `mergeNav` (T228 salmon pieces stay in `part_stretch`), water map + T508 ready signal, `part_line`,
  `part_geo` / `part_mark` / `part_stretch`.
- With the presentation-only fix, none of the above changes: same release, same parts, same sections lit per entry, same
  ▲/▼ order. Only `partName`'s words change.

### What is left
The 114 bare names ("N stretches · stretch k of n": Fort Nelson, Mackin, Blaney …) need each stretch's own ends from the
pipeline: an ADDITIVE `short` on `part_stretch` (each stretch's runs' ends, `short_label`'s words), read by `stretchEnds`
before its fallbacks. No part, index or consumer changes. A bundle + release rebuild; not done here.
