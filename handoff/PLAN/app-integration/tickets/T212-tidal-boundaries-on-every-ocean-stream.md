# T212 — tidal boundaries: a tidal split on every stream that enters the ocean (research first, no data changes yet)

Status: PARTIAL (41393956 — research done, proposals in the review Tidal tab; no curated data changed)

Source: owner 2026-10-27 — "critical"; start in the next work window.

## Goal
Find the tidal boundary (where freshwater regulation ends and tidal/DFO tidal rules begin) on every stream that enters the
ocean — at least every NAMED one — as candidate splits. Every ocean stream's mouth is labelled as tidal (e.g.
"mouth (tidal boundary)", or a second name/alias "tidal" on the split). Nothing is changed in curated data yet: this
ticket FINDS them all and proposes them, with confidence and sources, for the owner to accept later.

## Inputs to cross-reference
- The owner's earlier DFO tidal polygon: a .gpkg or .geojson holding ONE polygon of the DFO (tidal) boundary, which the
  owner EDITED by hand to match tidal boundaries found on streams. Not found by a quick search on 2026-10-27 (only
  data/source/bc_boundary.geojson and data/curated/gauges/tidal.json turned up). Search thoroughly: the whole repo incl.
  untracked/ignored files (`git status --ignored`, data/source, data/scratch, handoff/), git history of DELETED files
  (`git log --all --diff-filter=D --name-only -- '*.gpkg' '*.geojson'`), and the owner's ~/Downloads, ~/Desktop,
  ~/Documents (read-only). Also check pipeline/atlas/waters/added_streams/coastal.py and the reach run's `tidal`
  diagnostics for what tidal source the build reads today. If it cannot be found, ask the owner for the path.
  FOUND (owner 2026-10-27: "it is fetched from R2"): the `tidal_boundary` layer of data/source/bc_fisheries_data.gpkg
  (the build gpkg from the R2 build-assets bucket), built by data/build_tidal_boundary.py + process_tidal_boundary.py
  (last in git at 5c3319c0) from DFO PFMA subareas (`dfodfooy__dfo_bc_pfma_subareas_chs_v3_gshp`); see
  pipeline/regs/dfo_salmon/DATAFLOW.md (~line 440). The owner's hand edits live in that layer.
- Tidal-boundary text in the DFO salmon notices and the synopsis (e.g. "tidal waters", "tidal boundary signs", "below
  the CPR bridge"), and the tidal limits already curated (Fraser below Mission, Q42).
- Web search per stream where the above disagree or are silent (DFO tidal boundary descriptions, Fisheries Act
  schedules, maps).

## Output
- A candidate list: stream, proposed tidal split location (lat/lon, on the FWA line), source(s), confidence
  (high/medium/low), how the polygon and the text agree or disagree — as JSON + a short .md summary with counts.
- Shown in the curation review (like the DFO suggestions) for the owner to accept; accepted ones become curated splits
  labelled/aliased as tidal.
- Coverage: named ocean-entering streams found / total.

## Outcome (2026-10-27)
- Tool: `python -m pipeline.tools.audit.tidal_candidates --gpkg <bc_fisheries_data.gpkg> --atlas <atlas/full>
  --basemap data/source/bc.pmtiles --out handoff/PLAN/curation-review/tidal` (read only, ~2 min).
- Output: `handoff/PLAN/curation-review/tidal/tidal-candidates.json` (1,163 rows) + `tidal-candidates.md`.
  Coverage: 916 / 966 named ocean-entering streams (FWA 9xx mainstems + Fraser/Skeena/Nass) are met by the polygon;
  50 are missed (proposed at the FWA mouth, low). 197 named streams enter tidal water that is not the sea.
- Words found: the synopsis p.21 box "Tidal Boundaries on Rivers in Region 2" (16 streams), DFO tidal-area pages
  (Area 29 list + Areas 2/12/13/17/23/27), DFO salmon notices and synopsis rows, curated tidal splits, tidal gauges.
  19 streams: polygon and located words agree (within 300 m). Disagree: Capilano, Nimpkish, Copper Creek; Tlell's
  two texts differ (1 km vs 1.5 km) and the polygon misses it.
- Review: curation review **Tidal** tab (`#tidal/`), listed + on the map with the polygon's edge; "accept proposal"
  pre-fills a mark (scope `tidal`, Place y + the cut as a `NewSplit` labelled "<water>: mouth (tidal boundary)",
  alias "tidal boundary" when above the FWA mouth). The mark is the only write; T185 / T166 turn accepted marks
  into splits.
- Reuse (owner 2026-10-27, decisions.md): a curated splits.json split within `REUSE_M` = 25 m of the tidal point on
  the same blk (along the line; ground distance for an unresolved splits.json point) is reused — `proposed.reuse`
  carries its id + distance, the proposal aliases it "tidal boundary", and "accept proposal" names it (`NewSplit`
  anchor `split` + split_id) instead of a new cut. 5 of 1,163 reuse: Campbell (cement block sign 1631 m vs 1625 m,
  6 m), Fraser CPR bridge at Mission (10 m), Coquitlam Mary Hill Bypass (10 m), Pitt CPR bridge (18 m), Tlell sign
  (0 m). Turning a `split`-anchored NewSplit into an alias (not a cut) is T185's job.

## Owner 2026-10-28: the tidal piece is removed (T98 folded in)
Accepted tidal splits do more than label the mouth: the piece BELOW each one is removed from the freshwater map (no
freshwater rules; Fraser below Mission included). Built as T166 step 5 (walk tributaries first, then drop the piece).
Tributary check against these candidates: `handoff/PLAN/curation-review/tidal/tributary-impact.md` — 29 rules reach a
tidal piece through their trib walk; only 2 trib sections lie wholly below a cut; the owner's belief holds if the walk
runs on the full network. Review the Kitimat candidate (7137 m) before accepting: kitimat_river.r1 lies wholly below it.

## Follow-up (2026-10-27): where the owner edited, and what is left for him
- Tool: `python -m pipeline.tools.audit.tidal_triage edits --dfo-gpkg <DFO_TIDAL_BOUNDARY.gpkg from R2> --gpkg
  <bc_fisheries_data.gpkg>`, then `... web <findings.json ...> --basemap <bc.pmtiles>`, then `... triage` (read only).
- The unedited original can't be rebuilt (R2 holds only the edited source layer; gisp.dfo-mpo.gc.ca no longer
  resolves; the BC WFS doesn't publish subareas). Edits are found by fingerprint: DFO vertices sit on a 1 cm
  grid, hand-added ones don't → `tidal-edits.json` (68 sites, 18 on candidate streams; blind on DFO's own
  off-grid stretch of south Vancouver Island / Gulf Islands; vertex-only deletions unseen).
- Web research (DFO pages via web.archive.org / waves-vagues guides, the PFMA regulations, forums, news) →
  `tidal-web.json` (23 findings on 15 streams), a source next to the book text in the Tidal tab.
- Triage per candidate (`triage` key; `tidal-triage.md`): missed 5 (Capilano, Copper Creek, Nimpkish, Tlell,
  Chemainus), sanity 0, owner-edited 25, book agrees 11, no claim 1122. The Tidal tab defaults to missed + sanity.

## Pass 2 (2026-10-28): DFO's own area / boundary descriptions
- Tool: `python -m pipeline.tools.audit.tidal_pass2 extract` then `... propose --basemap data/source/bc.pmtiles`
  (read only, ~2 min). Pages: pac.dfo-mpo.gc.ca does not resolve from here, so the crawl went through
  web.archive.org (latest 2025-26 snapshots), discovered from the site's own links, not guessed URLs: 188 pages
  cached raw under `data/source/dfo_tidal_areas/2026-10-28/` (+ `manifest.json` with each snapshot): 33 tidal
  Area pages (1-29, 121; 1xx/130/142 are folded into them), 9 freshwater salmon region pages, 37 shellfish
  contamination pages, 107 others; plus SOR/2007-77 (PFMA regulation) fetched directly from laws-lois.
  Not fetched: a-s128/129/130 (404 in the archive) and ~6 non-area pages (archive timeouts).
- `handoff/PLAN/curation-review/tidal/pass2/entries.json`: 1,715 entries (tidal area 704, PFMA subareas 602,
  freshwater 142, shellfish 63, other 204), each with source URL, area, text, parsed geometry (DD°MM.mmm′ →
  lon/lat, NAD27 converted; a "following the shoreline" stretch or a new clause breaks a line; parallels kept)
  and what it implies: `is` 9 (the words name the boundary), `at_or_above` 1,089 (a line in tidal water),
  `at_or_below` 139 (a freshwater limit), `context` 478. Named landmarks are resolved from a gazetteer built from
  the PFMA brackets (e.g. Klinaklini's Wahshihlas Bay–Rubble Point line).
- `pass2/proposals.json`: 109 streams the entries bear on. Against the T212 candidates: **agrees 15**
  (Nimpkish, Chapman, Nanaimo, Salmon, Qualicum, Skeena, Nass, Fraser at Mission, Illiance, Kitsault, Wilauks,
  Pallant, Kauwinch, Lausmann, Yorkson), **consistent 5** (a bound the cut satisfies: Somass, Sarita, Toquaht,
  Chambers, Kanaka), **moves 1** (Tlell), **new 4** (Fraser side channels Annacis / Cannery / Morey / Parsons:
  DFO lines in tidal water cross them; the point is a lower bound — they are tidal below Mission anyway),
  **context 84** (named, nothing places it: 47 unlocated, 35 lines seaward of the FWA mouth — e.g. Quatlena,
  Kwatna Bay, Dean Channel, South Bentinck — which only say the estuary is tidal, 2 unnamed-bridge guesses).
- **Conflict with an owner mark: 1 — Tlell** (not overridden). DFO's tidal Area 2 page says the sign is ~1 km
  above the Hwy 16 bridge (5250 m); DFO's freshwater region 6 page and the synopsis say 1.5 km (5750 m, the
  mark). Two DFO pages disagree.
- What the owner expected mostly does not hold: DFO's mouth closures ("shoreward of a line between two fishing
  boundary signs", lat/lon lines at Dean Channel / Kwatna / Quatlena / South Bentinck) are drawn across the
  inlet or bay SEAWARD of the FWA mouth — they confirm the mouth is tidal but never say where tidal water ends
  up the river. The text that does name a boundary is rare (9 entries). DFO's default is stated on
  definitions-eng: "Unless otherwise specified, the tidal boundary is a straight line drawn between the two most
  seaward points of land … on either side of the mouth", i.e. the FWA-mouth default is DFO's own rule.
- Review app: `pipeline/regs/review/tidal.py` merges `pass2/proposals.json` into the candidates as evidence
  only (web rows "DFO pass 2 · …", a switchable point, new rows for the 4 new streams; Tlell is put back in the
  "missed" review queue). `proposed` / `polygon` / `sources` are untouched, so no mark goes stale (tested).
  NOT live yet: it loads when the review backend restarts (no curation-review/ file was edited).

## Pass 2b (owner 2026-10-28): "DFO confirms the mouth"
- New `implies` class `confirms_mouth` (59 entries): a DFO tidal-water area hugging a NAMED stream's own mouth
  ("the waters of the mouth of Namu River shoreward of a line …", "closes Koeye River estuary", "Mouth of Little
  Qualicum River", "<X> River Estuary"), plus a lat/lon line the entry names the stream for that passes seaward of
  its FWA mouth. A stream only named as a landmark ("a sign near Trent River") does not count (`mouth_names`).
- Per stream (`proposals.json` `dfo_mouth`, tolerance `MOUTH_TOL_M` = 150 m): **upgrade 39** (the T212 cut is at
  the mouth and no source names an upstream boundary → confidence high, "DFO confirms the mouth"), **conflict 23**
  (listed, not moved: the T212 cut is upstream — e.g. Kingcome 5.5 km, Kitimat 7.1 km, Klinaklini 2.8 km, Wakeman
  3.0 km, Koeye 1.1 km, Quatlena 0.4 km — or a source names an upstream boundary: Fraser at Mission, Campbell cement
  block, Pitt CPR bridge), **new 1** (Whispering Creek: a mouth candidate). Owner-mark conflicts: Deena Creek (marked
  at 550 m, DFO confirms the mouth) in addition to Tlell.
- Tidal tab: "DFO confirms mouth" checkbox (the bucket, all triage classes) with chips on each row; "accept N at
  the mouth" bulk-accepts the unmarked upgrade + new rows behind a confirm dialog — one mark each (P y, the cut at
  the FWA mouth, or the proposal's own split when it reuses a curated one / sits at 0 m). Conflicts and marked
  rows are skipped; existing marks untouched. 38 acceptable now (Capilano and Qualicum are already marked).

### DFO default: the mouth (owner 2026-10-28)
- DFO definitions page (cached `data/source/dfo_tidal_areas/2026-10-28/fm-gp/rec/definitions-eng.html`): "Unless
  otherwise specified, the tidal boundary is a straight line drawn between the two most seaward points of land …
  on either side of the mouth". `pipeline/regs/review/tidal.py` `apply_default` (at load, additive: `proposed`,
  `polygon`, `sources`, `confidence` untouched, no mark goes stale): `specified` = a word source / web finding
  located on the line, or a pass-2 entry naming the boundary ("is"); unlocated words (Mamin: "~10 km upstream of the
  tidal boundary"), polygon, gauge, guess don't count. Nothing specifies → `dfo_default` **high 732**, or
  **conflict 409** (polygon top crossing >75 m above the mouth — 381, of which 100 ≤150 m and 176 ≤300 m; a DFO
  line in tidal water >150 m up; pass-2 upstream source). 27 specified.
- Tidal tab: "DFO default: mouth" bucket + chips; "Accept all N at the mouth (DFO default)" takes EVERY default-high
  row in the whole queue (filters ignored) that is unmarked or owner-un-accepted, behind a confirm dialog — one mark
  each at the FWA mouth. Group progress bars (words name it / DFO confirms mouth / DFO default / conflicts / unmarked).
- **Owner 2026-10-28 (later): the polygon is ignored for cuts** (decisions.md). `apply_default` no longer reads the
  polygon: a default row's `proposed` is the FWA mouth (the file's proposal kept as `proposed_t212`, which `prints`
  holds marks against — nothing went stale); polygon alternates are labelled "ignored for cuts"; Nitinat is
  owner-stated (`OWNER_STATED`, head of Nitinat Lake, 23,306.5 m). Counts now: **default high 1131, default
  conflict 9, words name it 28; conflicts (words only) 17**; bulk-acceptable 1123. The 5 polygon accepts (Mamin,
  Adam, Amos, Asseek, Amor De Cosmos) re-marked at the mouth by the owner's call.
- **Boundary words never fall to the default (owner 2026-10-28, Serpentine).** `boundary_words` sweeps every source
  (word sources, web landmarks, pass-2 "is", DFO triangular-sign rows, `tidal_words` landmarks); `NOT_NAMING` keeps
  Mamin / Deena / Dean / Marble on the default. 7 left the default, all located: Serpentine (0 m, BNSF trestle
  seaward of the mouth) and Squamish (14 m) — already resolved by `tidal_words` but missed; Illiance, Kitsault,
  Wilauks (DFO triangular signs near the mouth, medium); Ksi Gingolx (Gingolx Highway / Kincolith River Bridge = the
  only road crossing, 1567 m, medium); Oyster ("yellow triangle signs at the estuary" → the mouth, low). "Locate"
  group: 0. Counts: words name it 35, default high 1124 (bulk-acceptable 1117), default conflict 9, conflicts 17.
- DFO Tidal Waters Sport Fishing Guide 2015 cached (`data/source/dfo_tidal_areas/2026-10-28/waves-vagues/Library/
  359784.pdf`): its "Tidal Water Boundaries" table is 14 Region 2 rivers + Fraser–Pitt, Squamish, Skeena — every one
  already in `tidal_words.LANDMARKS`. It also says "the Mamquam blind channel [is] considered tidal" — Mamquam is
  not a T212 candidate (a Squamish tributary): open.
- **Tidal tab groups (owner 2026-10-28):** four groups, every candidate in exactly one — Mouth (DFO default) /
  Words name the boundary (located) / Words name it — not located yet / Conflict (words vs the mouth, each other, or
  an owner mark) — plus Done (owner-marked); a progress bar each. Confirms-mouth, pass-2 buckets, triage classes and
  default-conflict are folded in (the evidence stays in the row's detail). Before: mouth 1124 / words 34 / locate 0 /
  conflict 10, done 24 of 1168. Group 1 auto-accepted by the owner's call: one owner mark per unmarked mouth row at
  the FWA mouth, note "owner 2026-10-28: auto-accepted — DFO default, no boundary words" (1110 recorded; existing marks
  untouched). 7 refused by the snap — the FWA mouth is >500 m from the atlas item's line or the stream is not in the
  registry: Skagit (1379 m), Campus (757), Caycuse (716), Dempster (524), Doobah (606), Hobiton (628), Nelson Creek
  (no registry item) — open: place by hand.
