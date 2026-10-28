# T224 — Name variants from local sources (municipal, ECCC, DFO, synopsis, OSM, BC GNIS), each with its real source

Status: IN PROGRESS — 2026-10-28: owner answers applied (decisions.md "T224"). Built: every name carries its real
source end to end (NameTuple label/ref → registry `name_sources` → bundle/core `name_source` → app search "also X ·
source" and a "Name from …" line under a non-official shown name); 183 accepted names in
`data/curated/waters/mined_names.json` (181 auto + 08MH079's two); review app **Names** tab (marks of scope `name`);
existing-variant fixes. NEXT: the next atlas build (registry + tiles names) + bundle/release; the owner's spot-check
and review marks → rerun `python -m pipeline.tools.mined_names` → next build.

Source: owner 2026-10-28: 08MH079's stream is FWA-unnamed but Abbotsford's municipal layer calls it "KILGARD CREEK", and
the app has no such variant — "we could find many more". Add-ons the same day: every variant carries its REAL source and
evidence wherever provenance shows, never a borrowed label (a municipal name is never "official"); give existing
sources human-readable labels and fix mislabelled ones.

## Today (how a variant reaches the app)
`data/curated/waters/name_variants.json` (3,695 entries, each name with a `source` code) + the added-streams
`name_variants` (`data/curated/waters/added_streams.json`, municipal names, all applied as source `alias`) →
`pipeline.atlas.graph.names.apply_name_variants` (name tuples with a source) → registry `variants` (BARE STRINGS: the
source is dropped here) → bundle `alias(item_id, alias, alias_key)` (no source column) → app search ranks an alias a
step behind the name and shows "also X". A water with no gazetted name DISPLAYS its highest-priority variant, so an
alias on an FWA-unnamed line becomes its name with no hint where it came from.

## Work
1. DONE (Claude): miner + candidates — `mine.py`, `fwa_index.py`, `osm_extract.py`; `candidates.json` (+ `tables.py` prints the tables); findings below.
2. ★ Owner: flow (search alias only vs display name) and the auto-accept rule (below, "Owner calls").
3. Carry provenance end to end: registry variants become `{name, source, source_ref, official, evidence}`; bundle
   `alias` gains `source` + `official` (schema + contract + `release` test), app search row says "also X · City of
   Abbotsford stream layer". Bundle re-cut, no promote without the owner.
4. Fix the mislabelled existing variants (below, "Existing variants"): Burnaby names stored as `alias`, the FWA
   gazetted "South Alouette River" stored as `alias`, 24 added-streams municipal variants whose city is not recorded.
5. Apply accepted candidates as name_variants entries (source per candidate), side atlas build, binding diff (a new
   name can make a rule bind), reach + deliver.

## Findings (2026-10-28; numbers from candidates.json)
Matching:
- LINES (municipal, OSM): matched to FWA within 25 m.
  - `blk_overlap` = the share of the FWA line along the source.
  - `source_overlap` = the share of the source along the FWA line.
- POINTS (ECCC stations, cached BC GNIS mouths): the station's bound or matched blk/wbk, else the nearest line within 150–200 m.
- RECORDS (DFO, synopsis): the name vs the item the record matches.
- Dropped: names already present (ignoring case or punctuation), and 54 rows on FWA lines the added-streams batch
  removed.

By source (high / medium / low):

| source | high | medium | low |
|---|---|---|---|
| ECCC station names | 4 | 14 | 197 |
| City of Abbotsford stream layer | 3 | 32 | 105 |
| Burnaby | 0 | 1 | 0 |
| Port Moody | 0 | 0 | 3 |
| Squamish | 0 | 0 | 4 |
| OSM (basemap, OSM 2026-03-05) | 206 | 604 | 7,832 |
| DFO salmon regulations | 0 | 11 | 4 |
| Synopsis | 0 | 5 | 0 |
| BC GNIS (cached WFS, ~440 features) | 0 | 3 | 2 |

Merged: 8,832 rows; 218 high (197 on FWA-unnamed lines), 553 medium, 8,061 low; 39 corroborated by two sources.

Confidence levels:
- High:
  - lines: both overlaps ≥ 80 %;
  - ECCC: a bound or reviewed station on an unnamed line;
  - any target: two sources agree.
  - A name with no water word is capped at medium.
- Low: mostly OSM or municipal names labelling a whole network by its main creek (flag `tributary_of_same_name`).

Notable rows:
- 08MH079 / blk 356314888 has two rival names: Kilgard Creek (Abbotsford, 69 % / 85 %, medium) and Lonzo Creek (North
  Fork) (the ECCC station, bound, high).
- Non-OSM highs: Gill Brook, Marshall, Shamrock, Smith Brook, Mckay, Jantz Stream, Corinne Brook (Abbotsford);
  Inkaneep Creek, Meadow Creek Diversion, Quinsam River Diversion Headpond, Noble Creek (ECCC).
- English names FWA gazettes under an Indigenous name: Chilanko River = Tsalhanqox, Pine Creek = Koosawu.áa Héen,
  Deserted River = ts'unay stulu.
- Likely an OSM error: Kicking Horse → "Otterhead River".

How a variant reaches the app: see "Today" above. No surface shows a variant's source. On an FWA-unnamed line any
alias becomes the DISPLAYED name: 11 municipal aliases do this now, e.g. "Tillicum Slough" on blk 356346715.

### Owner calls
1. Flow: (a) search alias only ("also X · <source>"), or (b) also the display name of an FWA-unnamed water. Today's
   code does (b) silently. Default: (a), with display only by a per-name yes.
2. Auto-accept rule: high, no rival name, not a tributary label, relation `unnamed`, and either ≥ 80 % line overlap or
   an ECCC station. 181 rows qualify (≈160 OSM-only). Proposed: as search alias only, never `different`.
   Default: no auto-accept.
3. 08MH079: Kilgard Creek and/or Lonzo Creek (North Fork).

### Variant record + labels (proposed)
Record: `{name, source, source_label, source_ref, official, evidence}`. `official` is true only for BC Geographical
Names.

Labels:

| code | label |
|---|---|
| gazette | BC Geographical Names (official, via FWA) |
| side-channel | Main channel's official name (inherited) |
| override | Project curation (owner decision) |
| gauge | ECCC hydrometric station name |
| regulation / synopsis | Freshwater Fishing Synopsis |
| stocking | Freshwater Fisheries Society stocking records |
| bathymetry / marker | BC lake bathymetry map (label) |
| vessel_restriction | Vessel Operation Restriction Regulations (SOR/2008-120) |
| lake_survey | BC lake survey table |
| dfo | DFO salmon regulations |
| alias | Alternate name (source not recorded); to be eliminated |
| municipal:\<city\> | City of Abbotsford stream layer, City of Burnaby waterways layer, City of Port Moody ESA streams layer, District of Squamish watercourses layer |
| osm | OpenStreetMap |

No display surface carries a source today, so there was nothing display-only to relabel. No bundle field changed,
so no re-cut was made. The labels need step 3.

### Existing variants: mislabelled or provenance lost (`existing_variants_audit` in candidates.json)
- "South Alouette River" (wbk 329292631): the FWA gazetted name, stored as `alias`. It should be gazette/official.
- 8 Burnaby names stored as `alias`, city lost. 7 of them (Third Beach, Second Beach, Ramsay, Eagle, Cranberry,
  Beecher, Chub) target FWA blks the added streams REMOVED, so those entries are dead; the names live on the minted
  added streams.
- added_streams.json: all 367 municipal variants are applied as `alias`.
  - For the 343 `added` names, the city can be recovered from the stream.
  - For the 21 `duplicate` and 3 `excluded_fwa` names, the city is not recorded.
  - Abbotsford was never run through this batch.
- `override` names with no recorded origin: Seven Mile Reservoir, Waneta Reservoir, Beaver Tree Lake, Lightning Lake.
  "Whiteswan Lake outlet stream" is a description shown as a name.
- Nothing non-gazetted is labelled gazette/official: `gazette` comes only from FWA GNIS.

## Acceptance
No variant in the bundle without a source; no non-gazetted name marked official; every accepted candidate traceable
to its source row; binding diff reviewed before promote.

Depends: owner calls (2). Size: M. Owner: yes (2). Default: search alias only, nothing auto-accepted.
