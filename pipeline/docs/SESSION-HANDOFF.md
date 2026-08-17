# Session handoff — 2026-08-17

Everything below is **uncommitted** (working tree). Province registry at `output/v2/full/registry.json`.
Full test suite: **152 passed, 8 skipped**.

> Prior handoffs (the 2026-08-15 override deep-dive, profiling, border cache, area work) are archived at
> the bottom under "Earlier handoff (2026-08-15)". This top section supersedes their coverage numbers.

## 0. Where coverage stands now

`PYTHONPATH="$PWD" .venv/bin/python -m pipeline.matching.coverage --registry output/v2/full/registry.json`

```
registry           19,335 items  (stream 11,645 / lake 7,640 / wetland 46 / area 4)
rows                1,393
matched               890   (name+MU)
override              443
feature_pin            13   -> reg→feature resolver (deferred, not a miss)
ambiguous               1
unmatched               5
skip                   41
HIT %               95.7%   (matched + override)
true misses             6
```

The 6 true misses are structural, not name gaps:
- `MACKENZIE CREEK` (4-21) — 4 distinct MacKenzie Creeks; two share region 4 with no MU split → needs an
  **override pin**, not a variant (a variant can't disambiguate identical names in one MU).
- `WHITESWAN/WHITETAIL LAKE'S INLET & OUTLET STREAMS`, `SKEENA/KISPIOX CONFLUENCE`, `MARSH POND`,
  `"BLUEY LAKE POTHOLES"` — multi-feature / ungazetted rows → the **resolver**, not the row matcher.

## 1. What this session changed (all in `pipeline/name_variants.json` + a little code)

### 1a. Contamination cleanup (lake name collisions)
The bathymetry/stocking/marker survey feeds had cross-attributed neighbour-lake names (a survey sheet
covering two lakes stapled both names to one wbk), which made one lake answer to another's name **in the
same MU** → ambiguous matches. Removed the harmful tuples only (cross-MU aliases like FOUNTAIN/CLIFF kept):
- **3 pairs**: One↔Two, Murray↔Cheslatta, Rick↔Johnny.
- **7 more (user-confirmed)**: Truda, Knouff (×2 sources), Darke, Drum, Turner, "Unnamed near Vanderhoof",
  Bear Creek. The other same-MU cases were left ("the rest are real" — chains / legit alt names).

### 1b. Side-channel name handling (`pipeline/graph/names.py`, `pipeline/registry/build.py`)
- **Direction gate** (names.py): a side channel only inherits the mainstem name when the mainstem is
  genuinely bigger (`_mag(main) > _mag(c)`). Stops the Stave from grabbing "Blind Slough".
- **Foreign-name exclusion** (build.py `build_registry`): a borrowed side-channel name (its gnis is not
  the item's own) is NOT a searchable variant → "Stave River" resolves to the Stave only, not Blind Slough.
- **Nameless fallback** (build.py): a channel with *no name of its own* keeps the inherited mainstem name
  as its search key (else it drops out entirely). Tests: `test_names.py`, `test_registry.py`.
- Net effect: Stave / Pitt / Widgeon / Cheslatta / Johnny / Two — all previously ambiguous — now resolve
  to a single item each. Confirmed **0 shared-gazette-label cases** province-wide (the fix is complete).

### 1c. Two new name feeds (dry-run → review doc → apply, with dedupe)
Review doc: `pipeline/docs/name_variants_more_proposed.md`. Both matched by **point-in-polygon / wbk**,
dropping names equal to the polygon's GNIS name, "Unnamed lake" placeholders, and descriptions.
- **A — `pipeline/name_variants_more.csv`** (Gazetteer, Vessel Operation Restriction Regs SOR-2008-120):
  **8** additions, `source="vessel_restriction"` (Sheep, Garbutts, Squaw, Hidden, Stink, Fisher, Muskrat,
  Provost Dam). Peckhams skipped (already GNIS_NAME_2). 12 river rows unmatched (need stream matching or
  skip); 1 typo coord (Jim Smith, lat==lon).
- **B — `ettt.csv`** (BC lake surveys), `source="lake_survey"`: **29** added after collision-pruning —
  ASCII-for-accented (Brûlé→Brule, François→Francois) and English names for renamed lakes
  (Chilko→Tŝilhqox Biny, Tatlayoko, Stum, Anah, Alexis, Chilcotin, Eagle, Puntzi — each backed by a
  `(formerly)` GNIS_NAME_2). Kept suspects Surprise + Wolfe.
- **New `NameSource` members** (`pipeline/models/enums.py`): `vessel_restriction`, `lake_survey`
  (low priority, searchable, never beat gazette — provenance recorded instead of degrading to `alias`).

### 1d. Collision guard on the additions
A post-apply scan (checking every added name against **all** GNIS names 1/2/3, not just primary) caught
**4** additions that would collide in a shared MU and removed them: North Cameron, Dace, Nisga'a Lakes,
Puntzi — in each case the real lake already holds the name. ⚠️ **Puntzi is genuinely ambiguous in the
source** (wbk:329595984 "Bendziny" was *formerly* Puntzi; wbk:329595987 "Punti/Puntzi") — flagged, not
resolved.

Backups of every name_variants edit are in the session scratchpad (`name_variants.*.json`).

## 2. How to run the parser (it is READY once this rebuild lands)

Flow (`pipeline/parsing/run_parse.sh`): **export batches → dispatch to Claude CLI → validate → ingest**.
```
REGISTRY=output/v2/full/registry.json bash pipeline/parsing/run_parse.sh
# knobs: BATCH_SIZE=40  MODEL=opus  CONCURRENCY=3  CLAUDE_BIN=claude
```
Prereqs (all satisfied): registry.json ✓, `pipeline/matching/overrides.json` (480) ✓, synopsis rows
(1,393) ✓, `parse_context` + PARSE/REVIEW prompts ✓, `bc_species.csv` ✓, `claude` CLI v2.1.233 ✓,
`pipeline/parsing/entries/` empty (fresh full parse).

The **matcher is the gate**: `batch_exporter` matches each row, batches the ~1,330 HITs, and **excludes**
the ~60 unmatched/ambiguous/feature_pin rows (hand-curate later — never guessed). Output is `EntryFiles`
in `pipeline/parsing/entries/`.

## 3. The real remaining gap: the RESOLVER (downstream of parsing)

The parser emits `EntryFile`s (species/dates/extent per registry item). The **resolver**
(`Entry × registry × sections → SectionRegs + CoverageReport`) that turns those into actual reach/section
assignments is **not built** — it is the biggest remaining piece. `feature_pin` rows (13) and the
multi-feature misses above all wait on it. Also open: lazy area-catalog wiring into `build.py`
(`pipeline/splits/area_catalog.py` exists but isn't called; membership layer defs not added).

## 4. Commands
```
# full production rebuild (~14 min; bakes name_variants + splits):
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.build --full --out output/v2/full --splits pipeline/splits.json
# coverage (no build):
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.matching.coverage --registry output/v2/full/registry.json
# tests:
PYTHONPATH="$PWD" .venv/bin/python -m pytest pipeline/tests/ -q
# refresh the code graph after edits:
graphify update .
```

## 5. Prioritized next steps
1. **Build the resolver** (§3) — the biggest gap; unblocks feature_pin + multi-feature rows.
2. **Run the parser** (§2) on the fresh registry → review EntryFiles.
3. `MACKENZIE CREEK` (4-21) + any other same-MU identical-name rows → override pins.
4. Decide the 12 river rows in `name_variants_more.csv` (stream-layer match vs skip) + the Jim Smith coord.
5. Wire the lazy area catalog into `build.py`.

---

# Earlier handoff (2026-08-15) — superseded coverage, still-valid architecture notes

> Kept for the override deep-dive, profiling results, border cache, and area design. **Coverage numbers
> here are stale** (that run predated the side-channel fix, the registry lake/wetland additions, and the
> archive-verbatim override revert; HIT was 81.3% then, 95.6% now).

## Override deep-dive (2026-08-15)
Ran the matcher WITH vs WITHOUT overrides over the 428 override-hit rows (`overrides_audit.md`): most
overrides are redundant with name+MU matching (the archive migration baked their name variants into the
registry). Only ~18 live overrides steer to a different item than plain matching. Regenerate anytime:
`... coverage --registry <reg> --tables` → `pipeline/docs/coverage/`.

## Profiling (2026-08-16)
`PIPELINE_PROFILE=1 --full`. Two slow stages were O(items × all-nodes) scans:
`apply_name_variants` (indexed by target id → each entry touches only its nodes) and `split_graph_at`
(index nodes by blk, edges by to_node). `pipeline/utils/profiling.py` is wired in; set `PIPELINE_PROFILE=1`.

## Border (cached BC boundary)
Border splits matter (Kootenay/Columbia leave and re-enter BC — regs stop at the line). The union of
full-res WMU polygons was the bottleneck; the province never changes, so the outline is precomputed once
to `data/bc_boundary.geojson` (`pipeline/splits/bc_boundary.py`). `--no-border` is a dev shortcut only;
production is `--full`. Regenerate only if the WMU layer changes:
`PYTHONPATH="$PWD" .venv/bin/python -m pipeline.splits.bc_boundary --gpkg data/bc_fisheries_data.gpkg`

## Area work (designed, partially built)
`cut: true|false` in `areas.json` is the sole cut trigger; cut areas stay eager, everything else is
membership-only. `pipeline/splits/area_catalog.py` (lazy catalog) built but **not wired into build.py**;
membership layer defs (parks_bc, wma, land_access, named watersheds, historic) not yet added.
`Extent.feature_types` (stream/lake/wetland) added + tested.
