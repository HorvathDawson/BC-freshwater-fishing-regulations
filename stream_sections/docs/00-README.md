# Stream Section Redesign — Working Folder

**Status:** Graph + naming + splits + border + tributary/EXCEPT chain BUILT & tested (53 pass /
11 skip; see `14`). Next step is the **match** stage. Runs on real data via `stream_sections/build.py`.
**Branch (intended):** `redesign/stream-sections` (create with
`git checkout -b redesign/stream-sections` — these untracked files follow onto it).
**Approach:** rebuild from scratch into `output/pipeline/v2/`, validate in parallel, then cut
over. **Not** an incremental patch of the old pipeline — old flow keeps running until v2 is
proven.

## The problem

Today's pipeline builds a **2.37 GB igraph of every FWA micro-segment**, runs a per-regulation
BFS over it, keys everything by micro `linear_feature_id`, and dynamically groups reaches by
`(watershed_code, name, reg_set)`. Hand river-splits are faked with duplicate override rows.
The client eagerly loads a **24.9 MB `tier0.json`** (two-thirds of which is fid highlight
lists) and resolves clicks through a `fid → reach` shard chain.

## The redesign in one paragraph

Merge FWA into **per-BLK chains**, then build **one inverted graph** — a **node is a stream**
(a BLK, subdivided into **sections** only at lakes + curated splits), an **edge is "flows
into"**. A mainstem is one node with many incoming tributary edges (no per-confluence
segmentation; fids never enter the graph). Sections are the matching/display/tile atom,
identified by `(name tuples, location_identifier, lake_wbk)` and a stable `section_id`, carry
**new cut geometry** (route-measure `substring`), `(name, source)` name tuples, and
**tributary reachability = ancestors** in the graph. Tiles become
**self-identifying by `section_id`**, which deletes the fid highlight lists, the `/api/resolve`
chain, and the mobile fid/poly tables — shrinking the boot payload to a sub-1 MB bootstrap.
Matching logic is unchanged; it just targets sections.

## Read in order

Docs are ordered by the **dataflow** (domain → graph → splits → names → regs → pipeline/storage →
matching → testing), then reference material, then the forward plan (`16`).

| File | Purpose |
|------|---------|
| `00-README.md` | This index + status |
| `01-domain-model.md` | Verified FWA terms (BLK/WSC/GNIS/fid/wbk/route-measure), `(name,source)` tuples, the single inverted graph, section identity |
| `02-current-pipeline-map.md` | **Legacy reference** — what exists today (to port logic/data from). We are NOT preserving its structure; see `10` for what actually must carry over. |
| `03-graph-design.md` | **The graph design — start here.** Merge→names→split→inverted graph (nodes=streams/lakes, edges=flows-into)→ancestor tributary walk *(implemented)* |
| `04-section-split-design.md` | Hand-split boundary system: any anchor → cut → auto `location_identifier`; incl. `area_boundary` (park closures) *(implemented)* |
| `05-name-variations.md` | Unified name-variations file (compiler + graph apply); `display` flag; wetland overlay *(implemented)* |
| `06-zone-regulations.md` | **MU overlay** for base regs + **curated `mu_boundary` splits** for Fraser-type differences *(planned)* |
| `07-pipeline-architecture.md` | Step DAG, partial reruns, build-in-v2-then-cutover |
| `08-data-structures.md` | Schema rationale + serialization + split-indexing. **Authoritative schema is `stream_sections/models.py`.** |
| `09-storage-and-client.md` | `section_id` self-identifying tiles, sub-1 MB bootstrap, lazy reg chunks *(planned)* |
| `10-matching-and-invariants.md` | Matching pinned onto sections; the hard invariants that survive the rebuild *(planned)* |
| `11-implementation-plan.md` | De-risk-first build order + concrete first steps |
| `12-testing.md` | **Testing (everything up to matching)** — test-as-you-build tiers, the visual anchor/section/tributary catalogue, and the status dashboard |
| `13-corrections-memo.md` | Regulation weirdness & corrections catalog (480 overrides classified) (+ `13-corrections-classified.json`) |
| `14-waterbody-split-curation.md` | **Curation source of truth** — the grouped, source-first split-curation model + workflow; canonical data is `waterbody-splits.json` (grouped by reg entry, self-sufficient). The old flat `14-locators-to-curate.{json,md}` is archived in `archive/` (superseded, round-trip-lossless). |
| `15-lake-splitting-design.md` | **Future option** — how lake subdivision would work (NOT built) |
| `16-phase5-match-plan.md` | **Forward plan** — Phase 5 (match): TDD review of the 4 selectors + blockers; the Garibaldi area-closure is shipped, matching is what's left |
| `17-manual-review-runbook.md` | Per-row writing rules + gotchas for resolving a locator (how to curate one row) |

## Implemented so far (branch `redesign/stream-sections`)

All code lives in top-level `stream_sections/` (outside `pipeline/`). Run with `.venv/bin/python`.

**Graph construction — DONE and validated:**
- `blk_chains.py` — load FWA fids + merge into per-BLK chains (route spans, under-lake runs,
  distinct `edge_types` so `2300` survives the merge).
- `names.py` — `(name, source)` tuples: gazette + side-channel (shared-WSC main channel) +
  manual overrides (`feature_display_names.json`). upstream-inherited = TODO.
- `graph.py` — **the single inverted graph** with **lakes as nodes**: a `StreamNode` is a
  stream **piece** (a BLK cut at its lake-runs) or a **lake** (one per `wbk`); `FlowEdge` "flows
  into" (`confluence`/`lake_in`/`lake_out`). Guards baked in at build: WSC-descendant filter on
  stream→stream edges + `EDGE_TYPE=2300` `is_barrier`; `ancestors(guarded=True)` = tributary
  closure. `build_section_geometries` writes a `node_id → geom` sidecar (graph is geometry-free).
- `cutting.py` — endpoint ids, geometry stitch, `substring` cut, node/section id.
- `build.py` — CLI: `--gnis`/`--bbox`/`--full` (+ `--splits`, `--tributaries-of`, `--lakes`);
  writes chains/graph/geometries pickles, `graph.gpkg`, and an integrity self-check.
- Validated on Adams extent: **10821 nodes (9254 pieces + 1567 lakes) / 10929 edges**, integrity
  OK (0 fids uncovered); Adams Lake outlet = Lower Adams, inlet = Upper Adams (the up/down-of-lake
  split falls out for free). 23 tests pass incl. both real-data regressions.

**Curated splits — DONE + grounded:** `splits.schema.md` (authoritative) + a real `splits.json`
verified against data (Adams Lake; Kootenay Idaho-border + Koocanusa; Hunlen Falls; Young Cr. Hwy 20;
Burnt Bridge ↑ Sitkatapa; Fraser 2‑18/3‑14). Anchors (`anchors.py`): `point`, `line`, `lake`,
`mu_boundary` (→ **one** split, median + `concern` if a weaving river crosses N×), `confluence`
(prefer `tributary_wsc` — **self-validates** the WSC-descendant relationship). `sectionizer.py`
applies them as graph nodes with **proximity pickup** (a curated point reuses a nearby lake/border
boundary — how Kootenay's border reaches resolve) and an optional `_concern` surfaced everywhere.
**Border (`border.py`):** cross-border BLKs are split at the BC outline (WMU union) and the outside
pieces flagged `out_of_bc` (geometry kept, dotted, not a barrier). **EXCEPT algebra
(`tributaries.py`):** "[Includes Tributaries] EXCEPT …" = `reach_except` set difference over the
split pieces (Atnarko/Bella Coola).

**Debug/report tools (temporary):**
- `export_gpkg.py` — `build.py` writes `graph.gpkg` for QGIS: `streams` (final section pieces:
  name tuples, `full_name`, `location_identifier`, `is_barrier`, `out_of_bc`, tributary count),
  `lakes`, `confluences`, `graph_nodes`/`graph_edges` (topology schematic), `anchors`,
  `split_points` (every resolved cut + `picked_up`/`concern`), optional `obstacles` (FISS
  fish-passage points), `tributaries`, `lake_io`.
- `oneoff/` — one-off bootstrap scripts (not part of the build): `name_variants_compile.py`
  (→ `name_variants.json`, docs/05; includes a `_MANUAL` grounded list) and `complex_regs_report.py`.

**Data layers:** `data/fetch_data.py` + `FWADataAccessor` now include `obstacles`
(`WHSE_FISH.FISS_OBSTACLES_PNT_SP`) — falls/dams as points, with `NEW_WATERSHED_CODE` renamed to
`WATERSHED_CODE_50K` so obstacles join to streams; the point source for falls-anchored splits and
future client display (replacing OSM `waterfalls`).

**Next:** the **match** step (regs → sections by name + `location_identifier`; MU overlay;
tributary/EXCEPT expansion). Everything upstream of match is built + tested (see `14`).

## Key verified facts driving the design (see `02` for evidence)

- BLK = single flow path; **BLK→WSC strictly 1:1**; side channels = separate BLKs sharing a
  WSC. → merge on **BLK**.
- FWA carries **route measures** (2D length matches to ~1 cm) → cut section geometry
  anywhere, mid-segment.
- Under-lake = `WATERBODY_KEY ∈ lakes∪manmade` (not edge_type). Lake → **one collapsed node**.
- Network flows downstream, near-DAG → **directionality removes most backtracking logic**
  (verify on Adams/Fraser before deleting the old excluded-WSC machinery).
- Client: **~16 MB of tier0 is fid highlight lists** that vanish with `section_id` tiles.

## Gating spikes — DONE (see `10`)

Both hard cases were tested against real data and **reversed two assumptions** — a win for
de-risking:
- **S1 braiding** (Chehalis/Harrison): SCC condensation is a **no-op** (FWA is already a DAG).
  The **WSC-descendant filter** stops the leak → **keep excluded-WSC logic.**
- **S2 lake barrier** (Kootenay/Columbia): lake-collapse does **not** sever the `2300` canal
  (it bypasses the lake polygon) → **keep the `EDGE_TYPE=2300` barrier.**
- **S3/S4**: BLK is a safe section atom (0.03% cross-BLK), lake-cut bounded (84.7% single
  section); handle the 0.69% all-2-point BLKs with interpolation.

Net: the section model is de-risked, but the WSC filter and 2300 barrier are **kept** (the
tributary walk applies them). Paste exact leak numbers into `test_graph.py` when building.

## Open decisions surfaced for the group

- `07` Q1–Q3: `mu_boundary` split label wording, overlay eager-vs-lazy, confirm all base regs
  reduce to `mu_id → reg_set`.
- `06`: bootstrap-Fuse vs server-side search to start.
- `02`: pick the `section_id` scheme (readable `blk:start_measure` vs sha1) and freeze it.

## Ground rules

- Graph → naming → border → curated splits → tributary/EXCEPT is implemented + tested; **match**
  is the next unbuilt stage (see `14` for the live status).
- After any real code change, run `graphify update .` (project CLAUDE.md).
- The three de-risking spikes in `03` gate deletion of legacy logic — run them first.
