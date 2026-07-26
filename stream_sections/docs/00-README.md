# Stream Section Redesign — Working Folder

**Status:** DESIGN complete, clean-slate. No implementation yet.
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

Merge FWA into **per-BLK chains**, contract them into a small **topology graph** (confluences
+ collapsed lake nodes), and cut them into **sections** — the matching/display/tile atom,
identified by `(name tuples, location_identifier, lake_wbk)` and a stable `section_id`.
Sections carry **new cut geometry** (route-measure `substring`), `(name, source)` name tuples,
precomputed **tributary reachability**, and a **zone → reg_set map**. Tiles become
**self-identifying by `section_id`**, which deletes the fid highlight lists, the `/api/resolve`
chain, and the mobile fid/poly tables — shrinking the boot payload to a sub-1 MB bootstrap.
Matching logic is unchanged; it just targets sections.

## Read in order

| File | Purpose |
|------|---------|
| `00-README.md` | This index + status |
| `01-current-pipeline-map.md` | **Legacy reference** — what exists today (to port logic/data from). We are NOT preserving its structure; see `08` for what actually must carry over. |
| `02-domain-model.md` | Verified FWA terms (BLK/WSC/GNIS/fid/wbk/route-measure), `(name,source)` tuples, the two granularities, section identity |
| `03-graph-design.md` | **The real graph design — start implementation here.** Merge→topology→sections, lake collapse, barriers, geometry re-cut, tributary reachability |
| `04-section-split-design.md` | General hand-split boundary system (any anchor → route measure → cut; auto `location_identifier`) |
| `05-pipeline-architecture.md` | Clean-slate step DAG, partial reruns, build-in-v2-then-cutover |
| `06-storage-and-client.md` | `section_id` self-identifying tiles, sub-1 MB bootstrap, lazy reg chunks, mobile rebuilt |
| `07-zone-regulations.md` | **MU overlay** for base regs + **curated `mu_boundary` splits** for Fraser-type (Fraser vs Similkameen) |
| `08-matching-and-invariants.md` | Matching pinned onto sections; the hard invariants that survive the rebuild |
| `09-data-structures.md` | Schema rationale + serialization + split-indexing. **Authoritative schema is `pipeline/sections/models.py`.** |
| `10-testing-plan.md` | Test-as-you-build tiers, the gating spikes (Chehalis/Kootenay), golden parity |
| `11-implementation-plan.md` | De-risk-first build order + concrete first steps |

## Implemented so far (branch `redesign/stream-sections`)

All code lives in top-level `stream_sections/` (outside `pipeline/`). Run with `.venv/bin/python`.

**Graph construction — DONE and validated:**
- `blk_chains.py` — load FWA fids + merge into per-BLK chains (route spans, under-lake runs).
- `names.py` — `(name, source)` tuples: gazette + side-channel (shared-WSC main channel) +
  manual overrides (`feature_display_names.json`). upstream-inherited = TODO.
- `topology.py` — contracted directed graph: lake-node collapse, degree-2 contraction,
  split at confluences + edge_type transitions (preserves the 2300 barrier granularity).
- `cutting.py` — endpoint ids, geometry stitch, `substring` cut, `section_id`.
- `build.py` — CLI: `--gnis`/`--bbox`/`--full`; writes chains+topology pickles, segments/
  nodes GeoJSON (WGS84, for QGIS/geojson.io), and a **self-validating coverage check**.
- Validated on Adams River: 100% open-fid coverage (0 missing/dup/extra), Adams = 1 BLK /
  361 segments, name tuples correct. 14 unit tests pass (`tests/`).

**Manual splits — schema + example DONE:** `splits.schema.md` + `splits.example.json`
(primary form: a single coord + `blk` (one cut) or `wsc` (cut main + all side channels));
`splits.load_split_defs` parses/validates. Anchor *resolution* is the next (sections) step.

**Next:** anchor resolution + `sectionizer.py` (cut at lakes/splits) + `tributaries.py`
(upstream walk with WSC filter + 2300 barrier) + the two real-data regression tests.

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
tributary walk applies them). Paste exact leak numbers into `test_topology.py` when building.

## Open decisions surfaced for the group

- `07` Q1–Q3: `mu_boundary` split label wording, overlay eager-vs-lazy, confirm all base regs
  reduce to `mu_id → reg_set`.
- `06`: bootstrap-Fuse vs server-side search to start.
- `02`: pick the `section_id` scheme (readable `blk:start_measure` vs sha1) and freeze it.

## Ground rules

- Nothing implemented yet — design only.
- After any real code change, run `graphify update .` (project CLAUDE.md).
- The three de-risking spikes in `03` gate deletion of legacy logic — run them first.
