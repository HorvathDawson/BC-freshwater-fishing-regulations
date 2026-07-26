# 10 — Testing Plan (build so you can test as you go)

Goal: never be confused about *why* something happened. Every step is independently testable
against a small, inspectable input before it touches the 4.9 M-feature dataset.

## Principle: three tiers per step

1. **Synthetic unit tests** — build a tiny hand-made graph/chain in code (as
   `test_tributary_logic.py` does today) and assert exact behavior. Fast, deterministic, no
   data dependency. This is where most logic is pinned.
2. **Small real-extract tests** — pull one or a few named watersheds from the gpkg (Adams,
   Fraser@Seabird, Chehalis/Harrison, Kootenay/Columbia, Similkameen, Wigwam) and assert
   real-world outcomes. These are the "known hard cases."
3. **Full-build validation** — counts, coverage, and golden parity against the legacy
   pipeline on the whole dataset, run before cutover.

Each `pipeline/sections/` module has a matching `pipeline/tests/sections/test_<module>.py`
(scaffolded, `@pytest.mark.skip`). **Remove the skip as you implement each module** — the
suite grows with the code, so a regression always points at the step you just changed.

## The gating spikes (run FIRST, before deleting legacy logic)

These decide whether the biggest simplifications are valid. Both guards now run **at
graph-build time** (WSC-descendant edge filter + `EDGE_TYPE=2300` `is_barrier` node), and both
regressions are **un-skipped**: `test_graph.py::test_chehalis_harrison_no_leak_via_wsc_filter`
and `::test_kootenay_columbia_no_leak_via_2300_barrier` (they build a small real extract and
skip only when the gpkg is absent). Empirical note: the WSC filter alone severs *both* named
cases (the Columbia/Kootenay canal drains into a different WSC branch); the 2300 barrier is
kept as independent defense for same-watershed canals, and a synthetic test pins it in
isolation.

| Spike | Question | Result | Finding |
|-------|----------|--------|---------|
| **S1 Braiding** | Can SCC condensation replace the watershed-code exclusion for the Chehalis/Harrison leak? | ❌ **NO** | FWA is already a DAG (0 non-trivial SCCs) → condensation is a **no-op**. Naive walk leaked 24/43 Harrison segs; the leak is confluence topology (mouth node's predecessor is the Harrison mainstem). **WSC-descendant filter** fixes it (145/145 Chehalis, 0 Harrison). **Keep excluded-WSC logic.** |
| **S2 Lake barrier** | Can lake-collapse replace the edge_type-2300 rule for Kootenay/Columbia? | ❌ **NO** | The canal **bypasses** Columbia Lake's polygon; the 6 `2300` segs (blk 356366076) sit under other waterbodies → lake-collapse catches 0/6, stops leak only by accident. Blocking `2300` segs → leak 86→0. **Keep the 2300 barrier.** |
| **S3 Geometry cut** | Is measure-`substring` safe? | ✅ mostly | 99.995% segment contiguity; measure-cut safe for ~99.3% of multi-segment BLKs. **0.69% are entirely 2-point** → need interpolation fallback. |
| **S4 Structural** | Is BLK a safe section atom? | ✅ **YES** | GNIS spanning >1 BLK = 0.03% (4/11,604). Lake-cut: 84.7% single section, 96.6% ≤3, max 180. |

**Outcome: keep BOTH the WSC-hierarchy filter and the 2300 barrier** — the spike disproved
the hope of deleting them. The simplification is now: BLK-atom sections + WSC-subtree
tributary filter (reframed as the drainage subtree, not a hack) + 2300 barrier + lake barrier
as a complement. The two regression tests below assert the leak is stopped **by those rules**.

## Per-step test coverage (what each locks in)

- **blk-chains** (`test_blk_chains.py`): fid ordering + contiguity; `up_m-down_m == length`;
  under-lake runs from wbk (not edge_type); sentinel-BLK skip.
- **names** (`test_names.py`): NameSource priority; Seabird→Fraser side-channel inheritance;
  upstream-inherited for unnamed headwaters; override wins.
- **graph** (`test_graph.py`): mainstem = one node (not per-confluence); tributary flows-into
  at the confluence measure; `ancestors` = tributaries (descendant not counted); **S1 + S2**.
- **cutting** (`test_cutting.py`): substring ~cm; 2-point fallback; section_id stable under
  unrelated split, changes on in-section split.
- **splits** (`test_splits.py`): every anchor → `(blk, measure)`; barrier vs non-barrier
  graph effect; resolved sidecar determinism.
- **sectionizer** (`test_sectionizer.py`): coverage == BLK coverage; Adams two-section split;
  location_identifier table (0/1/2 splits); uniqueness-within-gnis fail-loud; per-section
  minzoom.
- **tributaries** (`test_tributaries.py`): section-level aggregation over the graph walk;
  `tributary_only` excludes the named section; seed-set cache. (Flow guards — WSC filter, 2300
  barrier, lake barrier — are already asserted in `test_graph.py`; this layer only rolls the
  guarded ancestor sets up to sections.)
- **match/section_regs** (`test_section_regs.py`): Similkameen one section across MU; Fraser
  curated `mu_boundary` split → one reg set each; base regs are MU overlay; ported
  `effective_includes_tributaries`.

## Golden parity (before cutover)

- Capture legacy outputs as fixtures: current `tier0.json` + a sample of fid→reg
  assignments for the known-hard rivers.
- Assert the v2 pipeline reproduces the *regulation assignments* (modulo intended
  improvements: real Adams split, corrected Chehalis/Kootenay). Differences must be
  explainable — investigate any that aren't.
- Compare aggregate counts: #sections vs #named streams; #regs assigned; total tributary
  reach length per reg (should be ≈ legacy, minus the corrected leaks).

## Full-build sanity gates

- Section coverage == input BLK coverage (no dropped geometry).
- No orphan under-lake segments; every lake wbk → exactly one node.
- All `location_identifier`s unique within their gnis.
- Tile byte budget respected (tippecanoe `--maximum-tile-bytes`), minzoom sane.
- Client smoke: load webapp against `v2/`; click Adams (two sections), Fraser@Seabird,
  a `tributary_only` lake; confirm regs + tributaries + labels.

## Test data hygiene

- Prefer synthetic graphs for logic; use small real extracts only for the named cases.
- Keep a fixtures module that pulls the named watersheds by GNIS/WSC once and caches, so
  real-extract tests stay fast.
- Note: `pytest` must be installed in the run env (the repo's base conda interpreter lacks
  it) — the scaffolded tests import fine but need pytest to execute.
