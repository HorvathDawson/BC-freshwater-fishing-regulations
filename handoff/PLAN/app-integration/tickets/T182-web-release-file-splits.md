# T182 — Release file splits for the web's first search, tap and Conditions (from T129)

Status: PARTIAL — code + tests in; the first split release and the walk's re-cut are the owner's runs (Outcome).
Source: T129's measured walk on r-bb4331929428 (its ticket has the byte table) — proposals that change the
release format, so not done there.

Biggest first:
1. An unnamed tap fetches all of `conditions` (2.88 MB) only to walk `section_down` for "Tributary of …": give
   `section_down` (628 kB) its own file → unnamed tap 8.53 → ~6.3 MB.
2. "Show on the map" fetches all of `water` (2.74 MB) for a bbox in `part_geo` (578 kB): split `part_geo` out.
3. First search fetches `near` (1.99 MB) only for each result's "near X": carry each water's nearest town in core →
   first search ~3.08 → ~1.15 MB.
4. Conditions opens core (1.09 MB) to find lake gauges, and `gauge_clim` is 1.44 MB of `conditions`: split both.
5. Feed `index.json` on R2: `Content-Encoding: gzip` + short Cache-Control in the (not yet written) feed sync.
Not worth it: splitting region files (core + water are 87% of a tap). Owner call: prefetch core + wasm (1.75 MB) after
first paint?

Each split: new file id in the release contract, the loader reads it lazily, `WEB_BUDGET` gates tightened to the new
numbers, the pinned request walk (`app/tools/request-walk.mjs --pin`) re-cut, phone (offline) unaffected.
Size: M per item. Owner: Q164 (prefetch).

## Outcome

PARTIAL 2026-10-24. Items 1-4 built (release + app), item 5 moved to T86 (the feed sync is T86's), prefetch is Q164 (T186,
done). Left: a release cut with this code and the browser walk re-cut on it (commands below).

**The release (contract.py, build.py).** New files: `down` (section_down, down/1), `climate` (gauge_clim, climate/1,
bands still clim/2), `geo` (part_geo in microdegrees, geo/1). conditions loses section_down + gauge_clim and gains
`lake_section` (sid → the lowest station over every gauged lake holding it: `build.lake_section`); water loses part_geo;
core gains `item_near` (`build.core_near`: per water the ring town(s) at the smallest distance + kind handicap, exact in
hundredths of a km, so `describeBy` still names the same town — ties ship whole and the app breaks them by name). The
handicaps live in the contract (`NEAR_HANDICAP_KM`, emitted into pack.generated.ts) and are held equal to `@app/core`
`HANDICAP_KM` by `packages/data/src/release/nearTown.test.ts`. `FORMATS` now reads the contract (one list).
`regs_fixture` cuts `geo` too when its source release has one.

**The app.** `release/queries.ts`: DOWN_FROM → down, CLIMATOLOGY / CLIM_PARAMETERS → climate, PART_GEO_FOR_ORDS → geo;
`MOVED_FROM` + `fileFor` read a moved table from its old file when the manifest names no new one (the committed
fixture releases, any release cut before this) — nothing else falls back. `source.ts`: search's "near X" from core's
`item_near` (+ place), the lake road and the Conditions map's lake classes from conditions' `lake_section` (each with a
has-table check: an older core / conditions is read the old way). The regs store's `navRows` reads part_geo through
`geo()` (store.ts). Loader file names gained down / climate / geo.

**Measured** (no build: the builder's own functions run on r-98c43091c440's tables, read only, in a scratch dir —
`build.copy_file` / `core_near` / `lake_section`, gzip -9 as `ship` does). Files: core 1,091,749 → 1,217,684 (+126 kB,
item_near: 16,888 rows for 16,846 waters), water 2,742,623 → 2,164,049, conditions 2,872,210 → 809,936 (lake_section
150 rows), down 626,654, climate 1,435,868, geo 578,823; release total 14,048,972 → 14,175,404 (gate 15 MB).

| moment | files after | before | after | gate (was) |
|---|---|---:|---:|---:|
| first paint | manifest, paint, mask | 1,023,654 | 1,023,654 | 1,150,000 |
| first search | core (was + near) | 3,082,554 | 1,217,684 | 1,400,000 (new) |
| show on the map | geo (was water) | 2,742,623 | 578,823 | — |
| first tap, named | core, water, geo, access, regs-6 | 4,501,206 | 4,627,390 | 5,200,000 (5,000,000) |
| first tap, unnamed | core, water, access, regs-6, regs-sid, down | 8,590,503 | 5,892,308 | 6,600,000 (9,500,000) |
| open Conditions | flow-area, conditions | 4,151,534 | 997,511 | 1,150,000 (4,700,000) |

The named tap grows 126 kB (core's item_near; part_geo moved, not added) — so its gate rises 0.2 MB; every other
moment falls. `WEB_BUDGET` carries the new lists and gates (+ `first_search`); `build.gate_problems` prices them.

**Tests.** pipeline (no data): `test_release.py` — where each moved table lives and what each moment fetches, the
web-budget pricing (first tap now 5 files), `core_near` (Fernie beats Mosquito Flats; exact ties both ship; a town
with no position never; unknown kind), `lake_section` (lowest over every lake), `geo_e6`. app: `nearTown.test.ts`
(handicaps equal; first search opens core only; an older core reads near), `source.test.ts` (down / climate /
conditions' lake road on a split release, the old files on an old one; show-on-map = core + geo), `navReads.test.ts`
(boxes from geo). `release-budget.test.ts`: the pin-vs-budget check and the live gates run only on a split pin /
split live release; until then a test fails the moment the live release is split and the pin is not.
`release-perf.test.ts` resolves moved tables (`fileFor`). Ran: tsc -b, boundaries, platform, deps, knip, the
data/conformance/tools suites touched (39 files, 672 passed, 2 skipped = the two split-only checks); pytest
release/see-groups/lake-facts (no data) green.

**Left — the owner's runs, in order:**
```
# 1. the needs_bundle release tests on a real build in a temp dir (contract exactness, two builds byte-equal, gates)
scripts/heavy.sh t182-release 30 -- .venv/bin/python -m pytest -q pipeline/tests/test_release.py
# 2. a release with the split files (writes data/release/; or the whole deliver stage as usual)
scripts/heavy.sh deliver 30 -- .venv/bin/python -m pipeline.deliver all --build data/generated/atlas/full
# 3. re-cut the browser walk on it (app on a dev server, the release served on :39217), then the budget test
node app/tools/request-walk.mjs --url http://localhost:<port> --pin
REGS_STORE_REQUIRED=1 pnpm -C app exec vitest run tools/release-budget.test.ts
# 4. re-cut the regs fixture so it carries geo (optional; the frozen one reads the old way)
.venv/bin/python -m pipeline.deliver.release.regs_fixture
```
Expect from (3): the named tap fetches access + geo + water (prefetch took core + regs-2), the unnamed tap down (not
conditions), the first search nothing (core prefetched), Conditions conditions + flow-area. If the walk differs
(e.g. Conditions still asks core, or the unnamed tap reads geo), the gate lists in `contract.WEB_BUDGET` follow the
walk, not the other way.
