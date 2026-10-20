# APP-PREP — the work before the Regulations prototype is ported

PLANNING ONLY. These are ordered tickets in the integration spec's format (`handoff/PLAN/app-integration/README.md` §4).
They make the app ready to accept the port:
- one data source (the package in `MINIMAL-PACKAGING.md`);
- no logic in components;
- the owner's audit decisions;
- the regs-mode navigation shell;
- a map view that works like the prototype's, drawn from the tiles;
- a Conditions clean-up and a navigation clean-up.

Line refs are at HEAD 49c1f299 unless a ticket says otherwise. Ids are `Pnn` so they never collide with the
integration's `Tnn`.

**Conventions:** as integration README §3. Logic goes in `core`, hooks in `ui`, and components receive a view model and
draw it. One ticket is one commit, made with `git commit -- <paths>`. `pnpm -C app check` must be green. Never run the
LLM parser. Heavy jobs go through `handoff/tools/heavy.sh`. The atlas, tile and mill rebuilds named below are
**human-run**.

---

## 0. The regs-mode navigation flow (owner decision 2026-10-20)

The owner chose the prototype layout. Latched to one water, the bottom bar (Map · Search · Conditions · Spots) is
**hidden**. From there, a single button jumps to the **Conditions at the midpoint of the part being read**, and a clean
way leads back to the normal map views.

```
BROWSE MODE (bottom bar shown)                         WATER MODE (bottom bar hidden; one water latched)
┌──────────────┐  tap a water on the map               ┌──────────────────────────────────────────────┐
│ Map          │──────────────────────────────────────►│ view: REGS (the prototype page)               │
│ Search       │  pick a result                        │ ‹  Vedder River      [Today ▾]  [Map] [Cond.] │
│ Conditions   │  (Conditions tab: "Regulations" link) │ Vedder Crossing to the Fraser (part label)    │
│ Spots        │◄──────────────────────────────────────│ Keep · Gear · Methods · Licence (one scroll)  │
└──────────────┘  ‹ at the REGS root = back to where   └──────────────┬───────────────┬───────────────┘
      ▲           you came from (camera/tab/query        [Map]        │               │ [Conditions here]
      │           restored)                                            ▼               ▼
      │                                    view: MAP (the water map)         view: CONDITIONS
      │                                    ‹ Regulations · name · part       ‹ Regulations · name · part
      │                                    ▼ Downstream / ▲ Upstream          "Conditions at Vedder Crossing
      │                                    tap a part / a boundary mark        – midpoint of this part"
      │                                    ⤢ Whole river · Flows into ›       (ConditionsPanel for mid_sid,
      │                                    "Show regulations for this part"    weather at mid lon/lat)
      └──── "Explore the map ×" (water map only): leave water mode onto the Map tab, framed on this part
```

**Rules:**
1. **One back rule.** ‹, the Android back button and the iOS edge swipe all pop the water's view stack
   (CONDITIONS → REGS, MAP → REGS). At the REGS root, back leaves water mode and restores the browse state it came from:
   the tab, the map camera, the search query and the Conditions drill. Nothing else exits water mode, except
   "Explore the map ×" on the water map and a deep link.
2. **"Conditions here" uses the part's midpoint section** (`part_geo.mid_sid`, MINIMAL-PACKAGING §4), never
   `sections[0]`. Changing part on the water map or the Parts sheet moves it. Its view states its own place ("at
   <short label> – midpoint of this part"), so the reader knows it is not the whole water.
3. **The day is one state**: the Shell's `mapDay` (integration D7). The water's map, regs page and Conditions view all
   read it.
4. **Flows into ›** (the mouth exit) pushes the receiving water onto the water stack at the part it enters. Back returns
   to the previous water. The stack is capped at 8 frames.
5. **The bottom bar is never half-shown.** Water mode is a full-screen route with its own header. The browse tabs keep
   their state underneath, so leaving water mode is instant.
6. **The browse Conditions tab is unchanged as a mode.** Picking a section there shows ConditionsScreen with a
   "Regulations ›" link that enters water mode on the part holding that section.

This replaces integration **D1** (the bar stays) and **D2** (FaceBar above the header). See `QUESTIONS.md` Q6–Q8 for
the details the owner may want to adjust.

---

## 1. Order

```
Data safety & gates (parallel)   P01  P02  P03  P04  P05  P06
Pipeline packaging               P07 → P08 → P09            (P18 any time; tile rebuild human-run)
App data plumbing                P10 → P11                  (needs P08)
App clean-up                     P12  P13 → P16  P17        (P12/P13 independent of the data work)
Shell + map for the port         P13 → P14 → P15            (P15 needs P09 + P11)
```

| # | ticket | area | size |
|---|---|---|---|
| P01 | spots store: no silent loss, atomic save, serialised writes, awaited UI | app data | 1 day |
| P02 | feed freshness: no fabricated `fetchedAt`; `freshness()` wired | app data/core | ½ day |
| P03 | CI gaps: generated-TS paths, app-ci = `pnpm check` + `--check`s, fixture schema test | CI | ½ day |
| P04 | retire `update-hydro/in-season/stocking.yml` on this branch | CI | ¼ day |
| P05 | card gate on SHIPPED answers inside `deliver all` (blocks shipping) | pipeline | 1 day |
| P06 | Feb 29: one calendar rule for page and app | pipeline + core | ½ day |
| P07 | bundle format version + atomic `deliver all` (out-dir, promote all, `vintage.report_all`) | pipeline | 1 day |
| P08 | `deliver package`: the one app package + manifest + query-parity test | pipeline | 1–1½ days |
| P09 | part topology + camera hints: `part_geo`, `part_mark`, `water_exit` | pipeline | 1½ days |
| P10 | endpoints config, `fetchVerified`, manifest; `publish` script (human-run) | app data + tools | 1 day |
| P11 | `PackSource`: the app reads the package (web Cache Storage; native disk cache + atomic swap) | app data | 1½ days |
| P12 | data-flow clean-up: logic and sentences out of components (conditions, spots, date, map modes) | app | 2 days (3 commits) |
| P13 | explicit route state: `useRoute` (browse / water stack), one back rule | app ui | 1 day |
| P14 | water mode shell: bottom bar hidden, header, "Conditions here" at the midpoint, back | app ui | 1 day |
| P15 | water map view: part highlight + framing from tiles, ▼/▲, marks, mouth exit | app map/ui | 1½ days |
| P16 | Conditions tab clean-up pass | app ui | 1½ days |
| P17 | navigation/UI clean-up pass (tabs, search back, empty states, dead UI) | app ui | 1 day |
| P18 | tiles: vintage in the pmtiles metadata; `region_id` on all region features | pipeline (rebuild human-run) | ½ day + rebuild |

---

# P01 — spots store: no silent loss, atomic save, serialised writes, awaited UI

App data · Depends on — · Blocks — · Decisions — · Size 1 day

## Goal
A spot is never lost silently:
- A corrupt store file is kept aside, never overwritten by the next save.
- A crash mid-save never loses the file.
- Concurrent edits never resurrect or drop a spot.
- Every save is awaited, and a failure is shown to the reader.

## Files
- modify `app/packages/data/src/spots/store.native.ts` (load :24–34 returns `[]` on a parse error; save delete-then-move :39–40; `put`/`remove` :46–52)
- modify `app/packages/data/src/spots/store.web.ts` (:19–41; quota errors swallowed at :30)
- create `app/packages/data/src/spots/queue.ts` (a write queue shared by both stores)
- create `app/packages/ui/src/spots.ts` (`useSpots`)
- modify `app/apps/mobile/src/App.tsx` (:145–164: the refresh loop that overwrites edits, no try/catch)
- modify `app/packages/ui-native/src/Shell.tsx` (`void` saves :388, :392, :402, :420; unsaved edit dropped on a tab change :405–407)
- modify `app/packages/ui-native/src/SpotCapture.tsx` (stuck on "saving" when the save rejects :77–89; the When screen's back loops to itself :95)
- tests: `store.native.test.ts`, `store.web.test.ts`, `queue.test.ts`, `SpotCapture.test.tsx`

## Signatures
```ts
export class SpotsCorrupt extends Error { constructor(readonly keptAt: string, cause: unknown) }
export interface SpotsStore {
  load(): Promise<Spot[]>;                       // throws SpotsCorrupt (file renamed to spots.corrupt-<ts>); recovers from .writing
  put(spot: Spot): Promise<void>;                // serialised through the queue: read-modify-write is atomic
  remove(id: SpotId): Promise<void>;
}
export function serialised<T>(): (job: () => Promise<T>) => Promise<T>;
// ui
export function useSpots(store: SpotsStore): { spots: Async<Spot[]>; save(s: Spot): Promise<void>; remove(id: SpotId): Promise<void>; error: Error | null };
```
- **Native save:** write `spots.json.writing`, keep `spots.json.bak`, rename `.writing` → `spots.json`.
- **Native load:** prefer `spots.json`, else `.writing`, else `.bak`, in that order.
- **Web save:** throws on `QuotaExceededError`.

## Tests
- A corrupt file followed by `put` keeps the corrupt file, raises `SpotsCorrupt` and writes nothing.
- A crash simulated between write and rename leaves the spot recoverable on the next `load`.
- Two concurrent `put`s keep both spots.
- `remove` during a refresh never resurrects the spot.
- A web quota error rejects.
- In `SpotCapture`, a rejected save shows an error and leaves the sheet open.
- A tab change with an unsaved edit asks before dropping it.

## Acceptance
`pnpm -C app check`; `pnpm -C app exec vitest run spots SpotCapture`

## Notes
Audit top-10 #2 and research B. Spots bind to `item_id` and coordinates, never to `sid` (AGENTS 5): check that
`spots/model.ts` stores no sid. A package swap (P11) must not break saved spots.

---

# P02 — feed freshness: no fabricated `fetchedAt`; `freshness()` wired

App data/core · Depends on — · Blocks P16 · Size ½ day

## Goal
A reading whose file lacks `fetchedAt`, or has a malformed one, shows as **unknown age**, never as live. The
observation time is shown beside each reading, and the live index is re-polled while a Conditions view is open.

## Files
- `app/packages/data/src/feed/http.ts` (:214 `stamp = s ? Date.parse(s) : Date.now()`, used at :257, :272)
- `app/packages/data/src/bundle/source.ts` (:393, the same pattern)
- `app/packages/core/src/index.ts` (`freshness()` :29–37 is never called; `staleAfterMs` is unused)
- `app/packages/ui/src/hooks.ts` (`useStationReading` :139)
- tests beside each file

## Signatures
```ts
function stamp(s: string | undefined): number | null;            // null for missing or NaN
export function freshness(at: number | null, now: number): "live" | "stale" | "unknown";
```

## Tests
- A station file without `fetchedAt` → `"unknown"`.
- A value of `"garbage"` → `"unknown"`.
- A value 10 minutes old → `"live"`.
- A value 3 hours old → `"stale"`.
- The finished sentence is asserted whole (AGENTS 33).

## Acceptance
`pnpm -C app exec vitest run feed freshness`

---

# P03 — CI gaps

CI · Depends on — · Blocks P08 (its `emit_pack --check` joins the same step) · Size ½ day

## Goal
Every generated file is checked whenever either side changes, and the app CI runs the same gate as `pnpm check`.

## Files
- `.github/workflows/pipeline-ci.yml` (:19–27 trigger on `calendar.generated.*` only): add `app/packages/core/src/*.generated.*`, `pipeline/deliver/answers/reference/calendar.generated.js`
- `.github/workflows/app-ci.yml`:
  - run `pnpm check` itself;
  - add setup-python, so `emit_calendar --check`, `emit_gauge_policy --check` and `answers.model --check` run (they need no data);
  - add the missing colours, spinner and fixture steps.
- `app/package.json`: add an `answers` script to `check` (integration D14 / T05 does this too; land it once, here)
- create `app/packages/data/dev/fixture-schema.test.ts`: the fixture's `sqlite_master` vs `pipeline/deliver/bundle/schema.sql`
- the dev fixture regeneration (`build-fixture.mjs`) is **human-run**. It is 6 tables behind, and writes `shed_rule` where the bundle writes `trust_bands`.

## Acceptance
- A TS-only edit to `answers.generated.ts` fails CI (proved on a scratch branch).
- The fixture test fails today and passes after the regeneration.

---

# P04 — retire the three scheduled workflows on this branch

CI · Depends on — · Size ¼ day

## Goal
Merging to `main` cannot trigger crons that run code which no longer exists. Replacements come later, on the feed code
(owner).

## Files
- delete `.github/workflows/update-hydro.yml`, `update-in-season.yml`, `update-stocking.yml`
  - `update-hydro.yml` runs the archived `python -m pipeline.recurring.hydro.jobs`;
  - the other two run `scripts/update-*.sh`, which are not in the tree.
- `archive/README.md`: replace "dead until repointed" with "removed <date>; successors: P3-CRONS plan".

## Acceptance
`ls .github/workflows` shows `app-ci.yml` and `pipeline-ci.yml` only.

## Notes
Deletion approved by the owner (audit decision #3, 2026-10-20). `handoff/PLAN/P3-CRONS.md` holds the successor plan.

---

# P05 — the card gate runs on the shipped answers and blocks shipping

Pipeline · Depends on — · Blocks P07 (the promote waits for it), P08 · Size 1 day

## Goal
- `deliver all` runs `card_check` over the answers it just wrote, and **fails the chain** on any problem.
- `answers.about` and the package `meta` carry `{card_check: {problems: 0, parts_checked, keys_checked, code_sha}}`.
- An answers file without that stamp cannot be packaged.

## Files
- `pipeline/deliver/__main__.py` (:113–140): a `card_check` step after `answers`, before `store`
- `pipeline/deliver/answers/card_check.py` (`check_built(bundle, export_dir, items=None, dates=DATES)` :1029; `main` :1081): add `--all` / `--sample N` and a JSON report
- `pipeline/deliver/store/build.py`: refuse answers without a passing stamp
- `pipeline/tests/test_card_gate_in_chain.py`

## Tests
- A mutated answers file (one quota number changed) makes the step exit non-zero, and the chain stops before `store`.
  Mutation-pinned, so the gate cannot be tautological (memory "checks seeded from input launder").
- The stamp's `parts_checked` equals the answers' part count when run with `--all`.

## Acceptance
- `python -m pipeline.deliver all --out-dir <tmp>` on the live inputs (heavy: via `heavy.sh`) passes and stamps.
- `pytest pipeline/tests/test_card_gate_in_chain.py`

## Notes
- Audit F4: the gate found 1,907 problems on its first live run and has since run only on a 15-rule fixture.
- Measure the full run's time first. If it is over about 10 min, run the default as every part shown in the goldens
  plus a fixed 2,000-key sample, and `--all` in `pipeline build`.

---

# P06 — Feb 29: one calendar rule for page and app

Pipeline + core · Depends on — · Blocks integration T05 (`weekdayOf`) · Size ½ day

## Goal
Feb 29 is **day 60 with its real weekday**, everywhere. It is never clamped to Feb 28.

## The current difference (precise)

**The two sides use the same 366-day calendar for dates** (`pipeline/common/calendar_spec.py:52 day_index`: Feb 29 =
60, Mar 1 = 61 in every year). So nothing is off by one after Feb 28. The differences are:

- **App:** `app/packages/core/src/statusIndex.ts:44-47 dayOfYear` maps Feb 29 to **60**, as the spec does.
- **Page:** `pipeline/deliver/answers/reference/page_v36.js:31` maps **Feb 29 to Feb 28**:
  `TODAY = … md === 229 ? 228 : md`. Its comments at :19 and :29 say the page's year has 365 days. Its date box does the
  same (≈ :1517, `setMd`). Its weekday function at :132, `weekdayOf = md => WEEK[(new Date(Date.UTC(YEAR, …md…)))…]`,
  therefore takes **Feb 28's weekday** on Feb 29.
- **Today's data hides the date clamp:**
  - 0 status-index profiles differ between day 59 and day 60;
  - no answers segment starts at day 60 (per research C).
  - So the map and the page agree on open/closed for every water.
- **The real divergence is the weekday.** 23 rule keys decide by weekday across days 59–60. **Example:** the Zymoetz
  River below Limonite Creek ("Zymoetz River B", rule `zymoetz_river.r10`). Its Class II licence line reads "Fridays,
  Saturdays, and Sundays Jul 24 – May 31": a closure to non-guided non-resident aliens on those days.
  - **Feb 29 2036 is a Friday** (checked: Feb 28 2036 is a Thursday). The page answers with Thursday: no closure. The
    Python tap and the app's port, using the real weekday, answer Friday: closed.
  - The same happens in 2044 and 2048. In 2028 (Feb 29 a Tuesday) and 2032 (a Sunday) the two weekdays differ too,
    with no effect on this rule.
- **The latent risk:** a future rule with a segment starting at day 60 would never be shown by the page, and would be
  labelled "Mar 1".

## Recommendation
- One rule in `calendar_spec.py`: *a date is (day_index(month, day), its real weekday); Feb 29 is day 60; a range printed
  to Feb 28 already covers day 60 through `catalogue.range_days`* (AGENTS 57 FEB 29).
- Emit worked cases `[(iso, day, weekday)]` (2028-02-28/29, 2028-03-01, 2027-03-01, 2036-02-29) into all three generated
  files: `calendar.generated.{ts,json}` and `reference/calendar.generated.js`.
- Remove the page's two clamps and its 365-day `DAYS`.
- Add a data invariant: day 60 equals day 59 for every status profile and every segment, so the page's old reading
  could never have differed silently. When it fails, it names the rule.

## Files
- `pipeline/common/calendar_spec.py`, `pipeline/tools/emit_calendar.py`
- `app/packages/core/src/calendar.generated.*`, `app/packages/core/src/statusIndex.ts` (import the cases in its test)
- `pipeline/deliver/answers/reference/page_v36.js` (:19–31, :132, :1517). **Coordinate:** another agent owns
  `pipeline/deliver/answers` right now.
- tests: `pipeline/tests/test_feb29.py` (extend), `app/packages/core/src/calendar.test.ts`, a page-harness case (`port/hydrate.mjs` YEAR 2036)

## Acceptance
- `emit_calendar --check`; `pytest pipeline/tests/test_feb29.py`
- vitest `calendar`
- The Python tap on (Zymoetz part, 2036-02-29, non-guided non-resident alien) answers the Friday closure, and the page
  harness agrees.

---

# P07 — bundle format version + atomic `deliver all`

Pipeline · Depends on P05 · Blocks P08 · Size 1 day

## Goal
- The bundle says what format it is.
- A failed step never leaves a mixed vintage.
- The end check covers every artifact, not only tiles↔bundle.

## Files
- `pipeline/deliver/bundle/build.py:941`: `meta.format = "bundle/1"`, `meta.schema_sha256`. Today `schema` is the first comment line of `schema.sql`, identical in every revision.
- `pipeline/deliver/__main__.py:113-140`: write every step into `--out-dir <tmp>` (the default becomes a sibling temp dir), then promote all files with `os.replace` after the last step.
- `pipeline/common/vintage.py`: `report_all(strict=True)`, covering tiles · bundle · status index · export · answers · store · package (handles, reach, rule_ids, registry, answers_sha256)
- `pipeline/build.py:268-273`: call `report_all`
- tests: `test_deliver_atomic.py` (a step forced to fail leaves the live files' mtimes unchanged)

## Acceptance
`pytest -k "deliver_atomic or vintage"`. A heavy live run is not needed for the test.

---

# P08 — `deliver package`: the one app package + manifest + query parity

Pipeline · Depends on P05, P07 (P03 for the `--check` CI wiring) · Blocks P10, P11, integration T01/T03/T10 · Size 1–1½ days

## Goal
`python -m pipeline.deliver package` writes `data/generated/dist/<handles16>/{manifest.json, app-<sha16>.sqlite}` as
specified in `MINIMAL-PACKAGING.md` §2–§3:
- the E6 tables;
- the `regs_*` tables, byte-copied;
- the `status_index` blob;
- `part_section`;
- one `meta`.

There is no logic in the step. It is the last step of `deliver all`.

## Files
- create `pipeline/deliver/package/{__init__,__main__,build,schema}.py`, `pipeline/deliver/package/schema.sql`
- create `pipeline/tools/emit_pack.py` → `app/packages/data/src/pack.generated.ts` (table/column contract + format + schema sha; `--check`)
- `pipeline/deliver/__main__.py`: the `package` step
- `pipeline/deliver/store/build.py`: block deflate for `section_akey` / `section_place` (codec byte per block, audit smaller win; T02's blob codec already has `fflate`)
- `pipeline/common/curated.py`: `GENERATED.dist`
- tests: `pipeline/tests/test_package.py`

## Signatures
```python
def build(bundle: Path, store: Path, status_index: Path, atlas_meta: Path, out_dir: Path) -> Path   # returns the package path
def manifest(package: Path, atlas: Path, basemap: Path) -> dict                                   # pure: no clocks
```

## Tests
1. **Query parity:** for every SQL constant in `app/packages/data/src/bundle/queries.ts`, run it with the fixture's and
   the province's sample parameters against the bundle and against the package, and require identical rows. The test
   parses the TS file for the `export const NAME = \`…\`` strings, so a new query is covered automatically.
2. `regs_*` tables byte-equal the store's, and `store check` passes when run on the package through a table-prefix shim.
3. `status_index` blob == `status_index.bin`.
4. Meta digests: package `bundle.section_handles` == atlas, and the card-gate stamp is present.
5. `item.ord` == `regs_item.item_ord` for every `item_id` (19,641 / 19,641 today).
6. Determinism: two package runs give the same sha.

## Acceptance
- `pytest pipeline/tests/test_package.py -m needs_bundle` (unfiltered, not through rtk; memory "rtk pytest false pass")
- `python -m pipeline.tools.emit_pack --check`
- Sizes logged: raw, gzip-9 and br-11 of the package (expect ≈ 41 MB / 16 MB / 13 MB; record the real numbers in
  MINIMAL-PACKAGING §7).

## Notes
- Drop `entry` and use an `item.has_rows` flag for `WATERS_NEAR`'s EXISTS (`queries.ts`). This changes one query; the
  parity test runs against an equivalent query.
- `chart`, `release` and `stock_water` stay out until a producer exists.

---

# P09 — part topology + camera hints: `part_geo`, `part_mark`, `water_exit`

Pipeline · Depends on — (P08 copies the tables) · Blocks P15, P14 (`mid_sid`) · Size 1½ days

## Goal
The bundle carries, per named water, everything the prototype's map navigation needs **without geometry**:
- each part's mouth-first rank;
- each part's bbox (lon/lat) and its midpoint section and point;
- the boundary marks between parts (a graph: side channels and lake splits);
- the water's exit to its receiving water.

The rules are those of `pipeline/tools/water_map_export.py`, moved into the build and made source-of-truth. The
prototype's `watermap.js` is then rebuilt FROM these tables (one implementation).

## Files
- `pipeline/deliver/bundle/derived.py` (where `part` / `part_section` are built) + `build.py` (geometries are already loaded for `place_water` and gauges)
- `pipeline/deliver/bundle/schema.sql`:
  ```sql
  CREATE TABLE part_geo  (ord INTEGER, part_ix INTEGER, rank INTEGER NOT NULL, mid_sid INTEGER NOT NULL,
                          mid_lon REAL NOT NULL, mid_lat REAL NOT NULL, w REAL, s REAL, e REAL, n REAL,
                          PRIMARY KEY (ord, part_ix)) WITHOUT ROWID;
  CREATE TABLE part_mark (ord INTEGER, mark_ix INTEGER, lon REAL, lat REAL, label TEXT,
                          down_part INTEGER, up_parts TEXT NOT NULL,   -- JSON array of part_ix
                          PRIMARY KEY (ord, mark_ix)) WITHOUT ROWID;
  CREATE TABLE water_exit (ord INTEGER PRIMARY KEY, down_ord INTEGER, down_part INTEGER, label TEXT);
  ```
- `pipeline/tools/water_map_export.py`: read order and marks from these tables instead of recomputing them
- tests: `pipeline/tests/test_part_geo.py`

## Rules (stated, pinned)
- **Mouth-first order:** order parts as `water_map_export.order_items` does (via `section_down`, then km, side channels
  last). The input is the graph, not the gauge-shed-only `section_down`.
- **Stream midpoint:** the point at half the part's main-stem length (route measure where `section_span` has it,
  otherwise summed section length, mouth-first). `mid_sid` is the section containing it.
- **Lake midpoint:** `mid_sid = part.rep_sid`, and the point is the polygon's representative point.
- **Marks:** parts touching within 60 m, labelled by the split's name (`split.at` + label). Unlabelled marks carry
  `NULL`, and the app shows no text.
- **Exit:** `item_down` gives the receiving water; its part is the one holding the receiving section.

## Tests
- Vedder (gnis:3062): ranks equal the prototype's `watermap.js` part order, and the mark at Vedder Crossing has the right
  down/up parts.
- Cowichan and Kootenay Lake (KLWA) the same.
- Every part has a `mid_sid` that is in its `part_section`.
- Each bbox contains its midpoint.
- Sizes are logged.

## Acceptance
`pytest pipeline/tests/test_part_geo.py -m needs_bundle` on a side bundle (`--out bundle.next.sqlite`; never overwrite
live). The live promotion is a normal `deliver all`.

## Notes
Never key on `ord` across bundles: `ord` is the insertion rowid (research A3). These tables live and die with their
bundle.

---

# P10 — endpoints config, `fetchVerified`, manifest; `publish`

App data + tools · Depends on P08 · Blocks P11 · Size 1 day

## Goal
- The app reads one config, never `localhost` literals: `App.tsx:35-37,57,75,77` today.
- Every whole-file fetch is verified against the manifest's decoded bytes and sha256.
- The dev server serves `dist/`.
- A human-run `publish` uploads a dist to R2.

## Files
- create `app/apps/mobile/src/config.ts` (`ENDPOINTS` from Expo `extra`, dev default `http://localhost:39217`)
- create `app/packages/data/src/fetchVerified.ts`; delete the duplicated fetch bodies in `bundle/drivers/engine.web.ts:66-85` / `engine.native.ts:59-87` (and the `Content-Length` checks at :73 / :66; clear the native 15 s timer)
- create `app/packages/data/src/manifest.ts` (type from `pack.generated.ts`; refuse an unknown `format`)
- `app/tools/serve-tiles.mjs`: serve `data/generated/dist/<current>/` + the atlas/basemap under digest names. Keep `/province.sqlite` until P11 lands, then remove it. Send `Content-Encoding: br` for the package so the dev server proves compression works.
- create `pipeline/tools/publish.py` (human-run; R2 credentials from env; uploads immutable files first, `manifest.json` last)

## Signatures
```ts
export async function fetchVerified(url: string, expect: { bytes: number; sha256: string }, signal?: AbortSignal): Promise<Uint8Array>;
export interface Manifest { format: "app-manifest/1"; vintage: Vintage; package: PackRef; atlas: FileRef; basemap: FileRef; feeds: { url: string }; regulations: { valid_until: string } }
export async function loadManifest(url: string, signal?: AbortSignal): Promise<Manifest>;
```

## Tests
- A sha mismatch rejects.
- A br-encoded response with a compressed Content-Length is accepted when the sha matches.
- An unknown manifest format is refused.
- A test asserts no `localhost` literal under `app/packages` or `app/apps/*/src` except `config.ts`.

## Acceptance
`pnpm -C app check`; `pnpm -C app exec vitest run fetchVerified manifest`

---

# P11 — `PackSource`: the app reads the package

App data · Depends on P08, P10 · Blocks P14, P15, integration T01/T03/T11 · Size 1½ days

## Goal
The app opens ONE file:
- `makeBundleSource` runs over the package Db unchanged (same queries);
- the status index comes from the `status_index` blob;
- the vintage comes from the manifest and the package meta (no `atlas.meta.json`, no `bc_outside.geojson`).

**Native:**
- the package is cached at `documentDirectory/pack/app-<sha>.sqlite`;
- it is opened from disk;
- a new manifest downloads in the background, verifies, renames, and swaps at the next launch;
- the previous package is kept until the new one opens.

**Web:** Cache Storage keyed by the immutable URL. Both platforms show `regulations.valid_until` once it has passed.

## Files
- `app/packages/data/src/bundle/source.ts` (:61–78): check `pack.format`; stop turning a `HAS_ITEM_AREA` failure into `false`; observe `ready`
- create `app/packages/data/src/pack/{open.web.ts,open.native.ts,cache.native.ts}` (`.native/.web` pair: check-platform)
- `app/packages/ui/src/hooks.ts`:
  - `useStatusIndex` (:545) and `loadStatusIndex` (:521) read the blob, not a URL. Do not cache a failure for the
    session (audit 02#11).
  - `useVintage` (:483) compares the manifest atlas digest. Flow colours are gated on `ok === true` (`Shell.tsx:264-267`).
- `app/packages/map/src/runtime-style.ts`, `Map.web.tsx`: drop the `outside` geojson source (the background layer is the mask)
- `app/apps/mobile/src/App.tsx`: manifest → package → `Shell`
- tests: `open.native.test.ts` (swap atomicity with a fake FS), `pack-source.conformance.test.ts` (run `runConformance("pack", …)` over the fixture package, which also fixes audit 01#13's tautology)

## Acceptance
- `pnpm -C app check`
- conformance on the fixture package
- **Manual (web):** cold start fetches the manifest and the package only (network panel). Record bytes and time on a
  phone browser.
- **Manual (native):** airplane-mode relaunch opens the cached package.

## Notes
- Spike first (2 h): does this Expo SDK's `openDatabaseAsync(name, options, directory)` open a file in `documentDirectory`?
  If not, keep `deserializeDatabaseAsync` from the cached bytes (memory ≈ 41 MB), and say so in `deps.md`.
- The fixture package replaces `packages/data/dev/bundle.sqlite`, built by `build-fixture.mjs` or by
  `deliver package --items …` (integration D13).

---

# P12 — data-flow clean-up: logic and sentences out of components

App · Depends on — · Blocks P16 · Size 2 days, 3 commits (a conditions, b spots/date, c map modes)

## Goal
AGENTS 25/33: components draw a view model. Sentences are composed in `core` and pinned whole.

## Files
**(a) Conditions:**
- `ui-native/src/DonorPanel.tsx`: `bound` :59, `distance` :62–66, `share` :75–79, `because` :87–104, headline :252–261, hard-coded horizons `[0, 1, 3, 5]` :269, the WHY table :37
- `ui-native/src/ConditionsPanel.tsx`: station fallbacks, inline sentences such as :251, date formatting (prints UTC unlabelled at :245); the `any` at :43
- These move to `core/src/conditions.ts` (`donorReason`, `donorShare`, `estimateSentence`, `noGaugeSentence`, `HORIZON_LABELS` from `HORIZONS`) and to the hook `ui/src/panel.ts` `useConditionsView(source, section, day, horizon) → ConditionsVM`.

**(b) Spots and date:**
- `SpotScreen.tsx:352-372` `Trend` → `data/spots`
- `MapDay.tsx:18-35` `dayLabel`/`shiftDay` → `core/src/day.ts`
- `SpotCapture.tsx:157-170` day clamp, :26/:80 `captureSpot` → `useCaptureSpot`
- `SpotsScreen.tsx:97`: list by LOCAL date
- `DonorPanel.tsx:260` `Date.now()` → the chosen day

**(c) Map modes:**
- `Shell.tsx:266-340`, about 75 lines choosing modes, → `ui/src/mapModes.ts` `useMapModes()`
- the temperature legend at `Shell.tsx:642-644` (18/20 °C literals) → from the publisher's `_TEMP_BANDS`, through `emit_gauge_policy`
- `SearchScreen.tsx:244,262` distance wording → `core`

**Also:**
- fragment assertions → whole-string `toBe`: `DonorPanel.test.tsx` 23/45, `GaugeBadge.test.tsx` 17/23 (if GaugeBadge survives P16), `SpotScreen.test.tsx` 17/52, `SpotsScreen.test.tsx` 6/11
- `useAsync`: a keyed promise cache that includes the source identity (`source.info().version`), so `gaugeSeries` is not fetched 4× per panel and a package swap invalidates
- the false comment at `hooks.ts:347-349`

## Tests
Core: every sentence branch pinned whole (`traceSentence` 4 branches, `shareWord` boundaries, `donorReason` per WHY key).
Render tests assert the VM is drawn, not the wording logic.

## Acceptance
- `pnpm -C app check`
- `grep -nE "\`[^\`]*\\$\\{" app/packages/ui-native/src/{DonorPanel,ConditionsPanel}.tsx` returns no sentence templates

---

# P13 — explicit route state: `useRoute`, one back rule

App ui · Depends on — · Blocks P14, P15, P17 · Size 1 day

## Goal
- Replace the Shell's implicit priority chain (research B: `openSpot` → `adding` → `item` → search → conditions+section
  → map, with `TabBar` resets clearing `item`/`condSection`/`condAt`/`picked`) by one route value and one reducer.
- Back behaviour is a pure function, testable without rendering.
- The browse state underneath is kept when a water opens.

## Files
- create `app/packages/ui/src/route.ts` (+ `route.test.ts`)
- `app/packages/ui-native/src/Shell.tsx` (690 lines; state at :95–131; the render chain ≈ :380–480; `TabBar` at :405 and :675): read and write the route only. Target: under 400 lines.

## Signatures
```ts
export type View = "regs" | "map" | "conditions";
export interface WaterFrame { item: ItemId; part: number | null; tapSid: SectionId | null; view: View }
export type Route =
  | { mode: "browse"; tab: TabKey; cond: { section: SectionId; at: number | null } | null; spot: SpotId | null; adding: boolean }
  | { mode: "water"; stack: readonly WaterFrame[]; from: Extract<Route, { mode: "browse" }> };
export type RouteAction =
  | { type: "tab"; tab: TabKey } | { type: "openWater"; item: ItemId; tapSid?: SectionId; part?: number }
  | { type: "view"; view: View } | { type: "part"; part: number } | { type: "flowInto"; item: ItemId; part: number }
  | { type: "back" } | { type: "exploreMap" } | { type: "openSpot"; id: SpotId } | { type: "condSection"; section: SectionId };
export function reduce(r: Route, a: RouteAction): Route;
export function bottomBarShown(r: Route): boolean;          // false in water mode and while adding a spot
export function useRoute(initial?: Route): [Route, (a: RouteAction) => void];
```
The map camera of the `from` state is kept in `Shell`'s camera ref and restored on `back`. It is not in the route
(no re-render on pan).

## Tests (pure)
- browse/map → openWater → back = the same browse state.
- water regs → view map → back → regs → back → browse.
- flowInto twice → back, back returns to the first water.
- The stack cap is 8.
- exploreMap → browse map with `focus` on the current part.
- A tab action inside water mode is impossible: there is no bar.

## Acceptance
`pnpm -C app exec vitest run route Shell`; all existing Shell/WaterScreen render tests pass.

---

# P14 — water-mode shell: bottom bar hidden, header, "Conditions here", back

App ui · Depends on P13, P11, P09 (`mid_sid`) · Blocks integration T23, T29 · Decisions: replaces integration D1/D2 (§0) · Size 1 day

## Goal
Opening a water enters water mode:
- **Bar:** the bottom bar is gone.
- **Header:** ‹, the water's name, the date pill (`mapDay`, integration D7), and two actions, **Map** and **Conditions
  here**. The part label sits on two lines under it.
- **Body:** `RegulationsPlaceholder` until the port (T23) fills it.
- **Conditions here:** opens the Conditions view on `part_geo.mid_sid` with the heading "Conditions at <short label> –
  midpoint of this part" and "‹ Regulations".
- **Exit:** the back rule is P13's. The FaceBar and the CrossLink door on `WaterScreen` (:87–88, :115–134), which used
  `sections[0]`, are removed.

## Files
- create `app/packages/ui-native/src/water/WaterMode.tsx` (frame: header + the view slot), `WaterHeader.tsx`, `ConditionsHere.tsx` (wraps `ConditionsPanel` with the place line)
- create `app/packages/ui/src/water.ts`: `useWaterPart(pack, item, part | tapSid) → { partIx, label, short, midSid, midLonLat, bbox }` (reads `regs_part` + `part_geo`)
- `app/packages/ui-native/src/WaterScreen.tsx`: becomes the regs view body (placeholder); `Faces.tsx` / FaceBar deleted if nothing else uses it
- `app/packages/ui-native/src/Shell.tsx`: render `WaterMode` when `route.mode === "water"`; hide the `TabBar` when `!bottomBarShown(route)`
- tests: `WaterMode.test.tsx`, `water.test.ts`

## Tests
- A render in water mode has no `role="tablist"` bottom bar.
- "Conditions here" for Vedder part 0 renders ConditionsPanel with `section = mid_sid` (fixture) and the whole heading
  string.
- Back from Conditions shows regs, and back again shows the browse map.
- A tap on a mid-river section opens that section's part (`tapSid`), not part 0.

## Acceptance
`pnpm -C app check`; vitest `WaterMode water`; a manual web walk: Map → tap the Vedder → Conditions here → ‹ → ‹ lands
on the same camera.

## Notes
- The integration's `RegsHeader` (T23) later replaces `WaterHeader`'s body. Keep the props identical to T23's
  `RegsScreenProps.onMap / onConditions / onBack`, so T23 is a drop-in.
- An unnamed section (T42) has a single-section part: `mid_sid` is the section itself.

---

# P15 — the water map view: highlight and framing from tiles, ▼/▲, marks, mouth exit

App map/ui · Depends on P09, P11, P13 · Blocks integration T16 (`placeAction` "map"), T23 (`onMap`) · Size 1½ days

## Goal
The water map matches the prototype's navigation (research C; `regs-v11.html` `mapHtml` and the controls ≈ L1817–1842 @
49c1f299), drawn with the real map and tiles:
- **The current part:** feature-state `selected` on its sids (`part_section`); the other parts of the water get a second
  state `water`.
- **Camera:** framed by `part_geo` bounds; ⤢ Whole river/lake frames the item bounds.
- **▼ Downstream / ▲ Upstream:** follow `rank` and the marks graph. A mark with two upper parts opens a small chooser.
  The ends are disabled with "Lowest part" / "Highest part".
- **Marks:** tapping a boundary mark shows its label and its down/up parts.
- **Tapping a part** selects it.
- **"Flows into <name> ›"** at the lowest part opens the receiving water.
- **"Show regulations for this part"** returns to the regs view on that part.
- **"Explore the map ×"** leaves water mode.
- **No geometry is read from the package or `watermap.js`.**

## Files
- `app/packages/map/style/layers.source.json`: `selected` / `water` feature-state paint on stream/lake/wetland, using token colours (AGENTS 28; then `pnpm style:build`)
- `app/packages/map/src/Map.web.tsx`: a `focus={ {bounds} }` prop and a `marks` symbol source (points only)
- `app/packages/ui/src/waterMap.ts`: `useWaterMap(pack, item, part) → { parts:[{ix, rank, label, short}], current, down, ups, marks, exit, bounds, selected: SectionId[] }`, a pure nav step in `core/src/waterNav.ts`
- `app/packages/ui-native/src/water/WaterMapView.tsx`
- remove the trilateration / `querySourceFeatures` re-fit for a picked water (`ui/src/search.ts:145 usePickFocus`, `Map.web.tsx:501`); keep `usePickFocus` for search results only if they still need it
- tests: `waterNav.test.ts` (Vedder/Cowichan/KLWA fixtures copied from P09's tables), `WaterMapView.test.tsx`

## Tests
- On a fixture graph with a side channel: ▲ from the mouth part gives the main stem, and the chooser offers the side channel.
- At the top, ▲ is disabled with "Highest part".
- The exit opens the receiving item at `down_part`.
- `selected` equals `part_section` for the part.

## Acceptance
- `pnpm -C app check`
- **Manual web, side by side with the prototype** (`regs-v11.html` map) for the Vedder, Cowichan and KLWA: same part
  order, same marks, same labels.

## Notes
- Native: `Map.native.tsx` is a placeholder (:16–26). This ticket is web-only, and the native map is an owner question
  (Q9).
- A part highlight works only on loaded tiles, which is fine: the camera is framed on the part first.

---

# P16 — Conditions tab clean-up pass

App ui · Depends on P02, P12 · Size 1½ days

## Goal
The Conditions views say each thing once, in one vocabulary, with honest ages. This is the current screen inventory
(research B; lines per component): ConditionsScreen 67, ConditionsPanel 335, DonorPanel 510, GaugeTrace 139, Hydrograph
183, ChartControls 61, MiniMap 52, Faces 60, GaugeBadge 96, StatusChip 29.

## Concrete simplifications
1. **One "no gauge" sentence.** There are four wordings today:
   - `ConditionsPanel.tsx:251` "No gauge is entitled to speak for this water…";
   - `DonorPanel.tsx:37` "No gauge on this water is close enough in size…";
   - `SpotScreen.tsx:197`;
   - `SpotsScreen.tsx:128` "no gauge".
   Replace them with `core` `noGaugeSentence(reason)`.
2. **One answer at the top.** ConditionsPanel shows the DonorPanel headline, then a lead-station reading block, then a
   GaugeTrace fallback, which can be three overlapping answers. Keep the DonorPanel headline (estimate or reading + age)
   as THE answer, and fold the lead-station block into its first donor row.
3. **One horizon control.** Today the horizon is set in the map legend (`Shell.tsx:526`, `HORIZONS`) and again in
   DonorPanel (`[0,1,3,5]` :269). Keep one control, in the Conditions header, driven by `HORIZONS`.
4. **Ages everywhere:** show the observation time per reading (P02), and "unknown age" where `fetchedAt` is missing.
   Re-poll `index.json` every 5 min while the view is open.
5. **Collapse the explanations.** "Counts this much because…" rows and the notes go in a closed "How this estimate is
   made" fold. The headline, the chart and the donor list stay open.
6. **Chart:** Hydrograph + ChartControls stay. The disclaimer sentence moves into the fold. Delete the GaugeTrace
   fallback, or keep it only when no hydrograph series exists.
7. **Delete dead UI:**
   - `GaugeBadge.tsx` (no app importer; only `index.ts` and its test);
   - `StatusChip.tsx` `Chip`;
   - `core` `NO_GAUGE` / `NO_READING_HERE` if (1) replaces them;
   - the unused forecast fetch (research B).
8. **Empty state:** the Conditions tab with no section picked shows one hint line, "Tap a river to see its flow and
   temperature", over the gauge-dot map. Today there is none.
9. **Temperature legend** from the policy (P12c), not literals.
10. **Credits** are reached from the fold, not as a block at the end of every panel.
11. **Place line:** in water mode (P14), the panel's title is the part's place ("Conditions at Vedder Crossing –
    midpoint of this part"), not the bare water name. In browse mode it is the section's water + "this stretch".
12. **Local time:** every displayed time is local and labelled (`ConditionsPanel.tsx:245` prints UTC unlabelled).

## Files
`ConditionsScreen.tsx`, `ConditionsPanel.tsx`, `DonorPanel.tsx`, `GaugeTrace.tsx`, `Hydrograph.tsx`, `ChartControls.tsx`,
`Credits.tsx`, `GaugeBadge.tsx` (delete), `StatusChip.tsx`, `Shell.tsx` (legend), `core/src/conditions.ts`, tests.

## Tests
- Whole-string pins for each headline and empty state.
- A render test: exactly one headline region, one horizon control, and the fold closed by default.
- `rg -n "no gauge|No gauge" app/packages/ui-native/src` → only via `noGaugeSentence`.

## Acceptance
`pnpm -C app check`; a manual web walk on 3 sections: gauged, donor estimate, no gauge. Before/after screenshots go in
the commit message.

## Notes
Riffle (`app/design/riffle.html`) remains the design target for Conditions (memory "riffle is the design target"). Any
change here that departs from Riffle goes to the owner first (Q10).

---

# P17 — navigation and UI clean-up pass

App ui · Depends on P13 · Size 1 day

## Goal
Navigation is predictable and the shell is free of dead paths before the port adds a large screen.

## Items
1. The TabBar shows the active tab with `aria-selected` (AGENTS 34). Re-tapping the active tab resets that tab
   (map: back to BC framing; search: clear).
2. Search → result → water mode → ‹ returns to the results with the query and scroll position kept. Today Back goes to
   the map, framed.
3. Map tap on unnamed water: a short "Unnamed stream · Region N" sheet with "Regulations ›" (through `regs_section_akey`;
   integration T42) instead of nothing.
4. Remove stale comments citing gone paths (`App.tsx` header "FIXTURE"; research B), the untracked `packages/*/dist`
   (`noEmit` or a clean step in `pnpm check`), and `app/tools/render-setup.{js,d.ts}`.
5. Delete dead exports listed in audit §4.2 #7 (`regulations.ts` stays until T00 rewrites it; `pins.ts PinStore`,
   `LAKE_GAUGES`, `DEFAULT_VIEW`, `clearBasinCache`, `FACES_REQUIRED`, …).
6. Layers sheet: hide "Stocked" / "Surveyed" while their tables are empty (no producer). Counts come from
   `source.counts()` (AGENTS 32).
7. `MiniMap`: one map instance per screen at most (every MiniMap opens another map today).
8. `Map.web.tsx:657`: pass the numeric feature id, not `String(best.id)`, and drop the `Number()` round-trip in Shell.

## Acceptance
`pnpm -C app check`; a manual web walk of every tab and back path, recorded as a checklist in the commit message.

---

# P18 — tiles: vintage in the pmtiles metadata; `region_id` on every region feature

Pipeline (the tile rebuild is human-run, ≈ 1 h) · Depends on — · Blocks P11's tile check (until it lands, P11 compares the manifest only) · Size ½ day + rebuild

## Goal
- The atlas archive itself states `section_handles` + `registry_digest`, so a swapped archive cannot pass (audit F6).
- All 18 `region` features carry `region_id`: today 9 carry only `{"name": "1".."7b"}` and no id (research D).

## Files
- `pipeline/deliver/tiles/__main__.py` (:111–127): write the digests into the archive metadata (the tippecanoe/tile-join `--description` JSON, or patch the pmtiles metadata after the join; check which survives `tile-join`)
- `pipeline/deliver/tiles/export.py`: the region layer writer
- `pipeline/build.py:125-126`: add `pipeline/atlas/splits` + `CURATED.waters.areas` to the tiles stage key (audit F6)
- `pipeline/tests/test_tiles.py`: pin both

## Acceptance
`pytest pipeline/tests/test_tiles.py -m needs_tiles` after the human-run rebuild. Promote file by file (memory "tiles dir
holds basemap").

---

## 2. What the integration tickets inherit from this

See `app-integration/README.md` §0 and `decisions.md` D22–D26 (updated with this plan). In short:
- T01, T03, T04, T10 and T11 read the **package** (P08/P11), not a `/regs.sqlite` beside `/province.sqlite`.
- T05's `weekdayOf` follows P06.
- T16 and T23 sit in P14's water-mode shell, with the bottom bar hidden and FaceBar gone.
- T23's `onMap` is P15's water map.
- T42's unnamed-section door is P17 #3.
