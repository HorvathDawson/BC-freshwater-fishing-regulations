# APP-PREP — the work before the Regulations prototype is ported

PLANNING ONLY. These are ordered tickets in the integration spec's format (`handoff/PLAN/app-integration/README.md` §4).
**Revision 3 (2026-10-20, every owner answer folded in; QUESTIONS.md).**
- The app is **native-first and always offline**: Expo/React Native is the primary target, holding the full tiles and
  every data file, with an update check that downloads only what changed.
- The web app is compiled from the same tree and fetches the same release files lazily.
- The native map is built early (N1 spike, N2). Every ticket is accepted on native first, then web parity.
- Build artifacts (`data/generated/`) and the shipped release (`data/release/`) live apart (MINIMAL-PACKAGING §1).

They make the app ready to accept the port:
- one data flow (the release files in `MINIMAL-PACKAGING.md`: a few small gzip files, all on device on native, lazy on
  web; tiles geometry-only and versioned on their own);
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

> **SUPERSEDED IN PART 2026-10-22 (owner audit call 3, built in T71): "Keep only one Map entry (the view strip; drop the
> header's Map button)".** As built: the water's header is the view strip on every view (‹ back · × close · Regulations · Conditions · Map); the strip is the ONE Map entry; the regs header shows name · date pill · [Parts] (only when the part picker has a choice; otherwise an empty 44 px slot) — no [Map], no [Conditions here]. The `[Map] [Cond.]` header buttons in the drawing
> below, rule 2's "Conditions here" at `mid_sid` and the "next to Map" answer are history (integration decisions.md D2,
> D24 and "D2 / D24 against the view strip"). The back rule, the one day and the hidden bottom bar still hold.

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
```

**Rules:**
1. **One back rule.** ‹, the Android back button and the iOS edge swipe all pop the water's view stack
   (CONDITIONS → REGS, MAP → REGS). At the REGS root, back leaves water mode and restores the browse state it came from:
   the tab, the map camera, the search query and the Conditions drill. Back is always "back to where you
   came from" (owner). Nothing else exits water mode, except a deep link.
2. **"Conditions here" uses the part's midpoint section** (`part_geo.mid_sid`, MINIMAL-PACKAGING §10), never
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

This replaces integration **D1** (the bar stays) and **D2** (FaceBar above the header). **Owner answers (2026-10-20),
SUPERSEDED 2026-10-22 (call 3, T71 — no header Map, no "Conditions here"; the view strip is the one Map entry):**
- ~~"Conditions here" sits **next to Map** in the header.~~
- ~~It opens the midpoint section of the **part currently being read**.~~

---

## 1. Order

```
Native map first                 N1 (spike) → N2            (blocks every map-facing ticket)
Data safety & gates (parallel)   P01  P02  P03  P04  P05  P06
Pipeline packaging               P07 → P18 → P08 → P09      (P18's tile rebuild is human-run)
App data plumbing                P10 → P11                  (needs P08 + N2)
App clean-up                     P12  P13 → P16  P17        (P12/P13 independent of the data work)
Shell + map for the port         P13 → P14 → P15            (P15 needs N2 + P09 + P11)
Retire the middle-men            P20  P21                   (P20 after P08 + P11 and the port tooling switch)
```

| # | ticket | area | size |
|---|---|---|---|
| N1 | native map spike: MapLibre Native in the Expo app (pmtiles, feature-state, taps, frame time, device factor) | native map | 1 day, time-boxed |
| N2 | the native map behind a real adapter contract; web/native adapter parity | native map | 2–3 days |
| P01 | spots store: no silent loss, atomic save, serialised writes, awaited UI | app data | 1 day |
| P02 | feed freshness: no fabricated `fetchedAt`; `freshness()` wired | app data/core | ½ day |
| P03 | CI gaps: generated-TS paths, app-ci = `pnpm check` + `--check`s, fixture schema test | CI | ½ day |
| P04 | retire `update-hydro/in-season/stocking.yml` on this branch | CI | ¼ day |
| P05 | card gate on SHIPPED answers inside `deliver all` (blocks shipping) | pipeline | 1 day |
| P06 | Feb 29: one calendar rule for page and app | pipeline + core | ½ day |
| P07 | bundle format version + atomic `deliver all` (out-dir, promote all, `vintage.report_all`) | pipeline | 1 day |
| P08 | `deliver release`: the shipped files + manifest into `data/release/`; parity, perf and size gates | pipeline | 2 days |
| P09 | part topology + camera hints: `part_geo`, `part_mark`, `water_exit` | pipeline | 1½ days |
| P10 | endpoints config, `fetchVerified`, manifest v2: tile compatibility + per-file diff; `publish` (human-run) | app data + tools | 1 day |
| P11 | the app reads the release: native always offline (install + update check + atomic swap), web lazy; device perf | app data | 2–3 days |
| P12 | data-flow clean-up: logic and sentences out of components (conditions, spots, date, map modes) | app | 2 days (3 commits) |
| P13 | explicit route state: `useRoute` (browse / water stack), one back rule | app ui | 1 day |
| P14 | water mode shell: bottom bar hidden, header, "Conditions here" at the midpoint, back | app ui | 1 day |
| P15 | water map view: part highlight + framing from tiles, ▼/▲, marks, mouth exit | app map/ui | 1½ days |
| P16 | Conditions: the same content, better formatted | app ui | 1½ days |
| P17 | navigation/UI clean-up pass (tabs, search back, empty states, dead UI) | app ui | 1 day |
| P18 | tiles geometry-only, versioned on their own; the mask geojson + self-hosted glyphs/sprites written into the tile version folder; `region_id` fix | pipeline (rebuild human-run) | 1½ days + rebuild |
| P20 | debug-only middle-man JSON (answers/export/guide); no shipped `regs.sqlite`; drop `stats.json` + `record()` | pipeline | 1½ days |
| P21 | stop writing `blk_chains.pkl` (2.57 GB, no reader) | pipeline | ¼ day |

---

# N1 — native map spike: MapLibre Native in the Expo app, against the adapter contract

Native map · Depends on — · Blocks N2, and every map-facing ticket (P11, P14, P15) · Decisions: native-first (owner 2026-10-20) · Size 1 day (time-boxed spike)

## Goal
Owner Q9: web and native use the SAME `atlas.pmtiles` / `basemap.pmtiles` bytes and the same style. Only the source
differs (an R2 URL with range reads on web; a downloaded local file on native) and the renderer: MapLibre GL JS needs a
browser WebGL canvas, so native needs MapLibre Native through an RN binding (MINIMAL-PACKAGING §7). This spike checks the
binding, not the data. A throwaway branch answers four questions with measurements, on a mid Android phone and an iPhone:
1. **Rendering:** can `@maplibre/maplibre-react-native` (or the current MapLibre RN binding, through an Expo config
   plugin / dev client) render our `atlas.pmtiles` + `basemap.pmtiles` **from a local file** (always offline), with our
   runtime style?
   MapLibre Native's `pmtiles://` support and the binding's version are **NOT VERIFIED**.
2. **Feature-state:** does the binding support it (status colours, `selected`), or do we need the fallback, data-driven
   `match` expressions over the visible sid set?
3. **Taps:** can a tap return `section_id` (promoteId) as a number? Can `queryRenderedFeatures` run in a 24 px box?
4. **Speed:** frame time while panning at z9–14 with status colouring on 5,000 visible sids.

The spike also measures the access timings A1–A11 (MINIMAL-PACKAGING §3) on both phones, through expo-sqlite opening a
file from `documentDirectory`.

## Files (spike branch only; nothing merges)
`app/apps/mobile/app.json` (plugin), `app/packages/map/src/Map.native.tsx` (spike body), `app/deps.md` (the candidate
line), `handoff/PLAN/app-data/N1-SPIKE.md` (the results).

## Fallback measured too
`Map.web.tsx` inside a WebView on native: the same code, with a bridge for taps and feature-state. Record its frame time
and gesture feel beside the native binding.

## Acceptance
- `N1-SPIKE.md` answers questions 1–4 with numbers, a short screen recording per phone, and the chosen binding and
  version.
- A go/no-go on feature-state, with the fallback named.
- The device factor (phone ÷ laptop) for A1–A11, which MINIMAL-PACKAGING §3's budgets use from then on.

---

# N2 — the native map behind the adapter contract (native-first, web parity)

Native map · Depends on N1 · Blocks P14, P15, integration T23 (`onMap`) · Size 2–3 days (3 commits: render, interaction, parity)

## Goal
- `Map.native.tsx` (a stub today, `:16-26`) becomes a real MapLibre Native map implementing the same contract as
  `Map.web.tsx`.
- **Contract:** the style from `layers.source.json` / `runtime-style.ts`, pmtiles sources, `promoteId`, feature-state
  status / standing / `selected` (or N1's fallback behind the same API), a numeric feature id on tap, camera fit to
  bounds, point sources (gauges, marks), and the `outside-bc` mask background.
- **The adapters:** `packages/map/src/adapters/*` become REAL adapters (web and native), replacing today's tautological
  parity test (audit F14). Both `Map.*.tsx` use them.

## Files
- `app/packages/map/src/Map.native.tsx`, `app/packages/map/src/adapters/{contract,web,native}.ts`
- `app/packages/map/src/parity.test.ts`: rewritten to run the SAME op script against both adapters' recorded command
  logs (style layers, filters, states set, camera calls) and compare them
- `app/apps/mobile/app.json`, `app/deps.md`, `app/layers.json` (the binding allowed only in `packages/map`, AGENTS 28)
- the glyphs/sprites: self-hosted copies served beside the basemap (no CDN at run time)

## Signatures
```ts
export interface MapAdapter {
  setStyle(style: StyleSpec): void;
  setFeatureStates(source: "atlas", layer: "stream" | "lake" | "wetland", states: ReadonlyMap<number, Record<string, unknown>>): void;
  fitBounds(b: [w: number, s: number, e: number, n: number], opts?: { padding?: number; animate?: boolean }): void;
  onPress(cb: (layer: string, id: number) => void): () => void;
  setPoints(sourceId: "gauges" | "marks", fc: GeoJSON.FeatureCollection<GeoJSON.Point>): void;
  visible(layers: readonly string[]): Promise<number[]>;     // ids of rendered features (status colouring)
}
```

## Tests
- Adapter parity: one op script gives identical command logs on web and native (a mock native module).
- A tap on a fixture returns a number id.
- Render smoke on the RN test renderer.

## Acceptance
- `pnpm -C app check`.
- **Device run:** iOS + Android dev builds show the BC map with status colours, a tap opens a water, and pan frame time
  is within N1's budget.
- **Web run:** no change in behaviour (Playwright walk).
- `deps.md` lines added.

## Notes
From here on every new map or UI ticket is accepted **on native first, then web parity** (the same tree through Expo
web).

## Status (2026-10-21, after review) — evidence in `n2/` (`n2/README.md`)
| acceptance | state | evidence |
|---|---|---|
| `pnpm -C app check` | **met** | exit 0, unfiltered |
| Device run, Android: BC map, status colours, a tap opens a water | **met (emulator)** | `n2/android-review-00…05`: offline launch from the installed release; status by feature-state on MVT ids; a tap's id lights exactly the reach under the 24 dp box (`-02`) and opens the Fraser (`-03`); Map → Conditions → Map style swaps put status/standing back (`-01`); Conditions standings from the feed. Emulator (n1_pixel, Vulkan), not a phone. |
| Device run, Android: pan frame time within N1's budget | **met, at the edge (emulator)** | map SurfaceView presents (N1's method), 12 one-finger pans: Map tab median 16.7 / p95 19.8–20.3 / max 73 ms; Conditions 16.7 / 18.7–18.9 / 48 ms. N1 (Vulkan, feature-state): 16.7 / 18.5–19.6 / 64–68. |
| Device run, Android: no rotate, no tilt | **partly shown** | props pinned by tests; on the emulator a two-finger rotate left north up and a two-finger downward drag panned without pitch (`-06`) — but injected two-finger input (sendevent; adb has no multi-touch) did not register a pinch either, so it is not proof. |
| Device run, iOS | **open** | not built: CocoaPods is not installed on this machine (Expo's iOS build needs it), and the owner deferred iOS at N1. |
| Web run: no change in behaviour (Playwright walk) | **met** | `n2/web-walk/` run against this branch and against main (pre-N2 map code) on the same release: same ids (numbers), 32/32 visible streams coloured, Chilliwack Creek and Cultus Lake taps open the same sheets, an area-only tap opens nothing on both, Conditions 43/51 streams and 8/9 basins coloured, no console errors (`n2/web-*`). The walk found and fixed a web regression first (basin field blank after a zoom-out). |
| `deps.md` lines | **met** | binding 11.5.0 exact; Vulkan provisional pending a real phone |

Deviations, deliberate: the renderer adapter (`MapRenderer`: `states`, `fitBounds`, `press`, `visible`, `waterBox`,
`bounds`) has no `setStyle` or `setPoints` — the native style and the gauge points are declarative props on a phone
(`nativeStyle` / `<GeoJSONSource data>`), and a contract that pretended otherwise would be the tautology F14 named.
`onPressFeature`'s first argument is the tap group (`"stream"` / `"lake"`) on both maps; no caller reads it.

## N2-F1 (follow-up) — hatches and the gauge pill on native
The closure/marsh hatches (`no_access`, `no_access_permit`, `eco_reserve`, `park_closed`, `wetland`) and the gauge
label's pill are images made at run time (`hatch.ts`, `pill.ts`) and added with `addImage` on the web; the RN binding
has no raw-pixel image API. On a phone those layers draw their plain fill and the label wears a paper halo instead
(`native-style.test.ts` names the five). Route: write the generated images as PNGs into the release's map assets (or a
sprite) at build time and register them through the binding's `Images`. Size ½ day. Blocks nothing; P15 should know.

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
- `pipeline/deliver/answers/card_check.py` (`check_built(bundle, export_dir, items=None, dates=DATES)` :1029; `main` :1081): add `--all` and a JSON report
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
- **Owner Q13 ("worthless?"):** the gate adds no shipped bytes, only build time, and it re-proves every quota number
  on the shipped answers (its first live run found 1,907 wrong numbers). The question was only whether to sample, and it
  is dropped: the gate runs on everything.
- Audit F4: the gate found 1,907 problems on its first live run and has since run only on a 15-rule fixture.

---

# P06 — Feb 29: one calendar rule for page and app

Pipeline + core · Depends on — · Blocks integration T05 (`weekdayOf`) · Size ½ day

## Goal
The year keeps its 365 reading days (owner): **Feb 29 reads exactly the regulations of Feb 28, with its own real
weekday** for weekday and hours rules. Page, app and pipeline answer through one generated function.

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

## Recommendation (owner: keep the 365-day year; Feb 29 = Feb 28's regulations)
- **The rule:** `readingDay(date) = day_index(month, day)`, with Feb 29 → Feb 28's day (59). `weekday(date)` is the
  date's REAL weekday. One function in `calendar_spec.py`, emitted to `calendar.generated.{ts,json}` and
  `reference/calendar.generated.js` (AGENTS 40). The app's `statusIndex.ts:44-47 dayOfYear` and the page's clamps both
  call it.
- **Storage stays as is.** The 366 slots and Mar 1 = 61 remain, so `range_days` and the segment starts do not move, and
  slot 60 is simply never read. A data invariant, slot 60 == slot 59 for every status profile and segment, keeps old
  readers honest. Dropping the slot from storage is optional later cleanup: it saves nothing material.
- **Why the real weekday:** Feb 28's date rules are what the owner asked for. The weekday is a fact about the day, and
  "Fridays, Saturdays and Sundays" closures speak about weekdays.
- **Zymoetz, one line:** on Fri 2036-02-29 the Zymoetz below Limonite Creek reads Feb 28's rules on a Friday, so it is
  closed to non-guided non-resident aliens. Today's page uses Thursday and shows it open.
- **Worked cases** in the generated files: 2027-02-28, 2028-02-28/29, 2028-03-01, 2036-02-29 (Fri) → (reading day,
  weekday).

## Files
- `pipeline/common/calendar_spec.py`, `pipeline/tools/emit/calendar.py`
- `app/packages/core/src/calendar.generated.*`, `app/packages/core/src/statusIndex.ts` (import the cases in its test)
- `pipeline/deliver/answers/reference/page_v36.js` (:19–31, :132, :1517). **Coordinate:** another agent owns
  `pipeline/deliver/answers` right now.
- tests: `pipeline/tests/test_feb29.py` (extend), `app/packages/core/src/calendar.test.ts`, a page-harness case (`port/hydrate.mjs` YEAR 2036)

## Acceptance
- `emit_calendar --check`; `pytest pipeline/tests/test_feb29.py`
- vitest `calendar`
- On 2036-02-29 the app, the page harness and the Python tap all read day 59 with weekday Friday (the Zymoetz closure
  applies).

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

# P08 — `deliver release`: the shipped files + manifest into `data/release/`, with parity, perf and size gates

Pipeline · Depends on P05, P07, P18 (tile version id) (P03 for the `--check` CI wiring) · Blocks P10, P11, P20, integration T01/T03/T10 · Size 2 days

## Goal
`python -m pipeline.deliver release` writes `data/release/<data_release>/`. That is ONLY the shipped set
(MINIMAL-PACKAGING §1–§2):
- **the files:** `paint` (BCSI v2 status + flow runs + donor columns), `core`, `near`, `water` (regs-common +
  `part_section` + `akey_region` + the P09 topology), `regs-{1,2,3,4,5,6,7a,7b,8}`, `regs-sid`, `conditions` (without
  the panel tables; climatology float16);
- **the form:** each a `.gz` with a sha-named file; a `meta` per SQLite file (format, `section_handles`, regs versions
  where they apply, the card-gate stamp);
- **`manifest.json`** (`app-manifest/2`): per file url, bytes, gz bytes, sha256 and format; `tiles.compatible` /
  `current` / `assets` from P18; basemap; `regulations.valid_until`.

Build and debug artifacts stay in `data/generated/`. The step holds **no regulation logic**: it copies, cuts the region
shards, writes the paint runs, indexes, vacuums, gzips and hashes. It is the last step of `deliver all`.

## Files
- create `pipeline/deliver/release/{__init__,__main__,build,paint,shards,manifest}.py` and one `schema/<file>.sql` per file
- create `pipeline/tools/emit/pack.py` → `app/packages/data/src/pack.generated.ts` (per-file table/column contract, the paint layout, formats, schema shas, the manifest type; `--check`)
- `pipeline/deliver/__main__.py`: the `release` step; `pipeline/common/curated.py`: `GENERATED.release` (= `data/release`)
- tests: `pipeline/tests/test_release.py`, `test_release_perf.py`, `app/tools/release-perf.test.ts` (node:sqlite + sql.js)

## Tests
1. **Only shipped files:** `data/release/<r>/` contains exactly the manifest's files. Nothing under `data/generated/`
   is referenced by the manifest.
2. **Query parity:** every SQL constant in `app/packages/data/src/bundle/queries.ts` (parsed from the TS) gives
   identical rows against the bundle and against the file that owns its tables. The panel and donor queries are checked
   against the decoded paint arrays.
3. **Regs parity:** water + regs-* + regs-sid reproduce `regs.sqlite`'s tables byte-for-byte (a table-routing shim
   under `store check`). Every akey of a region's sections has its frames in that region's file.
4. **Paint parity:** the status half == `status_index.bin`; the flow half decodes to `section_panel` /
   `panel_member` / `basin_member` exactly.
5. **Pairing:** every file's `section_handles` == the tile version's; the card-gate stamp is present; `item.ord` ==
   water `item_ord` per `item_id`.
6. **Determinism:** two runs give identical shas.
7. **Size gate (gzip):** paint ≤ 1.0 MB, core ≤ 1.3 MB, each regs-<region> ≤ 1.5 MB, total ≤ 14 MB. Logged into
   `sizes.json` (measured today: MINIMAL-PACKAGING §2).
8. **Perf gate** (MINIMAL-PACKAGING §3):
   - p95 ≤ 2 ms for A1, A3, A5, A6, A7, A9 and ≤ 5 ms for A2, on both node:sqlite and sql.js;
   - each paint decode ≤ 30 ms;
   - `EXPLAIN QUERY PLAN` shows a SEARCH (PK/index), never a SCAN, for every hot query.

## Acceptance
- `pytest pipeline/tests/test_release*.py -m needs_bundle`, run unfiltered
- `python -m pipeline.tools.emit.pack --check`
- `pnpm -C app exec vitest run release-perf`

## Notes
- `entry` is dropped: `WATERS_NEAR`'s EXISTS reads an `item.has_rows` flag.
- `chart`, `release` (the stocking table) and `stock_water` stay out until a producer exists.
- Measurement scripts: `…/scratchpad/pack/{build_pkgs.py,paint.py,bench.mjs}`.

---

# P09 — part topology + camera hints: `part_geo`, `part_mark`, `water_exit`

Pipeline · Depends on — (P08 copies the tables) · Blocks P15, P14 (`mid_sid`) · Size 1½ days

## Goal
The bundle carries, per named water, everything the prototype's map navigation needs **without geometry**:
- each part's mouth-first rank;
- each part's bbox (lon/lat) and its midpoint section and point;
- the boundary marks between parts (a graph: side channels and lake splits);
- the water's exit to its receiving water.

The rules are those of `pipeline/tools/audit/water_map_export.py`, moved into the build and made source-of-truth. The
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
- `pipeline/tools/audit/water_map_export.py`: read order and marks from these tables instead of recomputing them
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

# P10 — endpoints config, `fetchVerified`, manifest v2 + tile compatibility, the per-file diff; `publish`

App data + tools · Depends on P08, P18 · Blocks P11 · Size 1 day

## Goal
- One config, never `localhost` literals (`App.tsx:35-37,57,75,77` today).
- `loadManifest` refuses an unknown format.
- `diff(held, manifest)` lists the files to download: per file by sha256. The tiles appear only when
  `tiles.current.id` changes, the basemap only when its id changes.
- `fetchVerified` checks the decoded bytes + sha256 from the manifest, never `Content-Length`. Everything is gzip.
- `pair` returns a typed result. On a mismatch it keeps the **last good pair**.
- The dev server serves `data/release/` as R2 will.
- A human-run `publish` uploads the tiles + assets first, then the data files, then `manifest.json` last.

## Files
- create `app/apps/mobile/src/config.ts`
- create `app/packages/data/src/fetchVerified.ts` (+ a resumable variant for native, writing `.part` files); delete the duplicated fetch bodies in `bundle/drivers/engine.web.ts:66-85` / `engine.native.ts:59-87` (and the `Content-Length` checks :73 / :66; clear the native 15 s timer)
- create `app/packages/data/src/manifest.ts` (types from `pack.generated.ts`): `loadManifest`, `diff`, `pair`
- `app/tools/serve-tiles.mjs`: serve `data/release/` (with `Content-Encoding: gzip` for `.gz`) and Range for the pmtiles
- create `pipeline/deliver/release/publish.py` (human-run; R2 credentials from env)

## Signatures
```ts
export async function fetchVerified(url: string, expect: { bytes: number; sha256: string }, signal?: AbortSignal): Promise<Uint8Array>;
export async function loadManifest(url: string, signal?: AbortSignal): Promise<Manifest>;
export interface Held { tiles: TileVersion | null; basemap: string | null; files: Readonly<Record<FileName, string /* sha256 */>> }
export function diff(held: Held, m: Manifest): { files: FileName[]; tiles: boolean; basemap: boolean; assets: boolean };
export type Pairing =
  | { ok: true; tiles: TileVersion; release: string }
  | { ok: false; reason: "tiles-incompatible" | "format" | "offline"; keep: { tiles: TileVersion; release: string } | null };
export function pair(m: Manifest, held: Held): Pairing;
```

## Tests
- A sha mismatch rejects.
- A gzip body is accepted on its decoded sha.
- `diff`:
  - a data-only release with the same tiles lists only the changed files and `tiles: false`;
  - a geometry release lists `tiles: true` + the data;
  - the basemap is untouched unless its id moved.
- `pair`: same tiles + new data → ok; incompatible → keep the old pair.
- No `localhost` literal outside `config.ts`.

## Acceptance
- `pnpm -C app check`; vitest `fetchVerified manifest`.
- **Native first:** a device dev build reads the manifest from the LAN dev server. **Then web.**

---

# P11 — the app reads the release: native always offline (install + update check), web lazy

App data · Depends on P08, P10, N2 · Blocks P14, P15, integration T01/T03/T11 · Size 2–3 days

## Goal

**Native (primary, always offline):**
- **First run** downloads every file in the manifest (data ≈ 12.5 MB gz, atlas 531 MB, basemap 918 MB, map assets) into
  `documentDirectory/release/`. Downloads are resumable and sha-verified, with progress. The map opens once tiles +
  paint are complete.
- **Every launch** opens the held set from disk, with no network needed. When online it runs the update check:
  `diff` (P10), download only new or changed files into `release/<new>/` (unchanged files linked or copied locally),
  verify, then an **atomic swap** of a `current` pointer at the next launch or a screen boundary. The previous set is
  kept until the new one has opened and paired.
- Tiles move only when the tile version changes.

**Web (same tree, parity):**
- Fetches paint first, then core, then each other file on first need: water + `regs-<region>` on the first water in a
  region (**a tap in another region fetches that region's file automatically**), regs-sid on the first unnamed tap,
  near on the first near-search, conditions on the first Conditions view.
- Files are cached in Cache Storage by immutable URL; tiles are range reads.

**Both:**
- Every SQLite file opens over the `Db` seam: native opens in place; web uses sql.js.
- `makeBundleSource` routes each query to the file that owns its table; panel and donor lookups use the decoded paint
  arrays.
- No `/province.sqlite`, `/status_index.bin`, `atlas.meta.json` or `bc_outside.geojson`.
- `regulations.valid_until` is shown when passed.

## Files
- `app/packages/data/src/bundle/source.ts` (:61–78): a query router over file handles; check each file's format; stop
  turning a `HAS_ITEM_AREA` failure into `false`; observe `ready`
- create `app/packages/data/src/release/{loader.ts, paint.ts, install.native.ts, update.native.ts, cache.web.ts, open.native.ts, open.web.ts}` (`.native/.web` pairs, check-platform)
- `app/packages/core/src/statusIndex.ts` → `paint.ts` (decode both halves; the BCSI v2 status half is unchanged)
- `app/packages/ui/src/hooks.ts`:
  - `useStatusIndex` (:545) / `loadStatusIndex` (:521) read paint and do not cache a failure;
  - `useVintage` (:483) → `usePairing`;
  - flow colours are gated on `ok === true` (`Shell.tsx:264-267`).
- `app/packages/map/src/runtime-style.ts`, `Map.*.tsx`: tile and basemap URLs from the held set (a local file on
  native); the `outside` mask from the release's `bc_outside.geojson` (a local file on native, fetched on web; P18)
- `app/apps/mobile/src/App.tsx`: manifest → install/update → paint → `Shell`; a first-run download screen
- tests: `loader.test.ts`, `update.native.test.ts` (diff → download only changed; a crash mid-swap keeps the old set; a
  failed sha keeps the old set), `pack-source.conformance.test.ts` (`runConformance("release", …)` over the fixture
  release, ending audit 01#13's tautology)

## Signatures
```ts
export type FileName = "paint" | "core" | "near" | "water" | `regs-${Region}` | "regs-sid" | "conditions";
export interface ReleaseLoader { db(name: Exclude<FileName, "paint">): Promise<Db>; paint(): Promise<Paint>; held(): Held; regionOf(akey: number): Region[] }
export async function install(m: Manifest, onProgress: (p: Progress) => void): Promise<Held>;          // native
export async function update(m: Manifest, held: Held): Promise<{ swapped: boolean; next: Held }>;      // native
```

## Acceptance
- `pnpm -C app check`; conformance on the fixture release.
- **Native, on the N1 phones:**
  - first-run install (the measured time and bytes go in the PR);
  - an airplane-mode relaunch works fully;
  - a data-only release downloads only the changed files (proved by the dev server's log);
  - a tile release swaps tiles and data together;
  - A1–A11 within MINIMAL-PACKAGING §3's budgets;
  - tap → regs page data < 20 ms; paint decode < 150 ms.
- **Web parity:** cold start fetches the manifest + paint (≈ 0.9 MB gz) before the first coloured map. Opening the
  Vedder adds core + water + regs-2. A tap in Region 5 then fetches regs-5 only.

## Notes
- The fixture release replaces `packages/data/dev/bundle.sqlite` (`deliver release --items …`, integration D13
  amended).
- A 1.45 GB first download is the cost of "always offline". The deferred tile optimisations would cut about 345 MB
  (MINIMAL-PACKAGING §5).

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
- `traceToGauge`, the walk over `section_down` (MINIMAL-PACKAGING §3 A8: p95 15.8 ms node / 28.4 ms wasm on the laptop,
  so about 150–280 ms on a phone):
  - it runs once per section in a cached hook, off the render path;
  - if the P11 device run shows over 50 ms, the conditions package stores each gauged section's route end and length
    (`section_gauge.down_steps`), so the walk is not needed to answer, only to draw.

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
  | { type: "back" } | { type: "openSpot"; id: SpotId } | { type: "condSection"; section: SectionId };
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
- A tab action inside water mode is impossible: there is no bar.

## Acceptance
`pnpm -C app exec vitest run route Shell`; all existing Shell/WaterScreen render tests pass.

---

# P14 — water-mode shell: bottom bar hidden, header, "Conditions here", back

App ui · Depends on P13, P11, P09 (`mid_sid`), N2 · Blocks integration T23, T29 · Decisions: replaces integration D1/D2 (§0; owner: Conditions here next to Map, midpoint of the current part) · Size 1 day

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
- create `app/packages/ui/src/water.ts`: `useWaterPart(pack, item, part | tapSid) → { partIx, label, short, midSid, midLonLat, bbox }` (reads the water package's `part` + `part_geo`)
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
- `pnpm -C app check`; vitest `WaterMode water`.
- **Native first** (iOS + Android dev builds): Map → tap the Vedder → Conditions here → ‹ → ‹ lands on the same camera.
  The Android back button and the iOS edge swipe follow the same rule.
- **Web parity:** the same walk in Expo web.
- **Perf:** tap → water mode drawn (placeholder body) < 100 ms on the N1 mid phone.

## Notes
- The integration's `RegsHeader` (T23) later replaces `WaterHeader`'s body. Keep the props identical to T23's
  `RegsScreenProps.onMap / onConditions / onBack`, so T23 is a drop-in.
- An unnamed section (T42) has a single-section part: `mid_sid` is the section itself.

---

# P15 — the water map view: highlight and framing from tiles, ▼/▲, marks, mouth exit

App map/ui · Depends on N2, P09, P11, P13 · Blocks integration T16 (`placeAction` "map"), T23 (`onMap`) · Size 1½ days

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
- Back (‹) returns to the regs view, then to where the reader came from (P13).
- **No geometry is read from the package or `watermap.js`.**

## Files
- `app/packages/map/style/layers.source.json`: `selected` / `water` feature-state paint on stream/lake/wetland, using token colours (AGENTS 28; then `pnpm style:build`)
- the map adapter (N2), on BOTH `Map.native.tsx` and `Map.web.tsx`: `fitBounds` and a `marks` point source
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
- **Native first,** side by side with the prototype (`regs-v11.html` map) for the Vedder, Cowichan and KLWA: same part
  order, same marks, same labels. Then the same check on web (parity).
- **Perf:** a ▼/▲ step (state change + camera start) < 50 ms on the N1 mid phone; the part's sid set comes from one
  keyed query (A9).

## Notes
- The native map comes from N2. If N1 found no feature-state on native, `selected` / `water` use N2's fallback (a
  `match` on the sid set) behind the same adapter call.
- A part highlight works only on loaded tiles, which is fine: the camera is framed on the part first.

---

# P16 — Conditions: the same content, better formatted

App ui · Depends on P02, P12 · Size 1½ days

## Goal
Owner (Q10): the Conditions content is already right; the menu is poorly formatted. **No new content.** This ticket
changes layout and formatting only, on top of P12 (logic and sentences moved to core/hooks) and P02 (honest ages). The
screens today (research B, lines per component): ConditionsScreen 67, ConditionsPanel 335, DonorPanel 510, GaugeTrace
139, Hydrograph 183, ChartControls 61, MiniMap 52, Faces 60.

## Formatting changes
1. **One reading order:** the place line → the headline (the existing DonorPanel headline: estimate or reading + its
   age) → the chart (Hydrograph + ChartControls) → the donor list → the explanations. Today the lead-station block and
   the GaugeTrace fallback can stack three answer-like blocks above the chart; they keep their content but move under
   the headline as its detail rows.
2. **One horizon control**, in the panel header, using the existing `HORIZONS`. Today it is in the map legend
   (`Shell.tsx:526`) AND in DonorPanel (`[0,1,3,5]`, `:269`). Same choices, one place.
3. **Long explanations fold, the words unchanged:** the "Counts this much because…" rows, the notes, the chart
   disclaimer and Credits go into one closed fold under the donor list.
4. **Consistent typography and spacing:** section kickers, `TYPE` sizes and `SPACE` steps as the rest of the app (the
   tokens from integration T22). One number style for readings, and units always beside the number.
5. **Times:** every time is local and labelled, the same format everywhere (`ConditionsPanel.tsx:245` prints UTC
   unlabelled), with the age next to each reading (P02).
6. **The "no gauge" case** is one layout. The wording is unified by P12 (`noGaugeSentence`); here it gets one place in
   the order above.
7. **Water mode (P14):** the panel's title line is the part's place ("Conditions at Vedder Crossing – midpoint of this
   part").
8. **Dead UI removed** (no visible change): `GaugeBadge.tsx` (no importer), `StatusChip.tsx` `Chip`, the unused forecast
   fetch.

## Files
`ConditionsScreen.tsx`, `ConditionsPanel.tsx`, `DonorPanel.tsx`, `GaugeTrace.tsx`, `Hydrograph.tsx`, `ChartControls.tsx`,
`Credits.tsx`, `Shell.tsx` (legend horizon), `GaugeBadge.tsx` (delete), `StatusChip.tsx`, render tests.

## Tests
- A render test: the block order above; exactly one horizon control; the fold closed by default.
- Every string present before the change is still present after it (a snapshot of the panel's text set, minus the
  deleted dead components). This is the "same content" guard.

## Acceptance
- `pnpm -C app check`.
- Before/after screenshots on **native (iOS + Android)**, then web, for 3 sections (gauged, donor estimate, no gauge),
  attached to the commit. The owner reviews them.

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
3. Map tap on unnamed water: a short "Unnamed stream · Region N" sheet with "Regulations ›" (through the regs-sid package;
   integration T42) instead of nothing.
4. Remove stale comments citing gone paths (`App.tsx` header "FIXTURE"; research B), the untracked `packages/*/dist`
   (`noEmit` or a clean step in `pnpm check`), and `app/tools/render-setup.{js,d.ts}`.
5. Delete dead exports listed in audit §4.2 #7 (`regulations.ts` stays until T00 rewrites it; `pins.ts PinStore`,
   `LAKE_GAUGES`, `DEFAULT_VIEW`, `clearBasinCache`, `FACES_REQUIRED`, …).
6. Layers sheet: "Stocked" / "Surveyed" are shown greyed out and disabled while their tables are empty (no producer; owner Q15). Counts come from
   `source.counts()` (AGENTS 32).
7. `MiniMap`: one map instance per screen at most (every MiniMap opens another map today).
8. `Map.web.tsx:657`: pass the numeric feature id, not `String(best.id)`, and drop the `Number()` round-trip in Shell.

## Acceptance
`pnpm -C app check`; a manual walk of every tab and back path on native (iOS + Android), then web, recorded as a checklist in
the commit message.

---

# P18 — tiles: geometry-only, versioned on their own; the mask file and self-hosted map assets beside them

Pipeline (the tile rebuild is human-run, ≈ 1 h) · Depends on — · Blocks P08 (`tiles.compatible`), P10, P11 · Size 1½ days + rebuild

## Goal
Tiles carry geometry and the sid key only, and a data-only release never rebuilds them (MINIMAL-PACKAGING §6).
- **Version id:** `"<handles16>.<tilebuild12>"` + `section_handles` + an informational `labels_registry` digest are
  written into the pmtiles metadata. A swapped archive cannot pass (audit F6).
- **The tiles stage key** (`pipeline/build.py:125-126`) is geometry-only: the atlas handles, `pipeline/atlas/splits`,
  `CURATED.waters.areas`, the tile code. `--labels` forces a label refresh.
- **`vintage.report_all`** pairs on `section_handles` only. A registry mismatch is a warning.
- **The mask stays a separate file, OUT of the tiles** (owner: putting it in the tiles blew up the tile size).
  - The tile step runs `pipeline.deliver.tiles.boundary`, which today is not in the build, and writes
    `bc_outside.geojson` (62.5 KB) into `data/release/tiles/<version>/`. It is geometry, versioned with the tiles.
  - It is listed in the manifest's `tiles` block (url, bytes, sha256). Native downloads it; web fetches it.
  - The style keeps drawing it as the `outside` fill (`runtime-style.ts:220,331-360`).
  - It is no longer read from `data/source/`.
- **Map assets:** the tile step generates the self-hosted glyphs (the fonts the style names) and sprites into
  `data/release/tiles/<version>/map-assets-<n>/`. The style points at them; no Protomaps CDN at run time (owner Q18).
- **`region_id`** on all 18 `region` features (today 9 carry only `{"name"}`).
- **The release copy** goes to `data/release/tiles/<version>/atlas-<version>.pmtiles`. The basemap goes to
  `data/release/basemap/<id>/`.
- **Inventory test:** the tile properties are exactly the §6.1 list. Adding a status, a rule or any non-geometry
  property fails.

## Files
- `pipeline/deliver/tiles/__main__.py` (:111–127), `export.py` (region writer; `LayerSpec.attrs` frozen), `boundary.py` (now a tile-step output into the version folder), a new `assets.py` (glyphs + sprites)
- `pipeline/deliver/tiles/basemap.py`: write into `data/release/basemap/<id>/`
- `pipeline/build.py` (geometry key, `--labels`); `pipeline/common/vintage.py` (`report_all`, shared with P07)
- tests: `pipeline/tests/test_tiles.py`, `test_vintage.py`

## Acceptance
- The tile tests after the human-run rebuild. Promote file by file (memory "tiles dir holds basemap").
- A dry run of a data-only `pipeline build` shows the tiles stage SKIPPED.
- **Native and web** draw the mask from the release's `bc_outside.geojson` and use glyphs from the release (no CDN request
  in the network log).

## Notes
The atlas maxzoom-13 overzoom and the basemap landuse filter are **deferred to their own effort, scheduled before the
first public native release** (owner, decided 2026-10-20). Always-offline makes them worth about −345 MB per install (est.).

---

# P20 — retire the middle-man JSON (answers, export, guide) and the separate regs file

Pipeline · Depends on P08 (packages exist), P11 (the app reads them), the port tooling switched (below) · Size 1½ days

## Goal
The bundling step writes only the pipeline's own databases and `dist/`. By default nothing writes `ui-rules-answers.json`,
`ui-rules-export.json`, `ui-rules-guide.json`, `regs.sqlite`, `feeds/stats.json` or `bc_outside.geojson`. The answers
and export models pass between steps in memory, and a `--debug-json` flag writes the JSON on request for a human.
(Owner decision 2026-10-20: remove middle-man data.)

## Files
- `pipeline/deliver/__main__.py`: `export` → `answers` → `store` → `package` in one process for the model hand-off (the
  bundle and verdicts steps stay separate subprocesses for memory, DATAFLOW P1b)
- `pipeline/deliver/export/`, `pipeline/deliver/answers/cli.py`, `pipeline/deliver/store/build.py`: take models, not paths; `--debug-json DIR`
- `pipeline/deliver/answers/reference/page_data.py`, `pipeline/tools/audit/water_map_export.py`, `app/design/regs-v11/port/{taps.py,verify_map.py,sync.mjs}`: read the packages (water / regs-frames / regs-sid) or the debug dump
- about 35 tests that `json.load` the export: switch them to `export.files.load()` over the in-memory model (AGENTS 58 already says so)
- `pipeline/gauges/feed/publish.py:644-646`: stop writing `stats.json`. Delete `record(station)` (`app/packages/data/src/feed/http.ts:179,219-223`, no caller). Map colouring reads `index.json` only (MINIMAL-PACKAGING §9).
- `regs.sqlite` is no longer written by default (its tables live in water / regs-* / regs-sid; owner Q12)

## Tests
- `deliver all --out-dir <tmp>` leaves only `bundle.sqlite`, `verdicts.sqlite`, `status_index.bin` (kept for the pipeline tests) and `dist/`.
- `--debug-json` reproduces today's three files byte-identically, so the round-trip pins stay valid.

## Acceptance
- `pytest -m "not slow"` green.
- `node app/design/regs-v11/port/sync.mjs --ref HEAD` runs on the packages.
- The memory note `bundle-is-the-only-source` still holds.

## Notes
**Coordinate with whoever owns `pipeline/deliver/answers` and the regs-v11 prototype at the time.** Both read these files
today.

---

# P21 — stop writing `blk_chains.pkl` (2.57 GB per atlas build, no reader)

Pipeline · Depends on — · Size ¼ day

## Goal
The atlas build stops writing a file nothing reads:
- it is written at `pipeline/atlas/build.py:990` (`write_artifact(chains, …"blk_chains.pkl")`);
- the chains are used in memory during the build (`build.py:467`, `:589`);
- the only mention of the file is the determinism hash list in `pipeline/tests/test_no_slivers.py:234`.

(Owner Q17.)

## Files
`pipeline/atlas/build.py` (drop the write; fix the module docstring at :7), `pipeline/tests/test_no_slivers.py:234`
(drop the name; the determinism twin still hashes `graph.pkl`, `geometries.pkl`, …), `pipeline/docs/03-graph-design.md`
(the outputs list).

## Acceptance
`pytest pipeline/tests/test_no_slivers.py -m slow` at the next human-run atlas build. The existing live file can be
deleted by the owner (a deletion: ask).

---

## 2. What the integration tickets inherit from this

See `app-integration/README.md` §0 and `decisions.md` D22–D26 (updated with this plan). In short:
- T01, T03, T04, T10 and T11 read the **packages** (P08/P11): `water` + `regs-frames-<region>` + `regs-sid`, lazily,
  not a `/regs.sqlite` beside `/province.sqlite`.
- Every RN integration ticket is accepted on native first (N2), then web parity.
- T05's `weekdayOf` follows P06.
- T16 and T23 sit in P14's water-mode shell, with the bottom bar hidden and FaceBar gone.
- T23's `onMap` is P15's water map.
- T42's unnamed-section door is P17 #3.
