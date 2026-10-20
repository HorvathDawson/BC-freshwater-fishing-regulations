# T12 — golden parity harness (loader, per-state inputs cache, `eqPlain`, exclusions, VM-fixture writer)

Phase 0 · Depends on T05 (creates `golden.ts`), T09 (`inputsOf`), T10 (fixture fallback) · Blocks T13–T21, T28, T30–T36 · Decisions — · Size ¾ day

## Goal
Every presenter ticket tests "100 % of golden states equal" with three lines: `for await (const c of cases("keep"))`, call the
presenter on `await c.inputs()` (or `c.page()` once T21 lands), `expect(eqPlain(actual, expected)).toEqual({ok: true})`. The harness
loads golden.json (8.8 MB) and taps.json once per test file, opens one regs store per file (the province store, else the
committed fixture — which holds all 134 states, so parity also runs in CI), memoises `RegsInputs` per state id, applies the
comparison rule of contracts.md §1/§5 (golden ≥ b999cfd7), skips excluded states WITH their reason, and writes frozen page
fixtures for the RN render tests.

## Where it lives (layers decision)
`layers.json`: core may import nothing (not `@app/data`, not `node:fs`, even in tests — `testOnlyImports` is only vitest and
@testing-library/react), and `node:sqlite`/`node:fs` are allowed only in `packages/data/src/bundle/drivers`. `app/conformance` is not a
layer (unchecked), is in the `logic` project (`conformance/**/*.test.ts`) and in `tsc -b` (`app/tsconfig.json` references it), and
already depends on `@app/core` + `@app/data`. So (coordinator decision, tests.md §1.2):
- **store + JSON loading + inputs cache → `app/conformance/src/regs/`**; presenter parity tests are `app/conformance/src/regs/*.parity.test.ts`;
- **pure pieces → `app/packages/core/src/regs/testing/`**: the golden JSON types and `norm`/`eqPlain`/`diffLines`, so core unit tests over
  hand fixtures use the same comparison; `conformance/src/regs/eq.ts` re-exports them (one implementation).

## Files
- create `app/packages/core/src/regs/testing/golden.ts` — types only: `GoldenFile`, `GoldenState`, `GoldenSheet`, `Tap`, `TapsFile`, `Block`
- create `app/packages/core/src/regs/testing/eq.ts` (+ `eq.test.ts`) — `norm`, `eqPlain`, `diffLines`
- do NOT export `testing/` from `app/packages/core/src/regs/index.ts` (test helpers stay off the public surface; conformance imports the subpath)
- modify `app/conformance/src/regs/golden.ts` (T05 created `golden()`/`taps()`/`provinceStore()`; T09 added `requestOf`/`inputsOf`) —
  add `cases`, the fixture fallback, the stale-store skip
- create `app/conformance/src/regs/eq.ts` — `export { norm, eqPlain, diffLines } from "@app/core/regs/testing"`
- create `app/conformance/src/regs/exclusions.ts` (+ `exclusions.test.ts`)
- create `app/conformance/src/regs/fixtures.ts` — the VM-fixture writer
- create `app/conformance/src/regs/harness.parity.test.ts` (the first parity test; `tap.parity.test.ts` is T05's)
- modify `app/vitest.config.ts` — alias `{ find: "@app/core/regs/testing", replacement: pkg("core", "src/regs/testing/index.ts") }` ABOVE `@app/core`
- create `app/packages/core/src/regs/testing/index.ts` — `export * from "./golden"; export * from "./eq";`
- modify `app/packages/core/package.json` — `exports`: add `"./regs/testing": "./src/regs/testing/index.ts"` (so `tsc -b` resolves it in conformance)

## Signatures
```ts
// app/packages/core/src/regs/testing/golden.ts  (types of golden.json @04765ab5, ≥ b999cfd7, and taps.json — tests.md §1.2 verbatim)
export type Block = "header" | "keep" | "gear" | "methods" | "licence" | "sheets" | "pins";
export interface GoldenSheet { title: string | null; sub: string | null; folds: { summary: string; open: boolean }[];
  pages: { text: string; pdf: number | null }[]; lines: string[]; nested: string[] }
export interface GoldenState { id: string; w: string; d: number; p: number; water: string; waterKind: string; part: string; md: number;
  sub: string; parts: unknown[]; header: unknown; keep: unknown; gear: unknown; methods: unknown;
  licence: Record<`${"adult" | "u16"}|${string}`, unknown>; sheets: Record<string, GoldenSheet | "@">; pins: unknown }
export interface GoldenFile { meta: { generator: string; ref: string; sha: string; version: string; misses: string[] };
  states: Record<string, GoldenState>; sheets: Record<string, GoldenSheet> }
export interface Tap { item: string; partIx: number; md: number; date: string; weekday: Weekday; item_ord: number; akey: number;
  seg: number; mom: number | null; nseg: number; starts: number[]; moments: Moment[] | null; s: number;
  cell: { ladder: number; rows: number; gear: number; licence: number; display: number }; displayByDay: number[] }
export interface TapsFile { meta: { year: number; golden: string; version: string; store: Record<string, string> };
  states: Record<string, Tap>; unnamed: { handle: number; akey: number; place: number; name: string }[] }

// app/packages/core/src/regs/testing/eq.ts  (contracts.md §1/§5, tests.md §1.2)
export const norm: (s: string) => string;                                   // s.replace(/\s+/g, " ").trim()
export function eqPlain(actual: readonly string[], expected: readonly string[]): { ok: true } | { ok: false; diff: string };
export function diffLines(a: readonly string[], e: readonly string[], ctx?: number): string;   // unified, line-numbered, ≤ 40 lines,
//   ends with "first difference at char N: expected <x>, got <y>"

// app/conformance/src/regs/golden.ts  (final)
export function golden(): GoldenFile;                     // fs read once per file (module memo), path from the repo root:
                                                          //   app/design/regs-v11/port/golden/golden.json
export function taps(): TapsFile;                         //   …/golden/taps.json
export function hydrate(): unknown;                       //   …/golden/hydrate.json (T06 oracle)
export function requestOf(id: string): { target: RegsTarget; day: Day; weekday: Weekday; today: Day };   // T09
/** REGS_PARITY=province|fixture|auto (default auto): the province store (DEV_REGS / REGS_STORE) when present and its meta.store_digest
 *  equals taps.json meta.store.store_digest ("90a32c3ccedbcfb0"); else the committed fixture DEV_REGS_FIXTURE (T10; its meta keeps the
 *  province digest); null (skip, reason printed once) when neither matches. Opened with null expectations. */
export function provinceStore(): Promise<RegsStore | null>;
export function storeLabel(): Promise<"province" | "fixture" | null>;
export function inputsOf(id: string): Promise<RegsInputs | null>;          // memoised per id (Map<id, Promise>) per test file
export interface Case {
  readonly id: string; readonly g: GoldenState; readonly tap: Tap;
  inputs(): Promise<RegsInputs>;                                            // = inputsOf(id) (non-null inside cases)
  page(who?: Who, tot?: "a" | "b"): Promise<RegsPage>;                      // buildRegsPage(inputs, {today: requestOf(id).today, who, tot, pid: id})
}
/** Every state for a block, minus exclusions(block) (each excluded id is reported with its reason in the afterAll summary).
 *  Yields nothing (and registers one it.skip with the reason) when provinceStore() is null. */
export function cases(block: Block): AsyncIterable<Case>;
/** The state ids for it.each (sync): Object.keys(taps().states) minus excluded ids for the block. */
export function caseIds(block: Block): readonly string[];
export function summary(block: Block, passed: number, total: number): string;   // "keep parity: 131/134 states equal · 3 excluded (…)"

// app/conformance/src/regs/exclusions.ts — tests.md §1.2 list, verbatim, + PINS_ERROR_IDS computed at load
export const EXCLUDE: readonly { block: Block; ids: readonly string[] | "*"; key?: RegExp; reason: string; until?: string }[];
export const PINS_ERROR_IDS: readonly string[];
export function excluded(block: Block, id: string, key?: string): { reason: string } | null;

// app/conformance/src/regs/fixtures.ts — the frozen pages the RN render tests mount (they cannot open a store)
export const FIXTURE_STATES: readonly ["vedder_1010", "vedder_615", "vedder_515", "klwa_1010", "bc1_501", "glenn_501"];
export const FIXTURE_DIR: string;   // <repo>/app/packages/ui-native/src/regs/fixtures
/** RegsPage minus the `sheet` function, plus `sheets`: every key the golden state can open (golden.states[id].sheets keys, "@" →
 *  golden.sheets) mapped through page.sheet(key). JSON: Infinity never appears in a VM (presenters print words). Keys sorted, 1-space indent. */
export function freezePage(page: RegsPage, sheetKeys: readonly string[]): FrozenRegsPage;
export type FrozenRegsPage = Omit<RegsPage, "sheet"> & { readonly sheets: Readonly<Record<string, SheetVM | null>> };
/** Called from harness.parity.test.ts: when process.env.UPDATE_REGS_FIXTURES === "1", writes <FIXTURE_DIR>/<id>.json for each
 *  FIXTURE_STATES id (who adult|resident); otherwise asserts each committed file equals a fresh freezePage (stale fixture = fail
 *  with "run UPDATE_REGS_FIXTURES=1 pnpm -C app exec vitest run conformance/src/regs/harness"). Skipped (it.skip, reason
 *  "buildRegsPage lands in T21") while buildRegsPage throws. */
export function syncFixtures(): Promise<void>;
```
`core/src/regs/testing/vm-fixtures.ts` (README §3) is the HAND-BUILT VM set for unit render tests; the frozen golden pages above are the
real-data mount (one per tab). T23–T27 import `../regs/fixtures/<id>.json` (a relative JSON import inside ui-native — resolves in vitest;
add `"resolveJsonModule": true` to `app/packages/ui-native/tsconfig.json` in T23 if tsc needs it).

## Ports
`app/design/regs-v11/port/golden.mjs` `gap()`/`lines()` (the normalisation `eqPlain` mirrors), the `P` profile table (tests.md §1.2
Profiles); check.mjs's per-check reporting style (one line per failure with the state id).

## Reads
`app/design/regs-v11/port/golden/{golden,taps,hydrate}.json`, the regs store via `@app/data/regs` + `@app/data/regs/node`.

## Tests
`app/packages/core/src/regs/testing/eq.test.ts` (store-free):
1. `norm("  Open\n  today ")` → `"Open today"`.
2. `eqPlain(["Open today"], ["Open  today"])` → `{ok:true}`; `eqPlain(["a","b"], ["a"])` → `{ok:false}` and the diff names line 2.
3. `diffLines(["Only 1 of the fish in this box may be over 50 cm (20 in) ⓘ"], ["Only 1 of the fish in this box may be over 50 cm (20 in)"])`
   contains `- "Only 1 of the fish in this box may be over 50 cm (20 in)"`, `+ "… ⓘ"` and `first difference at char 57`.
4. Head form a, the contracts.md §1 rule: `eqPlain([plainAll([lead, words])], [golden head.text])` with
   lead = `[num "2" big, text " a day per angler from streams"]`, words = `[text "of the ", num "4" big, text " a day per angler across Region 2, lakes included"]`
   → ok against `"2 a day per angler from streams of the 4 a day per angler across Region 2, lakes included"` (golden vedder_515
   `keep.groups[0].head.text`, ≥ b999cfd7: one space where the two elements abut).

`app/conformance/src/regs/exclusions.test.ts`: every `EXCLUDE` entry has a non-empty `reason`; `PINS_ERROR_IDS.length === 25` (facts.md A#25);
every listed id exists in `taps().states`.

`app/conformance/src/regs/harness.parity.test.ts` — **the first parity test** (T05/T09 sanity), `it.each(caseIds("keep"))`:
5. `inputsOf(id)` resolves; `inputs.key.akey === tap.akey`, `inputs.s === tap.s`, `inputs.cell` deep-equals `tap.cell` — all 134
   (vedder_515 → akey 886, s 1, `{ladder:1057, rows:1723, gear:320, licence:1, display:64}`; klwa_1010 → 1229, 1; klwa_1012 → 1229, 0;
   x15p7_1010 → 1039, 4).
6. `requestOf("vedder_515")` → `{target:{kind:"part", item:"gnis:3062", partIx:0}, day:{month:5,day:15}, weekday:"Friday", today:{month:10,day:10}}`.
7. `golden().meta.sha === taps().meta.golden` ("04765ab5") — the two oracles are from one sync; a mismatch fails with "re-run port/sync.mjs".
8. Memo: `inputsOf("vedder_515") === inputsOf("vedder_515")` (same promise); 134 states warm ≤ 3 s (T09 budget).
9. `storeLabel()` is "province" when `data/generated/bundle/regs.sqlite` exists with store_digest 90a32c3ccedbcfb0, else "fixture".
10. `syncFixtures()` (skipped until T21).
Summary line printed in `afterAll`: `harness parity: 134/134 states tap-equal (store: province)`.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs/testing conformance/src/regs/exclusions conformance/src/regs/harness
REGS_PARITY=fixture pnpm -C app exec vitest run --project logic conformance/src/regs/harness     # CI path: green without the province store
```

## Notes
- **CONVENTION CHANGE** (README §3 "Tests"): presenter parity tests live in `app/conformance/src/regs/<block>.parity.test.ts`, not
  `core/src/regs/*.test.ts`; core keeps store-free unit tests (hand fixtures, `eqPlain` from `core/src/regs/testing`). Sibling tickets
  written against the old convention (T13 `strip.test.ts`, T14 `banner.test.ts` "golden through testing/golden.ts; skipIf no province
  store") move their parity cases to `conformance/src/regs/strip.parity.test.ts` etc. and use `cases()`; `it.skipIf(!hasProvinceRegs())`
  becomes "cases() yields nothing" (the fixture fallback makes it run in CI).
- **CONTRACT CHANGE:** `@app/core` gains the subpath export `./regs/testing` (types + eq only, no I/O). It is not re-exported from the core
  index, so app bundles never include it.
- golden.json is 8.8 MB: read once per test FILE (vitest isolates modules per file); never `JSON.parse` it per case. One store per file too
  (module memo in `provinceStore`), closed in a file-level `afterAll`.
- The fixture fallback is why T10 keeps the province `store_digest` in the fixture's meta: the stale-store check compares that digest with
  taps.json, so a province store rebuilt without re-running `port/sync.mjs` is a SKIP with the reason "regs store 1234… is not the one
  taps.json was cut from (90a32c3ccedbcfb0) — run port/sync.mjs", never a wall of false failures.
- `quotes.mjs` (tests.md §1.2) checks every string these tickets quote is still in golden — run it after a sync.
