# self-check.md — three tickets dry-run against the real code (2026-10-20, tree @ b999cfd7, prototype v33 @ 04765ab5)

Method: read the ticket top to bottom as the implementer would; open every path it names; compare every existing signature
and line ref to the file; run whatever can be run before any TypeScript exists. Fixes were made in the tickets themselves.

## T03 — `openRegsStore`

| claim | checked | result |
|---|---|---|
| `BundleInfo` in `app/packages/data/src/index.ts` :40–62 has `version, validUntil, sectionHandles, reachDigest` | read :38–64 | ✓ (ends :63) — `ruleIdsSha256` is genuinely new |
| `source.ts info()` :71–78 reads `meta.get("reach_digest") \|\| null` | read :68–80 | ✓ |
| `fixture.ts info()` returns `sectionHandles:"fixture-local", reachDigest:null` | read :100–108 | ✓ |
| `node.ts` exports `openBundle(path)`, `openInMemory(schemaSql, seedSql)`, `DEV_BUNDLE` via `new URL("../../../dev/…")` | read the file | ✓ — six `../` from `drivers/` reach the repo root (drivers→bundle→src→data→packages→app→repo) |
| `openRegsInMemory` uses `readFileSync` | `node.ts` imports only `existsSync` from `node:fs` | **✗ → fixed**: the ticket now says to import `readFileSync` |
| `vitest.config.ts` `pkg(name, entry)` takes an entry; project named `logic` | read :14, :39–41, :78 | ✓; the two new aliases must sit above `{find:"@app/data"}` (:41) as the ticket says |
| `pipeline/deliver/store/schema.sql` creates `meta`, `static`, `zdict` | grep | ✓ (:16, :23, :26) |
| test 1 constants: `zdict('display_frame')` 31 462 bytes, `counts.akeys` 2268, digests d768…/cad1…/16be…, store digest 90a3… | sqlite3 on `data/generated/bundle/regs.sqlite` and `bundle.sqlite` | ✓ all equal; `bundle.sqlite` meta carries `rule_ids_sha256 = 16be720be08b0fca` |
| refusal wording mirrors `decodeStatusIndex` | read `core/src/statusIndex.ts` :90–112 | ✓ ("built for atlas …, the map is … — refused") |

Verdict: implementable as written after the one fix.

## T13 — year strip + status line

| claim | checked | result |
|---|---|---|
| runs = run-length of `display_frame.strip` over the book year (Feb 29 dropped), each day at its own weekday | `port/verify_map.py` (decodes the store exactly so) | ✓ 134/134 states equal golden `keep.strip.bar[].flex`; status word 134/134 |
| `taps.json displayByDay` is per-day at the day's own weekday (page v36 `segOf`) | first version used the day's FIRST moment — **✗ → fixed** in `port/taps.py` before the ticket was written (klwa_1010 Sat → s 1, klwa_1012 Mon → s 0) | ✓ |
| prototype refs `flagBar` 945, `flagLegend` 949, `span` 1363 @ 04765ab5 | `app/fixtures/golden/inventory.json` | ✓ |
| page v36 refs `waterStrip0` 459, `nextOpen` 443, `runOf` 488, `strip` 238, `dayStatus` 231, `daySegs` 134 | read `page_v36.js` :129–142, inventory | ✓ |
| `segOf(w, md) = segAt(w, md, weekdayOf(md), false)` | read `page_v36.js` :137–139 | ✓ — the reason taps.py had to change |
| needs every segment's rows frame ("Trout release only until …") | not in the first `RegsInputs` | **✗ → fixed**: `RegsInputs.year.rowsBySeg` added to contracts.md (T13 mark) and requested from T09 |
| expected strings | `node app/design/regs-v11/port/quotes.mjs` | the strings it still flags in T13 are word constants / templates, not golden sentences |

Verdict: implementable; the data-side mapping is already proved, so a T13 failure can only be in the TS.

## T23 — `RegsScreen` in `WaterScreen`

| claim | checked | result |
|---|---|---|
| `WaterScreen({source, item, palette, onBack, onConditions})` at :32, duplicate `type Face` :30 | read :25–60 | ✓ |
| Shell water mount at :423; `mapDay` `useState<Date \| null>` :97; `setPicked(id); setTab("map")` path | read :418–452, grep | ✓ mount :423, mapDay :97; the search path is at **:450** (ticket said :447) **→ fixed** |
| `CrossLink` "moved from WaterScreen, unchanged" into `RegsScreen` | grep | **✗** `CrossLink` is a private function (WaterScreen.tsx :141); RegsScreen cannot import it **→ fixed**: `RegsScreenProps.footer?: ReactNode`, WaterScreen passes its CrossLink |
| `FaceBar({palette, face, onFace, onBack, backLabel})` | facts.md B (read `Faces.tsx`) | ✓ — FaceBar drawn without `onBack` (the regs header has the ‹) |
| `MapDay({palette, day: Date\|null, onDay})` reusable in a `DaySheet` | facts.md B | ✓ (not exported from the ui-native index — DaySheet imports `../MapDay` relatively, inside the package) |
| `TabBar` stays under the water screen (D1 a) | Shell :675 | ✓ no shell change needed for navigation |
| `useRegulations(store, target, day, who)` returning a page | T11 defines `useRegulations(store, target, mapDay): Async<RegsInputs \| null>` | **✗ → fixed**: T11 gained `useRegsPage(inputs, who, today)`; T23 now chains `useRegsStore → useRegsTarget → useRegulations → useRegsPage` (T35 too) |

Verdict: implementable after the two fixes.

## Cross-ticket consistency (after all agents finished)

- Parity tests: all golden parity lives in `app/conformance/src/regs/*.parity.test.ts` (core may not import `@app/data`/`node:fs`;
  `layers.json` has no `conformance` entry, so the harness may import `@app/data/regs/node`). T12 builds it; T13–T21, T30–T36
  consume `cases()`, `inputs()`, `inputsOf()`, `requestOf()`, `eqPlain()`; render tests mount VM fixtures from `writeVmFixtures`.
- `pinChain`: `port/pinreplay.mjs` replays every golden pin transition on 134/134 states — T28 transcribes it.
- Contract: every ticket's CONTRACT CHANGE note was applied to contracts.md (marked `// (TNN)`), 10 conflicts resolved in
  contracts.md §6 (sheet keys without a state id + `goldenKeyOf`; T15b's CaveatVM over T42's TimedVM; `pos:pos`; `size.rule`;
  Line-typed ladder lines; numeric `leadN`; `item.sub` composed by T16; hook signatures; `RuleIndex`; `DisplayFrame`).
- Hydration oracle: `port/hydrate.mjs` (page v36's own `describe`/`settle`/`buildModel` in node) reproduces taps.json's segment
  index on 134/134 — T06–T08 cite its output.
- Ticket quotes: `node app/design/regs-v11/port/quotes.mjs` → 436 / 645 quoted runs found verbatim in the goldens; the rest are
  word constants with no golden state, code, error messages and elided excerpts (checked per agent).
