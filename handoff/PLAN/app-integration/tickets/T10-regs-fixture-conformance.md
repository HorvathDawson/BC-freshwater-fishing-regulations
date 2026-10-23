# T10 — regs fixture store (subset tool) + conformance suite

Status: DONE — c7b7708c.

Phase 0 · Depends on T03, T04, T09 · Blocks T11 (render tests), T12, T21+ in CI without the province store · Decisions — · Size 1 day

## Goal
A small, committed regs store `app/packages/data/dev/regs.sqlite` (~2–3 MB) cut from the province store, holding every water
the golden states name (so all 134 states and the 20 unnamed handles resolve without the 14 MB province file) plus every
fixture-bundle water the store knows, and a conformance suite that runs the same tap questions against the fixture (always)
and the province store (when present).

## Files
- create `app/tools/build-regs-fixture.mjs`
- create `app/tools/build-regs-fixture.test.ts` (project `logic`: determinism + subset invariants over the committed file)
- create `app/packages/data/dev/regs.sqlite` (generated, committed)
- modify `app/package.json` — `"regs-fixture": "node tools/build-regs-fixture.mjs"` (NOT in `check`, see Notes)
- create `app/conformance/src/regs.ts` — the suite
- create `app/conformance/src/regs.fixture.test.ts`, `app/conformance/src/regs.province.test.ts` — the runners
- modify `app/deps.md` — nothing (no new dependency: node:sqlite + fflate already declared by T02)

## Signatures
```js
// app/tools/build-regs-fixture.mjs   usage: node tools/build-regs-fixture.mjs [--src <regs.sqlite>] [--out packages/data/dev/regs.sqlite]
//   src default: $REGS_STORE ?? <repo>/data/generated/bundle/regs.sqlite ; exits 2 with "no province regs store at …" when absent
```
```ts
// app/conformance/src/regs.ts
import type { RegsStore } from "@app/data/regs";
export interface RegsConformanceCases {
  /** golden-state taps the store must answer exactly (taps.json states; the fixture holds all 134). */
  readonly taps: readonly { id: string; item: string; partIx: number; md: number; weekday: Weekday;
                            akey: number; s: number; cell: CellIds }[];
  readonly unnamed: readonly { handle: number; akey: number; place: number; name: string }[];
  /** an item the store must NOT know (fixture: one dropped by the subset; province: "gnis:nope"). */
  readonly absentItem: string;
}
export function runRegsConformance(label: string, open: () => Promise<RegsStore | null>, cases: RegsConformanceCases): void;
```

### What the subset keeps (ids are NEVER renumbered — taps.json ids must hold in the fixture)
| table | kept |
|---|---|
| `meta` | every row copied, then `generated_by = 'pnpm regs-fixture'`, `fixture_of = <src store_digest>`; digests (`section_handles`, `reach_digest`, `rule_ids_sha256`, `store_digest`) left as the province's — the fixture answers as that build |
| `str`, `static`, `zdict`, `place`, `seglist`, `segmom`, `rule`, `display_rule`, `lic`, `entry`, `ruleset`, `lset` | all rows (str 416 KB, zdict 586 KB, rule 319 KB, display_rule 102 KB; the rest small) — `rules()` must see all 3,406 |
| `item` | items = taps.json `states[*].item` (42) ∪ the fixture bundle's `item.item_id` (`packages/data/dev/bundle.sqlite`, 250 rows) that exist in the store (37) → **78 items** (verified 2026-10-20) |
| `part`, `runs`, `xpart`, `picker` | the kept items' parts; the `runs`/`xpart`/`picker` ids they reference |
| `akey` | the kept parts' non-null akeys ∪ taps.json `unnamed[*].akey` → **183** |
| `cell` | every `(akey, s)` of kept akeys (832 rows) |
| `ladder_frame`, `rows_frame`, `gear_frame`, `licence_frame`, `display_frame` | ids referenced by kept cells (536 / 417 / 112 / 41 / 175) |
| `ladder_verdict` | the `common` and per-row verdict ids inside the kept ladder frames (decode with fflate + zdict) |
| `rows_decided`, `rows_row` | the `[h, w]` decided ids and row ids inside the kept rows frames |
| `licence_hold`, `licence_doc`, `licence_u16`, `licence_answer` | `[h, d, u?]` of kept licence frames; answer ids inside kept docs |
| `section_akey`, `section_place` | only the blocks holding taps.json's 20 unnamed handles, REWRITTEN with every other entry zeroed (8,192 / 4,096 bytes kept) |

Blobs are copied byte-for-byte (still `0x02` + the same `zdict`), so the fixture exercises the real codec.
Determinism: `rmSync(out)`, `PRAGMA page_size = 4096`, `PRAGMA journal_mode = OFF`, `CREATE` from `pipeline/deliver/store/schema.sql`,
insert every table in primary-key order, `VACUUM`; no clock, no randomness. Two runs → identical bytes (sha256 printed).

### The suite (`runRegsConformance`)
For the opened store (`open()` null → every case `it.skip` with the reason):
1. `openRegsStore` already ran inside `open()` with expectations `{null, null, null}` for the fixture (the fixture bundle has no
   `rule_ids_sha256` and `reach_digest = ''` → `BundleInfo` nulls, facts.md #18 — exactly `decodeStatusIndex`'s null-means-not-held)
   and the province bundle's three digests for the province store.
2. Each tap (`it.each`, title = state id): `loadRegsInputs(store, {kind:"part", item, partIx}, {month, day}, weekday, 2026)` →
   `key.akey === akey`, `s === s`, `cell` deep-equals.
3. Each unnamed handle: `loadRegsInputs(store, {kind:"section", handle}, {month:10, day:10}, "Saturday", 2026)` → `key.akey`, `place.name`.
4. `absentItem` → rejects `RegsInputsError`.
5. `store.statics()`: `glossary.version === 5`, `profiles.length === 60`; `store.rules()` size 3406 (the fixture keeps all rules).

Runners:
```ts
// app/conformance/src/regs.fixture.test.ts
runRegsConformance("regs fixture", async () => openRegsStore(openRegsDb(DEV_REGS_FIXTURE),
  { sectionHandles: null, reachDigest: null, ruleIdsSha256: null }), casesFromTaps({ absentItem: "name:Agassiz Slough" }));
// app/conformance/src/regs.province.test.ts
runRegsConformance("regs province", async () => hasProvinceRegs() ? openRegsStore(openRegsDb(DEV_REGS),
  { sectionHandles: "d768d6e135e62b99", reachDigest: "cad1c21fe6eb9ba9", ruleIdsSha256: "16be720be08b0fca" }) : null,
  casesFromTaps({ absentItem: "gnis:nope" }));
```
(`casesFromTaps` reads `app/design/regs-v11/port/golden/taps.json` via `golden.ts` `taps()` from T05; `"name:Agassiz Slough"` is a
fixture-bundle pseudo-id the regs store does not have, so it is absent from both — the case proves the refusal path.)

## Ports
`app/tools/build-fixture.mjs` (determinism pattern :40–56: page size, journal off, schema from the pipeline's `schema.sql`);
`pipeline/deliver/store/build.py` `_write` (:246–) table order; `store/common.py` `BLOCK_BITS` (:197).

## Reads
Province store, every table; `packages/data/dev/bundle.sqlite` `item.item_id`; `app/design/regs-v11/port/golden/taps.json`.

## Tests
`app/tools/build-regs-fixture.test.ts` (over the committed fixture; no province needed):
1. `SELECT count(*) FROM item` = 78; `FROM akey` = 183; `FROM cell` = 832; `FROM rule` = 3406; `FROM display_rule` = 3406; `FROM static` = 25; `FROM zdict` = 23.
2. Every `cell` frame id exists in its family table; every verdict/decided/row/hold/doc/u16/answer id referenced by a kept frame exists
   (decode with `unpackBlob`).
3. `meta.format = 'regs-store/4'`, `meta.section_handles = 'd768d6e135e62b99'`, `meta.fixture_of = '90a32c3ccedbcfb0'`.
4. File size < 3.5 MB.
5. (province present) re-running the tool into a temp path gives a file whose sha256 equals the committed one.
Conformance: `regs.fixture.test.ts` — 134 taps + 20 unnamed + 3 store cases green in CI with no province store;
`regs.province.test.ts` — same against the province store, skipped when absent.

## Acceptance
```sh
(cd app && pnpm regs-fixture) && git diff --exit-code app/packages/data/dev/regs.sqlite    # deterministic: no diff on re-run
pnpm -C app check                                                                          # `fixture` gate unaffected (see Notes)
pnpm -C app exec vitest run --project logic conformance/src/regs tools/build-regs-fixture
```

## Notes
- **The `fixture` gate** (`node tools/build-fixture.mjs && git diff --exit-code packages/data/dev`) diffs the WHOLE `packages/data/dev`
  directory: a locally rebuilt, uncommitted `regs.sqlite` fails it — intended (commit the fixture with the change that regenerated it).
  `regs-fixture` is NOT added to `check`: its source (the 14 MB province store) is not in git, so CI cannot rebuild it. Staleness is
  caught instead by the conformance runner (taps.json ids must resolve) and test 5 when the province store is present.
- The fixture is paired with NO bundle: the dev fixture set (`/bundle.sqlite`, sectionHandles "fixture-local") is a different handle
  space. Pairing tests pass null expectations; the app never opens this file (App.tsx `REGS` is the province store, T01).
- Python's `Store()` refuses the fixture (`family()` asserts ids 0..n-1 and `verify` recomputes `store_digest`) — expected; the fixture
  is a TS test artifact. Do not run `pipeline.deliver.store check` on it.
- Regenerate after any store rebuild (`python -m pipeline.deliver store`), together with `port/sync.mjs` (taps.json ids move with it).
