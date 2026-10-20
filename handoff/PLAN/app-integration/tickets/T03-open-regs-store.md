# T03 — `openRegsStore`: format, digests, section versions, refusal; `Db` export; aliases; node helper

Phase 0 · Depends on T00, T02 · Blocks T04, T09, T10, T11, T12 · Decisions — · Size ½ day

## Goal
One function opens a regs store over any `Db` (node:sqlite in tests, sql.js on web, expo-sqlite on the phone — the store is a
plain SQLite file, so `loadBundle(url)` already opens it) and REFUSES it unless it is the format, the section layout versions
and the build this app reads: `meta.format = regs-store/4`, `meta.layout.versions = {ladder 1, rows 26, gear 12, licence 6,
display 16}`, `static answers.glossary.version = 5`, and its three digests equal the province bundle's
(`BundleInfo.sectionHandles / reachDigest / ruleIdsSha256`). A refused store is a typed error the UI shows as
"Regulations unavailable", never a partly-read page. `Db` becomes importable; `@app/data/regs` and `@app/data/regs/node`
resolve in vitest and Metro.

## Files
- create `app/packages/core/src/regs/versions.ts` (+ `export * from "./versions"` in `regs/index.ts`)
- create `app/packages/data/src/regs/store.ts` (open + meta; T04 adds the read methods to the same object)
- create `app/packages/data/src/regs/index.ts` — `export * from "./store"; export * from "./blob";` (T04/T09 append `reads`, `inputs`)
- create `app/packages/data/src/regs/store.test.ts`
- modify `app/packages/data/src/index.ts` — `BundleInfo` (:40–62): add `ruleIdsSha256`; add `export type { Db, Row, Cell } from "./bundle/db";`
- modify `app/packages/data/src/bundle/source.ts` — `info()` (:71–78)
- modify `app/packages/data/src/fixture.ts` — `info()` (:104–105)
- modify `app/packages/data/src/bundle/drivers/node.ts` — add `DEV_REGS`, `hasProvinceRegs()`, `openRegsDb()`
- modify `app/packages/data/package.json` — `exports`: add `"./regs": "./src/regs/index.ts"`, `"./regs/node": "./src/bundle/drivers/node.ts"`
- modify `app/vitest.config.ts` — `alias` (two lines, ABOVE `{ find: "@app/data", … }`)

## Signatures
```ts
// app/packages/core/src/regs/versions.ts
/** The regs store format and answers section versions this app was written against (contracts.md header). A store that
 *  says anything else is refused (T03) — its frames may not mean what the presenters read. Bump together with a port. */
export const REGS_FORMAT = "regs-store/4" as const;
export const KNOWN_VERSIONS = Object.freeze({ ladder: 1, rows: 26, gear: 12, licence: 6, display: 16, glossary: 5 } as const);
export type SectionName = "ladder" | "rows" | "gear" | "licence" | "display";

// app/packages/data/src/regs/store.ts
import type { Db } from "../bundle/db";
export class RegsStoreError extends Error {
  constructor(readonly why: "format" | "codec" | "version" | "digest" | "missing" | "damaged", message: string);
}
/** What the paired bundle says (BundleInfo). null: not held to it (a bundle that predates the field, the fixture). */
export interface RegsExpect { readonly sectionHandles: string | null; readonly reachDigest: string | null; readonly ruleIdsSha256: string | null }
export interface RegsMeta {
  readonly format: typeof REGS_FORMAT;
  readonly blobCodec: "json" | "deflate" | "zdict";
  readonly sectionHandles: string; readonly reachDigest: string; readonly ruleIdsSha256: string; readonly storeDigest: string;
  readonly answersSha256: string;
  readonly versions: { readonly [K in SectionName]: number };
  readonly glossaryVersion: number;
  readonly layout: { readonly top: readonly string[]; readonly sections: Readonly<Record<SectionName, readonly string[]>> };
  readonly counts: Readonly<Record<string, number>>;   // meta.counts: akeys 2268, cells 9175, items 19641, parts 20461, …
}
export interface RegsStore {
  readonly db: Db;
  readonly meta: RegsMeta;
  /** zdict per table (loaded once at open; 23 rows, ~640 KB). */
  dict(table: string): Uint8Array | null;
  /** unpackBlob with the table's dictionary. */
  val<T = unknown>(table: string, blob: Uint8Array): T;
  close(): void;
  // …T04 adds the read methods (see T04).
}
export async function openRegsStore(db: Db, expect: RegsExpect): Promise<RegsStore>;
export function asBytes(c: import("../bundle/db").Cell): Uint8Array;   // Uint8Array as is; ArrayBuffer wrapped; else RegsStoreError("damaged")

// app/packages/data/src/bundle/drivers/node.ts (additions)
/** The province regs store `python -m pipeline.deliver store` writes (not in git). REGS_STORE overrides (a side build). */
export const DEV_REGS: string = process.env.REGS_STORE
  ?? fileURLToPath(new URL("../../../../../../data/generated/bundle/regs.sqlite", import.meta.url));
export const hasProvinceRegs = (): boolean => existsSync(DEV_REGS);
/** The regs fixture T10 writes (in git). */
export const DEV_REGS_FIXTURE: string = fileURLToPath(new URL("../../../dev/regs.sqlite", import.meta.url));
export function openRegsDb(path: string = DEV_REGS): Db { return openBundle(path); }
/** An in-memory regs store from `pipeline/deliver/store/schema.sql` (read here: only drivers may use node:fs) + seed SQL,
 *  for tests that need a shape the built store does not have (a refused format, a bumped version). */
export const REGS_SCHEMA: string = fileURLToPath(new URL("../../../../../../pipeline/deliver/store/schema.sql", import.meta.url));
export function openRegsInMemory(seedSql: string): Db { return openInMemory(readFileSync(REGS_SCHEMA, "utf8"), seedSql); }
```
(`node.ts` is at `app/packages/data/src/bundle/drivers/`; six `../` reach the repo root — `DEV_BUNDLE` uses three to reach `packages/data/`.)
`node.ts` today imports only `existsSync` from `node:fs` (self-check 2026-10-20): change it to `import { existsSync, readFileSync } from "node:fs";`.

`BundleInfo` edit (`app/packages/data/src/index.ts`, after `reachDigest`):
```ts
  /** The bundle's `meta.rule_ids_sha256`: the export rule list its rule sets index. The regs store must carry the same — a
   *  store from another export numbers rules differently, and every rix it names would be another rule. null: not held to it. */
  ruleIdsSha256: string | null;
```
`source.ts` `info()`: `…, reachDigest: meta.get("reach_digest") || null, ruleIdsSha256: meta.get("rule_ids_sha256") || null };`
`fixture.ts` `info()`: `…, sectionHandles: "fixture-local", reachDigest: null, ruleIdsSha256: null };`

`vitest.config.ts` alias — insert before `{ find: "@app/data", replacement: pkg("data") }` (string aliases match by prefix, so
`@app/data` would otherwise swallow `@app/data/regs`):
```ts
  { find: "@app/data/regs/node", replacement: pkg("data", "src/bundle/drivers/node.ts") },
  { find: "@app/data/regs", replacement: pkg("data", "src/regs/index.ts") },
```

`openRegsStore` sequence (each failure throws `RegsStoreError`, message ends "— refused"):
1. `SELECT k, v FROM meta` → Map. Missing `format` → `("missing", "regs store: no meta.format — refused")`.
2. `format !== REGS_FORMAT` → `("format", "regs store: format regs-store/3, this app reads regs-store/4 — refused")`.
3. `blob_codec ∉ {json, deflate, zdict}` → `("codec", …)`.
4. `layout = JSON.parse(meta.layout)`; for each `SectionName`, `layout.versions[name] !== KNOWN_VERSIONS[name]` →
   `("version", "regs store: rows section version 27, this app reads 26 — refused")` (name every mismatching section, comma-joined).
5. `SELECT tbl, d FROM zdict` → Map (all 23; `d` through `asBytes`).
6. `SELECT j FROM static WHERE k = 'answers.glossary'` → `val("static", j).version !== 5` → `("version", "regs store: glossary version 4, this app reads 5 — refused")`.
7. digests, in this order, only where `expect.x !== null`: `section_handles` vs `expect.sectionHandles`
   (`"regs store: built for atlas d768d6e135e62b99, the map is fixture-local — refused"`), `reach_digest` vs `expect.reachDigest`
   (`"… cut from rule bindings X, the bundle's are Y — refused"`), `rule_ids_sha256` vs `expect.ruleIdsSha256`
   (`"… numbers rules for export X, the bundle's are Y — refused"`). Wording mirrors `decodeStatusIndex` (core/src/statusIndex.ts:104–111).
8. Return the store; `close()` → `db.close?.()`.
`meta.store_digest` is NOT recomputed (Python `Store(verify=True)` hashes every row — too slow on a phone; the build already
proved it). The digests in 7 are the pairing guarantee.

## Ports
`pipeline/deliver/store/decode.py` `Store.__init__` (:23–34: format check, zdict load), `Store.static` (:51–55);
`core/src/statusIndex.ts` `decodeStatusIndex` refusal pattern (:94–111); `ui/src/hooks.ts` `loadStatusIndex` null-means-not-held (:521–541).

## Reads
`meta.{format, blob_codec, layout, section_handles, reach_digest, rule_ids_sha256, store_digest, answers_sha256, counts}`,
`zdict(tbl, d)`, `static('answers.glossary').version`.

## Tests
`app/packages/data/src/regs/store.test.ts` (project `logic`).

Province (`it.skipIf(!hasProvinceRegs())`, `openRegsDb()`):
1. `openRegsStore(db, {sectionHandles:"d768d6e135e62b99", reachDigest:"cad1c21fe6eb9ba9", ruleIdsSha256:"16be720be08b0fca"})`
   resolves; `meta.versions` deep-equals `{ladder:1, rows:26, gear:12, licence:6, display:16}`; `meta.glossaryVersion === 5`;
   `meta.blobCodec === "zdict"`; `meta.storeDigest === "90a32c3ccedbcfb0"`; `meta.counts.akeys === 2268`; `dict("display_frame")!.length === 31462`.
2. all three expectations `null` → resolves (the fixture pairing).
3. wrong digests, one at a time → rejects `RegsStoreError` with `why === "digest"` and the message naming both values:
   `sectionHandles:"fixture-local"`; `reachDigest:"0000000000000000"`; `ruleIdsSha256:"ffffffffffffffff"`.
4. Pair with the real bundle: `makeBundleSource(openBundle(<repo>/data/generated/bundle/bundle.sqlite)).info()` (skip if absent)
   → `{sectionHandles:"d768d6e135e62b99", reachDigest:"cad1c21fe6eb9ba9", ruleIdsSha256:"16be720be08b0fca"}`; feed it to `openRegsStore` → resolves.

Store-free (always run) — an in-memory store via `openRegsInMemory(seedSql)` (node.ts; the test itself imports no `node:*`, which
`layers.json` forbids outside `bundle/drivers` even in tests) seeded with `meta` + one `static` row (raw-JSON blob, codec `json`):
5. seed `format='regs-store/4'`, `blob_codec='json'`, `layout='{"top":[],"sections":{},"versions":{"ladder":1,"rows":26,"gear":12,"licence":6,"display":16}}'`,
   digests `a`,`b`,`c`, `static('answers.glossary', '{"version":5,"terms":[]}')` → resolves with expect `{a,b,c}`.
6. same with `format='regs-store/3'` → `why === "format"`, message `"regs store: format regs-store/3, this app reads regs-store/4 — refused"`.
7. `versions.rows = 27` → `why === "version"`, message contains `"rows section version 27, this app reads 26"`.
8. glossary `{"version":4,…}` → `why === "version"`, message contains `"glossary version 4"`.
9. `blob_codec='brotli'` → `why === "codec"`.
10. no `meta` rows → `why === "missing"`.
`app/packages/data/src/bundle/source.test.ts`: extend the existing `info()` case — the fixture bundle (`DEV_BUNDLE`) reports
`ruleIdsSha256: null` (it has no `rule_ids_sha256` row) and `reachDigest: null` (`reach_digest = ''`).

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/data/src/regs/store packages/data/src/bundle/source
```

## Notes
- CONTRACT CHANGE: applied to contracts.md (§3 data layer `BundleInfo.ruleIdsSha256`; header `versions.ts`). `loadStatusIndex` is unchanged.
- The regs node helper lives in `bundle/drivers/node.ts` because `layers.json` lets ONLY `packages/data/src/bundle/drivers` name
  `node:sqlite`/`node:fs`; `@app/data/regs/node` is an alias for that same file (no second driver). The store is generic SQLite, so
  no new driver is needed on any platform: web/native open it with `loadBundle(REGS)` (T11).
- Why refuse on versions rather than "best effort": a rows-26 presenter reading rows-27 frames draws plausible wrong numbers
  (the base+deltas invariant in AGENTS). The refusal is the same contract as the prototype's `PAIR_ERR` (page_v36 :11–14).
- The fixture bundle's `section_handles` is `fixture-588ceed04ef45f27` in its meta and `"fixture-local"` from `makeFixtureSource`;
  neither equals a regs store's — the dev "fixture set" therefore never shows Regulations (by design, T01 note).
