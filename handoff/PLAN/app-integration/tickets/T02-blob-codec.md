# T02 — blob codec (fflate dict-inflate) + UTF-8 decode

Status: DONE — 53ea8298.

Phase 0 · Depends on T00 · Blocks T03, T04 · Decisions — · Size ½ day

## Goal
`unpackBlob()` turns any `j`/`x` blob of the regs store into its JSON value exactly as `pipeline/deliver/store/common.py`
`unpack` + `json.loads` do, on every platform (Hermes has no `TextDecoder`). fflate becomes a declared dependency of `@app/data`.

## Files
- modify `app/packages/data/package.json` — `"dependencies": { "@app/core": "workspace:*", "fflate": "0.8.3" }`
- modify `app/deps.md` — add after the `sql.js` / `@types/sql.js` lines:
  ``- `fflate` — inflates the regs store's blobs (raw DEFLATE with a per-table preset dictionary, `meta.blob_codec = zdict`);
  ~8 KB min+gz for inflate only, pure JS so it runs on Hermes, the web and node alike. Already in the tree through `pmtiles`
  (0.8.3); declared here because `@app/data` imports it directly. zlib preset dictionaries are why `DecompressionStream`
  (no dictionary option, absent on Hermes) cannot do it.``
- modify `app/layers.json` — `layers["packages/data"].mayImport`: `["@app/core"]` → `["@app/core", "fflate"]`
- create `app/packages/data/src/regs/blob.ts`
- create `app/packages/data/src/regs/blob.test.ts`
- run `pnpm -C app install` (lockfile: `fflate` moves from transitive to a direct dep of `@app/data`; same version, no download)

## Signatures
```ts
// app/packages/data/src/regs/blob.ts
export class RegsBlobError extends Error {}
/** Byte 0: 0x01 raw DEFLATE (wbits -15); 0x02 raw DEFLATE with `dict`; anything else is the JSON text itself
 *  (a JSON text never starts with 0x01/0x02 — store/schema.sql header). Throws RegsBlobError on 0x02 with dict null,
 *  an empty blob, or bytes that are not JSON. */
export function unpackBlob(b: Uint8Array, dict: Uint8Array | null): unknown;
/** The inflated bytes (exported for the codec test and T04's diagnostics). */
export function inflateBlob(b: Uint8Array, dict: Uint8Array | null): Uint8Array;
/** UTF-8 → string without TextDecoder (Hermes). Chunked: never String.fromCharCode(...hugeArray). */
export function utf8(b: Uint8Array): string;
```

`utf8` algorithm (the store's largest inflated blob is rows_row ~18.6 KB, gear frames ~5 KB, statics up to ~200 KB):
```ts
export function utf8(b: Uint8Array): string {
  const parts: string[] = []; let units: number[] = [];
  for (let i = 0; i < b.length;) {
    const c = b[i++]!;
    let cp: number;
    if (c < 0x80) cp = c;
    else if (c < 0xe0) cp = ((c & 0x1f) << 6) | (b[i++]! & 0x3f);
    else if (c < 0xf0) cp = ((c & 0x0f) << 12) | ((b[i++]! & 0x3f) << 6) | (b[i++]! & 0x3f);
    else cp = ((c & 0x07) << 18) | ((b[i++]! & 0x3f) << 12) | ((b[i++]! & 0x3f) << 6) | (b[i++]! & 0x3f);
    if (cp > 0xffff) { cp -= 0x10000; units.push(0xd800 + (cp >> 10), 0xdc00 + (cp & 0x3ff)); }
    else units.push(cp);
    if (units.length >= 8192) { parts.push(String.fromCharCode(...units)); units = []; }
  }
  parts.push(String.fromCharCode(...units));
  return parts.join("");
}
```
(Same decoding as `core/src/statusIndex.ts:66` `utf8`, which is private and per-code-point `+=` — fine for ids, quadratic-ish for
200 KB statics. Do not import it; core keeps its own.)

`inflateBlob`:
```ts
import { inflateSync } from "fflate";
const head = b[0];
if (b.length === 0) throw new RegsBlobError("regs store: an empty blob");
if (head === 0x01) return inflateSync(b.subarray(1));
if (head === 0x02) { if (!dict) throw new RegsBlobError("regs store: a dictionary-deflated blob in a table with no dictionary");
                     return inflateSync(b.subarray(1), { dictionary: dict }); }
return b;
```
`unpackBlob = JSON.parse(utf8(inflateBlob(b, dict)))` wrapped so a parse error becomes `RegsBlobError("regs store: a blob is not JSON")`.

## Ports
`pipeline/deliver/store/common.py` `unpack` (:81–91), `DEFLATED, DEFLATED_DICT = b"\x01", b"\x02"` (:51); `decode.Store.val` (decode.py :48–49).

## Reads
Blob columns (`common.py` `BLOB_COLUMNS` :56–65): `static.j`, every frame family `.j`, `item.x`, `xpart.j`, `picker.j`, `runs.j`,
`rule.j`, `lic.j`, `entry.j`, `ruleset.j`, `lset.j`. Dictionaries: `zdict(tbl, d)` — 23 rows, one per table above (except `str`);
sizes 1,410 B (`licence_frame`) … 32,768 B.

## Tests
`app/packages/data/src/regs/blob.test.ts` (project `logic`).

Store-free (always run):
1. raw JSON: `unpackBlob(new TextEncoder().encode('{"a":1}'), null)` → `{a:1}` (TextEncoder is fine in a node test).
2. 0x01: hex `01ab562a2e492c292d56b252ca2fcf53d2514acec92fcecc4b57b28a8ed501ca15651628591919d40200` → `{status:"own",closing:[],strip:20}`
   (made with `fflate.deflateSync(raw, {level: 9})`; Python `zlib.decompressobj(-15)` gives the same bytes).
3. UTF-8: hex `5b2252697669c3a8726520e2809320e2809c6f6be2809d20f09f909f225d` → `["Rivière – “ok” 🐟"]` (2-, 3- and 4-byte sequences, a surrogate pair).
4. `utf8` of 100,000 × `"é"` (200,000 bytes) has length 100,000 and does not throw (chunking).
5. refusals: `unpackBlob(new Uint8Array([2, 0x03, 0x00]), null)` throws `RegsBlobError` /no dictionary/; `unpackBlob(new Uint8Array([]), null)`
   throws /empty blob/; `unpackBlob(new Uint8Array([0x78]), null)` throws /not JSON/.

Province store (`it.skipIf(!hasProvinceRegs())`; open with `openBundle(DEV_REGS)` from `../bundle/drivers/node` — T03 adds
`DEV_REGS`/`hasProvinceRegs`; until T03 lands use the literal path `data/generated/bundle/regs.sqlite` resolved from the repo root).
For each row: `SELECT j FROM <t> WHERE <k> = ?` and `SELECT d FROM zdict WHERE tbl = '<t>'`, then `JSON.stringify(unpackBlob(j, d))`
starts with the expected prefix (all are `0x02` blobs — every blob in the built file is, facts.md #4):

| table | key | blob bytes → inflated | expected prefix of `JSON.stringify(value)` |
|---|---|---|---|
| display_frame | id 64 | hex `0223F6D25B00` (6 → 40) | `{"status":"own","closing":[],"strip":20}` (whole) |
| licence_frame | id 1 | hex `028316BD00` (5 → 7) | `[1,1,0]` (whole) |
| ladder_frame | id 1057 | 29 → 262 | `[3973,[[0,0],[1,1],[2,2],[3,3],[4,3971],` |
| rows_frame | id 1723 | 29 → 315 | `[["RB","CT","LMB","SMB","BCB","CRA","KO","LW","MW","WSG","ST","GB","DV","LT","EB"],{"RB":[3820,3821]` |
| gear_frame | id 320 | 487 → 5101 | `{"counts":{"bait_possession_kg":{"by":[3322,0],"over":[],"cites":[3322]}` |
| display_rule | id 529 | 21 → 312 | `{"kind":"gate","title":"No fishing, Jun 1–30","closure":true,"plain":"No fishing, Jun 1–30.","book":[[22,24]]` |
| rule | rix 529 | 139 → 844 | `{"entry_id":"r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4","rule_id":"chilliwack_vedder` |
| ladder_verdict | id 0 | 9 → 70 | `[[3079,3333,3339,3355,3356,3359],[],[3351,3352],[],[],[[3364,1,3079]]]` (whole) |
| rows_decided | id 0 | 20 → 160 | `{"status":"keep","win":3083,"daily":20,"narrow":null,"lines":[]` |
| rows_row | id 0 | 156 → 18,606 | `{"kind":"keep","pool":3091,"win":null,"members":["EB","GB","CT","DV","LT","RB","ST"]` |
| licence_hold | id 0 | 8 → 230 | `{"holds":[131,96,98,106,110,111,128],"designations":[]` |
| licence_answer | id 0 | 20 → 1179 | `{"documents":[{"doc":"basic_licence"` |
| licence_doc | id 0 | 7 → 121 | `[0,1,0,2,3,0,1,0,2,3,4,5` |
| licence_u16 | id 0 | 22 → 1376 | `[{"residency":"resident","label":"B.C. resident","none_needed":true` |
| picker | id 0 | 8 → 134 | `{"choices":[{"parts":[0],"closed":false,"sections":1` |
| xpart | id 0 | 7 → 81 | `{"ruleset":"1","licensing_set":"0","steelhead":"possible","steelhead_rules":true}` (whole) |
| runs | id 0 | 10 → 59 | `[{"from":"source","to":"mouth","km_from":1.46,"km_to":0}]` (whole; Python prints `0.0`, JSON.stringify `0`) |
| ruleset | set_id 393 | 32 → 523 | `{"sections":5,"reach":[525,526,527,528,529,530,3077` |
| static | k `licence.profile_dims` | — | `[["residency",["resident","non_resident","non_resident_alien"]],["age",["16_plus","under_16"]]` |

Plus one sweep: every row of `display_frame` (1,027) and `display_rule` (3,406) unpacks without throwing (< 2 s in node).

## Acceptance
```sh
pnpm -C app install --frozen-lockfile=false && git diff --stat app/pnpm-lock.yaml   # fflate listed under packages/data
pnpm -C app check                                                                  # deps + boundaries see fflate
pnpm -C app exec vitest run --project logic packages/data/src/regs/blob
```

## Notes
- fflate 0.8.3's `inflateSync(data, { dictionary })` is what Python's `zlib.decompressobj(-15, zdict=zd)` reads — verified
  2026-10-20 in node on every row in the table above. `inflateSync` is RAW inflate (no zlib header) — do not use `unzlibSync`.
- `Uint8Array` blobs: node:sqlite and sql.js return `Uint8Array`; expo-sqlite returns `Uint8Array` too. If a driver returns
  `ArrayBuffer`, `Db` callers wrap with `new Uint8Array(x)` in T04 (`asBytes(cell)`), not here.
- Keep `utf8` in data (not core): core's statusIndex has its own private copy by design (facts.md §B); a shared helper would make
  core export a byte utility it does not otherwise need.
