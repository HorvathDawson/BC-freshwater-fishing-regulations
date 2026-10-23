# T04 — store reads: items, parts, picker, akey, segments, cells, frame codecs, rules, statics, section blocks

Status: DONE — bb28757f, d6d3ee0e.

Phase 0 · Depends on T02, T03 · Blocks T05 (parity test), T09, T10 · Decisions — · Size 1 day (if it overruns: T04a = item/part/
akey/seg/cell/section reads; T04b = the five frame codecs + rules + statics)

## Goal
`RegsStore` answers every question the page asks of `regs.sqlite`, each as one or two indexed SQL reads, returning the
answers-file values exactly as `pipeline/deliver/answers/encode.py`'s codecs decode them (`decode_frame`), so the TS side
sees the same `RowsValue`/`LicenceValue`/`StatusFrame` the Python reference and page v36 see. The per-family interning is
undone here, never in core.

## Files
- modify `app/packages/data/src/regs/store.ts` (the object `openRegsStore` returns)
- create `app/packages/data/src/regs/reads.ts` (row mappers + codecs, pure over `RegsStore.val`)
- create `app/packages/data/src/regs/reads.test.ts`

## Signatures
```ts
// added to RegsStore (store.ts). Every method is async (Db is).
export interface ItemRow {
  readonly ord: number; readonly id: string; readonly name: string; readonly kind: string;
  readonly shown: boolean; readonly picker: number | null; readonly sections: number; readonly outsideBc: number;
  readonly steelhead: 1 | 2 | null; readonly entries: readonly number[]; readonly x: Record<string, unknown> | null;
}
export interface PartRow {
  readonly itemOrd: number; readonly partIx: number; readonly akey: number | null;
  readonly facts: PartFacts | null;            // null when part.ord IS NULL (outside B.C.) — decode.py :103–110 shape
  readonly sections: number;
}
export interface PlaceRow { readonly place: number; readonly regions: readonly string[]; readonly label: string; readonly names: PlaceNames }
export interface RulesetRow { readonly sections: number; readonly reach: readonly number[]; readonly trib: readonly number[] }
export interface StoredRule { readonly rix: number; readonly export: ExportRule; readonly facts: RuleFacts }

interface RegsStore {                     // (additions)
  itemOrd(itemId: string): Promise<number | null>;
  item(itemOrd: number): Promise<ItemRow>;
  parts(itemOrd: number): Promise<readonly PartRow[]>;
  picker(id: number): Promise<Picker>;
  akey(akey: number): Promise<AkeyRow>;
  segStarts(seg: number): Promise<readonly number[]>;
  segMoments(mom: number): Promise<readonly Moment[]>;          // segmom.ix → statics.moments[i]
  cell(akey: number, s: number): Promise<CellIds>;
  cells(akey: number): Promise<readonly CellIds[]>;            // every segment of the key, s = index
  ladder(id: number): Promise<LadderFrameWire>;
  rows(id: number): Promise<RowsValue>;
  gear(id: number): Promise<GearValue>;
  licence(id: number): Promise<LicenceValue>;
  display(id: number): Promise<DisplayFrame>;               // StatusFrame | TidalStatus (tidal keys: status "tidal", e.g. x31p_1010 display 930)
  ruleset(setId: number): Promise<RulesetRow>;
  rules(): Promise<ReadonlyMap<number, StoredRule>>;          // all 3,406, once
  lics(): Promise<ReadonlyMap<number, unknown>>;              // all 135, once
  statics(): Promise<Statics>;                                // all 25 static rows, once
  str(id: number): Promise<string>;
  sectionAkey(handle: number): Promise<number>;               // 0 = outside B.C.
  sectionPlace(handle: number): Promise<number | null>;       // place index; null = outside B.C.
  place(ix: number): Promise<PlaceRow>;
  unnamedName(handle: number): Promise<string | null>;
}
```

### SQL (verbatim; `?` params; every read is by PRIMARY KEY except where noted)
| method | SQL |
|---|---|
| itemOrd (map built once, lazily) | `SELECT i.item_ord AS o, s.text AS id FROM item i JOIN str s ON s.id = i.item_id` → `Map<id, ord>` (19,641 rows; `item.item_id` is a str ref with no index — facts.md #7) |
| item | `SELECT item_ord, item_id, shown, picker, name, kind, sections, outside_bc, entries, steelhead, x FROM item WHERE item_ord = ?` (+ `str(item_id)`, `str(kind)`, `x` via `val("item", x)` when not null, `entries` JSON text) |
| parts | `SELECT part_ix, akey, ord, label_s, short_s, runs_s, place_s, hint_s, km, closed_all_year, paper_licence, sections FROM part WHERE item_ord = ? ORDER BY part_ix` → `facts = ord == null ? null : {order: ord, label: str(label_s), short: str(short_s), runs: str(runs_s), place: place_s == null ? null : str(place_s), hint: str(hint_s), km, closed_all_year: !!closed_all_year, paper_licence: JSON.parse(paper_licence)}` |
| picker | `SELECT j FROM picker WHERE id = ?` → `val("picker", j)` |
| akey | `SELECT akey, ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, kind, tidal, seg, mom, nseg FROM akey WHERE akey = ?` → `AkeyRow` (`kind` via `str`, booleans from 0/1) |
| segStarts | `SELECT starts FROM seglist WHERE seg = ?` → `JSON.parse` |
| segMoments | `SELECT ix FROM segmom WHERE mom = ?` → `JSON.parse(ix).map(i => statics.moments[i])` |
| cell | `SELECT ladder, rows, gear, licence, display FROM cell WHERE akey = ? AND s = ?` |
| cells | `SELECT s, ladder, rows, gear, licence, display FROM cell WHERE akey = ? ORDER BY s` (assert `s` = 0..nseg-1, else `RegsStoreError("damaged")`) |
| family blob | `SELECT j FROM <table> WHERE id = ?` (table from the fixed list below — never interpolate caller input) |
| ruleset | `SELECT j FROM ruleset WHERE set_id = ?` → `{sections, reach, trib: trib ?? []}` |
| rules | `SELECT r.rix, r.j AS e, d.j AS f FROM rule r JOIN display_rule d ON d.id = r.rix ORDER BY r.rix` (assert rix = 0..3405) |
| lics | `SELECT lix, j FROM lic ORDER BY lix` |
| statics | `SELECT k, j FROM static` (25 rows) |
| str | `SELECT text FROM str WHERE id = ?` (cached Map) |
| sectionAkey | `SELECT b FROM section_akey WHERE block = ?` with `block = handle >> 12`; value = `b[2*(h & 4095)] \| b[2*(h & 4095)+1] << 8`; no row → 0 |
| sectionPlace | `SELECT b FROM section_place WHERE block = ?`; v = `b[h & 4095]`; `v ? v - 1 : null`; no row → null |
| place | `SELECT place, regions, label, names FROM place WHERE place = ?` (`regions`/`names` JSON text, `label` str ref) |

### Codecs (in `reads.ts`; each mirrors `pipeline/deliver/answers/encode.py` @04765ab5)
```ts
const FAMILY = { ladderFrame: "ladder_frame", ladderVerdict: "ladder_verdict", rowsFrame: "rows_frame",
  rowsDecided: "rows_decided", rowsRow: "rows_row", gearFrame: "gear_frame", licenceFrame: "licence_frame",
  licenceHold: "licence_hold", licenceDoc: "licence_doc", licenceAnswer: "licence_answer", licenceU16: "licence_u16",
  displayFrame: "display_frame" } as const;

// LadderCodec.decode_frame (:664–674) — but RESOLVED to wire verdicts, not to rule-id dicts: core T07 reads them as page_v36 verdictList does.
//   blob = [commonVerdictId, [[fishIx, v] | [fishIx, vNone, vHatchery, vWild]]]
export async function decodeLadder(st: RegsStore, id: number): Promise<LadderFrameWire>;
//   → { common: verdict(commonVerdictId), rows: rows.map(r => [statics.fish[r[0]], …(r.length === 2 ? [v,v,v] : r.slice(1)).map(verdict)]) }
//   verdict(i) = val("ladder_verdict", …) = [speaks, beside, shown, nym, partly[[rix,[by…]]], lost[[rix, reasonIx, byRix]]]

// RowsCodec.decode_frame (:720–727): blob = [spp, {S: [h|null, w|null]}, [rowIds], steelhead_line]
export async function decodeRows(st: RegsStore, id: number): Promise<RowsValue>;
//   → { spp, fish: {S: {hatchery: h == null ? null : decided(h), wild: w == null ? null : decided(w)}}, rows: rowIds.map(row), steelhead_line }

// LicenceCodec.decode_frame (:744–750): blob = [h, d, u?]
export async function decodeLicence(st: RegsStore, id: number): Promise<LicenceValue>;
//   → { holds: hold(h), profiles: doc(d).map(answer) /* 60 */, ...(u !== undefined ? { under_16: u16(u) } : {}) }

// FrameCodec (:690–704): gear and display blobs ARE the frame.
export async function decodeGear(st: RegsStore, id: number): Promise<GearValue>;
export async function decodeDisplay(st: RegsStore, id: number): Promise<DisplayFrame>;
```
The `rows` decode is the page's `SEC.rows.frames`/`decided`/`rows` (page_v36 `rowsAt` :216–222) pre-joined; core T08 still
hydrates rule refs (`R(ix)`). Decided/row/verdict sub-blobs are shared across frames — cache them by id too.

### Statics mapping (`Statics` in core `inputs.ts`)
| field | static k |
|---|---|
| glossary | `answers.glossary` | fish | `answers.fish` (36 codes, the ladder's fish index) |
| moments | `answers.moments` | unnamed | `answers.unnamed` (`{first: 2095, parts}`) |
| strip | `display.strip` | reasons | `display.reasons` | displayLicensing | `display.licensing` |
| countNouns | `gear.count_nouns` | gearMoments | `gear.moments` | conductMeans | `gear.conduct_means` |
| gearMethods / gearParent / provinceMethods | `gear.methods` / `gear.parent` / `gear.province_methods` |
| profiles / profileDims / profileLabels / residencyLabels | `licence.profiles` / `licence.profile_dims` / `licence.profile_labels` / `licence.residency_labels` |
| exportSpecies / exportLicences / exportConduct | `export.species` / `export.licences` / `export.conduct` |
| ladderReasons / ladderReasonState | `ladder.reasons` / `ladder.reason_state` |
(`answers.about`, `answers.spec`, `answers.schemas` are not loaded into `Statics` — T03 reads what it needs.)

### Caching policy
- Once per store (lazy, first use): `zdict` (at open, T03), `statics()`, `rules()` (3,406 rule + display_rule blobs, inflated
  ~2.5 MB of JS objects), `lics()`, the `itemOrd` map, `str` hits.
- Per id, unbounded `Map` per family (frames, verdicts, decided, rows_row, licence sub-tables, picker, ruleset, akey, seglist,
  segmom, section blocks): ids are immutable within a store; a session touches a few hundred. `close()` drops every cache.
- Concurrency: cache the PROMISE, not the value, so two concurrent taps share one read.

## Ports
`pipeline/deliver/store/decode.py` `Store.answers` (:57–151: akey row → key tuple, part columns → display part), `export_subset`
(:160–206: rule ids `entry_id::rule_id`, rulesets), `_cell`/`place_of`/`unnamed_name`/`akey_of` (:209–239);
`pipeline/deliver/answers/encode.py` `LadderCodec.decode_frame`/`decode_verdict` (:664–688), `RowsCodec.decode_frame` (:720–727),
`LicenceCodec.decode_frame` (:744–750), `FrameCodec.decode_frame` (:702–704); `store/common.py` `BLOCK_BITS = 12` (:197).

## Reads
All of the tables above. Contracts.md §3 store mapping rows: item, parts, key, frames, rules, unnamed, blobs.

## Tests
`app/packages/data/src/regs/reads.test.ts` (project `logic`, `it.skipIf(!hasProvinceRegs())`, one `openRegsStore(openRegsDb(), nulls)` per file).
Expected values from `app/design/regs-v11/port/golden/taps.json` @04765ab5 and the store (verified 2026-10-20):
1. `itemOrd("gnis:3062")` → 7670; `itemOrd("wbk:-22")` → 11620; `itemOrd("nope")` → null. `item(7670).name` → "Vedder River", `.kind` → "stream".
2. `parts(7670)` → one part: `{partIx:0, akey:886, sections:5, facts:{order:0, label:"From the mouth up to the Vedder Crossing Bridge",
   short:"Mouth → Vedder Crossing Bridge", km:12.95, closed_all_year:false, paper_licence:[3368,3384]}}`.
3. `akey(886)` → `{akey:886, ruleset:393, licensingSet:2, steelheadWater:true, steelhead:1, steelheadRules:true, kind:"stream", tidal:false, seg:74, mom:null, nseg:4}`;
   `segStarts(74)` → `[1,122,153,183]`.
4. `akey(1229)` (klwa) → `seg:3, mom:21, nseg:2, licensingSet:null, kind:"lake"`; `segStarts(3)` → `[1,1]`; `segMoments(21)` →
   `[{weekdays:["Monday","Tuesday","Wednesday","Thursday","Friday"],hours:null},{weekdays:["Saturday","Sunday"],hours:null}]`.
5. `cell(886, 1)` → `{ladder:1057, rows:1723, gear:320, licence:1, display:64}` (taps vedder_515); `cell(886, 2)` → `{2457,1724,146,1,305}`;
   `cells(886).length === 4`.
6. `display(64)` → `{status:"own", closing:[], strip:20}`; `display(305).status` → "closed" and `closing[0][0] === 529`.
7. `rows(1723)`: `spp` = `["RB","CT","LMB","SMB","BCB","CRA","KO","LW","MW","WSG","ST","GB","DV","LT","EB"]`, `steelhead_line` = "known_with_rules",
   `rows.length === 8`, `rows.map(r => r.kind)` = `["keep","release","keep","keep","keep","release","keep","closed"]`, `rows[0].pool === 3091`,
   `rows[0].daily === 2`, `fish.LMB.hatchery.status === "keep"` and `.daily === 20` and `.win === 3083`; `fish.RB.hatchery.win === 527`, `fish.RB.wild.win === 3098`.
   (Equals page v36's `rowsAt` on vedder_515 — the hydrate oracle T06 generates.)
8. `ladder(1057)`: `common` = `verdict(3973)`; `rows[4][0]` = `statics.fish[4]` = "CT" and its three wire verdicts are verdict 3971 (2-element row);
   every row's fish code ∈ `statics.fish`.
9. `licence(1)`: `profiles.length === 60`; `holds` has key `holds`; `under_16` present (3 residencies).
10. `gear(320)`: has `summary`, `counts.bait_possession_kg.by` = `[3322, 0]`.
11. `rules()`: size 3406; `get(529).export` → `entry_id "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4"`, `rule_id "chilliwack_vedder_rivers.r8"`;
    `get(529).facts.kind === "gate"`, `.closure === true`, `.book` = `[[22,24]]`; `get(3092).facts.kind === "sizecap"`.
12. `ruleset(393)` → `sections:5`, `reach.length === 101`, `trib` = `[]`, `reach.includes(529)`.
13. Unnamed (taps.json `unnamed[]`, all 20): `sectionAkey(h) === akey`, `sectionPlace(h) === place`, `unnamedName(h) === name`. Spelled out:
    handle 1178240 → block 287, akey byte offset 5376, place byte 2688 → akey **2191**, place **12**, `"Unnamed stream · Region 5"`;
    handle 634 → 2097, 0, `"Unnamed stream · Region 1"`; handle 1251183 → 2226, 9, `"Unnamed wetland · Region 4"`.
    `sectionAkey(2_000_000_000)` → 0 and `sectionPlace` → null (absent block).
14. `statics()`: `profileDims` = `[["residency",["resident","non_resident","non_resident_alien"]],["age",["16_plus","under_16"]],["guidance",["non_guided","guided"]],["status",["none","indian_bc_resident","metis","disabled","aged_65_plus"]]]`;
    `profiles[0]` = "resident/16_plus/non_guided/none"; `fish.length === 36`; `glossary.version === 5`, `glossary.terms.length === 29`; `strip[1].label === "closed"`.
15. **Cell parity over taps.json**: for every one of the 134 states, `cell(state.akey, state.s)` deep-equals `state.cell` (T05 adds the segment
    resolution; here `s` is taken from taps.json).

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/data/src/regs/reads
```

## Notes
- CONTRACT CHANGE: applied to contracts.md (§3 `DisplayFrame`).
- CONTRACT CHANGE: applied to contracts.md (§3 `LadderFrameWire`, data-layer row types).
- `rules()` loads every rule rather than "the rules a frame cites": frames cite rix in dozens of places (decided `win`/`narrow`/
  `lines[].r|outer`/`roles`/`lift_notes`, row `pool`/`win`/`everyone`/`groups`/`conds`/`wins`, gear `by`/`cites`, display
  `closing`/`unless_opened`/`inherited.by`, ladder verdict lists), and a missed one turns into page v36's NORULE stand-in on screen.
  One-time cost measured in node: < 150 ms; the device budget is in T09.
- Section blocks: 478 blocks × (8 KB + 4 KB) — read one block per lookup, cache it; never load all.
- page v36's `A.keys[k]` index = `akey - 1` (decode.py :63–64); `A.parts[item][pi]` = `part.akey - 1`. Python's `STEELHEAD_NAME`
  maps 1/2 to "known"/"possible" — the app keeps the integers (contracts.md `AkeyRow.steelhead: 1 | 2 | null`).
