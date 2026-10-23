# T09 — `loadRegsInputs()`: the whole tap → `RegsInputs` (+ the year's frames, the merged parts)

Status: DONE — 2721902a.

Phase 0 · Depends on T04, T05, T06 · Blocks T07 (parity), T08 (parity), T10, T11, T12, T13–T21 · Decisions — · Size 1 day
(build order T06 → T09 → T07 → T08: this ticket needs `ruleIndex`/`viaOf` from T06, not `settle`)

## Goal
One async function reads everything the Regulations page needs for one tap — the water, its parts and picker, the part's key,
the day's segment and five frames, every segment's ladder/rows/display frames (the year), the per-day display frame over
the leap calendar, the part's rule views, licensing words, statics, and (for a closed-all-year picker group) the sibling
parts' closures — and returns a `RegsInputs` that core can turn into a page without another read. Also: which part a tapped
section opens.

## Files
- create `app/packages/data/src/regs/inputs.ts` (+ `export * from "./inputs"` in `data/src/regs/index.ts`)
- create `app/packages/data/src/regs/inputs.test.ts` (province; skipIf)
- modify `app/packages/core/src/regs/inputs.ts` — `RegsInputs` final shape (below)
- modify `app/conformance/src/regs/golden.ts` (created by T05) — add `requestOf(id)` and `inputsOf(id)` (tests.md §1.2 signatures)

## Signatures
```ts
// app/packages/core/src/regs/inputs.ts  (FINAL; replaces contracts.md §3 RegsInputs)
export interface MergedPart { readonly partIx: number; readonly label: string; readonly sections: number; readonly broad: RuleView | null }
export interface RegsYear {
  readonly segStarts: readonly number[];
  readonly moments: readonly Moment[] | null;
  readonly cells: readonly CellIds[];                     // index = s, length = key.nseg
  readonly ladderBySeg: readonly (LadderFrameWire | null)[];
  readonly rowsBySeg: readonly (RowsValue | null)[];
  readonly displayBySeg: readonly (DisplayFrame | null)[];
  readonly displayByDay: readonly (DisplayFrame | null)[]; // 366, leap calendar, index d-1; the SAME object for equal frame ids
}
export interface RegsInputs {
  readonly target: RegsTarget; readonly day: Day; readonly weekday: Weekday;
  readonly calendarYear: number;                            // the selected date's year: names every day's weekday (page YEAR)
  readonly item: { readonly id: string; readonly ord: number; readonly name: string; readonly kind: string; readonly sections: number;
                   readonly parts: readonly (PartFacts | null)[]; readonly partSections: readonly number[];
                   readonly picker: Picker | null; readonly steelhead: 1 | 2 | null } | null;
  readonly partIx: number;
  readonly place: { readonly label: string; readonly name: string } | null;   // an unnamed section: place label + names[kind]
  readonly key: AkeyRow | null;                             // null = outside B.C. (or an item with no rule set on this part)
  readonly s: number | null; readonly cell: CellIds | null;
  readonly frames: { readonly ladder: LadderFrameWire; readonly rows: RowsValue; readonly gear: GearValue;
                     readonly licence: LicenceValue; readonly display: DisplayFrame } | null;
  readonly year: RegsYear | null;
  readonly ruleset: { readonly reach: readonly number[]; readonly trib: readonly number[] } | null;
  readonly rules: RuleIndex;                                // T06: every store rule, viewed through THIS part's via/rank
  readonly lics: ReadonlyMap<number, unknown>;
  readonly statics: Statics;
  readonly mergedParts: readonly MergedPart[] | null;      // page mergedHtml: only when the part's picker group is closed and has ≥ 2 parts
}

// app/packages/data/src/regs/inputs.ts
export class RegsInputsError extends Error {}
export async function loadRegsInputs(store: RegsStore, target: RegsTarget, day: Day, weekday: Weekday,
                                     calendarYear: number): Promise<RegsInputs>;
/** The part a reader's tap opens: the first part of `item` whose akey is the tapped section's (section_akey), else the first
 *  part with a key, else 0. A section two parts share resolves to the lower part_ix (page v36 has no section → part map). */
export async function targetFor(store: RegsStore, item: string, section: number | null): Promise<RegsTarget>;
/** For the sheets/date pill: the display frame of every leap day for a key (exported for T13/T42 tests). */
export function displayByDay(year: Omit<RegsYear, "displayByDay">, calendarYear: number): (DisplayFrame | null)[];

// app/conformance/src/regs/golden.ts (additions)
export function requestOf(id: string): { target: RegsTarget; day: Day; weekday: Weekday; today: Day };
/** memoised per id per test file; null when provinceStore() is null (skip). calendarYear 2026 (taps.json meta.year). */
export function inputsOf(id: string): Promise<RegsInputs | null>;
```

### The read sequence (`kind: "part"`; every step awaits the store's cached promise — parallel where marked ∥)
1. ∥ `statics()`, `rules()`, `lics()` — once per store (T04 caches).
2. `ord = itemOrd(target.item)`; null → `RegsInputsError("regs: no water <id> in the regs store")`.
3. ∥ `item(ord)`, `parts(ord)`; then `picker(item.picker)` when not null. `partIx` out of range → `RegsInputsError`.
4. `part = parts[partIx]`; `part.akey == null` → return with `key/s/cell/frames/year/ruleset = null` (outside B.C.: contracts.md
   `status: "outside"`); `rules = ruleIndex(stored, new Map(), species)`.
5. `key = akey(part.akey)`; ∥ `segStarts(key.seg)`, `key.mom == null ? null : segMoments(key.mom)`, `cells(key.akey)`, `ruleset(key.ruleset)`.
6. `s = segmentIndex(starts, leapDay(day), moments, weekday, false)` (T05). `cell = cells[s]`.
7. ∥ for every DISTINCT id over `cells[*].ladder`, `.rows`, `.display`: `ladder(id)`, `rows(id)`, `display(id)`; and for `cell` only:
   `gear(cell.gear)`, `licence(cell.licence)`. `ladderBySeg[i] = L[cells[i].ladder]` etc. (dedupe: a key's segments often share frames).
8. `displayByDay`: for `d` in 1..366: `sd = moments == null ? segmentIndex(starts, d, null, null)
   : d === 60 ? daySegments(starts, 60)[0] : segmentIndex(starts, d, moments, weekdayOf(dayOfLeap(d), calendarYear), false)`;
   `displayByDay[d-1] = displayBySeg[sd]` (taps.json regenerated this way: each day at ITS OWN weekday; Feb 29 at its first moment).
9. `rules = ruleIndex(stored, viaOf(ruleset), statics.exportSpecies)` (T06).
10. `mergedParts`: `g = picker?.choices.find(c => c.parts.includes(partIx))`; if `g?.closed && g.parts.length >= 2`: for each `i` of
    `g.parts`: `p = parts[i]`; `p.akey == null` → `broad null`; else `k = akey(p.akey)`, `si = segmentIndex(segStarts(k.seg), leapDay(day),
    moms(k), weekday, false)`, `f = display(cell(k.akey, si).display)`, `Ri = ruleIndex(stored, viaOf(ruleset(k.ruleset)), sp)`,
    `broad = broadOf(f, Ri)[0] ?? null` (T07's `broadOf` — implement the 3-line sort here if T07 has not landed; T07 then replaces it);
    `{partIx: i, label: p.facts?.label ?? "", sections: p.sections, broad}`.
`kind: "section"`: `a = sectionAkey(handle)`; `a === 0` → outside (key null, place null, item null); else `pl = sectionPlace(handle)`,
`place(pl)`, `key = akey(a)`, `place = {label: placeRow.label, name: placeRow.names[key.kind]}` ("Unnamed stream · Region 5"), then
steps 5–9 with `item = null`, `partIx = 0`, `mergedParts = null`.

### Which rules are loaded
ALL 3,406 (T04 `rules()`, once per store), viewed through the part's `via` (T06 `ruleIndex`). Not "the rix any frame cites": the
cited set spans ladder verdicts (incl. `lost` `by`), decided `win/narrow/lines[].r|outer/roles/lift_notes`, rows `pool/win/narrow/
everyone/groups/conds/wins`, gear `by/cites/specs/main/circumstantial`, display `closing/unless_opened/inherited.by`, and the part's
ruleset (`makePlace.cands`) — a deep walk would have to know every schema; one miss is a NORULE on screen. `ruleIndex` builds views
lazily (`get(rix)` memoised), so the per-tap cost is the rules actually asked.

### Timing budget
Node (province store, M-series): cold first tap ≤ 300 ms (statics + rules ≈ 150 ms + item map 17 ms, measured 2026-10-20); warm tap
≤ 20 ms; all 134 golden states warm ≤ 3 s total. Device target (Hermes, mid Android): cold ≤ 1.2 s (prewarm `statics()`/`rules()`
in `useRegsStore`, T11), warm ≤ 150 ms. The test asserts only the node totals (×3 headroom).

## Ports
page_v36.js @04765ab5: `makePlace` 117–125 (the part's key `A.parts[id][pi]`, `A.keys[k]`, `DWATERS[id].parts[pi]`), `segStarts`
126–128, `momentsOf` 129–130, `segOf` 139–140, `frameIx` 142, `choicesOf`/`groupOf`/`partLabel`/`mergedHtml` 477–486;
`encode.py` `tap` (:944–957) / `tap_key` (:958–) — the Python tap this mirrors; `decode.py` `unnamed_name` (:220–232).

## Reads
Every T04 method. Contracts.md §3 store mapping (item, parts, key, s, frames, rules, unnamed, blobs).

## Tests
`app/packages/data/src/regs/inputs.test.ts` (project `logic`, `it.skipIf(!hasProvinceRegs())`, one store per file):
1. vedder_515: `loadRegsInputs(st, {kind:"part", item:"gnis:3062", partIx:0}, {month:5,day:15}, "Friday", 2026)` →
   `item.ord 7670`, `item.name "Vedder River"`, `item.kind "stream"`, `item.sections 5`; `key.akey 886`; `s 1`;
   `cell` `{ladder:1057, rows:1723, gear:320, licence:1, display:64}`; `year.cells.length 4`; `year.segStarts [1,122,153,183]`;
   `frames.display` `{status:"own", closing:[], strip:20}`; `year.displayByDay[166]` (day 167, Jun 15) `.status === "closed"`;
   `rules.get(529).rank === 0 && .via === "reach"`; `mergedParts === null`; `place === null`.
2. vedder_615 (`{6,15}`, "Monday") → `s 2`, cell `{2457,1724,146,1,305}`; vedder_1010 (`{10,10}`, "Saturday") → `s 3`, cell `{1056,1722,146,1,1}`.
3. klwa_1010 (`wbk:-22`, `{10,10}`, "Saturday") → `s 1`, cell `{3452,2381,433,2,111}`; klwa_1012 ("Monday") → `s 0`, cell `{3451,2380,433,2,111}`;
   `year.moments.length === 2`; `year.ladderBySeg.length === 2` and the two differ (3451 vs 3452 frames).
4. x15p7_1010 (`gnis:39325` part 7) → `s 4`, cell display 704; x31p_1010 (`wbk:329504244`) → `key.tidal === true`, `frames.display.status === "tidal"`.
5. ape_501: `rules.get(1644)` → via "trib", rank 1.
6. Every golden state (`taps.json`): `key.akey`, `s`, `cell` equal the state's; `year.displayByDay.map(f => id of f)` equals
   `displayByDay` (compare by the frame id: keep a `WeakMap<frame, id>` in the test or compare `cells[sd].display`).
7. `mergedParts`: chw0_1010 (closed all year, picker group of 2) → 2 entries, labels and `sections` summing to 8 (golden banner
   `"Closed all year on 2 stretches (8 sections)"`), `broad[0]` ranks: one `0` (This water, p.22), one `-1` (Federal / parks, p.9).
   States whose group is open → `null`.
8. Unnamed: `{kind:"section", handle:1178240}`, `{10,10}`, "Saturday" → `item null`, `key.akey 2191`, `key.kind "stream"`,
   `place {label:"Region 5", name:"Unnamed stream · Region 5"}`; handle 2_000_000_000 → `key null`, `place null`.
9. `targetFor(st, "gnis:3062", null)` → `{kind:"part", item:"gnis:3062", partIx:0}`; for cowichan (`cow1_1010`'s item) with a section
   handle taken from that part (`SELECT … FROM section_akey` is opaque — pick one with `sectionAkey(h) === parts[1].akey` from the
   bundle's `part_section`, or skip when no bundle) → `partIx 1`.
10. Errors: unknown item → `RegsInputsError /no water/`; partIx 99 → `RegsInputsError`.
11. Timing: 134 states warm in ≤ 3 s (node).

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/data/src/regs/inputs
```

## Notes
- CONTRACT CHANGE: applied to contracts.md (§3 `RegsInputs`, `RegsYear`, `loadRegsInputs`, `targetFor`; `item.sub` removal §6 #7).
- `displayByDay` entries are shared frame objects (≤ nseg distinct) — 366 references, not 366 copies.
- page v36 decides an outside-B.C. part by `pl.k == null`; the store says the same with `part.akey IS NULL` (157 of 20,461 parts).
- The section → part choice is not in the data (two parts of one item may share an akey). Decision recorded here, not guessed:
  lowest `part_ix`. Raise in decisions.md if the owner wants the map's tap to win differently.
