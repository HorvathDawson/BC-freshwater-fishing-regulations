# T07 — core hydrate 2: ladder verdicts, `settle()` context

Phase 0 · Depends on T05, T06, **T09** (the parity test needs `loadRegsInputs`; build order T06 → T09 → T07 → T08) · Blocks T08, T13, T14, T15, T19, T36 · Decisions — · Size 1 day

## Goal
page v36's "what the ladder says of the place today" as pure TS: per fish and origin every rule's state (`ladderAt`), and the
day's context every box above the card reads (`settle`): speakers/beside/shown rules, rules in undrawn parts over the year
(`inpart`), lifted rules, the closures that close the water (`broad`), and the timed / side / standing / angler-closure subsets.
Also per-day status over the year from `inp.year`, so T13/T14 can ask "closed on day d?" and "which closures on day d?" without
re-reading the store.

## Files
- create `app/packages/core/src/regs/ladder.ts` (+ `ladder.test.ts`)
- create `app/packages/core/src/regs/settle.ts` (+ `settle.test.ts`)
- create `app/packages/core/src/regs/place.ts` — `RegsPlace` (page `makePlace` without the globals)
- modify `app/packages/core/src/regs/index.ts` — export the three
- create `app/conformance/src/regs/settle.parity.test.ts` (store + `app/design/regs-v11/port/golden/hydrate.json` oracle; `inputsOf` from T09)

## Signatures
```ts
// ladder.ts — replaces SEC.ladder.{verdicts, frames, reasons, reason_state}, A.fish (T04 resolved verdict ids already)
export type VerdictState = "speaks" | "beside" | "shown" | "not_yet_mapped" | "lifted" | "displaced" | "moot";
export interface Verdict {
  readonly state: VerdictState;
  readonly partly: readonly number[] | null;   // the lifting rules when only partly lifted (wire `partly` [[rix,[by…]]])
  readonly reason?: string;                     // ladder.reasons[r] for a lost rule
  readonly by?: number;                         // the rix that beat or lifted it
}
export type Origin = "none" | "hatchery" | "wild";
/** fish code → origin → rix → verdict, in the frame's fish order (ascending static answers.fish index). */
export type Ladder = ReadonlyMap<string, Readonly<Record<Origin, ReadonlyMap<number, Verdict>>>>;
export function verdictList(v: VerdictWire, reasons: readonly string[], reasonState: readonly string[]): Map<number, Verdict>; // :146–153
export function ladderAt(frame: LadderFrameWire | null, reasons: readonly string[], reasonState: readonly string[]): Ladder | null; // :154–162

// place.ts — page makePlace (:117–125) over RegsInputs: no D.waters/A.parts/A.keys/DWATERS globals
export interface RegsPlace {
  readonly key: AkeyRow | null;                 // null: outside B.C. (page w.key == null)
  readonly R: RuleIndex;                        // T06 ruleIndex over the part's via
  readonly cands: readonly RuleView[];          // every rix in the part's ruleset reach+trib, in page_data order (reach first, then trib)
  readonly starts: readonly number[]; readonly moments: readonly Moment[] | null;
  readonly part: PartFacts | null;              // page `disp`
  readonly tidal: boolean;                      // page isTidal :143
}
export function makePlace(inp: RegsInputs): RegsPlace;

// settle.ts — page settle (:163–182), statusAt (:183), isClosedAt (:184)
export interface Settled {
  readonly s: number; readonly day: Day;
  readonly L: Ladder | null;
  readonly status: DisplayFrame | null;         // the display frame of (akey, s)
  readonly speaks: readonly RuleView[]; readonly beside: readonly RuleView[]; readonly shown: readonly RuleView[];
  readonly inpart: readonly { readonly rule: RuleView; readonly now: boolean }[];  // every not_yet_mapped rule over the year
  readonly lifted: ReadonlyMap<number, number>;  // rix → the rix that lifted it
  readonly broad: readonly RuleView[];           // status.closing, most fish first, then rank
  readonly timed: readonly RuleView[];           // beside with a tcond
  readonly side: readonly RuleView[];            // beside with fields.side
  readonly standing: readonly RuleView[];        // shown with kind "standing"
  readonly anglerclosed: readonly RuleView[];    // speaks with kind "anglerclosure"
}
export function settle(inp: RegsInputs, place: RegsPlace): Settled;               // the tap's day (inp.day, s = inp.key cell)
/** The same context for segment `s` (another moment of the day, T15 timedHtml; another day, T14's chain). */
export function settleAt(inp: RegsInputs, place: RegsPlace, s: number, day: Day): Settled;
/** page broad ordering over any display frame (T14 closersAt on other days of a run). */
export function broadOf(frame: DisplayFrame | null, R: RuleIndex): readonly RuleView[];
/** Status of a leap-calendar day 1..366 from inp.year.displayByDay (page statusAt): base | own | closed | tidal | null. */
export function statusOn(inp: RegsInputs, leapDay: number): "base" | "own" | "closed" | "tidal" | null;
export function isClosedOn(inp: RegsInputs, leapDay: number): boolean;
```

`settle` algorithm (port exactly — order matters for every list):
1. `L = ladderAt(inp.year.ladderBySeg[s])`. `st = new Map<number, VerdictState>()`, `lifted = new Map()`; for each fish `S` of `L` in
   frame order, for each `[ix, v]` of `L.get(S).none` in Map order: `v.state === "lifted"` → `lifted.set(ix, v.by)`; else
   `if (!st.has(ix) || v.state === "speaks") st.set(ix, v.state)`.
2. `speaks/beside/shown = [...st].filter(state).map(([ix]) => R.get(ix))` (st insertion order).
3. `inpart`: the union, in first-seen order, of every `not_yet_mapped` rix over ALL segments `0..nseg-1`
   (`ladderAt(inp.year.ladderBySeg[i])`, fish order, origin "none"); `now` = rix ∈ today's not_yet_mapped (from `st`).
4. `status = inp.year.displayBySeg[s]`; `closing = status?.closing ?? []` (tidal frames have none);
   `broad = closing.map(([ix, fs]) => [R.get(ix), fs]).sort((a, b) => b[1].length - a[1].length || a[0].rank - b[0].rank).map(x => x[0])`
   (Array.prototype.sort is stable — same as V8 in the page).
5. `timed = beside.filter(r => r.tcond)`, `side = beside.filter(r => r.f.side)`, `standing = shown.filter(r => r.kind === "standing")`,
   `anglerclosed = speaks.filter(r => r.kind === "anglerclosure")`.
Memoise per `(inputs object, s)` in a `WeakMap<RegsInputs, Map<number, Settled>>` (page `w._seg['C'+s]`); `inpart`'s year walk once per inputs (page `w._nym`).

## Ports
page_v36.js @04765ab5: `verdictList` 146–153, `ladderAt` 154–162, `settle` 163–182 (incl. `settle.with_` 167), `statusAt` 183,
`isClosedAt` 184–186, `makePlace` 117–125, `segStarts` 126–128, `momentsOf` 129–130, `isTidal` 143–145, `frameIx` 142.
Python cross-reference: `encode.LadderCodec.decode_verdict` 676–688 (same states, rule ids instead of rix).
Hidden globals replaced: `SEC.ladder.*`, `A.fish`, `SEC.display.frames`, `SEC[name].at[w.k][s]` (→ `inp.year.*BySeg[s]`),
`w._seg`/`w._nym` caches (→ WeakMap), `PLACE` (→ `RegsPlace` argument).

## Reads
`inp.year.ladderBySeg[s]` (`LadderFrameWire`), `inp.year.displayBySeg[s]` and `.displayByDay[d-1]` (`DisplayFrame`:
`status`, `closing [[rix, fish[]]]`, `unless_opened`, `inherited`), `inp.statics.ladderReasons`, `.ladderReasonState`,
`inp.key.{tidal, seg, mom, nseg}`, `RuleView.{rank, tcond, f.side, kind}`.

## Tests
Store-free (`ladder.test.ts`): 
1. `verdictList([[3079,3333,3339,3355,3356,3359],[],[3351,3352],[],[],[[3364,1,3079]]], reasons, reasonState)` (ladder_verdict 0) →
   Map of 9: 3079, 3333, 3339, 3355, 3356, 3359 speaks, 3351 shown, 3352 shown, 3364 `{state: reasonState[1], reason: reasons[1], by: 3079}` (copy `ladder.reasons` /
   `ladder.reason_state` literals from the store into the fixture).
2. `partly` [[i,[a,b]]] sets `partly` = `[a,b]` on an existing entry and is ignored for an absent `i` (page :150).
3. A 2-element ladder row `[fish, v]` gives the same Map for none/hatchery/wild; own verdicts override `common` (page `new Map([...base, ...own])`).

Store-backed parity (`app/conformance/src/regs/settle.parity.test.ts`, `it.skipIf(!hasProvinceRegs())`, inputs via `inputsOf(id)` — added to `app/conformance/src/regs/golden.ts` by T09):
4. For all 134 states, `settle(inp, makePlace(inp))` projected to rix lists equals `app/design/regs-v11/port/golden/hydrate.json` `states[id].settle`:
   `{status: status?.status ?? null, broad, speaks, beside, shown, inpart: [[rix, now]], lifted: [[rix, by]], timed, side, standing, anglerclosed}`.
5. Literals (page v36's own output, `app/design/regs-v11/port/golden/hydrate.json` @04765ab5, YEAR 2026):

| state | s | status | broad (rix) | speaks/beside/shown | other |
|---|---|---|---|---|---|
| vedder_515 | 1 | own | — | 74 / 0 / 17 | lifted `[[3316,3378],[3403,3404]]`; standing `[3351,3352]`; inpart `[]` |
| vedder_615 | 2 | closed | `[529]` (key `r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r8`, rank 0, via reach) | 59 / 0 / 17 | `describe(broad[0])` = "No fishing for any game fish, Jun 1–30" |
| vedder_1010 | 3 | own | — | 72 / 0 / 17 | lifted `[[3316,3378],[3095,530],[3101,530],[3403,3404]]` |
| klwa_1010 (Sat) | 1 | own | `[3127,3143,3144,3145,3403]` | 64 / 0 / 22 | lifted `[[3316,3378],[3127,1363],[3135,1362],[3137,1363]]` |
| klwa_1012 (Mon) | 0 | own | same 5 | 62 / 0 / 22 | the weekday changes `speaks` (64 → 62) |
| cow0_1010 | 3 | closed | `[r1:cowichan_river_see_map_below@1-4::cowichan_river.r6, zp:white_sturgeon_licence::white_sturgeon_licence.r4]` | 57 / 0 / 18 | inpart `[[78,true],[79,true]]` |
| x14p2_1010 | 3 | own | 2 | 73 / 1 / 25 | side `[2216]` |
| x17p3_1010 | 7 | own | 2 | 72 / 0 / 25 | anglerclosed `[2105]` ("Angling prohibited for non-guided non-resident aliens on Saturdays and Sundays …") |
| x15p7_1010 | 4 | closed | `[575,578]` | 63 / 0 / 17 | the hours window: s 4 is outside it |
| x31p_1010 | 0 | tidal | — | — | `statusOn(inp, 284) === "tidal"` |
   Status census over the 134: own 101, closed 26, base 6, tidal 1. `timed` is empty on every golden state (page v36 data): add one
   store-free case with a beside rule carrying `tcond` to cover the filter.
6. `statusOn` / `isClosedOn` for vedder (akey 886): leap days 153..182 (Jun 1–30; `displayByDay[152..181]` = 305) closed, 152 and 183 not; `isClosedOn(inp, 167) === true`.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs/ladder packages/core/src/regs/settle conformance/src/regs/settle.parity
```

## Notes
- CONTRACT CHANGE: applied to contracts.md (§3 `RegsYear.ladderBySeg`/`displayBySeg`). T09 loads them (nseg ≤ 27, mean 4.0).
- page v36 `settle` is called with the page's md and `segOf(w, md)` (weekday of YEAR, inside=false); the app passes the tap's `s`
  from T05 `segmentIndex` with the selected date's own year. Golden states use 2026.
- `lifted` keeps only the LAST `by` per rix (Map.set over fish order) — page behaviour; do not "fix".
- A `RegsPlace` with `key === null` (outside B.C.) has no ladder: `settle` returns empty lists and `status: null` (page `frameIx`
  undefined → null), never throws.
