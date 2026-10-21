# T08 — core hydrate 3: rows model (`rowsAt`, `hydRow`, `buildModel`, `dayStatus`)

Phase 0 · Depends on T07, T09 (parity inputs) · Blocks T13, T15, T17–T19, T21, T36 · Decisions — · Size ¾ day

## Goal
page v36's card model: the answers' rows frame of a segment with every rule ref resolved to the part's `RuleView`
(`rowsAt`), the day's `CardModel` (`buildModel`) the Keep tab reads, and per-day species status over the year
(`dayStatus`/`strip`) that T13's trout line needs. Pure; no globals; typed.

## Files
- create `app/packages/core/src/regs/card.ts` (+ `card.test.ts`)
- modify `app/packages/core/src/regs/index.ts`
- create `app/conformance/src/regs/card.parity.test.ts` (store + `app/design/regs-v11/port/golden/hydrate.json`; `inputsOf` from T09)

## Signatures
```ts
// card.ts
export type CardStatus = "keep" | "nolimit" | "release" | "closed";            // page st_: no_limit → nolimit
export const KEEPISH: (s: CardStatus) => boolean;                               // keep | nolimit (:187)
export function st_(s: "keep" | "no_limit" | "release" | "closed"): CardStatus; // :188
export const RANKSTAT: Readonly<Record<CardStatus, number>>;                    // {keep:3, nolimit:3, release:2, closed:1} (:228)

export interface HydLine {                    // a decided line / row fact, rule refs resolved (:189–196)
  readonly t: string; readonly r: RuleView;
  readonly a?: number; readonly b?: number;   // b: null → Infinity
  readonly take?: number; readonly outer?: RuleView;
  readonly daily?: number;                    // null → Infinity
  readonly status?: CardStatus;
  readonly raw: Readonly<Record<string, unknown>>;   // the wire line (o, keepO, min, max, says, only, capped, carve, …)
}
export interface HydFact extends HydLine {    // :204–205
  readonly members: ReadonlySet<string>; readonly rules: readonly RuleView[];
  readonly carveOf?: { readonly a: number; readonly b: number; readonly take: number };
}
export interface HydRes {                     // one fish × origin (:197–203)
  readonly S: string; readonly o: "hatchery" | "wild"; readonly status: CardStatus; readonly win: RuleView;
  readonly daily: number | null;              // null for release/closed; Infinity for no limit
  readonly narrow: RuleView | null; readonly lines: readonly HydLine[];
  readonly roles: ReadonlyMap<RuleKey, { readonly role: string; readonly by: RuleKey | null }>;
  readonly liftNotes: readonly { readonly by: RuleView; readonly q: Readonly<Record<string, unknown>> }[];
}
export interface HydRow {                     // :206–215
  readonly kind: CardStatus; readonly pool: RuleView | null; readonly win: RuleView | null;
  readonly members: readonly string[]; readonly allMembers: readonly string[];
  readonly daily: number | null; readonly narrow: RuleView | null;
  readonly everyone: readonly HydFact[];
  readonly groups: readonly { readonly members: readonly string[]; readonly facts: readonly HydFact[] }[];
  readonly prot: Row["prot"]; readonly wins: readonly RuleView[];
  readonly liftNotes: readonly { readonly by: RuleView; readonly q: Readonly<Record<string, unknown>> }[];
  readonly scope: Badge | null;
  readonly conds: readonly (Readonly<Record<string, unknown>> & { readonly r: RuleView | null; readonly b: number })[] | null;
  readonly items: Row["items"] | null; readonly real: RealDaily | null;
  readonly raw: Row;                          // group, elsewhere, week, restates, presence are read from raw (T15–T19)
}
export interface CardRows {                   // :216–222
  readonly spp: readonly string[];
  readonly R: Readonly<Record<string, { readonly H: HydRes | null; readonly W: HydRes | null }>>;
  readonly rows: readonly HydRow[];
  readonly line: RowsValue["steelhead_line"];
}
export interface CardModel {                  // buildModel :225–227
  readonly R: CardRows["R"]; readonly rows: readonly HydRow[]; readonly spp: readonly string[];
  readonly line: RowsValue["steelhead_line"]; readonly missingRows: boolean;
  readonly broad: readonly RuleView[];        // settled.broad only when status is "closed", else []
  readonly status: "base" | "own" | "closed" | "tidal" | null;
  readonly ctx: Settled;
}
export function hydLine(R: RuleIndex, l: Decided["lines"][number]): HydLine;
export function hydRes(R: RuleIndex, d: Decided | null, S: string, o: "hatchery" | "wild"): HydRes | null;
export function hydFact(R: RuleIndex, l: Row["everyone"][number]): HydFact;
export function hydRow(R: RuleIndex, x: Row): HydRow;
export function rowsAt(inp: RegsInputs, place: RegsPlace, s: number): CardRows | null;   // reads inp.year.rowsBySeg[s]; memoised per (inp, s)
export function mainRes(H: HydRes | null, W: HydRes | null): HydRes | null;              // :223
export function buildModel(inp: RegsInputs, place: RegsPlace, settled: Settled): CardModel;
/** page dayStatus (:231–237) on a book day (MMDD), with the year that names its weekday (page YEAR). */
export function dayStatus(inp: RegsInputs, place: RegsPlace, md: Md, members: readonly string[], year: number):
  "keep" | "release" | "closed" | "none";
/** page strip (:238–242): run-length [[status, days]] over the 365 book days (DAYS, no Feb 29). */
export function strip(inp: RegsInputs, place: RegsPlace, members: readonly string[], year: number):
  readonly (readonly ["keep" | "release" | "closed" | "none", number])[];
```
Port notes (exactly as page v36):
- `hydRes.daily`: `d.daily == null ? (d.status === "no_limit" ? Infinity : null) : d.daily`; `roles` keyed by the rule KEY
  (`R.get(r).key`), `by` the key or null; `hydRow.daily`: `x.daily == null ? (kind === "nolimit" ? Infinity : null) : x.daily`.
- `hydFact`: `rules = (l.rules || [l.r]).map(R.get)`; `carve_of [a, b|null, take]` → `{a, b: b ?? Infinity, take}`.
- `hydRow.conds`: each `{...c, r: c.r == null ? null : R.get(c.r), b: c.b === null ? Infinity : c.b}` (page compares `=== null`; an
  absent `b` stays `undefined` — keep that).
- `rowsAt` iterates `Object.entries(fish)` in the frame's key order (= `spp` order in the data; do not sort).
- `dayStatus`: `M = place.moments`; `wk = M ? daySegments(starts, leap(md)).filter(j => !M[j].hours) : []`; segments =
  `wk.length > 1 ? wk : [segmentIndex(starts, leap(md), M, weekdayOf(md, year), false)]`; best by `RANKSTAT`, `nolimit` → "keep";
  `"none"` when no segment has rows.

## Ports
page_v36.js @04765ab5: `KEEPISH` 187, `st_` 188, `hydLine` 189–196, `hydRes` 197–203, `hydFact` 204–205, `hydRow` 206–215,
`rowsAt` 216–222, `mainRes` 223–224, `buildModel` 225–227, `RANKSTAT` 228–230, `dayStatus` 231–237, `strip` 238–242
(`DAYS` 21, `segOf` 139). Hidden globals replaced: `SEC.rows.{frames, decided, rows, at}` (→ `inp.year.rowsBySeg`, already
joined by T04), `w._seg['R'+s]` (→ WeakMap memo), `YEAR` (→ `year` param), `PLACE` (→ `place`).
T13's `speciesYear(inp, members)` is `strip(...)` expanded to 365 per-day states — T13 calls `dayStatus`, it does not re-port it.

## Reads
`inp.year.rowsBySeg[s]` (`RowsValue`: `spp`, `fish{S:{hatchery, wild}}` → `Decided{status, win, daily, narrow, lines, roles,
lift_notes}`, `rows[]` → `Row{kind, pool, win, members, all_members, daily, narrow, everyone, groups, prot, wins, lift_notes,
scope, conds, items, real_daily}` + raw `group/elsewhere/week/restates/presence`, `steelhead_line`), `settled.{s, status, broad}`,
`place.{starts, moments}`.

## Tests
Store-free (`card.test.ts`): a hand-built `RowsValue` with one keep row (pool 3091, daily 2), one no_limit row (daily null), one
closed row, and a `RuleIndex` stub:
1. `st_("no_limit") === "nolimit"`; `hydRow(...).daily === Infinity` for the no_limit row, `null` for closed; `kind` "nolimit".
2. `mainRes(H keep, W release) === H`; `(H release, W keep) === W`; `(H release, null) === H`; `(null, null) === null`.
3. `hydFact` with `carve_of [30, null, 0]` → `carveOf {a:30, b:Infinity, take:0}`; a fact without `rules` → `rules = [R.get(r)]`.

Store-backed parity (`card.parity.test.ts`, every one of the 134 states vs `app/design/regs-v11/port/golden/hydrate.json` `states[id].model`, `Infinity` ↔ "Infinity"):
4. `buildModel(...)` projected to `{spp, line, missingRows, status, broad:[rix], rows:[{kind, pool:rix|null, win:rix|null, daily, narrow:rix|null, members}],
   R:{S:{H:{status, daily, win:rix, narrow:rix|null}|null, W:…}}}` deep-equals the oracle.
5. Literals (page v36's own output):
   - **vedder_515** (s 1): `line` "known_with_rules"; `spp` `["RB","CT","LMB","SMB","BCB","CRA","KO","LW","MW","WSG","ST","GB","DV","LT","EB"]`;
     rows (kind/pool/daily/members): keep/3091/2/`[EB,GB,DV,LT,ST]` · release/–/null/`[KO]` · keep/3088/15/`[LW,MW]` · keep/3083/20/`[LMB,SMB]` ·
     keep/3084/20/`[BCB]` · release/–/null/`[WSG]` · keep/3085/25/`[CRA]` · closed/–/null/`[]`;
     `R.RB` = H release win 527, W release win 3098; `R.LMB.H` keep 20 win 3083; `Object.keys(R).length === 15`; `broad` `[]`.
   - **vedder_615** (s 2): rows `closed/[BCB,EB,GB,CRA,CT,DV,KO,LT,LW,LMB,MW,RB,SMB,ST,WSG]` · `closed/[]`; every `R[S].H.win.rix === 529`; `broad` `[529]`; `status` "closed".
   - **vedder_1010** (s 3): 9 rows; row 1 = keep/pool 530/daily 4/`[RB]`; `R.RB.H` keep 4 win 530, `R.RB.W` release win 3098; `R.CT.H` keep 2 win 3091.
   - **klwa_1010** (Sat, s 1) vs **klwa_1012** (Mon, s 0): Saturday has the kokanee row keep/pool 1366/daily 5/`[KO]`, Monday has release/`[KO]`;
     both have nolimit/pool 1362/daily Infinity/`[LMB,SMB]` and nolimit/1363/`[YP]`; `line` null; `status` "own" and `broad` `[]`
     (the settle broad `[3127,3143,3144,3145,3403]` is NOT copied when the water is open).
   - **x15p7_1010** (s 4, hours window): two closed rows, `R.ST.H.win.rix === 575`, `broad` `[575, 578]`.
   - Census: `nolimit` rows appear in 24 states (klwa_1010 klwa_1012 cow1_1010 cow2_1010 cow3_1010 klmb_1010 …); `missingRows` false in all 134.
6. **WITHDRAWN 2026-10-21 (owner rule, review):** `dayStatus` / `strip` / `RANKSTAT` are not ported — they rank the members' answers over the day's weekday moments, a per-day status the store does not hold (Kalamalka kokanee coloured "keep" on a Monday the store answers "release"). PIPELINE NEED: each rows_row carries its year strip (run-length `[[status, days]]` over the 365 book days); T13 reads it. Formerly: `dayStatus` / `strip` vs `app/design/regs-v11/port/golden/hydrate.json` `states[id].dayStatus` for every row of every state (year 2026). vedder_515 row 0
   (`[EB,GB,DV,LT,ST]`): runs sum to 365.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs/card conformance/src/regs/card.parity
```

## Notes
- CONTRACT CHANGE: applied to contracts.md (§3 `CardModel` pointer, `RegsYear.rowsBySeg`). `CardModel` is defined in this ticket.
- `Infinity` is a legal `daily`: keep it in core (page compares `daily === Infinity`); presenters print "no limit" from `kind`,
  never from the number. Do not JSON-serialise a `CardModel` (Infinity → null) — the parity test maps it explicitly.
- `roles` is keyed by rule KEY not rix (page :201) because the sheets look rules up by key.
- page v36 `buildModel` takes `md` and re-settles; the app passes the `Settled` from T07 so the day is decided once.
