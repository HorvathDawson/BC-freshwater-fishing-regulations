# T13 — presenter: year strip + status line

Phase 1 · Depends on T00 (line/words), T07 (`settle()` → `Settled`), T08 (`CardModel` rows), T09 (`RegsInputs.year`), T12 (golden harness) · Blocks T21, T24 · Decisions — · Size ½ day

Target: **regs v33 @ 04765ab5** (prototype = `app/design/regs-v11.html` at that commit; line refs below are that file, script
from line 892) and `pipeline/deliver/answers/reference/page_v36.js` @ 04765ab5. Always re-read refs from
`app/design/regs-v11/port/golden/inventory.json` after a bump.

## Goal
`stripOf(inp, settled, card, today)` returns the Keep tab's year strip: the status line ("Open today", "Closed on Jun 15 Opens Jul 1", "Open today Trout release only until Mar 31"), the 365-day bar as runs of the display frame's strip bits,
the today marker, the month initials and the legend in the data's own labels. Nothing is drawn yet (T24 draws it).

## Files
- create `app/conformance/src/regs/strip.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/strip.ts`, `app/packages/core/src/regs/strip.test.ts`
- modify `app/packages/core/src/regs/words.ts` (constants below), `app/packages/core/src/regs/model.ts` (StripVM as below)

## Signatures
```ts
// strip.ts — ports page_v36 waterStrip0 / nextOpen / runOf / strip / dayStatus / daySegs, prototype flagBar / flagLegend / span
import type { RegsInputs, Settled, CardModel } from "./inputs";
import type { Day, StripVM } from "./model";

/** The 365 days of the book's year (no Feb 29), as page v36 DAYS: day i → {month, day}. Exported for T14/T19. */
export const BOOK_DAYS: readonly Day[];
export function bookIndex(d: Day): number;                         // DAYS.indexOf(md); Feb 29 → index of Feb 28 (never asked)
/** display frame of book day i: inp.year.displayByDay[leapDayOfYear(BOOK_DAYS[i]) - 1] (drop leap index 59). */
export function displayOnBookDay(inp: RegsInputs, i: number): StatusFrame | null;
export function isClosedOn(inp: RegsInputs, i: number): boolean;   // frame?.status === "closed"
export function nextOpen(inp: RegsInputs, d: Day): Day | null;     // page_v36 nextOpen 443
export interface Run { readonly now: string; readonly all: boolean; readonly from?: Day; readonly until?: Day; readonly next?: Day; readonly then?: string }
export function runOf(perDay: readonly string[], d: Day): Run;     // page_v36 runOf 488–494 over a 365-long day-state array
export function speciesYear(inp: RegsInputs, members: readonly string[]): readonly string[]; // page_v36 strip/dayStatus 231–242 → 365 states "keep"|"release"|"closed"|"none"
export function spanWords(a: number, b: number): string;          // prototype span 1363–1365 ("Jun 1–30", "Jul 4–Nov 15")
export function stripOf(inp: RegsInputs, settled: Settled, card: CardModel, today: Day): StripVM | null;
```
StripVM (model.ts) — v33 shape; supersedes contracts.md §2.2 where they differ:
```ts
export interface StripVM {
  readonly status: "open" | "closed";
  readonly line: readonly { kind: "status" | "when" | "soon" | "soonOpen"; text: string }[];  // golden keep.strip.line[].text, cls wsstat/muted small/wsoon/wsoon open
  readonly runs: readonly { bits: number | null; days: number }[];   // golden keep.strip.bar[].flex, 365 days total
  readonly todayAt: number;                                         // bookIndex(day) / 365 (0..1)
  readonly months: readonly string[];                               // words.MONTH1
  readonly legend: readonly { bit: number; mark: "✓" | "✕" | null; label: string }[];
}
```
Words (`words.ts`): `STRIP_OPEN = "Open"`, `STRIP_OPEN_EXCEPT = "Open, except some parts"`, `STRIP_CLOSED = "Closed"`,
`TODAY_LC = "today"`, `ON_DAY = (md) => "on " + md`, `OPENS = (md) => "Opens " + md`, `CLOSED_FROM = (md) => "Closed from " + md`,
`CLOSED_SPAN = (span) => "Closed " + span`, `TROUT_STATE = { closed: "closed", release: "release only" }`,
`TROUT_LINE = (state, tail) => "Trout " + state + tail`, `ALL_YEAR = " all year"`, `UNTIL = (md) => " until " + md`,
`MON3`, `WD3`, `MONTH1 = ["J","F","M","A","M","J","J","A","S","O","N","D"]`, `TROUTISH_STRIP = ["RB","CT","EB","GB","LT","DV"]`.

## Ports
- **page_v36 `waterStrip0` 459–468** → `stripOf`:
  - `openNow = settled.status.status !== "closed"`; `partShut = openNow && settled.inpart.some(r => r.now && r.family === "retention")`.
  - status word: `!openNow` → "Closed"; `partShut` → "Open, except some parts"; else "Open".
  - when: `today` → "today", else "on " + MON3 day ("on Jun 15"); then ` (${WD3})` appended when `inp.year.moments` is set
    AND the day has >1 segment (`daySegs` 134: every segment whose start equals the start on/before the day) — "today (Sat)",
    "on Oct 12 (Mon)".
  - soon: closed → `nextOpen` (scan k = 1..365 book days forward, wrapping) → `"Opens " + fmt` (kind `soonOpen`).
    Open → first closed book day in k = 1..21 → `"Closed from " + fmt`, **then the v33 rewrite (prototype buildKeep 1031–1033)**:
    `CLOSED_FROM` is then replaced: the first strip run that starts after today's index and has bit 1 → `"Closed " + spanWords(runStart, runEnd)` ("Closed Jun 1–30").
    Else the trout line: first `card.rows` row with `!pool`, kind closed|release, members ∩ `TROUTISH_STRIP` (page v36 uses
    `MODEL.rows`, T08) → `runOf(speciesYear(inp, row.members), day)` → "Trout release only all year" | "Trout closed until Nov 15".
- **page_v36 `strip` 238 / `dayStatus` 231** → `speciesYear`: per book day, over the day's weekday-moment segments without
  hours (`daySegs` filtered `!moments[j].hours`, >1) else the day's segment; best status by RANKSTAT {keep 3, nolimit 3,
  release 2, closed 1} of `mainRes(H, W)` per member; nolimit → keep. **Needs the rows frame of every segment** (see Notes).
- **page_v36 `runOf` 488–494**: verbatim over a 365 array, wrapping; `all` when the run covers the year.
- **prototype `flagBar` 945–948** → runs: run-length of `displayOnBookDay(i).strip` (null frame → `bits: null`).
- **prototype `flagLegend` 949–953** → legend: the set of bits: a run with bit 1 adds 1 only; else adds 0 and each of 2,4,8,16
  present; sorted ascending. bit 0 → mark "✓", bit 1 → "✕", others null; label = `inp.statics.strip.find(x => x.bit === b).label`.
- **todayAt**: page v36 `segBar` 243 `idx/365` (golden `left:calc(77.26% - 1px)` → 0.7726 for Oct 10).
- No strip at all when the water is tidal or outside (page v36 `waterStrip` returns `tidalHtml` → T14 banner) — `stripOf` → null.

## Reads
`inp.year.displayByDay[366]` (`StatusFrame.strip`, `.status`), `inp.year.segStarts`, `inp.year.moments`,
`inp.year.rowsBySeg` (new, see Notes), `inp.statics.strip[].{bit,label}`, `settled.status`, `settled.inpart[].{now,family}`,
`card.rows[].{pool,kind,members}`, `inp.key.tidal`.

## Tests
Golden parity lives in `app/conformance/src/regs/strip.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
1. **Parity, all 134 states**: `stripOf(...)` vs `golden.states[id].keep.strip`: `line.map(x=>x.text)` = `strip.line[].text`;
   `runs.map(r=>r.days)` = `bar[].flex`; bits → class: `bits & 1 ? "f-closed" : ["f-open"|"f-rel", fly, flies, bait]` joined and
   whitespace-collapsed = `bar[].cls` collapsed; `todayAt` → `left:calc(${(todayAt*100).toFixed(2)}% - 1px)` = `today`;
   legend `(mark ? mark+" " : "") + label` = `legend[].text`. x31p_1010 (tidal) → null (golden strip null).
2. vedder_1010 → line `["Open","today"]`; runs `[120,31,30,184]`, cls `f-open`, `f-open f-fly f-bait`, `f-closed`, `f-open`;
   today `left:calc(77.26% - 1px)`; legend `["✓ open","✕ closed","fly fishing only","bait ban"]`.
3. vedder_615 → `["Closed","on Jun 15","Opens Jul 1"]`, today `45.21%`, status "closed".
4. vedder_515 → `["Open","on May 15","Closed Jun 1–30"]` (the v33 span rewrite of `CLOSED_FROM`).
5. klwa_1012 → `["Open","on Oct 12 (Mon)"]`, runs `[365]`, legend `["✓ open"]`, today `77.81%`.
6. x15p7_1010 → `["Closed","today (Sat)","Opens Nov 1"]`; runs `[120,184,61]`.
7. x4p_1010 → `["Open, except some parts","today"]`.
8. x16p1_1010 → `["Open","today","Trout release only all year"]`; iron_720 → `["Open","on Jul 20","Trout release only until Nov 30"]`,
   runs `[181,153,31]`.
9. cow0_1010 → `["Closed","today","Opens Nov 16"]`, runs `[184,135,46]`, legend `["✓ open","✕ closed","bait ban"]`.
10. Unit (no store): `spanWords` over book indices 151..180 → "Jun 1–30"; 184..318 → "Jul 4–Nov 15"; `runOf` on an all-"keep"
    array → `{all:true}`; `bookIndex({month:10,day:10}) === 282`.

## Acceptance
`pnpm -C app check` green; `pnpm -C app exec vitest run conformance/src/regs/strip.parity packages/core/src/regs/strip` → 134/134 parity.

## Notes
- Contract: StripVM (365-day book year, `bits: number | null`, `line.kind` "when"/"soonOpen", `legend[].label`, `todayAt = bookIndex/365`) and `RegsInputs.year.rowsBySeg` — applied to contracts.md (marked `// (T13)`). The coordinator verified the run-lengths against golden `bar[].flex` 134/134.
- The weekday of each book day is the **2026** weekday (prototype `wday` uses year 2026; page v36 `weekdayOf` uses `YEAR`); the
  app uses the selected day's year. taps.json `displayByDay` was regenerated with each day at its own 2026 weekday.
- Golden `strip.lines` follows contracts.md §1: one space between separate elements ("Open today Trout release only until Mar 31"); compare with `eqPlain`.
- `Closed from` survives only when no later closed run exists in the strip (the rewrite's loop finds none) — keep both branches.
