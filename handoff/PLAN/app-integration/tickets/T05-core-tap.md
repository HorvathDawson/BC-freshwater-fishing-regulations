# T05 — core tap: `segmentIndex`, `weekdayOf`, `profileIndex`; taps.json parity; `answers --check` in `pnpm check`

Status: DONE — f9d3cf12, 89f0d3d2, 46ccd2d0 (redo).

Phase 0 · Depends on T00, T04 · Blocks T07, T09, T33 · Decisions — · Size ½ day

## Goal
The three pure lookups that turn "this day, this angler" into store coordinates, in core, proven equal to the Python reference
(`encode.segment_index`) on every golden state, including the weekday-split (klwa) and hours-window (x15p7) keys. `pnpm check`
also fails when `answers.generated.ts` drifts from the Python model (it is only checked in CI today, facts.md #19).

## Files
- create `app/packages/core/src/regs/tap.ts` (+ `export * from "./tap"` in `regs/index.ts`)
- create `app/packages/core/src/regs/tap.test.ts` (store-free unit cases)
- create `app/conformance/src/regs/tap.parity.test.ts` (taps.json × the province store)
- create `app/conformance/src/regs/golden.ts` (minimal: `golden()`, `taps()`, `provinceStore()`, types `GoldenState`, `GoldenSheet`, `Tap` — tests.md §1.2;
  reads the JSON with `node:fs` — conformance is not a `layers.json` layer, and imports `@app/data/regs/node`)
- modify `app/package.json` — `scripts`

## Signatures
```ts
// app/packages/core/src/regs/tap.ts
import { DAYS_BEFORE_MONTH, WEEKDAYS, CALENDAR_DAYS, type Weekday } from "../calendar.generated";
import { dayOfYear } from "../statusIndex";
import type { Moment } from "../answers.generated";
import type { Day, Who } from "./model";
export class TapError extends Error {}
/** ANSWERS-SPEC §2 / encode.segment_index (:923–941): s = bisect_right(starts, day) - 1; a repeated start → the moment holding
 *  `weekday` and (hours null or hours.in === inside). `day` is the leap-calendar day 1..366 (dayOfYear). Throws TapError where
 *  Python raises: day out of range, a repeated start without a weekday, no moment holds the weekday. */
export function segmentIndex(starts: readonly number[], day: number, moments: readonly Moment[] | null,
                             weekday: Weekday | null, inside?: boolean): number;
/** Every segment sharing the day's start (page_v36 daySegs :134–136) — one unless the key has moments. */
export function daySegments(starts: readonly number[], day: number): readonly number[];
/** The weekday of a calendar day in a given year (page_v36 weekdayOf :132, with YEAR explicit). */
export function weekdayOf(d: Day, year: number): Weekday;
/** The leap-calendar day of a Day (= dayOfYear). */
export function leapDay(d: Day): number;
/** `${residency}/${age}/${guidance}/${status}` in static licence.profile_dims order → index into static licence.profiles.
 *  age "adult" → "16_plus", "u16" → "under_16". Throws TapError when the dims are not the four this app knows or the profile
 *  is absent. */
export function profileIndex(who: Who, dims: readonly (readonly [string, readonly string[]])[], profiles: readonly string[],
                             extra?: { guidance?: "non_guided" | "guided"; status?: string }): number;
export const AGE_DIM: Readonly<Record<Who["age"], string>>;   // { adult: "16_plus", u16: "under_16" }
```
`segmentIndex` body (port, no `bisect` in JS):
```ts
if (!(day >= 1 && day <= CALENDAR_DAYS)) throw new TapError(`regs: day ${day} is not 1..${CALENDAR_DAYS}`);
let s = -1; for (let j = 0; j < starts.length; j++) if (starts[j]! <= day) s = j; else break;   // starts are sorted
const rep = (s > 0 && starts[s - 1] === starts[s]) || (s + 1 < starts.length && starts[s + 1] === starts[s]);
if (!moments || !rep) return s;
if (!weekday) throw new TapError(`regs: day ${day} reads differently by weekday or hour — name one`);
const first = starts.indexOf(starts[s]!);
for (let i = first; i <= s; i++) { const m = moments[i]!; if (m.weekdays.includes(weekday) && (m.hours === null || m.hours.in === !!inside)) return i; }
throw new TapError(`regs: no moment of day ${day} holds ${weekday} (inside=${!!inside})`);
```
(page_v36 `segAt` :137–138 returns -1 instead of throwing; the data never reaches it — taps.json resolves all 134.)

`app/package.json` scripts — add the script and put it after `calendar` in `check`:
```json
    "answers": "cd .. && .venv/bin/python -m pipeline.deliver.answers.model --check",
    "check": "pnpm run boundaries && pnpm run platform && pnpm run style && pnpm run colours && pnpm run spinner && pnpm run fixture && pnpm run gauge-policy && pnpm run calendar && pnpm run answers && pnpm run deps && pnpm run typecheck && pnpm run test",
```

## Ports
`pipeline/deliver/answers/encode.py` `segment_index` (:923–941); page_v36.js `daySegs` (:134–136), `segAt` (:137–138), `segOf`
(:139–140), `weekdayOf` (:132–133), `dayOf` (:24); `licence.profiles` order = `itertools.product` over `profile_dims`
(verified: index = 20·res + 10·age + 5·guidance + status).

## Reads
`seglist.starts`, `segmom.ix` → `static answers.moments`, `static licence.profile_dims`, `static licence.profiles`.

## Tests
`app/packages/core/src/regs/tap.test.ts` (store-free; literals from taps.json @04765ab5):
1. Vedder (starts `[1,122,153,183]`, no moments): `leapDay({month:5,day:15})` = 136 → `segmentIndex(…, 136, null, null)` = 1 (vedder_515);
   `leapDay({6,15})` = 167 → 2 (vedder_615); `leapDay({10,10})` = 284 → 3 (vedder_1010); day 121 → 0, day 122 → 1. `leapDay({2,29})` = 60, `leapDay({3,1})` = 61.
2. Weekday split (klwa, starts `[1,1]`, moments `[{weekdays:Mon–Fri,hours:null},{weekdays:[Saturday,Sunday],hours:null}]`):
   day 284, "Saturday" → **1** (klwa_1010); day 286, "Monday" → **0** (klwa_1012); weekday null → throws `TapError` /name one/.
3. Hours window (x15p7, starts `[1,1,122,122,228,228,306,306]`, 8 moments alternating `hours.in` false/true, every weekday):
   day 284 "Saturday" → **4** (inside false, x15p7_1010); same with `inside=true` → 5; day 1 → 0.
4. x32p0 (starts `[1,92,167,197,197,245,306]`, moments 3/4 hours `in` false/true): day 284 → 5 (not repeated → bisect); day 200 "Monday" → 3, inside → 4.
5. `segmentIndex([1], 0, …)` and `(…, 367, …)` throw `/is not 1..366/`.
6. `weekdayOf({month:10,day:10}, 2026)` → "Saturday"; `{10,12}` → "Monday"; `{5,15}` → "Friday"; `{6,15}` → "Monday"; `{2,29}, 2028` → "Tuesday".
7. `daySegments([1,1,122,122], 130)` → `[2,3]`; `daySegments([1,122,153,183], 167)` → `[2]`.
8. `profileIndex` with the store's dims/profiles (literal copies in the test): adult/resident → 0 ("resident/16_plus/non_guided/none",
   label "B.C. resident, 16 or older"); u16/resident → 10; adult/non_resident → 20; u16/non_resident → 30; adult/non_resident_alien → 40;
   u16/non_resident_alien → 50 ("From outside Canada, under 16"); `{guidance:"guided"}` adult/resident → 5; dims with a 5th axis → throws.

`app/conformance/src/regs/tap.parity.test.ts` (`it.skipIf(!hasProvinceRegs())`, reads `app/design/regs-v11/port/golden/taps.json`
through `app/conformance/src/regs/golden.ts` `taps()` / `provinceStore()` — T05 CREATES that file with just `golden()`, `taps()`,
`provinceStore()` and the `Tap` type, with the exact signatures of tests.md §1.2; T12 adds `requestOf`, `inputsFor`, `cases`, `eq.ts`, `exclusions.ts`):
9. For all **134** states: `akey(state.akey)` → `seg`, `mom`; `segStarts(seg)` deep-equals `state.starts`; `segMoments(mom)` deep-equals
   `state.moments` (null when `mom` null); `segmentIndex(starts, leapDay({month: md/100|0, day: md%100}), moments, state.weekday, false)`
   === `state.s`; `cell(akey, s)` deep-equals `state.cell`. Name the moment-split states in the test so a regression names them:
   klwa_1010 s 1, klwa_1012 s 0, x17p3_1010 s 7, x17p4_1010 s 9, x19p6_1010 s 6, x15p7_1010 s 4, x32p0_1010 s 5.
10. Year: for 1 ≤ d ≤ 366, the display id of `cell(akey, segmentIndex(starts, d, moments, weekdayOf(dayOf⁻¹(d), 2026)))` equals
    `state.displayByDay[d-1]` (taps.json regenerated: each day at ITS OWN 2026 weekday; day 60 = Feb 29 at its first moment — use
    `daySegments(starts, 60)[0]` there).

## Acceptance
```sh
pnpm -C app check                                     # now runs `answers` (exit 0 today, facts.md #1)
pnpm -C app exec vitest run --project logic packages/core/src/regs/tap conformance/src/regs/tap.parity
(cd app && pnpm run answers)                          # prints the model check, exit 0
```

## Notes
- Year: the app's weekday is the SELECTED day's calendar year (`Date.getFullYear()`), the prototype's is 2026. Golden states all use
  2026, so tests pass the year explicitly.
- Feb 29 (day 60) in a common year: the page never asks it (page_v36 TODAY maps 229→228); `dayOfYear(new Date(2027,1,28))` = 59.
  The app's date pill offers Feb 29 only in a leap year; `weekdayOf({2,29}, 2027)` is a programmer error → TapError.
- `answers --check` needs `.venv` (as `calendar` and `gauge-policy` already do).
