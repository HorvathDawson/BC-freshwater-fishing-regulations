# T00 — core `regs/` skeleton: model + line + words, `regulations.ts` rewritten

Phase 0 · Depends on — · Blocks every other ticket · Decisions — · Size ½ day

Target: **regs v33 @ 04765ab5** (`app/design/regs-v11.html`, `pipeline/deliver/answers/reference/page_v36.js` unchanged
since 18bc2a11). Line refs are @04765ab5; re-read them from `app/design/regs-v11/port/golden/inventory.json` after a bump.

## Goal
The empty shape every later ticket fills: `app/packages/core/src/regs/` with the view-model types of contracts.md §2
(`model.ts`), the rich-text runs of §1 (`line.ts`, implemented — it is small and every presenter needs it), the inputs types
of §3 (`inputs.ts`, types only), and `words.ts` (the frozen word table + `ALL_WORDS`, which the `no-hand-facts` render test
subtracts against). `regulations.ts` stops being the "NOT INTEGRATED" placeholder and becomes the documented entry of the
integration. Nothing visible changes in the app.

## Files
- create `app/packages/core/src/regs/index.ts` — `export * from "./model"; export * from "./line"; export * from "./words"; export * from "./inputs";`
  (later tickets append their modules here: `tap`, `rules`, `names`, `describe`, `place`, `ladder`, `settle`, `card`, …)
- create `app/packages/core/src/regs/model.ts` — every type of contracts.md §2 verbatim (HeaderVM … SheetVM, LadderStepVM) **plus**
  `buildRegsPage` declared but not implemented (see Signatures; body `throw new Error("buildRegsPage: T21")`)
- create `app/packages/core/src/regs/line.ts` + `app/packages/core/src/regs/line.test.ts`
- create `app/packages/core/src/regs/words.ts` + `app/packages/core/src/regs/words.test.ts`
- create `app/packages/core/src/regs/inputs.ts` — contracts.md §3 types, with the T04/T06/T07/T09 additions listed below
- rewrite `app/packages/core/src/regulations.ts` (exists, 83 lines)
- modify `app/packages/core/src/index.ts` (exists): replace the header sentence "REGULATION RECORDS ARE NOT HERE: they are not
  integrated, and `./regulations` is the typed placeholder they plug into — read it first." with "The Regulations page's logic is
  `./regs` (re-exported through `./regulations`) — pure functions from a hydrated `RegsInputs` to a `RegsPage`."; keep
  `export * from "./regulations";` (it now carries `./regs`).

## Signatures
```ts
// regs/model.ts  (contracts.md §2, verbatim, with these exact edits)
export type { Weekday } from "../calendar.generated";   // REUSE the generated type, do not redeclare it
export interface Day { readonly month: number; readonly day: number }
export interface Who { readonly age: "adult" | "u16"; readonly residency: "resident" | "non_resident" | "non_resident_alien" }
export type RegsTarget = { kind: "part"; item: string; partIx: number } | { kind: "section"; handle: number };
export type Tab = "keep" | "gear" | "methods" | "licence";
export interface RegsPage { readonly version: "regs-v33"; /* …rest exactly as contracts.md §2… */ }
export function buildRegsPage(inp: RegsInputs, opts: { today: Day; who: Who; tot?: "a" | "b"; pid?: string }): RegsPage; // T21
// pid: test-harness page id only; sheet keys carry no state id (contracts.md §1 / §6 #1)
// …HeaderVM, KeepVM, KeepBlock, StripVM, InheritedVM, BannerVM, GroupVM, GroupHeadVM, OriginVM, TreeNodeVM, TierVM, KindVM,
//    ReleaseRowVM, FishPicVM, NotKeptVM, PlainRowVM, OtherFishRowVM, WhereNotVM, GearVM, GearLineVM, ChangeVM, SourceLineVM,
//    BaitVM, YesNoVM, MethodsVM, LicenceVM, LicenceBlockVM, SheetVM, SheetBlock, LadderStepVM — verbatim from contracts.md §2.

// regs/line.ts  (contracts.md §1)
export type RuleKey = string;   // `${entry_id}::${rule_id}`
// SheetKey (app forms, no state id; ProfileKey, parseSheetKey/appKeyOf/goldenKeyOf/profileKeyOf) and Run (book with
// `pdf: number | null; bare?: true`; link open = SheetKey | {day} | {tab}) — copy VERBATIM from contracts.md §1 (the final,
// consolidated types; T18/T36 amended them). Do not re-type them here.
export type Line = readonly Run[];
export function text(s: string): Run;                                  // { t: "text", s }
export function plain(l: Line | null | undefined): string;            // runs concatenated AS IS, then /\s+/g → " ", trim
export function plainAll(ls: readonly (Line | null | undefined)[]): string; // separate Lines/fields: joined with ONE space, then collapse (golden gap())
export function squash(s: string): string;                            // s.replace(/\s+/g, " ").trim()
export function nbsp(s: string): string;                              // /(\d) (cm|in)\b/g → "$1 $2"
export function boldN(s: string, n: number | null, size?: "big" | "inline"): Line;

// regs/words.ts
export const WORDS: Readonly<{ /* named constants, see below */ }>;
export const ALL_WORDS: ReadonlySet<string>;
export function wordsIn(l: Line): string[];   // the text/link strings of a Line that are not data (helper for no-hand-facts)

// regs/inputs.ts — contracts.md §3 + the additions T04/T06/T07/T09 make (declare them NOW so later tickets only fill bodies)
export interface RegsInputs { /* contracts.md §3 + `year` as in T09 + `mergedParts` (T09/T14) */ }
export interface AkeyRow { /* contracts.md §3, verbatim */ }
export interface CellIds { readonly ladder: number; readonly rows: number; readonly gear: number; readonly licence: number; readonly display: number }
export type VerdictWire = readonly [speaks: readonly number[], beside: readonly number[], shown: readonly number[],
  nym: readonly number[], partly: readonly (readonly [number, readonly number[]])[], lost: readonly (readonly [number, number, number])[]];
export interface LadderFrameWire { readonly common: VerdictWire; readonly rows: readonly (readonly [fish: string, none: VerdictWire, hatchery: VerdictWire, wild: VerdictWire])[] }
export interface ExportRule { /* contracts.md §3, verbatim; plus  readonly dimension?: string; readonly binds?: string */ }
export interface RuleView { /* contracts.md §3 verbatim ({rix, key, facts, export}); T06 widens it IN THIS FILE */ }
export interface Statics { /* contracts.md §3, with exportSpecies typed as SpeciesBook (T06) */ }
```

`words.ts` structure (the contract for every later ticket that adds words):
```ts
/** Every English string a regs presenter may emit that is NOT data. Grouped by the page_v36 / prototype function that says it.
 *  A function word takes its data as parameters and returns a string; ALL_WORDS holds each constant and each function's
 *  FIXED fragments, so the no-hand-facts test can subtract data strings and assert the rest ⊆ ALL_WORDS. */
export const WORDS = Object.freeze({
  // tabs (golden header.tabs)
  TAB_KEEP: "What you can keep", TAB_GEAR: "Gear", TAB_METHODS: "Methods", TAB_LICENCE: "Licence",
  // header
  TODAY: "Today", MAP: "Map", PARTS: "Parts",
  // rich text
  BOOK: "Rule book",                     // plain(): `${BOOK} p.${printed} ↗`
  TERMS_MANY: "What these words mean",   // plain() of a term run with 2+ ids
  TERM_ONE: "ⓘ",
  // describe() (T06 fills: FROM_STREAMS " from streams", FROM_LAKES " from lakes", NO_FISHING_FOR "No fishing for ", …)
  // missing / refusal (T03/T06 fill: MISSING "Missing from the answers file:", NO_WORDS "(no words in the answers for ", …)
} as const);
export const ALL_WORDS: ReadonlySet<string> = new Set(Object.values(WORDS).filter((v): v is string => typeof v === "string"));
```
Day/month words are NOT duplicated here: `MONTHS`, `WEEKDAYS` come from `../calendar.generated` (generated) — `WD3 = WEEKDAYS.map(w => w.slice(0,3))`
and `MONTH1 = MONTHS.map(m => m[0])` are derived constants in `words.ts` and their members are added to `ALL_WORDS`.

`regulations.ts` after the rewrite (whole file):
```ts
/**
 * REGULATIONS — the Regulations page (regs v33), ported from the prototype `app/design/regs-v11.html` @ 04765ab5 and
 * `pipeline/deliver/answers/reference/page_v36.js`. Data: the regs store `regs.sqlite` (format regs-store/4), read by
 * `@app/data` `loadRegsInputs()`; logic: `./regs` (pure); hooks: `@app/ui` `useRegulations`; components: `@app/ui-native` `regs/`.
 * Spec: handoff/PLAN/app-integration/. Rules kept from the old placeholder: licensing never affects open/closed; the angler
 * is unknown until they say (who defaults to adult resident, T11); every sentence is data or a words.ts constant.
 */
export * from "./regs";
```
The old exports (`RegulationsAvailability`, `REGULATIONS`, `RegulationRecord`, `WaterRegulations`, `regulationsFor`) are
**removed**: `grep -rn` over `app/packages app/apps app/conformance app/tools` (excluding `dist/`) finds no importer besides
`core/src/index.ts` (verified 2026-10-20); the only other mentions are doc comments in
`app/packages/ui-native/src/RegulationsPlaceholder.tsx:7` and `WaterScreen.tsx:8` — leave those for T23, which deletes the placeholder.
Keeping dead types would give the app two "shapes the UI will consume".

## Ports
- `line.ts` `nbsp`, `boldN`: prototype `buildKeep.nbsp`, `buildKeep.boldN` (regs-v11.html, ranges in inventory.json @04765ab5).
- `plain()`/`plainAll()`: port/golden.mjs `gap()`/`lines()` normalisation as of golden b999cfd7 (one space where two ELEMENTS abut,
  none inside a Line; `/\s+/g → " "`, trim) — contracts.md §1.
- No page_v36 code is ported here.

## Reads
None (types only).

## Tests
`app/packages/core/src/regs/line.test.ts` (project `logic`, no store):
1. Within one Line runs concatenate AS IS (contracts.md §1, golden ≥ b999cfd7): `plain([{t:"text",s:"Open"},{t:"text",s:"today"}])` → `"Opentoday"`;
   between separate Lines one space: `plainAll([[{t:"text",s:"Open"}],[{t:"text",s:"today"}]])` → `"Open today"`.
2. `plain([{t:"num",s:"2",size:"big"},{t:"text",s:" a day per angler from streams"}])` → `"2 a day per angler from streams"`
   (golden `states.vedder_515.keep.groups[0].head.lead`); the head's two Lines `plainAll([lead, words])` → golden
   `head.text` = `"2 a day per angler from streams of the 4 a day per angler across Region 2, lakes included"`.
3. `plain([{t:"text",s:"Only 2 of the 4 may be from streams"},{t:"term",ids:["daily_quota"]}])` → `"Only 2 of the 4 may be from streamsⓘ"`
   (no space is invented inside a Line — a presenter that wants one writes it); two ids → `"…streamsWhat these words mean"`.
4. `plain([{t:"book",printed:21,pdf:23}])` → `"Rule book p.21 ↗"` (golden vedder_515 tree[0].origin.pages[0].text).
5. `nbsp("over 50 cm (20 in)")` → `"over 50\u00a0cm (20\u00a0in)"`; `nbsp("50cm")` unchanged.
6. `boldN("Only 2 of the 4 may be from streams", 2)` → `[{t:"text",s:"Only "},{t:"num",s:"2",size:"inline"},{t:"text",s:" of the 4 may be from streams"}]`;
   `boldN("No limit", null)` → `[{t:"text",s:"No limit"}]`; `boldN("12 or 2", 2)` bolds the `\b2\b` (not the "2" in "12"); `plain(boldN(x, n)) === x`.
7. `plain(null)` → `""`; collapse + trim: `plain([{t:"text",s:" a  "},{t:"text",s:"b"}])` → `"a b"`; `plainAll([null, [{t:"text",s:"x"}]])` → `"x"`.

`app/packages/core/src/regs/words.test.ts`:
1. `Object.isFrozen(WORDS)`; every value is a non-empty string or a function.
2. `ALL_WORDS.has("What you can keep")`, `.has("Rule book")`, `.has("Fri")`, `.has("J")`.
3. **tabs equal the golden**: for `id` in `["vedder_515","klwa_1010"]`, `[WORDS.TAB_KEEP, WORDS.TAB_GEAR, WORDS.TAB_METHODS, WORDS.TAB_LICENCE]`
   equals `golden.states[id].header.tabs.map(t => t.text)` = `["What you can keep","Gear","Methods","Licence"]` (read through
   T12's harness when it lands; until then assert the literal).

Typecheck test: `app/packages/core/src/regs/model.test.ts` — a `satisfies` block building one minimal `RegsPage` object literal
(every required field) so a contract edit that forgets a field fails `tsc`.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs
grep -rn "regulationsFor\|WaterRegulations\|RegulationRecord" app/packages app/apps app/conformance --include='*.ts' --include='*.tsx' | grep -v dist   # only doc comments in ui-native
```

## Notes
- CONTRACT CHANGE: applied to contracts.md (§2 `version`, `Weekday` re-export).
- CONTRACT CHANGE: applied to contracts.md (§2 opts `pid?` — test harness only; keys carry no state id, §6 #1).
- CONTRACT CHANGE: applied to contracts.md (§3 `CellIds`, `VerdictWire`, `LadderFrameWire`, `RegsYear`, `MergedPart`; §1 `text`/`plainAll`/`squash`; header: `regulations.ts`).
- Do NOT export `answers.generated` from `core/src/index.ts` wholesale (it has a `Place` that would collide); `inputs.ts` imports
  the generated types it needs and re-exports only those: `RowsValue, Row, Decided, FishAnswer, GearAnswer, GearValue, StatusFrame,
  LicenceValue, ProfileAnswer, Holds, TidalHolds, UnderSixteen, RuleFacts, PartFacts, Picker, Choice, Moment, MomentHours,
  ClockTime, GlossaryTerm, StripState, InheritedLift` (+ `Place as AnswersPlace`).
- Name every new core export with no collision against `core/src/index.ts` today (checked: no `Day`, `Tab`, `Who`, `Line`, `Run`,
  `plain`, `nbsp`, `boldN`, `RegsInputs` exist).
