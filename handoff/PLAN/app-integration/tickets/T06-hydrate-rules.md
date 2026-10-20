# T06 — core hydrate 1: rule views, species names, `describe()` (+ the page-v36 hydrate oracle)

Phase 0 · Depends on T00, T04 (types only) · Blocks T07, T08, T09, T13–T19, T30–T36 · Decisions — · Size 1 day
(split if needed: T06a = `port/hydrate.mjs` oracle + `names.ts`/`time.ts`; T06b = `rules.ts` + `describe.ts`)

## Goal
The decode layer of page v36 (L18–L115 @04765ab5, unchanged since 18bc2a11) as pure, parameterised TS: every rule becomes a
`RuleView` (export words + display facts + the per-part `via`/`rank` + expanded species + date windows + time condition), and
`describe(rule, book)` says it in the page's words. No globals: each function takes what page v36 read from a global. A node
generator runs page v36's own code over its own data and writes the oracle these functions are tested against
(golden.json holds rendered pages, not `describe()` outputs).

## Files
- (exists, built 2026-10-20) `app/design/regs-v11/port/hydrate.mjs` → `app/design/regs-v11/port/golden/hydrate.json` (1.3 MB) — commit both with this ticket
- create `app/packages/core/src/regs/names.ts` (+ `names.test.ts`) — species words
- create `app/packages/core/src/regs/time.ts` (+ `time.test.ts`) — `fmtMd`, `rangeTxt`, `whenDates`, `timeCond`, `winTxt`
- create `app/packages/core/src/regs/rules.ts` (+ `rules.test.ts`) — `RuleView`, `ruleView`, `noRule`, `RuleIndex`
- create `app/packages/core/src/regs/describe.ts` (+ `describe.test.ts`) — `bands`, `bandTxt`, `describe`, `closedGate`, `covers`, `cutWords`
- modify `app/packages/core/src/regs/inputs.ts` — widen `RuleView`, type `Statics.exportSpecies` as `SpeciesBook`
- modify `app/packages/core/src/regs/words.ts` — the constants below
- modify `app/packages/core/src/regs/index.ts` — export the four modules
- create `app/conformance/src/regs/describe.parity.test.ts` — the store-backed parity (uses `provinceStore()` from `app/conformance/src/regs/golden.ts`, created by T05)

## Signatures
```ts
// names.ts — replaces page_v36 globals FISH, GROUPS, PROT, SALM, GAME (D.species = static export.species)
export interface SpeciesBook {
  readonly fish: Readonly<Record<string, { readonly name: string }>>;
  readonly groups: Readonly<Record<string, { readonly name: string; readonly members: readonly string[]; readonly open?: boolean }>>;
  readonly protected?: Readonly<Record<string, { readonly name: string }>>;
  readonly salmon?: Readonly<Record<string, { readonly name: string }>>;
}
export function gameFish(sp: SpeciesBook): readonly string[];                       // GAME = groups.ALL_GAME_FISH.members (:51)
export function expand(sp: SpeciesBook, codes: readonly string[] | null | undefined): string[];   // :48
export function spName(sp: SpeciesBook, code: string): string;                     // :52
export function lc(s: string): string;                                              // :53 — proper-noun list in WORDS.LC_KEEP
export function groupFor(sp: SpeciesBook, codes: readonly string[]): string | null; // :54
export function join(n: readonly string[], conj?: string): string;                  // :55, conj default WORDS.AND (" and ")
export function lcNames(sp: SpeciesBook, codes: readonly string[], conj?: string, max?: number): string;  // :56
export function Names(sp: SpeciesBook, codes: readonly string[], conj?: string, max?: number): string;   // :57
export function writtenName(sp: SpeciesBook, f: { species?: readonly string[]; species_except?: readonly string[] }, conj?: string): string; // :58
export function cap(s: string): string;  export function clean(s: string | null | undefined): string;    // :32–33

// time.ts — replaces CALENDAR.months (MON) and DAYS (uses ../calendar.generated MONTHS)
export type Md = number;                                   // MMDD, page v36's day key (Feb 29 = 229 is never produced)
export function fmtMd(md: Md): string;                     // :27 "Jul 4"
export function rangeTxt(a: Md, b: Md): string;            // :62 "Jul 4", "Jul 4–15", "Jul 4–Aug 2"
export function whenDates(w: When | null | undefined): [Md, Md][];   // :61
export function timeCond(w: When | null | undefined): string;       // :63–69
export function winTxt(wins: readonly (readonly [Md, Md])[]): string; // :70
export interface When { dates?: readonly { from_month: number; from_day: number; to_month: number; to_day: number }[];
  weekdays?: readonly string[]; hours?: { start: ClockTime; end: ClockTime } | null }

// rules.ts — replaces RULES, RIX, D.rid, DRULES, NORULE, RX, and makePlace.R's per-part rank (:73–91, :122–125)
export interface RuleView {                       // inputs.ts (widened; contracts.md §3)
  readonly rix: number; readonly key: RuleKey;    // `${entry_id}::${rule_id}`
  readonly export: ExportRule; readonly facts: RuleFacts | null;
  readonly f: Readonly<Record<string, unknown>>;  // = export.fields (page `r.f`)
  readonly kind: RuleFacts["kind"] | null;        // page `r.k`
  readonly baseRank: number;                      // export.prov.rank
  readonly via: "reach" | "trib";                 // the part's ruleset list holding it (absent: "reach", page :122)
  readonly rank: number;                          // via trib && baseRank >= 0 → 1, else baseRank (page makePlace.R)
  readonly species: readonly string[]; readonly speciesExcept: readonly string[];
  readonly wins: readonly (readonly [Md, Md])[]; readonly tcond: string;
  readonly part: string | null;                   // fields.undrawn_part
  readonly notes: readonly string[];              // WORDS.UNCERTAIN + why | WORDS.NO_REASON ; WORDS.CURATOR + review_reason
  readonly missing: boolean;                      // the NORULE stand-in
}
export function ruleView(rix: number, exp: ExportRule, facts: RuleFacts | null, sp: SpeciesBook, via: "reach" | "trib"): RuleView;
export function noRule(rix: number): RuleView;    // page NORULE :88 — key "#<rix>", rank 4, label/verbatim WORDS.missingRule(rix)
export interface RuleIndex { get(rix: number): RuleView; has(rix: number): boolean; readonly size: number }
/** page makePlace.R over a part: `via` from the part's ruleset {reach, trib}; any rix not in the store → noRule(rix). */
export function ruleIndex(stored: ReadonlyMap<number, { export: ExportRule; facts: RuleFacts }>,
                          via: ReadonlyMap<number, "reach" | "trib">, sp: SpeciesBook): RuleIndex;
export function viaOf(ruleset: { reach: readonly number[]; trib: readonly number[] } | null): ReadonlyMap<number, "reach" | "trib">;

// describe.ts
export interface Band { readonly a: number; readonly b: number; readonly take: number | null }   // b Infinity = no top
export function bands(r: RuleView): Band[];                       // :94
export function bandTxt(a: number, b: number): string;            // :95
export function closedGate(r: RuleView): boolean;                 // :90
export function covers(r: RuleView, S: string): boolean;          // :91
export function describe(r: RuleView, sp: SpeciesBook): string;   // :97–112
export function cutWords(t: string, n: number): string;           // :113
```

`words.ts` additions (each a constant or a function whose fixed fragments go into `ALL_WORDS`):
`AND " and "`, `OR " or "`, `MORE (n) => " + ${n} more"`, `LC_KEEP` (the 16 proper-noun prefixes of :53: Dolly, Arctic, Nooksack, Salish,
Cultus, Enos, Coastal, Westslope, Misty, Vananda, Paxton, Hadley, Morrison, Rocky, Speckled, Charlotte), `TROUT "trout"`, `FISH "fish"`,
`MIN_BEFORE "min before "`, `MIN_AFTER "min after "`, `FROM_STREAMS " from streams"`, `FROM_LAKES " from lakes"`,
`NO_FISHING_FOR "No fishing for "`, `ALL_GAME "all game fish"` → `ANY_GAME "any game fish"`, `RELEASE_EVERY "Release every "`,
`NO_LIMIT_ON "No limit on "`, `KEEP "Keep "`, `A_DAY " a day"`, `SHARED ", shared"`, `NONE ", none "`, `INSIDE "Inside the day’s total: "`,
`AT_MOST "at most "`, `RELEASE_ANY "release any "`, `A_MONTH "month"`, `A_YEAR "year"`, `SMALLER ". Smaller fish don’t count"`,
`UNDER (b) => "under ${b} cm"`, `OVER (a) => "over ${a} cm"`, `NO_WORDS (key) => "(no words in the answers for ${key})"`,
`UNCERTAIN "Uncertain placement: "`, `NO_REASON "no reason given"`, `CURATOR "Curator still to settle: "`,
`MISSING_RULE (ix) => "(rule ${ix}: missing from the page’s data)"`, `MISSING_DATA (ix) => "Missing from the page’s data: rule ${ix}"`.

`app/design/regs-v11/port/hydrate.mjs` (BUILT; node only, no Chrome/network/.venv, 0.7 s): stages `page_v36.js` at `--ref` (stage.mjs),
reads `page_v36.html`'s `#data/#answers/#cases` and `calendar.generated.js` at the same ref (`git show`), evaluates page_v36.js up to the
line before `function segBar` (1–242 @04765ab5) with `YEAR = taps.json meta.year` (2026), and writes
`app/design/regs-v11/port/golden/hydrate.json`:
```
{ meta: {generator, ref, sha, golden: "04765ab5", version: "v33", year: 2026, decodeLines: "1-242"},
  describe: {<rix>: string},                       // describe() of all 726 rules in page v36's slice
  rules: {<rix>: {key, kind, baseRank, species, speciesExcept, wins, tcond, part, notes}},
  states: {<stateId>: {
    s, via: {<rix>: "trib"}, ranks: {<rix>: rank},   // only trib entries / only rank ≠ baseRank
    cites: [rix…],                                   // every rix the state's settle + model name (describe() each from the top-level map)
    settle: {status, broad, speaks, beside, shown, inpart: [[rix, now]], lifted: [[rix, by]], timed, side, standing, anglerclosed},
    model: {spp, line, missingRows, status, broad, rows: [{kind, pool, win, daily, narrow, members}],
            R: {<S>: {H: {status, daily, win, narrow} | null, W: …}}},     // Infinity written "Infinity"
    dayStatus: [ [[status, nDays]…] per model row ] } } }   // page strip(w, row.members), 365 book days
```
Run: `node app/design/regs-v11/port/hydrate.mjs [--ref HEAD]` → `hydrate: s equals taps.json on 134/134 states, 726 rules described
-> app/design/regs-v11/port/golden/hydrate.json` (exit 1 on any mismatch). Add it to `port/sync.mjs` after `taps.py` (this ticket).

## Ports
page_v36.js @04765ab5 (= 18bc2a11): `cap` 32, `clean` 33, `FISH`/`expand`/`PROT`/`SALM`/`GAME`/`spName`/`lc`/`groupFor`/`join`/
`lcNames`/`Names`/`writtenName` 47–60, `whenDates` 61, `rangeTxt` 62, `timeCond` 63–69, `winTxt` 70–72, `RULES`/`RIX` 73–87,
`NORULE` 88, `RX` 89, `closedGate` 90, `covers` 91–93, `bands` 94, `bandTxt` 95–96, `describe` 97–112, `cutWords` 113–115,
`makePlace.R` 122–125, `fmtMd` 27, `RANK` 25 (words only: T14 uses it).
Hidden globals replaced by parameters: `D.species` (→ `SpeciesBook`), `D.rules`/`D.rid`/`DRULES` (→ `stored` map from T04 `rules()`),
`CALENDAR.months` (→ `calendar.generated` `MONTHS`), the part's `rules [[ix, via]]` (→ `viaOf(store.ruleset(akey.ruleset))` —
page_data.py :341 builds it from the same ruleset's `reach`/`trib` lists; no rix is in both, verified over all 2,111 rulesets).
What changes: `r.notes`/`r.wins` are frozen arrays; `RuleView` is a plain object (page uses `Object.create(base)` per place).

## Reads
`rule.j` `{entry_id, rule_id, type, family, dimension, label, parts, verbatim, fields{species, species_except, origin, water, take,
unlimited, period, when{dates, weekdays, hours}, undrawn_part, review_reason}, prov{rank, uncertain, why}}`; `display_rule.j`
`{kind, closure, bands, plain}`; `static export.species {fish, groups, protected, salmon}`; `ruleset.j {reach, trib}`.

## Tests
Store-free (`names.test.ts`, `time.test.ts`, `describe.test.ts` over hand-built `RuleView`s; `SpeciesBook` = a literal subset of
`export.species` with RB, CT, EB, GB, DV, LT, LMB, SMB, groups `ALL_GAME_FISH`, `TROUT_CHAR`, `CHAR`, `SA`, `BASS`):
1. `lcNames(sp, ["EB","GB"])` → "brook trout and brown trout"; `["DV","LT"]` → "Dolly Varden/bull trout and lake trout" (LC_KEEP);
   `(["RB","CT","EB","GB","DV"], " or ", 3)` → "rainbow trout, cutthroat trout + 3 more"; `["LMB","SMB"]` → "bass" (groupFor);
   `spName(sp,"TROUT_CHAR")` → "Trout and char"; `spName(sp,"ZZ")` → "ZZ".
2. `writtenName(sp, {species:["TROUT_CHAR"], species_except:["CHAR"]})` → "trout" (rix 3067); `{species:[]}` → "fish".
3. `bandTxt(0,50)` → "under 50 cm"; `(50, Infinity)` → "over 50 cm"; `(30,50)` → "30–50 cm".
4. `rangeTxt(601,630)` → "Jun 1–30"; `(704,715)` → "Jul 4–15"; `(704,802)` → "Jul 4–Aug 2"; `(704,704)` → "Jul 4".
5. `timeCond({hours:{start:{offset_min:60,solar:"sunset"},end:{offset_min:-60,solar:"sunrise"}}})` → "60 min after sunset–60 min before sunrise" (rix 576);
   `{hours:{start:{at:"12:00",offset_min:0},end:{at:"00:00",offset_min:0}}}` → "12:00–00:00" (rix 1539); `{weekdays:["Saturday","Sunday"]}` → "Sat, Sun" (rix 1366).
6. `ruleView` via/rank: baseRank 0, via "trib" → rank 1 (ape_501, rix 1644 `r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8::atnarko_bella_coola_rivers.r10`);
   baseRank -1 via "trib" → rank -1; `noRule(7)` → key "#7", rank 4, missing true, `describe` → WORDS.MISSING_RULE(7) text.

`describe()` literals (page v36's own output, `app/design/regs-v11/port/golden/hydrate.json` `describe[rix]`; also asserted store-free with the rule's real `rule.j`/`display_rule.j`
copied into the test as fixtures):
| rix | kind | `describe` |
|---|---|---|
| 529 | gate (closure) | `No fishing for any game fish, Jun 1–30` |
| 522 | gate | `No fishing for any game fish` |
| 1367 | gate (release, weekdays) | `Release every kokanee (Mon, Tue, Wed, Thu, Fri)` |
| 576 | gate (hours) | `No fishing for any game fish (60 min after sunset–60 min before sunrise)` |
| 1539 | gate (hours, window) | `No fishing for any game fish, Jul 15–Aug 31 (12:00–00:00)` |
| 3083 | pool | `Keep 20 bass a day, shared` |
| 1366 | pool (weekend) | `Keep 5 kokanee a day (Sat, Sun)` |
| 3095 | subcap | `Inside the day’s total: at most 2 hatchery trout and char from streams` |
| 3092 | sizecap | `Inside the day’s total: at most 1 over 50 cm (trout and char)` |
| 3097 | size | `Dolly Varden/bull trout and lake trout: release any under 60 cm` |
| 3101 | size (origin, water) | `Hatchery trout and char from streams: release any under 30 cm` |
| 3380 | annual | `10 hatchery steelhead a year` |
| 3079 | possession | `Possession: 2 days’ quota. Fish at home don’t count` |
| 2281 | possession_cap | `Have no more than 2 lake trout in possession. Fish at home don’t count` |
| 3359 | caught | `Any fish snagged — even by accident — must be released` |
| 78 | vessel (default → verbatim) | `Speed restriction (8 km/h)` |
| 3351 | standing (default) | `Within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap.` |

Store-backed (`app/conformance/src/regs/describe.parity.test.ts`, `it.skipIf(!hasProvinceRegs())`):
7. For every rix in `app/design/regs-v11/port/golden/hydrate.json` `.describe` (726): `describe(ruleIndex(store.rules(), new Map(), book).get(rix), book)` === the oracle string.
8. For every rix: `{key, kind, baseRank, species, speciesExcept, wins, tcond, part, notes}` deep-equal `hydrate.json.rules[rix]`.
9. For every state: with `via = viaOf(await store.ruleset((await store.akey(state.akey)).ruleset))`, every `hydrate.json.states[id].via`
   entry is "trib" in `via` and every `ranks[rix]` equals `ruleIndex(…, via, …).get(rix).rank`.

## Acceptance
```sh
node app/design/regs-v11/port/hydrate.mjs && git diff --exit-code app/design/regs-v11/port/golden/hydrate.json
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs/names packages/core/src/regs/time packages/core/src/regs/rules packages/core/src/regs/describe conformance/src/regs/describe.parity
```

## Notes
- CONTRACT CHANGE: applied to contracts.md (§3 `RuleView`, `RuleIndex`, `Statics.exportSpecies: SpeciesBook`). A RuleView is PER PART.
- page v36's `D.rules` is the slice page_data.py cut (726 rules); the store has 3,406. rix are the same numbers (page_data.py :323,
  `rid = {str(rix[k]): k}`; rule 529 compared field-by-field — identical `rule.j`).
- `describe()`'s `w.replace(/^all game fish$/,…)` matches the WHOLE written name only — "all game fish" inside a longer list is left alone.
- `lc()`'s proper-noun list is data the page typed; keep it in `words.ts` (`LC_KEEP`) so a new species name is a words edit.
- The oracle runs page v36 in node with `YEAR = 2026` (taps.json's year). It needs no `.venv` and no Chrome; it reads only committed files.
