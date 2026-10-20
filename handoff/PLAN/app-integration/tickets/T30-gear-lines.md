# T30 — presenter: gear lines, changes line, item sources, closed note, day line

Phase 3 · Depends on T00 (line/words), T04 (statics), T05 (`weekdayOf`), T06 (`RuleView`), T09 (`RegsInputs.frames.gear`,
`year.displayByDay`), T12 (golden harness), T13 (`strip.ts` book-day helpers) · Blocks T31, T32, T34, T35 · Decisions: D6 (pages carry `pdf` for the link) ·
Size ≤ 1 day

## Goal
`core/src/regs/gear.ts` turns one day's `GearAnswer.summary` into the Gear tab's top block exactly as regs v33 draws it:
the day line ("Today, Oct 10" / "Fri, May 15"), an optional fly-only **mode** line, then the labelled lines
**Lines · Hooks · Weight on the line**, each value big, each with its ⓘ terms, its quiet **changes line** (chip "Next",
dates bold, the data's words) and its **source line** ("Province · Region 2 · This water’s ›" + "Rule book p.N ↗").
On a closed day the tab is one sentence: "No fishing here until Jul 1." It also owns the two helpers every other
Gear/Methods item uses: `sourceLine()` (who-words from the rule's LEVEL, not from scraped chip text) and `closedUntil()`.

## Files
- create `app/packages/core/src/regs/gear.ts`
- create `app/packages/core/src/regs/gear.test.ts` (unit)
- create `app/conformance/src/regs/gear.parity.test.ts` (golden, block `gear`)
- modify `app/packages/core/src/regs/words.ts` — add `GEAR` (below)
- modify `app/packages/core/src/regs/model.ts` — the CONTRACT CHANGEs in Notes

## Signatures
```ts
// from T13 strip.ts (do NOT re-implement): BOOK_DAYS (365, no Feb 29), bookIndex(d), displayOnBookDay(inp, i), spanWords(a, b)
export function bookBits(inp: RegsInputs): readonly (number | null)[];   // 365: displayOnBookDay(inp, i)?.strip ?? null — shared with T31

// gear.ts
export interface GearCtx {
  readonly day: Day; readonly weekday: Weekday; readonly today: boolean;
  readonly rules: RuleIndex;                  // T06/T09 (contracts §3)
  readonly glossary: ReadonlySet<string>;     // statics.glossary.terms ids (tipFor drops a term the glossary lacks)
  readonly chips: ChipWords;                  // learnChipWords(gear) — computed once per page
}
export type ChipWords = ReadonlyMap<string, string>;            // level word -> chip word, e.g. "B.C." -> "Province"
export function levelWord(r: RuleView): string;                 // page v36 levelTag minus " rule": "B.C." | "Region 2" | "Named area" | "Downstream" | "Federal or park" | "This water’s"
export function learnChipWords(g: GearAnswer, rules: RuleIndex): ChipWords;
export function whoWord(r: RuleView, chips: ChipWords): string; // chips.get(levelWord(r)) ?? levelWord(r)
export function sourceLine(open: SheetKey /* srcs:… app form, no state id */, rixs: readonly number[], ctx: GearCtx): SourceLineVM | null;
export function dayLine(ctx: Pick<GearCtx, "day" | "weekday" | "today">): string;   // "Today, Oct 10" | "Fri, May 15"
export function closedUntil(bits365: readonly (number | null)[], today365: number): string;
export function changeVM(when: string, says: string, inForce: boolean): ChangeVM;
export function gearLines(g: GearAnswer, ctx: GearCtx): { mode: GearVM["mode"]; lines: readonly GearLineVM[] };
export function buildGearTop(inp: RegsInputs, ctx: GearCtx): Omit<GearVM, "bait">;   // T31 adds bait; buildRegsPage composes
```

`words.ts` additions (verbatim from the prototype / page v36; nothing else is literal in `gear.ts`):
```ts
export const GEAR = {
  TODAY: "Today",
  GLAB: [["lines", "Lines", ["lines"]], ["hook", "Hooks", ["attach", "hook"]], ["weight", "Weight on the line", ["weight"]]] as const,
  CHIP_NOW: "Now", CHIP_NEXT: "Next", IN_FORCE: "· in force today", ALL_YEAR: "All year",
  SOURCES: "Sources", SOURCES_ARIA: "Sources: ", SRC_ARROW: "›",
  CLOSED_UNTIL: (mon3: string, d: number) => `No fishing here until ${mon3} ${d}.`,
  CLOSED_ALL_YEAR: "No fishing here on any day of the year.",
  LEVEL: { 4: "B.C.", 3: "Region", 2: "Named area", 1: "Downstream", [-1]: "Federal or park", 0: "This water’s" },   // levelTag minus " rule"
  CHIP:  { 4: "Province", 2: "Named area", 1: "From downstream", [-1]: "Federal / parks", 0: "This water" },     // page v36 RANK[*].t; rank 3 ("Region") is never learned
} as const;
```

## Ports (@04765ab5; inventory names)
- `dayLine` 1373 → `dayLine()`: `today ? GEAR.TODAY : WD3[weekday]` + ", " + `MON3 d`.
- `closedUntil` 1457–1463 → `closedUntil()`: walk j = 1…364 from `today365`, cyclic (`(t + j) % 365`); first day with
  `bits != null && !(bits & 1)` → `CLOSED_UNTIL`; none → `CLOSED_ALL_YEAR`. Input is `bookBits(inp)`, `today365 = bookIndex(day)`.
- `levelOf` 1375 / page v36 `levelTag` L806 → `levelWord()`: `rank = r.export.prov.rank ?? 4`; 4 → "B.C."; 3 →
  `Region ${/^z(\d+)/.exec(entry_id)[1]}` (no match → "Region"); 2 "Named area"; 1 "Downstream"; −1 "Federal or park";
  else "This water’s". (The prototype reads it back from the rule sheet's sub "B.C. rule · …"; the port computes it.)
- `learnChipWords` 1377–1379 + `wordOf_` 1380 → `learnChipWords()` / `whoWord()`. **Exact mapping** (verified against the
  v33 snapshot: the only `.gsrc` chips are in v36's `line` pane, and every learned pair is `B.C.→Province`,
  `This water’s→This water`, `Region N→Region` (ignored)): the learned set = the ranks of the rules v36 `gearParts` gives a
  `gRow` chip (page v36 L1209–1220):
  `counts[s].by[0]` for s ∈ {`lines_per_angler`, `terminal_attachments_per_line`, `points_per_hook`, `weight_per_line_kg`,
  `hooks_per_line`} plus `flies_per_line` only when `terminal_attachments_per_line` is absent; `elements["barb:barbed"].by[0]`
  when its verdict is `ban`; `elements["lure:artificial_lure"].by[0]` when `ban`. For each such rule with rank ≠ 3:
  `chips.set(levelWord(r), GEAR.CHIP[rank])`. Everything else keeps its level word — this is why golden has both
  "This water ›" (10 states: a rank-0 rule set a gear count) and "This water’s ›" (57), and "Downstream ›" (13; never learned).
- `itemSrc` 1464–1474 → `sourceLine()`: rixs deduped in first-seen order, dropped when not in `ctx.rules` (the prototype
  drops keys absent from SNAP); none left → `null`. `pages` = every `facts.book` `[printed, pdf]` of those rules, first pdf
  per printed page, sorted by printed. `who` = distinct `whoWord` in key order joined " · ", empty → "Sources"; then " ›".
  `aria` = "Sources: " + words joined ", " (or "rules"). `keys` = the rule keys (for the T36 `srcs:` sheet).
- `gnote` 1475 → `changeVM()`: chip `inForce ? "Now" : "Next"` (the `chg=a` ◷ variant is not ported).
- `buildGear` 1476–1572, the summary half (`sm`, `val`, `rowOf`, `notesFor`, `citesAt`, `tipFor`, the rest-terms splice):
  - `parts = summary.parts.filter(p => summary.says.toLowerCase().includes(p.says.toLowerCase()))`.
  - mode: the first part `p.part === "mode"` → `{ value: p.says (unchanged), terms: tip(p.terms), changes, src }`.
  - GLAB rows in order lines · hook(`attach`,`hook`) · weight; a row with no part is not drawn. `values` = each part's
    `val(p, label)`: drop a leading `a `/`an `; drop the label + " " prefix (case-insensitive); cap1.
  - `rowOf(change)`: mode drawn → "mode"; `/\bfly\b|\bflies\b|hook|lure/i` → hook; `/weight|sinker|float/i` → weight;
    `/\blines?\b|\brods?\b/i` → lines; else hook; a row not drawn → the LAST drawn row. Every summary change is
    `changeVM(c.when, c.says, false)` — summary changes are never in force (chip always "Next"; golden 3/3).
  - each block's source: `sourceLine(\`srcs:gl|${k}\`, [...partCites, ...changeCitesRoutedHere])`.
  - terms: `tip(ts) = ts.filter(t => ctx.glossary.has(t))`; `rest = summary.terms` minus every part's terms (before the
    glossary filter); `tip(rest)` is appended to the LAST block's terms **only if that block has a source line and no
    changes** (the prototype's `</span><p class="isrc"` splice matches nothing otherwise) — vedder_1010 Weight gets
    `["fly_fishing"]` this way.
- page v36 `gearParts` L1191–1259 is NOT ported for words here (its tags/rows are the old kit); only its `gRow` rule set
  above (for chip words) and T31/T32's parts.

## Reads
`inp.frames.gear.summary{says, parts[{part, says, cites, terms}], terms, changes[{when, says, cites}]}`,
`.counts[slot].by`, `.elements[k]{verdict, by}`; `inp.frames.display.status` (closed day); `inp.year.displayByDay[].strip`;
`inp.rules.get(rix).{key, facts.book, export.entry_id, export.prov.rank}`; `statics.glossary.terms[].id`.

## Tests (golden = `app/design/regs-v11/port/golden/golden.json` v33)

**Where the tests live** (tests.md §1.2): core may not import `@app/data` or `node:fs`, so every golden-driven case below
is in `app/conformance/src/regs/gear.parity.test.ts` over T12's harness — `for await (const {id, g, page} of cases("gear"))`,
`requestOf(id)` (today Oct 10, keys compared via `goldenKeyOf(key, id)`), `eqPlain(plainLines(vm), goldenLines(g))`, one `it(id)` per state, skipped
when `provinceStore()` is null. ``gear.test.ts`` keeps only unit cases over small hand-built `GearAnswer`/`LicenceValue` literals
(cases marked *unit* below).
Cases 1–6 and 8–9 are parity; 7 too (it reads golden sheets); *unit*: `levelWord` over hand RuleViews of ranks 4/3(`z2:`)/2/1/−1/0, `closedUntil` over a hand 365 array, `changeVM`, the rest-terms splice, `rowOf` routing.

1. **Parity, all 134 states**: `buildGearTop(inputsOf(id), ctx(id))` (`inputsOf` = the harness's memoised `RegsInputs` behind `page`; export it from `golden.ts` if T12 did not) vs `golden.states[id].gear` — `closed`, `day`,
   `mode`, `lines[]` (`k`, `label`, `values`, `terms`, `changes` (`chip`, `when + ":"`, `plain(change)` = `text`, `inForce`),
   `src` (`who`, `open`, `pages[].pdf`, `"Rule book p." + printed + " ↗"` = `pages[].text`)). Expected tallies: 27 closed,
   107 open (all with 3 lines), 11 with a mode, 8 lines with `src: null` (vedder_515, cow1/2/3_1010, x13p0, x16p8, x17p7,
   x17p1 — the Weight line of the 8 "Fly fishing only" states), 3 lines with a change (vedder_1010, x16p3, x17p4 Hooks) and 3 modes with a change (vedder_515, cow1_1010, cow3_1010).
2. `vedder_1010` Hooks: `values` `["One hook, lure or fly","Single barbless hook, no trebles"]`, `terms` `["single_barbless_hook"]`,
   change `{chip:"Next", when:"May 1–31", says:"fly fishing only", inForce:false}`,
   `src.who` `"Province · Region 2 · This water’s ›"`, `open` `"srcs:gl|hook"` (golden `"srcs:gl|vedder_1010|hook"` = `goldenKeyOf(open, "vedder_1010")`), pages printed `[8, 21, 22]` pdf `[10, 23, 24]`;
   Lines `["1 line"]` src `"Province ›"`; Weight `["Up to 1 kg (2.2 lb)"]` terms `["fly_fishing"]` (the rest term); `day` `"Today, Oct 10"`.
3. `vedder_515`: `day` `"Fri, May 15"`; mode `{value:"Fly fishing only", terms:["fly_fishing"]}`, change plain
   `"Jul 1–Apr 30: one hook, lure or fly and weight on the line up to 1 kg (2.2 lb)"`, src `"This water’s · Province ›"`;
   Hooks `["One fly","Single barbless hook, no trebles"]` src `"Province · Region 2 ›"`; Weight
   `["No float or strike indicator, no sinker or split shot, no lure, spinner or other attracting device"]`, `src: null`.
4. `vedder_615` (closed): `closed` `"No fishing here until Jul 1."`, `day: null`, `mode: null`, `lines: []`.
   `x31p_1010` (and chw0, x5p0, x9p2, x13p2, x15p9, x16p7, x17p5, x18p6, x19p5, x22p2, x23p1 — 12 states): `closed` `"No fishing here on any day of the year."`.
5. `klmb_1010` Lines `["1 line from shore; any number from a boat"]` src `"Province · This water’s ›"` pages `[8, 37]`;
   `klwa_1010` Lines `["1 line (2 while alone in a boat)"]` src `"Province ›"`.
6. `x16p3_1010` Hooks change plain `"Dec 1–May 31: artificial fly only"`, src `"Province · Region 1 · This water’s ›"`.
7. **Level words**: for every `rule:K` sheet in `golden.sheets` and every `states[*].sheets`, `levelWord(rule(K))` ===
   `sheet.sub.split(" rule · ")[0]` when the sub has " rule · " (golden levels: B.C., This water’s, Region 1–5, Named area,
   Downstream, Federal or park).
8. **Chip learning**: over all 107 open states the set of learned pairs ⊆ {`B.C.→Province`, `This water’s→This water`};
   `vedder_1010` learns only `B.C.→Province`.
9. `bookBits(inputsOf("vedder_1010"))` has length 365; its run-lengths equal golden `keep.strip.bar[].flex` (same source as T13).

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs/gear conformance/src/regs/gear.parity
```
100 % of the 134 golden `gear` objects equal on the fields above (bait excluded — T31).

## Notes
- **Golden is v33** (`meta.sha 04765ab5`); a v32 golden reads "Single barbless hook" — re-run `sync.mjs` if `meta.version` ≠ v33.
- **365 vs 366**: `inp.year.displayByDay` is indexed by the leap calendar (366, `dayOfYear`); the prototype's dated
  changes and `closedUntil` run over the book's 365 days (2026). Use T13's book-day helpers here or golden "Jan 1–Feb 28" shifts by a day after February.
- Sheet keys carry no state id (contracts §1, §6 #1): `srcs:gl|hook`. Golden keys embed it (`srcs:gl|vedder_1010|hook`); tests
  map with `goldenKeyOf(key, id)` / `appKeyOf(goldenKey, id)` (T36 `sheetKey.ts`). No `pid` in `GearCtx`.
- CONTRACT CHANGE: applied to contracts.md (§2 `buildRegsPage` opts `pid?`, §2.3 `GearVM.day`, `SourceLineVM`; keys carry no pid, §6 #1).
- `ChangeVM.when` has no colon; golden `when` has it ("May 1–31:"). `plain(change)` = `${when}: ${says}` + (inForce ? " · in force today" : "").
- More than one `mode` part never occurs in the v33 data; take the first and assert in a test (`parts.filter(mode).length ≤ 1` on all 107).
