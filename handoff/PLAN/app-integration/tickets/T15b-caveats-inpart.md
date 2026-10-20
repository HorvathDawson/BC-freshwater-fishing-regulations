# T15b — presenter: the top caveat blocks (in-part spots, one side, certain times, some anglers, unplaced)

Phase 1 · Depends on T00, T06 (`describe()`, `RuleView`), T07 (`Settled.{inpart, side, anglerclosed, s}`, per-segment display +
ladder), T09 (`RegsInputs.year`), T12, T13 (`rangeTxt`/`spanWords`, `daySegs`) · Blocks T21 (removes its 9-state exclusion), T25 · Decisions — · Size 1 day

Target **regs v33 @ 04765ab5**: page_v36 `timedHtml` 553–583, `anglerClosures` 584–587, `uncertainHtml` 588–591, `inPartHtml`
592–603, `sideHtml` 604–607, `momentTxt` 547, `answerTxt` 549, `winTxt` 70, `timeCond` 63, `friendlyWhy` 850, `clean` 33,
`daysTxt` 543; `renderCard` 702–716 (order); prototype buildKeep 1046 (the FIRST `.caveats` becomes "Where you can’t fish",
T15 — every other caveat block stays where page v36 drew it) and `citeBooks` 969 (a Rule book link after each rule button).

## Goal
The warnings page v36 draws between the strip and the quota card on an open day: "ⓘFishing is different in some spots today" (golden fuses the decoration span into the summary text)
(with "Everywhere else, follow this page.", the spots in force today, and "Other dates (3)" folded), "⚠ On one side of the
channel only", "⚠ At certain times", "⚠ Closed to some anglers", "? Check: rules for parts of this water that couldn’t be placed
on the map". Each item is a rule's words, its where/when, and its Rule book page; tapping it opens the rule sheet.

## Files
- create `app/conformance/src/regs/caveats.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/caveats.ts`, `caveats.test.ts`
- modify `words.ts`, `model.ts` (CaveatVM — already in contracts.md §2.2 as `KeepVM.caveats`)

## Signatures
```ts
// caveats.ts — ports page_v36 inPartHtml, sideHtml, anglerClosures, timedHtml, uncertainHtml (+ momentTxt, answerTxt, winTxt, daysTxt, friendlyWhy)
import type { RegsInputs, Settled, CardModel, RuleView } from "./inputs";
import type { CaveatVM, Line } from "./model";

export function inPartOf(s: Settled, inp: RegsInputs, fams: readonly string[]): CaveatVM | null;
export function sideOf(s: Settled, fams: readonly string[]): CaveatVM | null;
export function anglerOf(s: Settled): CaveatVM | null;
export function timedOf(inp: RegsInputs, s: Settled, settledAtSeg: (seg: number) => Settled): CaveatVM | null;
export function uncertainOf(inp: RegsInputs, fams: readonly string[]): CaveatVM | null;
/** renderCard order, open day only: inPart, side, angler, timed, uncertain — each with fams ["retention","access"]. */
export function caveatsOf(inp: RegsInputs, s: Settled, settledAtSeg: (seg: number) => Settled): readonly CaveatVM[];
```
```ts
// model.ts (contracts.md §2.2)
export interface CaveatVM {
  readonly kind: "inpart" | "side" | "angler" | "timed" | "uncertain";
  readonly hot: boolean;                 // inpart only: a non-boat rule is in force today (details open, warm colour)
  readonly title: string;                // "Fishing is different in some spots today" / "⚠ On one side of the channel only" …
  readonly calm: string | null;          // inpart only: "Everywhere else, follow this page." …
  readonly items: readonly CaveatItemVM[];
  readonly later: readonly CaveatItemVM[] | null;   // inpart: "Other dates (N)" fold when some are in force today and some not
}
export interface CaveatItemVM { readonly what: Line; readonly where: string | null; readonly rule: RuleKey | null; readonly fish: string | null; readonly book: Line }
```
Words: `INPART_HOT = "Fishing is different in some spots today"`, `INPART_BOAT = "Boat rules in some spots"`,
`INPART_SOME = "Some spots have their own rules"`, `CALM_HOT = "Everywhere else, follow this page."`,
`CALM_BOAT = (kind) => "Fishing rules are the same everywhere on this " + (kind === "lake" ? "lake" : "river") + "."`,
`CALM_LATER = "Not in force today. The map can’t draw these places yet, so check where you are."`,
`WHERE = (p) => "Where: " + p`, `WHERE_SIGNS = "Where: some spots, marked by buoys and signs. Look for signs."`,
`OTHER_DATES = (n) => "Other dates (" + n + ")"`, `SIDE_TITLE = "⚠ On one side of the channel only"`, `TIMED_TITLE = "⚠ At certain times"`,
`ANGLER_TITLE = "⚠ Closed to some anglers"`, `UNCERTAIN_TITLE = "? Check: rules for parts of this water that couldn’t be placed on the map"`,
`HALF_ONLY = (side) => side + " half only"`, `TODAY_PAREN = (m) => "today (" + m + ")"`, `NO_RULE = "no rule"`, `UNPLACED_WHY = "the regulations don’t say exactly where"`,
`MAP_IN_REGS = "(map $1 in the regulations)"`, `INPART_IC = "ⓘ"`.

## Ports
1. **inPartOf** (592–603): `u = settled.inpart.filter(r => fams.includes(r.family))`; none → null. `now = u.filter(r => r.now)`, `later` the rest;
   `boat = r.family === "vessel"`; `hot = now.some(r => !boat(r))`; `onlyBoat = now.length && now.every(boat)`. title/calm per the three
   cases. Item: what = `clean(r.export.parts?.what || r.export.label).replace(/ — in part:.*$/, "")` + (r.wins → `[" · ", winTxt(r).slice(2)]`
   in the muted run); where: `cap(r.part).replace(/\(map ([A-Z])\)/, MAP_IN_REGS)`; when it matches `/various|do not identify|buoys and signs/i`
   → WHERE_SIGNS, else WHERE(p); book = the rule's Rule book runs (citeBooks). items = now.length ? now : later; later fold only when both exist.
   Golden x4p_1010: title "Fishing is different in some spots today" (the page puts "ⓘ" before it), calm "Everywhere else, follow this page.",
   items "Rainbow trout and Char — release all · May 15–Mar 14" / "No fishing · Sep 1–Dec 31", `OTHER_DATES(3)`.
2. **sideOf** (604–607): `settled.side` in fams; item what = `clean(parts.what || label)`, where = `cap(parts.side || f.side + " half only")` +
   (parts.where → " · " + where). Golden x14p2_1010: "No fishing" · "West half of the channel only — on the east half, this water's other
   regulations apply · between fishing boundary signs near Kitimat Hatchery outfall" Rule book p.51.
3. **anglerOf** (584–587): `settled.anglerclosed` → items what = `r.export.label` (no where). Golden x17p3_1010: "Angling closed to non-guided
   non-resident aliens, Sep 1-Oct 31, on Saturdays and Sundays — between Nilkitkwa Lake and the Nilkitkwa River confluence" Rule book p.50.
4. **timedOf** (553–583): only when the key has moments and today has ≥ 2 segments (`daySegs`). Three sources, in this order:
   (a) another of today's segments is closed while today's is not → its closers (those closing every asked game fish, else the one closing
   most), less any that closes today's segment too → what = `cap(describe(r).replace(/ \(.*\)$/, ""))`, where = `momentTxt(M[j])`;
   (b) per species not in a `rows[].week`, the answer words (`answerTxt(mainRes(H, W))`) differ across today's open segments → what =
   `spName(S) + ": " + groups "<answer> <days>[ hours]"` joined "; " (" Mon–Sun" dropped), where = TODAY_PAREN(momentTxt(M[cur])) + ": " + today's answer,
   `fish = S` (opens the species, no rule); (c) an angler closure speaking on some but not all of today's segments and not today's →
   what = describe, where = `r.tcond || daysTxt(...)`. Needs `settledAtSeg(j)` (display + ladder + rows of segment j — T07/T08 for another
   segment of the same day; T13's `rowsBySeg` holds the rows). Golden x15p2_1010: "No fishing for any game fish" · "60 min after sunset–60 min before sunrise" Rule book p.23.
5. **uncertainOf** (588–591): the item's `uncertain` rule list (store `item.unresolved` → rix) in fams → what = `clean(label)`, where = `friendlyWhy(prov.why)`.
6. `caveatsOf`: only on an open day with a status (`renderCard` 707–708), in the order inPart, side, angler, timed, uncertain. KeepVM.order gets
   `{caveat: i}` for each, after the banner/inherited and before the rows (T21).

## Reads
`settled.{inpart[]{now,family,part,wins}, side, anglerclosed, s}`, `RuleView.{key, facts.book, export.{label, parts.{what,side,where}, fields.side, prov.why}}`,
`inp.year.{segStarts, moments, rowsBySeg}`, `inp.item.unresolved` (T09: expose as `uncertain: readonly number[]`), `inp.item.kind`, describe().

## Tests
Golden parity lives in `app/conformance/src/regs/caveats.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
The golden has no field per caveat block, only `keep.order` and `keep.lines`. Compare: (1) `keep.order` selector for each block
(`details.inpart.hot` / `details.inpart` / `div.caveats.top` / `div.caveats.top.check`) and (2) the block's lines = the slice of golden
`keep.lines` between the strip's legend line and the first group title, against `[ (kind === "inpart" ? "ⓘ" : "") + title, calm?, ...items.map(i => [plain(i.what), i.where, plain(i.book)].filter(Boolean).join(" ")), later ? OTHER_DATES(n) : null, ...later ]`
(contracts.md §1 rule: one space between separate elements). All 9 states:
1. x4p_1010 → kind inpart, hot, title "Fishing is different in some spots today", calm "Everywhere else, follow this page.", 2 items, later 3
   ("No fishing · Mar 15–May 14 Where: Entire area north of Albas (map B in the regulations) Rule book p.32 ↗" among them).
2. x14p2_1010 → side, 1 item ending "Rule book p.51 ↗".
3. x15p2_1010 → timed, "No fishing for any game fish 60 min after sunset–60 min before sunrise Rule book p.23 ↗".
4. x17p3_1010, x17p4_1010, x17p7_1010 → angler, "Angling closed to non-guided non-resident aliens, Sep 1-Oct 31, on Saturdays and Sundays — between Nilkitkwa Lake and the Nilkitkwa River confluence Rule book p.50 ↗".
5. x19p6_1010 → angler, "… Jul 1-Dec 31, on Saturdays and Sundays — between the Chimdemash Creek confluence and the Zymoetz River confluence (1500 m upstream) Rule book p.53 ↗";
   x19p7_1010, x19p4_1010 → "… between the Shegunia River confluence and the Sedan Creek confluence Rule book p.53 ↗".
6. Every other open state → `caveats` empty (125 states), and every closed/tidal state → empty.
7. Unit: `momentTxt`, `daysTxt(["Saturday","Sunday"]) === "Sat–Sun"`, `daysTxt(["Monday","Tuesday","Wednesday","Thursday","Friday"]) === "Mon–Fri"`.

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/caveats.parity packages/core/src/regs/caveats` → 9/9 blocks equal, 0 extra blocks on the other 125 states; T21's
caveat exclusion list deleted.

## Notes
- No v33 state draws an `uncertain` block or a boat-only in-part block; both are covered by unit fixtures only.
- The "ⓘ" before the in-part title is a decoration span, not a glossary button — `INPART_IC`, not a `term` run.
- `timedOf` (b) opens a species sheet (`sp:`/`other:`), not a rule — `CaveatItemVM.fish` carries it.
