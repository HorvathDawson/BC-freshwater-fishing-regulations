# T36 — presenter: every sheet (`page.sheet(key) → SheetVM`)
Phase 4 · Depends on T06, T07, T08 (hydrate: rule views, ladder verdicts, rows model), T12 (harness), T15 (possession row
opens `pos:`), T19 (other-fish rows open `other:`), T20 (fish manifest + tips), T30/T33 (item / licence source keys) ·
Blocks T37, T38, T42 · Decisions D5 (frozen target v33 @ 04765ab5), D6 (book links), D13 (fixture store) · Size 1 day
(+½ if the ladder port from T19 is not reusable — see Notes)

> **T36 AS BUILT, part 1 (regs v38 @ 7fb6809f):** `sheetKey.ts` (`parseSheetKey`, `appKeyOf`, `goldenKeyOf`,
> `profileKeyOf`; all 7,184 golden per-state keys round-trip) and `sheets.ts` (`sheetOf`, `sheetLines`, `levelTag`,
> `ruleSub`, `glossBlocks`) build **`rule:`, `gl:` (one or several terms), `pos:pos`, `hatch`** — sheets.parity 134/134
> states, 3,594 sheets (rule 2,714 · gl 735 · pos 107 · hatch 38). `page.sheet(key)` is wired; every other key returns null.
> NOT built yet: `src:` (129), `other:` (788) — core, next; `srcs:` (659) needs T30's `page.gear.*.src.keys`; `licsrc:` (642)
> needs T33's licence `from` groups; `sp:` (1,274) needs T20's `FISH_TIPS` (not generated yet); `parts` (98) needs each
> part's status on the day, which `RegsInputs` does not carry. PIPELINE NEED: the release has no printed → PDF page map
> (`Statics.pdfPages` is `{}`), so glossary pages draw unlinked — keyed in exclusions.ts until the pipeline ships it.
> Contract additions: `SheetBlock` para `tone: "mono"`; `RuleIndex.byKey?`.

> **T36 AS BUILT, part 2 (regs v38 @ 7fb6809f, release r-1bb06fa28daf):** `sheets/sources.ts` (`srcs:` from T30–T32's
> `SourceLineVM.keys`, `licsrc:` from T33's `licenceSources`, `src:rN` from decided.ts `rowSources` — page v36
> openRowSources/srcCard/ROLE in groupedSources), `sheets/fish.ts` (`sp:` — manifest pictures + credits + `FISH_TIPS`, now
> emitted by app/tools/emit-fish.mjs from page v36's own FISHART table), `sheets/other.ts` (`other:oN` — pictures, then the row
> as a Keep `group` block, or page v36's row: value, sentence or weekday box, presence, names, short keeping windows, the
> ladder with where each rule applies, the folded rules that don't apply, "See every regulation line ›"). sheets.parity
> **134/134 states, 7,083 sheets** compared (rule 2,714 · gl 735 · hatch 38 · pos 107 · srcs 659 · licsrc 642 · src 129 ·
> sp 1,274 · other 785); 5 golden lines substituted by design (exclusions.ts SHEET_SUBST: the Bella Coola snapshot who-word ×4,
> klmb's stored record role ×1). NOT compared: `parts` (98) — PIPELINE NEED: each part's status on the day (sheetOf null);
> 3 Other fish rows on page v36's legacy pooled card path (decisions D19: x18p5_1010 o0, x22p1_1010 o1, x22p3_1010 o1).
> Contract additions: `SheetBlock` `group` / `rowTitle`, `sources.overruled`, `SourceRowVM.notes/trib`, `SheetCtx.env`,
> `sheetLines(vm, {groupLines})`; gearPage.ts's local `goldenSrcKey` replaced by `goldenKeyOf`.

## Goal
`buildRegsPage(...).sheet(key)` returns the finished sheet for every key a page can open — rule, glossary term(s),
possession, item sources, licence sources, row sources, other-fish (incl. the weekday story and "How this was decided"),
fish, How to tell, parts — as a `SheetVM` of `SheetBlock`s. Components (T37) draw blocks and never compute. For every
golden state, every sheet the state can open equals golden (title, sub, folds, pages, body lines, nested keys).

## Files
- create `app/packages/core/src/regs/sheets.ts` — `sheetOf(ctx, key)` dispatcher + one builder per kind (below)
- create `app/packages/core/src/regs/sheets/rule.ts`, `gloss.ts`, `pos.ts`, `srcs.ts`, `licsrc.ts`, `src.ts`, `other.ts`, `fish.ts`, `hatch.ts`, `parts.ts`
- create `app/packages/core/src/regs/sheets.test.ts` (A — hand fixtures + literals) and `app/packages/core/src/regs/sheetKey.ts` (+ `sheetKey.test.ts`)
- create `app/conformance/src/regs/sheets.parity.test.ts` (B — every state × every sheet)
- modify `app/packages/core/src/regs/model.ts` (contract changes below), `app/packages/core/src/regs/line.ts` (book `bare`),
  `app/packages/core/src/regs/words.ts` (sheet words), `app/packages/core/src/regs/page.ts` (`sheet` closes over the inputs)

## Signatures
```ts
// sheetKey.ts — the app's keys carry no state id (a RegsPage is one state)
export type SheetKey =
  | `rule:${RuleKey}` | `gl:${string}`            // gl: one id, or ids joined by "," (multi-term sheet)
  | `sp:${string}` | "hatch" | "parts"
  | `srcs:${"bait" | "ways" | "always" | "boats"}` | `srcs:gl|${"mode" | "lines" | "hook" | "weight"}`
  | `licsrc:${ProfileKey}` | `other:o${number}` | "pos:pos" | `src:r${number}`;
export type ProfileKey = "adult" | "adult_non" | "adult_alien" | "u16_res" | "u16_non" | "u16_alien";
export function parseSheetKey(k: string): SheetKey | null;             // refuses unknown shapes
/** golden ⇄ app (test-only use; lives here so T37's deep links and the harness agree) */
export function appKeyOf(goldenKey: string, sid: string): SheetKey;     // "other:vedder_1010|o0" → "other:o0"; "srcs:gl|vedder_1010|hook" → "srcs:gl|hook"; "licsrc:lic|vedder_1010|u16_res" → "licsrc:u16_res"
export function goldenKeyOf(k: SheetKey, sid: string): string;
export const profileKeyOf: (who: Who) => ProfileKey;                    // {u16, non_resident} → "u16_non"

// sheets.ts
export interface SheetCtx { readonly inp: RegsInputs; readonly page: Omit<RegsPage, "sheet">; readonly today: Day }
export function sheetOf(ctx: SheetCtx, key: SheetKey): SheetVM | null;   // null: the key does not apply here (never throws)
export function sheetLines(vm: SheetVM): { title: string; sub: string | null; folds: { summary: string; open: false }[];
  pages: { text: string; pdf: number | null }[]; lines: string[]; nested: string[] };   // the golden form (§ Notes "join rules")
```

CONTRACT CHANGE: applied to contracts.md (§1 `SheetKey`/`ProfileKey`/`appKeyOf`/`goldenKeyOf`/`profileKeyOf`, `Run.book` `pdf: number | null; bare?`; §2.6 `SheetVM.key`, `SheetBlock`, `LadderStepVM`, `SourceRowVM`, `sheetOf`/`sheetLines`; §2.2 `OtherFishRowVM.open`, `PossessionVM.open`; §2.5 `LicenceVM.src.open`).

## Ports (regs-v11.html @ 04765ab5 · page_v36.js @ 04765ab5 — re-read refs from `port/golden/inventory.json`)
| key | prototype | page v36 (the body HTML the snapshot pre-renders) | change in the port |
|---|---|---|---|
| shell | `head` 1668 (`<h2>` title, `Done`, sub `<p>`) | `openSheet` 794 | Done/grab are T37's, not blocks |
| `rule:` | `sheetHtml` rule branch (1684+; `appliesOf` 941, `bookLinks` 930) — title `r.title`, sub `r.sub.replace(/^(.+?) rule · \1$/,'$1')` | `openRule` 1156: title `clean(label)`, sub `levelTag(r)` 806 + `" · " + prov.entry_name`; body "The exact words", quote `clean(verbatim)`, `dr.plain` small, notes "⚑", fold "Technical detail" (mono key + fields JSON) | applies paras first (`RuleFacts.applies`, `.applies_trib`); pages paragraph last = one `book` run per `RuleFacts.book` pair |
| `gl:` | `glossBody` 937, multi-term branch ("What these words mean", `<section class="gterm"><h3>`) | — | title = `GlossaryTerm.term`; body: para `says`; example `"For example: " + example`; fold "In the book’s words" (quote); pages: `"Rule book"` + bare book runs (`pageLink`) |
| `pos:` | `sheetHtml` pos branch (1700; POS_ built 1055) | `possessionHtml` 626 | own examples from every `rows[].group.possession` (`example` + bold `doubles`); generic `glossary.possession_quota.example` only when none; term `says` para only when it does not contain the plain rule (the `norm1` containment test, verbatim) |
| `srcs:` | `itemSrc` 1464 (SRCS keys), `sheetHtml` srcs branch 1693, `levelOf` 1375 | — | one `rule` block per key: title, level (`levelOf` = sub before " rule · "), applies, book |
| `licsrc:` | `LICSRC` 1417 (`mine.from.{t,b}`) | `licParts` 1309 "from" body | title `from.t` ("Where each one comes from"); body head/para blocks per `holds[]` group |
| `src:` | `groupedSources` 1674 | `openRowSources` 796, `rowSources` 780, `srcCard` 788, `ROLE` 776, `WINS` 778 | title `rowTitle(row)`; sub `"<water> · <Mon D>. The more specific rule speaks first: …"`; head "These decide it (N)"; rows grouped by `prov.who`; fold "Overruled (N)" |
| `other:` | `weekSheet` 1332, the other-fish sheet branch (1711): pictures row, `scopeLines` 1669, `citeBooks` 969, `details.decide` → head | `rowHtml` 1147, `valueHtml` 673, `summaryHtml` 1114, `ladderHtml` 1077 + `SAYS/QORDER/LOSES/byName` 1071–1076, `weekHtml` 993 | the weekday box from `row.week` (status, says+"."), parts + `restates` as ladder lines, ladder lines de-duplicated per step |
| `sp:` | sp branch: one `image` per manifest picture + credit `"<caption>[ · <phase>] · <credit> (<source>)"`, "What to look for" + tips | `openFish` 765 (tips from `FISHART.SPECIES`) | tips come from `fish.generated.ts` (T20) — see contracts.md §3 `FISH_TIPS` |
| `hatch` | hatch branch: `SNAP.howtell` + `section.htw` with `glossBody('hatchery_wild')` | `adiposeHtml` 717 | `adipose` block (two figures) + para + `term` block |
| `parts` | parts branch | — | rows only; the prototype's own note about having no map is NOT ported (D1) |

## Reads
`inp.rules` (`RuleView.facts`: `title, plain, book, applies, applies_trib, subsets`; `.export`: `label, verbatim, fields, prov.{rank,entry_name,who}`, `entry_id`, `rule_id`),
`inp.statics.glossary.terms[]` (`id, term, says, example, pages, quote`), `inp.statics.fish`, `inp.frames.rows.rows[i].{group.possession, week, restates, presence, kind, members, all_members}`,
the ladder verdicts (T07 `settle()` → roles per species), `inp.frames.licence` (T33's `from` groups), the fish manifest + tips (T20),
the item's parts (`inp.item.parts[]` label/short/km + `closed_all_year`), `page.gear.*.src.keys` (T30), the PDF page map (`SYNOPSIS_PDF` + `RuleFacts.book` pairs already carry `[printed, pdf]`; glossary `pages` are printed only → map via `statics` `pdfPages`, CONTRACT CHANGE: applied to contracts.md (§3 `Statics.pdfPages`)).

## Tests
**A — `app/packages/core/src/regs/sheets.test.ts`** (hand-built `SheetCtx` from `testing/inputs-fixtures.ts`; every expected
string below is copied from golden.json @ 04765ab5, path in a comment):
1. `gl: says, closed fold, pages` — fixture glossary `bait_ban` → `sheetLines(vm)`:
   title `"Bait ban"`, sub `null`, folds `[{summary:"In the book’s words", open:false}]`, pages `[{text:"p.4 ↗",pdf:6},{text:"p.8 ↗",pdf:10}]`,
   lines `["No natural bait of any kind — worms, roe, insects, fish parts — for any fish, while the ban runs. Lures and flies are fine.", "In the book’s words", "Bait Ban: the use of natural bait is prohibited in waters with a bait ban. Bait may be banned year round or seasonally. During the period when bait is banned it is banned for all angling and for all species.", "Rule book p.4 ↗ p.8 ↗"]` (golden `sheets["gl:bait_ban"]`).
2. `gl: under_16` — title `"Anglers under 16"`, last line `"Rule book p.6 ↗"`, pages `[{text:"p.6 ↗",pdf:8}]` (`sheets["gl:under_16"]`).
3. `gl: multi-term` — key `gl:bait_ban,single_barbless_hook` → title `"What these words mean"`, two `term` blocks titled `"Bait ban"`, then the second term's title; no golden state opens one (meta: 0 comma keys) — this is the check `sheet-book-words`' multi case.
4. `rule: B.C.-wide` — `rule:zp:conduct::conduct.r1` → title `"Do not waste the fish you catch"`, sub `"B.C. rule · Everywhere in B.C."`,
   lines[0..3] `["A B.C.-wide rule — everywhere in B.C.", "The exact words", "Waste the fish you catch.", "Technical detail"]`, lines[4] starts with `"zp:conduct::conduct.r1 { "`, last `"Rule book p.8 ↗ Rule book p.10 ↗"`, folds `[{summary:"Technical detail",open:false}]`.
5. `rule: this water + applies_trib` — `rule:r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r8` → title `"No fishing, Jun 1-Jun 30 — downstream of Vedder Crossing Bridge"`, sub `"This water’s rule · Chilliwack River"`, lines[0..4] `["This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Chilliwack River.", "Inherited from Chilliwack River: its row covers its tributaries.", "The exact words", "No Fishing June 1-30", "No fishing, Jun 1–30."]`, last `"Rule book p.22 ↗"`.
6. `pos: own examples` (vedder_1010) — title `"Possession"`, lines `["Possession: 2 days’ quota. Fish at home don’t count.", "The fish you have with you at any time — in camp, a cooler or a vehicle.", "For example:", "With 4 trout and char a day you may have 8: 2 days’ fish. Possession doubles the 4 a day, not the 2 from streams.", "With 15 whitefish a day you may have 30: 2 days’ fish.", "With 20 bass a day you may have 40: 2 days’ fish.", "In the rule book’s words", "possession quota: the number of fish of any species that an angler may have in their possession at any given time, EXCEPT at place of ordinary residence. In most instances, the possession quota is two times the daily quota. See Tables for exceptions.", "Source › Rule book p.21 ↗"]`, folds `[{summary:"In the rule book’s words",open:false}]`, nested `["rule:z2:possession_quota::possession_quota.r1"]`; block order para, para, example-head, list, fold, source (`r20g-possession-fold`); no line contains `"5 trout"` (`r20h-possession-own-example`).
7. `pos: generic fallback` — same fixture with every `group.possession` removed → one example line `"For example: With a daily quota of 5 trout you may have 10: 2 days' fish."` (store glossary 5 `possession_quota.example`; no golden state has it — 107/107 pos sheets are own-example).
8. `pos: term line dropped when it restates` — fixture whose `possession_quota.says` contains the plain rule minus "Possession: " → no second para.
9. `srcs: bait` (vedder_1010 `srcs:bait`) — title `"Sources"`, lines `["Invertebrate may be used, in streams B.C. A B.C.-wide rule — everywhere in B.C. Rule book p.8 ↗", "No fin fish other than roe B.C. A B.C.-wide rule — everywhere in B.C. Rule book p.8 ↗"]`, nested `["rule:zp:bait::bait.r4","rule:zp:bait::bait.r1"]`.
10. `srcs: gl|hook` — 3 rule rows; row 2 = `"Single barbless hook, in streams Region 2 From the Region 2 table — every stream in Region 2. Rule book p.21 ↗"`.
11. `src: rows titled` (vedder_1010 `src:r0`) — title `"Trout and char"`, sub `"Vedder River · Oct 10. The more specific rule speaks first: this water, then from downstream, a named area, the region, then the province."`, lines[0..3] `["These decide it (12)", "Region 2 · region-wide", "Keep up to 4 trout and char a day Trout/char: 4", "Sets the number Rule book p.21 ↗"]`, folds `[{summary:"Overruled (4)",open:false}]`; every `SourceRowVM.title` = `RuleFacts.title`; none starts with `"ALL STEELHEAD"` (`rule-titles`).
12. `other: release row` (vedder_1010 `other:o0`) — title `"Kokanee"`, lines[0..7] `["Kokanee ›", "↩ Release ⓘ", "You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Jun 1–30.", "How this was decided", "What you can keep", "✓ Release every one here.", "Region 2", "In streams, release every kokanee. Release From the Region 2 table — all of Region 2. Rule book p.21 ↗"]`, folds `[{summary:"Rules that don’t apply here (1)",open:false}]`, last line ends `"See every regulation line ›"`.
13. `other: weekday sheet words` (klwa_1010 `other:o0`) — lines[0..3] `["Kokanee ›", "Today · Sat This water", "Weekends: keep 5 · Weekdays: release", "Keep up to 5 on Saturdays and Sundays; release Monday to Friday."]`; the `weekBox` precedes the head "How this was decided"; no line before that head matches `/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d/` or `/Release every one here|\d+ a day here|\buntil\b/` (`week-in-words`, `week-sheet-one-story`).
14. `other: week parts + restates` (klwa_1010 o0) — ladder lines include `"Sat–Sun Keep up to 5 Today This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Kootenay Lake — Lower West Arm. Rule book p.37 ↗"` (`today: true`), a line starting `"Mon–Fri Release every one This water’s own row"` and the restate starting `"Kokanee from the Lower and Upper West Arms together: no more than 5 a day."` exactly once (`week-parts-decided`, `restates-one-line`, `ur3-combined-plain`); no line matches `/combined daily quota/i`.
15. `sp: credits + tips` — `sp:RB` lines `["Rainbow trout · Freshwater Fisheries Society of BC (GoFishBC)", "What to look for", "Pink or red band along the side", "Small black spots, mostly above the middle of the side", "Rows of black spots on the tail"]`; `sp:ST` has two image blocks, credits `"Steelhead · ocean · Freshwater Fisheries Society of BC (GoFishBC)"` and `"Steelhead · spawning · WDFW (eRegulations WA)"`.
16. `hatch sheet` — title `"Hatchery or wild?"`, sub `"The adipose fin"`, lines[0..3] `["Wild · adipose fin there", "Hatchery · clipped, a healed scar instead", "Look on the back, just in front of the tail. A small fin there means wild. Only a scar means hatchery. If you can’t tell, treat the fish as wild.", "Hatchery or wild (adipose fin)"]`, last `"Rule book p.13 ↗ p.80 ↗"`, folds `[{summary:"In the book’s words",open:false}]` (`howtell-has-hatchery`).
17. `every fold closed` — over all 144 global + all per-state goldens: `folds.every(f => !f.open)` and the VM's folds all `open:false`.
18. `book label` — every `book` run plain()s to `/^Rule book p\.\d+ ↗$/` or (bare) `/^p\.\d+ ↗$/` directly after a "Rule book" text run (`rule-book-label`).
19. `unknown key` — `sheetOf(ctx, "other:o99")` → `null`; `parseSheetKey("date")` → `null` (the date sheet is D7's, not a SheetVM).

**A — `sheetKey.test.ts`**: every golden per-state key of every state round-trips `goldenKeyOf(appKeyOf(k, sid), sid) === k`
(17 key shapes: `licsrc:lic|SID|{6}`, `other:SID|oN`, `pos:SID|pos`, `src:SID|rN`, `srcs:{always,bait,ways,boats}|SID`, `srcs:gl|SID|{mode,lines,hook,weight}`).

**B — `app/conformance/src/regs/sheets.parity.test.ts`**: `it.each(states)` → for each key in `g.sheets` (resolving `"@"`
to `golden.sheets[key]`; `licsrc:` built with that key's profile) `expect(eqPlain(sheetLines(page.sheet(appKeyOf(k))), golden)).toEqual({ok:true})`
on title, sub, folds, pages, lines, nested (nested mapped through `appKeyOf`). Exclusions: `parts` (prototype note line —
compare `choice` items only), `rule:` "Technical detail" JSON line compared as `JSON.parse` equality (tests.md §1.2).
Expected: 134/134 states, ~7,100 sheet comparisons, 144 global.

## Acceptance
```sh
pnpm -C app vitest run packages/core/src/regs/sheets packages/core/src/regs/sheetKey
pnpm -C app vitest run conformance/src/regs/sheets.parity     # needs data/generated/bundle/regs.sqlite (skips without)
pnpm -C app check
```

## Notes
- **Golden join rules** (`sheetLines`, golden b999cfd7+): golden `lines()` splits at block elements; where two elements
  abut inside a line it now inserts ONE space (no fused words). So `"Keep up to 4 trout and char a day Trout/char: 4"`
  (title + quote of a `SourceRowVM`) and `zp:conduct::conduct.r1 { "conduct": … }` (fold key + fields JSON). `sheetLines`
  = contracts §1 `plain()`: runs of one Line concatenate as is, separate Lines/fields/blocks join with one space, then
  `/\s+/g → " "` + trim. The RN components draw them as separate Text — the join is the comparison form only.
- The prototype's per-state keys carry the state id (`other:vedder_1010|o0`); the app's do not (`other:o0`). `appKeyOf`
  is the only bridge — T37 deep links and the harness both use it.
- `other:` is the largest port: the row's own article (`rowHtml`/`valueHtml`/`summaryHtml`) + `ladderHtml`. T19 ports the
  Other-fish **list** row only; if T19 did not already produce a `RowSummaryVM`, this ticket adds `core/src/regs/ladder.ts`
  (`ladderOf(row, ctx): SheetBlock[]`) — the extra ½ day.
- CONTRACT CHANGE: applied to contracts.md (§3 `FISH_TIPS` in `fish.generated.ts`; T20's generator emits it from fishart.js with a stub `window`).
- CONTRACT CHANGE: applied to contracts.md (§3 `Statics.pdfPages`).
- Rule-level words: `levelTag` (B.C. rule / Region N rule / Named area rule / Downstream rule / Federal or park rule /
  This water’s rule) and `levelHead` go into `words.ts` as `LEVEL_TAG`, `LEVEL_HEAD` keyed by rank — shared with T30's
  source line ("Province" chip words are T30's `CHIPWORD`).
- The date sheet ("Which day?") is prototype-only (D7 decides the app's day picker) — not a `SheetKey`.
