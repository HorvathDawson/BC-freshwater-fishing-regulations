# T15 — presenter: inherited line, where-you-can't-fish, possession row

Phase 1 · Depends on T00, T06 (`RuleView`, rule `plain`), T07 (`Settled.standing`, verdict roles), T08 (`CardModel.R`), T12 · Blocks T21, T25 · Decisions — · Size ½ day

Target **regs v33 @ 04765ab5** (prototype script from line 892; page_v36.js same commit).

## Goal
Three small Keep blocks: the "By another water’s rule" line under the strip (a lift another water's row carries here, with its
Rule book page and the nearer row's caution), the "Where you can’t fish" list at the foot (standing no-fishing place rules, the
data's sentences with their book pages), and the card-level **Possession** row (label "Possession", value "2 days’ quota", a chevron) that opens its
sheet. All three are data words plus the labels listed below.

## Files
- create `app/conformance/src/regs/standing.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/standing.ts`, `app/packages/core/src/regs/standing.test.ts`
- modify `words.ts`, `model.ts`

## Signatures
```ts
// standing.ts — ports page_v36 inheritedHtml 611–615, whereHtml 616–620, possessionHtml 626–636;
//               prototype buildKeep 1042–1045 (inherited), 1046–1051 (nofish), 1053–1058 (the posrow)
import type { RegsInputs, Settled, CardModel, RuleView } from "./inputs";
import type { InheritedVM, WhereNotVM, Line, SheetKey } from "./model";

export function inheritedOf(inp: RegsInputs): InheritedVM | null;
export function whereNotOf(settled: Settled): WhereNotVM | null;
export interface PossessionVM { readonly label: string; readonly value: string; readonly says: string; readonly rule: RuleKey; readonly term: "possession_quota"; readonly open: SheetKey }
export function possessionRules(card: CardModel, inp: RegsInputs): readonly RuleView[];   // main first, then the differing ones
export function possessionOf(card: CardModel, inp: RegsInputs, settled: Settled): PossessionVM | null;
```
Types (model.ts; supersede contracts.md §2.2):
```ts
export interface InheritedVM { readonly label: string; readonly lines: readonly { says: Line; caution: string | null }[] }
export interface WhereNotVM { readonly heading: string; readonly scope: string | null; readonly items: readonly { text: Line; rule: RuleKey }[] }
// KeepVM.possession: PossessionVM | null
```
Words: `INHERITED_LABEL = "By another water’s rule"`, `WHERE_NOT = "Where you can’t fish"`, `NO_FISHING_COLON = "No fishing: "`,
`POSSESSION = "Possession"`.

## Ports
1. **inherited** — page v36 `inheritedHtml` 611 (label) + prototype 1042–1045 (the line it draws instead):
   `xs = inp.frames.display.inherited ?? []`; none → null. Per x: `says = [text x.says, text " ", ...bookRuns(rules.get(x.by).facts.book)]`
   (`<p data-inherited>${says} ${bookLinks(by)}</p>`), `caution = x.caution ?? null`. Label `INHERITED_LABEL` (page v36 words;
   the prototype copies it from v36's `.lbl`).
2. **where you can't fish** — page v36 `whereHtml` 616: group `settled.standing` rules with `facts.where` by that heading; the
   prototype keeps only the FIRST `.caveats` block of the card (1046), which is this list whenever any standing rule has a
   `where` (true in 133/134 states). Item text: `facts.plain` → `.replace(/^No fishing /, "No fishing: ").replace(/\.$/, "")`;
   no plain → `NO_FISHING_COLON + lc(clean(export.verbatim).replace(/\.$/, ""))`. Then `[text " ", ...bookRuns]` (the book link is its own element after the rule button: one space, contracts.md §1 —
   golden "… obstacle or leap Rule book p.9 ↗"). Heading
   `WHERE_NOT`; `scope` = the group heading when it is not /^where you can.t fish$/i (never in v33 → null).
3. **possession** — page v36 `possessionHtml` 626 + prototype 1053–1058. Rules: every rule key with role `"possession"` in any
   species' hatchery/wild verdict of `card.R` (T08 `hydRes.roles`), resolved to `RuleView`; `main` = the one with no species,
   else the highest `fields.per_daily`; `ex` = others whose `per_daily` differs. `says = [main, ...ex].map(r => r.facts.plain).join(" ")`.
   **Replace the prototype's foot regex with the data**: value = the main rule's `facts.plain` parsed once, here, with the two
   prototype patterns (kept verbatim so a data wording change fails a test, not silently):
   `/^ⓘ?\s*(Possession)\s*:\s*([^(.]+?)\s*(?:\((.*?)\))?\.?\s*Source\s*$/` or `/^ⓘ?\s*(Possession)\s*:\s*([^.]+?)\.\s*(.*?)\.?\s*Source\s*$/`
   applied to the page-v36 foot text (the sentence followed by its Source button) — i.e. on `plain` alone use `/^Possession\s*:\s*([^(.]+?)\s*(?:\(.*?\))?\.?\s*$/`
   then `/^Possession\s*:\s*([^.]+?)\.\s*.*$/`; group 1 = value ("2 days’ quota"). No match → null (and log).
   Shown only when the card is drawn: `settled.status.status !== "closed"` and a status exists (page v36 `renderCard` 702–716).
   `open = "pos:main"`; the sheet (T36) reads `says`, `rule`, `term`.
4. The caveat blocks page v36 also renders at the top (`inPartHtml` 592, `sideHtml` 604, `anglerClosures` 584, `timedHtml` 553,
   `uncertainHtml` 588) are **not** in this ticket — see Notes.

## Reads
`inp.frames.display.inherited[]{says, by, caution}`, `inp.rules.get(rix).{facts.book, facts.plain, facts.where, export.verbatim,
export.fields.per_daily, export.fields.species, key}`, `settled.standing`, `settled.status`, `card.R[S].{H,W}.roles`.

## Tests
Golden parity lives in `app/conformance/src/regs/standing.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
1. Parity, 134 states: `inherited` (label, `plain(says)` = golden `lines[].text`, cautions), `whereNot` (heading, scope,
   `plain(text)` = golden `items[].text`, book pages = `items[].pages`), `possession` (label, value, `open` ↔ golden `open` with the
   `${stateId}|` prefix normalised by T12's `sheetKeyOf`).
2. glenn_501 → `{label:"By another water’s rule", lines:[{says:"Open Apr 1–Jun 14: the Duncan River’s exemption from the spring stream closure covers its tributaries. Rule book p.36 ↗", caution:"Trout Lake’s row, nearer this stream, prints no such exemption; the Duncan River’s is applied here because its row covers all its tributaries."}]}` (book pdf 38).
3. iron_720 → one line "Open Jul 15–Aug 31: the Quinsam River’s exemption from the summer stream closure (MUs 1-1 to 1-6) covers its tributaries. Rule book p.18 ↗", caution null.
4. vedder_1010 whereNot → heading "Where you can’t fish", scope null, items
   "No fishing: within 23 m (75 ft) downstream of the lower entrance to any fishway, canal, obstacle or leap Rule book p.9 ↗" and
   "No fishing: within a 100 m (330 ft) radius of any government facility operated for counting, passing or rearing fish (e.g., fishway, fish hatchery) unless otherwise designated Rule book p.9 ↗".
5. vedder_1010 possession → `{label:"Possession", value:"2 days’ quota"}`; 107 states have it, all "2 days’ quota"; the 27 closed/tidal states → null.
6. x31p_1010 (tidal) → whereNot null, possession null, inherited null.

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/standing.parity packages/core/src/regs/standing` → 134/134 on these three blocks.

## Notes
- Contract: `InheritedVM.label: string`, `WhereNotVM.items[].rule`, `KeepVM.possession: PossessionVM` (`says`, `rule`, `term`; `open` = `"pos:main"`) — applied to contracts.md (marked `// (T15)`).
- The top caveat blocks (`div.caveats.top`, `details.inpart.hot`; 9 states) are **T15b** (`tickets/T15b-caveats-inpart.md`).
- Separators follow contracts.md §1: inside one Line the presenter writes its spaces; the inherited sentence and the where item each get a
  `" "` text run before their book runs (golden "… tributaries. Rule book p.36 ↗", "… leap Rule book p.9 ↗").
