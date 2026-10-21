# T14 — presenter: closed banner + chain (+ the tidal banner)

Phase 1 · Depends on T00, T06 (`describe()`, `RuleView`), T07 (`Settled`: `status`, `broad`), T09, T12, T13 (`BOOK_DAYS`,
`runOf`, `nextOpen`, `spanWords`, `StripVM.line`) · Blocks T21, T25 · Decisions — · Size ¾ day

Target **regs v33 @ 04765ab5** (prototype script from line 892; page_v36.js same commit).

## Goal
`bannerOf(inp, settled, strip, today)` returns the banner drawn on a closed day — headline, the run sentence, the chain of
closures that together cover the run (each with its level, Source and Rule book page), the reopening link with its day — after
the prototype's "say each sentence once" pass against the strip line. It also returns the tidal banner (page v36 `tidalHtml`)
for a tidal part, and the "Closed all year on N stretches" list (page v36 `mergedHtml`).

## Files
- create `app/conformance/src/regs/banner.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/banner.ts`, `app/packages/core/src/regs/banner.test.ts`
- modify `words.ts`, `model.ts` (BannerVM below)

## Signatures
```ts
// banner.ts — ports page_v36 closersAt 517–519, closedBanner 520–541, mergedHtml 481–486, tidalHtml 450–454;
//             prototype buildKeep "say each sentence once" 1064–1070 and the closechain rewrite 1071–1073
import type { RegsInputs, Settled, RuleView } from "./inputs";
import type { BannerVM, StripVM, Day, Line } from "./model";

export function closersAt(inp: RegsInputs, settledOn: (d: Day) => Settled, d: Day): readonly RuleView[];
export function chainOf(inp: RegsInputs, settledOn: (d: Day) => Settled, today: Day): { rules: readonly RuleView[]; unless: ReadonlySet<number> };
export function sayOnce(strip: StripVM | null, raw: RawBanner): BannerVM;            // the dedupe, pure over words
export function bannerOf(inp: RegsInputs, settledOn: (d: Day) => Settled, strip: StripVM | null, today: Day): BannerVM | null;
export interface RawBanner { big: string | null; para: string | null; golink: string | null; chain: BannerVM["chain"]; reopen: Day | null; merged: BannerVM["merged"] }
```
BannerVM (supersedes contracts.md §2.2):
```ts
export interface BannerVM {
  readonly kind: "closed" | "tidal";
  readonly big: readonly string[];            // golden keep.banner.big ("Closed all year", "Tidal water"; [] after dedupe)
  readonly paras: readonly string[];          // golden keep.banner.paras
  readonly chain: readonly { text: string; scope: string; rule: RuleKey; book: Line }[];   // golden keep.banner.chain
  readonly reopen: { text: string; day: Day } | null;    // golink: "See the rules from then ›" (+ "Opens again Jul 1." if not said before)
  readonly merged: { label: string; items: readonly { text: string; level: string | null; rule: RuleKey | null; book: Line }[] } | null;
}
```
Words: `CLOSED_ALL_YEAR = "Closed all year"`, `CLOSED_OPENS_AGAIN = (md) => "Closed · opens again " + md`,
`NO_FISHING_RUN = (span) => "No fishing for any game fish " + span + "."`, `TOGETHER = " Together these closures cover it:"`,
`OPENS_AGAIN_LINK = (md) => "Opens again " + md + ". See the rules from then ›"`, `SOURCE = "Source"`,
`RANK = {"-1":"Federal / parks","0":"This water","1":"From downstream","2":"Named area","3":"Region","4":"Province"}`,
`MERGED = (n, s) => "Closed all year on " + n + " stretches (" + s + " sections)"`, `TIDAL_WATER = "Tidal water"`.

## Ports
1. **page_v36 `closersAt` 517**: of `status.closing` (`[rix, species[]]`) keep those closing the most species (`top`), or — when
   `top < all` (all = GAME species closed by any) — those not a strict subset of a longer one. Map rix → `RuleView`.
2. **page_v36 `closedBanner` 520–541** (only when `settled.status.status === "closed"`):
   - `REOPEN = nextOpen(inp, today)` (T13; page v36 1499). `run = runOf(waterSegs, today)` where waterSegs = per book day
     "closed"|"keep" from `isClosedAt` (T13).
   - unless set: `status.unless_opened` of today and of each day of the run; chain = for d = run.from … run.until (≤ 366 steps),
     `closersAt(d)` appended in order without repeats; empty → `[settled.broad[0]]`. Line text = unless ? `facts.unless_opened`
     : `describe(r) + "."`. Dedupe: keep the first of each `(describe(r), rank)`.
   - big = REOPEN ? `CLOSED_OPENS_AGAIN(fmt(REOPEN))` : "Closed all year". para (REOPEN only) = `NO_FISHING_RUN(rangeTxt(from, until))`
     + `TOGETHER` when chain > 1. golink (REOPEN only) = `OPENS_AGAIN_LINK(fmt(REOPEN))`.
   - `!settled.broad.length` → page v36 prints a `missing(...)` note: the app returns `{kind:"closed", big:["Closed"], paras:[], chain:[], …}`
     and logs (no golden state hits it).
3. **prototype closechain rewrite 1071–1073** per chain line: text `.replace(/^No fishing for any game fish,?\s*/, "")
   .replace(/^from streams,\s*/, "From streams, ")`, trimmed; `scope = RANK[rank]`; `book` = Rule book runs of
   `rule.facts.book` (prototype citeBooks wraps "Source" + "Rule book p.N ↗"). Golden `chain[].text` = text + " " + scope.
4. **prototype say-each-sentence-once 1064–1070** (`sayOnce`): `key(t) = t.toLowerCase().replace(/\bagain\b/g,"")
   .replace(/[^a-z0-9]+/g," ").trim()`; seen ← every strip line text first (T13 `StripVM.line`, in order); then big split at
   " · " keeps fresh phrases (rejoined " · "; none left → dropped); each para dropped if not fresh; the golink split at
   `/(?<=\.)\s+/` keeps fresh sentences joined " ". The link's "See the rules from then ›" survives because it is new words.
5. **page_v36 `mergedHtml` 481–486**: when the item's picker group holding this part is `closed` and has ≥ 2 parts: label
   `MERGED(n, sections)`; each part: `partLabel · N section(s)` + the level of that part's `broad[0]` and its book.
6. **page_v36 `tidalHtml` 450–454** (strip absent, `inp.key.tidal`): `{kind:"tidal", big:["Tidal water"], paras:[display frame
   note / water.tidal.guide]}` — the words are data.

## Reads
`settled.status.{status,closing,unless_opened}`, `settled.broad`, `RuleView.{rix,key,facts.unless_opened,facts.book,export.prov.rank}`,
`describe()` (T06), `inp.year.displayByDay`, `inp.item.picker.choices[]{parts,closed,sections}`, `inp.mergedParts` (new),
`inp.frames.display` tidal note.

## Tests
Golden parity lives in `app/conformance/src/regs/banner.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
1. Parity over all 134 states: `big`, `paras`, `chain.map(c => c.text + " " + c.scope)` = golden `chain[].text`, `chain[].scope`,
   `reopen?.text` = golden `golink[].text`; `merged` vs the golden `banner.lines` tail (`eqPlain`, contracts.md §1: one space between separate elements, e.g. "Jun 1–30. This water Source Rule book p.22 ↗"). 27 states have a banner.
2. vedder_615: big `[]` (both phrases already said by the strip "Closed"/"Opens Jul 1"); paras `["No fishing for any game fish Jun 1–30."]`;
   chain `[{text:"Jun 1–30.", scope:"This water"}]` with book "Rule book p.22 ↗"; reopen `{text:"See the rules from then ›", day:{7,1}}`.
3. cow0_1010: paras `["No fishing for any game fish Jul 4–Nov 15. Together these closures cover it:"]`; chain texts
   `"Jul 4–14."`, `"Jul 15–Aug 31."`, `"From streams, Jul 15–Aug 31."` (scope "Named area", Rule book p.13), `"Sep 1–Nov 15."`; reopen day Nov 16.
4. chw0_1010 (closed all year): big `["Closed all year"]`, paras `[]`, chain `[{text:".", scope:"This water"}]` (see Notes), merged label
   `"Closed all year on 2 stretches (8 sections)"`, two items ("… · 7 sections" This water p.22; "… · 1 section" Federal / parks p.9).
5. x31p_1010 (tidal): `{kind:"tidal", big:["Tidal water"], paras:["Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp. See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required."]}`.
6. Unit `sayOnce`: strip line `["Closed","on Jun 15","Opens Jul 1"]` + big "Closed · opens again Jul 1" → big `[]`; golink
   "Opens again Jul 1. See the rules from then ›" → "See the rules from then ›".

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/banner.parity packages/core/src/regs/banner` → 134/134.

## Notes
- **Prototype bug pinned by the golden**: a closed-all-year chain line whose sentence is only the game-fish closure (`describe(r) + "."` with nothing after the prefix) is
  rewritten to "." (chw0_1010 and every closed-all-year state). Port faithfully for parity, and raise it with the owner
  (decisions.md): proposed fix = keep the sentence when the rewrite leaves no words.
- Contract: `BannerVM.{kind, merged}`, `chain[].scope: string`, `chain[].rule: RuleKey`, and `RegsInputs.mergedParts` — applied to contracts.md (marked `// (T14)`). `reopen.day` is the link's day; golden `golink[].day` is null because the prototype stores it as `data-md` — test the day against `nextOpen`.
- `settledOn(d)` re-settles on another day of the run; T07 must expose settle for an arbitrary day from `inp.year` (status + closing
  are display-frame facts, so `displayByDay` suffices — no ladder needed for closersAt).
- **RESOLVED (batch 9, display 18, owner rule 2026-10-21):** which closures close the water on a day is the pipeline's — each
  closed display frame carries `closers` (display.py `closers_of`, page v36 closersAt's choice); banner.ts `closersAt` only reads
  it.
