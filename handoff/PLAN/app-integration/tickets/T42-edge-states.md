# T42 — edge states: unnamed section, tidal, outside B.C., hours-window moments, weekday rows

Status: DONE — b2e116c6, 0e36a2d5 (caveat look → T82).

Phase 4 · Depends on T05 (`segmentIndex` with moments), T09 (`loadRegsInputs` for a section handle), T13–T21 (Keep),
T16 (header), T23 (RegsScreen), T36 (weekday `other:` sheet) · Decisions D10 (hours windows), D11 (unnamed header) ·
Size 1 day

## Goal
The five states the common path does not cover read right, in core and on screen:
1. **Unnamed section** (a map tap on water with no name — `RegsTarget {kind:"section", handle}`): header
   `"Unnamed stream · Region 5"`, no part label (the empty slot keeps the header one size — `header-one-shape`), no Map/Parts
   action; the page otherwise normal (D11 a).
2. **Tidal** (`akey.tidal` / `display_frame.status === "tidal"`): the Keep tab is the "Tidal water" banner only — no strip,
   groups, Other fish, possession, where-not; status dot faint; Gear/Methods closed lines as golden; Licence empty.
3. **Outside B.C.** (`key === null`): one line `"This part of the water is outside B.C.: no B.C. rules apply."`; tabs
   other than Keep draw nothing; `licence: null`.
4. **Hours-window moments** (a moment with `hours`, e.g. a night closure): the card is the day's moment at
   `inside=false` and a top caveat `"⚠ At certain times"` lists each other moment's decision with its rule and book page
   (page v36 `timedHtml`, D10 a).
5. **Weekday rows** (`Row.week`): the Other-fish row's value is `week.status` ("Weekends: keep 5 · Weekdays: release"); its
   sheet tells the weekday story (T36) — this ticket owns the Keep-side pieces and the end-to-end check.

## Files
- create `app/packages/core/src/regs/edge.ts` — `edgeKind(inp)`, `tidalKeep(inp)`, `outsideKeep()` (the timed caveat is T15b's `timedOf` → `CaveatVM{kind:"timed"}`; this ticket only verifies it end to end)
- create `app/packages/core/src/regs/edge.test.ts` (A), `app/conformance/src/regs/edge.parity.test.ts` (B),
  `app/packages/ui-native/src/regs/Edges.test.tsx` (C) — the timed block is drawn by T27's caveat block (no `TimedCaveat.tsx`)
- modify `app/packages/core/src/regs/page.ts` (T21): branch on `edgeKind` before building groups; `app/packages/core/src/regs/model.ts` (contract changes)
- modify `app/packages/core/src/regs/words.ts`: `OUTSIDE_BC`, `UNNAMED_*` (the timed title is T15b's `TIMED_TITLE`) only if the store does not carry them (it carries `place.names[kind]` — use that)

## Signatures
```ts
// edge.ts
export type Edge = "normal" | "unnamed" | "tidal" | "outside";
export function edgeKind(inp: RegsInputs): Edge;      // key == null → outside; key.tidal || frames.display.status === "tidal" → tidal; target.kind === "section" → unnamed (still built normally)
export function tidalKeep(inp: RegsInputs): KeepVM;   // order ["banner"], banner {big:["Tidal water"], paras:[<tidal note>], chain:[], reopen:null}
export function outsideKeep(): KeepVM;                // order ["outside"]
// timed: T15b `timedOf(inp, s, settledAtSeg): CaveatVM | null` (kind "timed"), placed by T21 as KeepBlock { caveat: n } — no TimedVM
```
CONTRACT CHANGE: applied to contracts.md (§2.2 `KeepBlock "outside"`, `KeepVM.outside`; timed stays T15b `CaveatVM`, no `TimedVM` — §6 #2).

## Ports (page_v36.js @ 04765ab5)
- `renderCard` 701: `PLACE.k == null` → `<p class="foot">This part of the water is outside B.C.: no B.C. rules apply.</p>`;
  `isTidal(PLACE)` → `waterStrip` only (which renders `tidalHtml` 449: banner "Tidal water" + `water.tidal.guide` or the
  frame's `status.note`).
- `timedHtml` 553–583 with `daysTxt` 543, `hoursTxt`, `momentTxt` 547, `answerTxt`: per other moment j of the day
  (`daySegs`): (a) a closure of some hours/days → `cap(describe(r))` + `where = momentTxt(M[j])`; (b) a fish whose answer
  differs → `"<Fish>: <answer> <days>[ <hours>]"` + `"today (<moment>): <answer>"` (skipped for fish whose row has `week`);
  (c) an angler closure of some days → `describe(r)` + `r.tcond || daysTxt(...)`. Book runs via the prototype's `citeBooks`.
- prototype `buildKeep` @04765ab5 keeps `.caveats.top` (golden x15p2_1010 `keep.order` = `["div.wstrip","div.caveats.top","article.row.ledger.g28",…]`).
- unnamed: store `section_akey`/`section_place` (contracts §3 "unnamed"), `place.names[akey.kind]`.

## Reads
`inp.key` (null / `.tidal` / `.mom`), `inp.year.moments`, `inp.year.segStarts`, display frames of each moment's segment
(`cell(akey, s)` for the other s of the same start — T05 `segmentIndex(starts, day, moments, weekday, inside)` gives each),
`display_frame.{status, closing, note}`, `RuleView.facts.title/plain/book`, `export.fields.when` (tcond), `rows[].week.{status, says, parts}`,
`inp.place.{label, name}`, static `answers.unnamed`.

## Tests
**A — `edge.test.ts`** (hand inputs):
1. `outside` — `key: null` → `page.status === "outside"`, `keep.order` `["outside"]`, `keep.outside === "This part of the water is outside B.C.: no B.C. rules apply."`, `licence === null`, gear/methods `closed === null && lines.length === 0`.
2. `unnamed header` — target `{kind:"section", handle:1178240}` with taps.json `unnamed` row `{akey:2191, place:12, name:"Unnamed stream · Region 5"}` → `header.name === "Unnamed stream · Region 5"`, `header.place === null`, `header.action === null`, tabs still 4 (D11).
3. `timed caveat words` — a two-moment fixture (all-week, `hours {start: sunset+60, end: sunrise−60, in:false}` and the `in:true` moment closed) → `keep.caveats[0].kind === "timed"`, `.title === "⚠ At certain times"`, `plain(items[0].what) === "No fishing for any game fish"`, `items[0].where === "60 min after sunset–60 min before sunrise"`.
4. `weekday row` — klwa_1010 rows fixture: the Kokanee Other-fish row `week === "Weekends: keep 5 · Weekdays: release"`; `segmentIndex` picks the Saturday moment on 2026-10-10 and the Monday moment on 2026-10-12 (`taps.states.klwa_1010.s` vs `klwa_1012.s`).
**B — `edge.parity.test.ts`** (store present):
5. `tidal x31p_1010` — `plain(keep)` lines equal golden `["Tidal water", "Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp. See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required."]`; `header.dot === "faint"`, `header.place === null`, `header.action === null`; gear `closed === "No fishing here on any day of the year."` (golden — see Notes); methods the same.
6. `hours x15p2_1010` — `keep.order[1]` deep-equals `{ caveat: 0 }` and `keep.caveats[0].kind === "timed"`; lines after the strip are golden `["⚠ At certain times", "No fishing for any game fish 60 min after sunset–60 min before sunrise Rule book p.23 ↗", "Trout and char", …]`; header action `{kind:"parts", label:"Parts", aria:"Other parts — now 2 stretches"}`.
7. `every moment state` — for the 12 golden states whose tap has moments (klwa_1010, klwa_1012, x15p2_1010, x15p7_1010, x17p3/4/5/7_1010, x19p4/6/7_1010, x32p0_1010) the whole Keep tab equals golden (these are the states the common-path parity skipped until this ticket, if T21 excluded them).
**C — `Edges.test.tsx`**:
8. outside → one `Text` with the outside line, no tablist sections below Keep content, no Licence section.
9. tidal → the banner text, no `YearStrip`, no group headings.
10. unnamed → header heading `"Unnamed stream · Region 5"`, no button named `/map|parts/i`, header height style identical to a named water's (same `minHeight` on the label slot).
11. timed → `"⚠ At certain times"` rendered as a kicker (`testID="kicker:timed"`) in `restricted`-free `sub` colour (CSS 827: `.caveats .lbl{color:var(--sub)}`), each item a button opening its `rule:` sheet.

## Acceptance
```sh
pnpm -C app vitest run packages/core/src/regs/edge packages/ui-native/src/regs/Edges conformance/src/regs/edge.parity
pnpm -C app check
# D: node app/tools/compare-regs.mjs --state x31p_1010 ; --state x15p2_1010 (T41)
```

## Notes
- **D20 (b):** golden tidal Gear/Methods read `"No fishing here on any day of the year."` — the prototype's `closedUntil` over
  a year whose every display frame is "tidal", not "closed"; wrong for tidal water. Implement the `edgeKind === "tidal"` branch
  in T30's `closedUntil` (Gear and Methods show page v36 `tidalHtml`'s "Tidal water" note instead) and list `x31p_1010` in
  `exclusions.ts` for the gear/methods blocks with reason "D20: prototype closedUntil on tidal". File against the prototype.
- No golden state covers outside B.C. or an unnamed section (taps.json `unnamed` has 20 handles, no states): their tests
  are literal-driven; T41's walk adds one unnamed handle via the deep link `?section=<handle>`.
- D10 (b) would draw each moment as its own card; the `CaveatVM{kind:"timed"}` stays and gains nothing — only `KeepTab` changes.
- The caveat colour: the prototype's `.caveats` has a `--restricted` left rule and `--sub` label (CSS 113–117, 827) — the
  rule is a status colour on a non-status element; T39's palette gate treats `testID="kicker:timed"`'s border as status
  (it marks "restricted at certain times") — document it in T39's allow-list if the gate flags it.
