# T19b — presenter: plain rows (a row page v36 draws without a quota card) + the "How this was decided" ladder

Phase 1 · Depends on T00, T06 (`describe`, `say` = `facts.plain`), T07 (`Settled.lifted`, verdict roles), T08 (`CardModel` rows, `R`),
T12, T13 (`speciesYear`, `runOf`, `spanWords`/`rangeTxt`), T17 (`rowTitle`, `scopeOf`), T18 (`pictures`), T19 (`splitRows`) ·
Blocks T21 (removes its 25-state exclusion), T26/T27 (render), T36 (`other:` sheet reuses `ladderOf`) · Decisions — · Size 1½ days (split at the ladder if needed)

Target **regs v33 @ 04765ab5**: page_v36 `rowHtml` 1146–1155 (non-pool head), `valueHtml` 672–679, `summaryHtml` 1113–1144 (the
`!row.pool` branch, 1115–1117), `runTxt` 506–516, `closedRuns` 496–505, `runUntil` 505, `nextYr` 504, `winTxt` 70, `namedIn` 774,
`presHtml` 988, `weekHtml` 994, `ladderHtml` 1078–1112, `rowSources` 781–789, `SAYS`/`QORDER`/`LOSES`/`byName` 1072–1075, `WINS` 779,
`levelHead` 808, `say` 778; prototype buildKeep krel28 1077–1085 (the release-only species line), `.lres` drop for week rows 1086–1087,
restates notes 1088–1095, `.looks, .lthumb` removed 1035.

## Goal
The rows on the Keep tab that are not quota cards (31 of the 32 golden plain rows are release-only or closed rows: "Char ↩ Release",
"Steelhead ⊘ Closed"): the title with its pill and ⓘ, the one-sentence status through the year ("You can fish for them, but release
every one. No keeping at any time of year. The whole water is closed Jul 4–Aug 31."), the species line with pictures, the steelhead
presence line, and the folded "How this was decided" ladder (what decides it, level by level, each rule's sentence + its role + Rule
book page; "Rules that don’t apply here (N)"), then "See every regulation line ›".

## Files
- create `app/conformance/src/regs/plainrow.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/plainrow.ts`, `plainrow.test.ts`, `app/packages/core/src/regs/ladder.ts`, `ladder.test.ts`
- modify `words.ts`, `model.ts` (PlainRowVM, LadderVM — contracts.md §2.2)

## Signatures
```ts
// plainrow.ts — page_v36 rowHtml (non-pool), valueHtml (pill), summaryHtml (!pool branch), runTxt, closedRuns, namedIn; prototype krel28
export function plainRowOf(rowIx: number, env: TreeEnv & { settled: Settled; today: Day }): PlainRowVM;
export function runSentence(row: CardRow, inp: RegsInputs, today: Day): string;   // the .sum sentence
// ladder.ts — page_v36 rowSources + ladderHtml (members undefined: the Keep-tab form; T36's other: sheet passes members)
export function rowSources(row: CardRow, card: CardModel, settled: Settled, inp: RegsInputs): readonly LadderItem[];
export function ladderOf(row: CardRow, env: { card: CardModel; settled: Settled; inp: RegsInputs }, members?: readonly string[], res?: string): LadderVM;
```
```ts
// model.ts
export interface PlainRowVM {
  readonly row: number;
  readonly title: string;                       // rowTitle
  readonly pill: { kind: "release" | "closed"; text: string; term: string } | null;   // "↩ Release" + ⓘ catch_and_release / "⊘ Closed" + ⓘ no_fishing_for
  readonly scope: string | null;                // "Separate from your N …" (A0) when a pooled row lands here (klmb_1010, see Notes)
  readonly sum: string | null;                  // the run sentence (null for a week row: `week` is drawn instead)
  readonly week: { says: string; parts: readonly { days: string; says: string; today: boolean; book: Line }[] } | null;   // page v36 weekHtml
  readonly presence: Line | null;               // row.presence + ⓘ steelhead
  readonly names: string | null;                // prot / namedIn list joined " · " (folded "Which fish (N)" when > 5)
  readonly release: { names: string; pictures: readonly FishPicVM[] } | null;   // prototype krel28 (release row with members)
  readonly ladder: LadderVM;                    // folded "How this was decided"
  readonly more: { text: string; open: SheetKey };
}
export interface LadderVM {
  readonly label: string;                       // "How this was decided"
  readonly sections: readonly { q: string; result: string | null; steps: readonly LadderStepVM[] }[];   // "What you can keep" / "Size" / "Other limits" / "Other rules"
  readonly lost: readonly { text: string; status: string; rule: RuleKey; book: Line }[];                // "Rules that don’t apply here (N)"
}
// LadderStepVM is contracts.md §2.6's: { level, lines: [{ text, tag, rule, won }] } — tag = the role words ("Release", "Carry limit"), plus `book: Line` (T19b)
```
Words: `PILL_RELEASE = "↩ Release"`, `PILL_CLOSED = "⊘ Closed"`, `SUM_CLOSED = (until) => "Don’t fish for them" + (until ? " until " + until : "") + ". If one bites, carefully release it."`,
`SUM_RELEASE = (until) => "You can fish for them, but release every one" + (until ? " until " + until : "") + "."`, `NEVER_KEEP = "No keeping at any time of year. "`,
`ALL_YEAR_SENT = "All year."`, `WHOLE_CLOSED = (r) => "The whole water is closed " + r + "."`, `CLOSED_RANGE = (r) => "Closed " + r + "."`, `NEXT_YEAR = " next year"`,
runTxt's `NOW`/`THEN` tables and its sentence frames ("You can keep them until …. Then … from ….", " Then the whole water is closed ….", " You can keep them from …."),
`DECIDED = "How this was decided"`, `LADDER_Q = ["What you can keep","Size","Other limits","Other rules"]`, `RES_CLOSED = "Closed here."`, `RES_RELEASE = "Release every one here."`,
`RES_NOLIMIT = "No daily limit here."`, `SAYS`, `LOSES`, `BYNAME`, `LEVEL_HEAD` (page v36 tables verbatim), `RULES_NOT_APPLY = (n) => "Rules that don’t apply here (" + n + ")"`,
`WHICH_FISH = (n) => "Which fish (" + n + ")"`, `MORE` (T19), `CHECK = "✓ "`.

## Ports
1. **head** (rowHtml 1152 non-pool + valueHtml 676–678): title = rowTitle; pill: closed → "⊘ Closed" + ⓘ `no_fishing_for`; release → "↩ Release" + ⓘ `catch_and_release`.
2. **sum** (summaryHtml 1115–1116, `!row.pool`): `sg = speciesYear(members)` (T13), `never = !sg.includes("keep")`, `un = !never ? runUntil(sg, today) : ""`,
   `rt = never ? ((closed ? "" : NEVER_KEEP) + closedRuns(sg)).trim() : runTxt(sg, today, tail=true)`; sentence = SUM_CLOSED/SUM_RELEASE(un) + (rt ? " " + rt
   : un ? "" : win.wins.length ? " " + cap(winTxt(win).slice(2)) + "." : ""). `closedRuns` merges a New-Year-spanning run and says "The whole water is
   closed …" when the water itself is closed on both ends (waterSegs from `isClosedOn`). A `week` row draws `weekHtml` instead (and the prototype drops
   its `.lres` verdict, 1086).
3. **release line** (prototype krel28 1077–1085): `!group.headline && kind === "release"` and members with a name or picture → names = `members.map(fishName).join(" · ")`,
   pictures(members) — inserted right after the sum. Golden "Brook trout · Dolly Varden/bull trout · Lake trout Brook trout › Bull trout › Lake trout ›".
4. **presence** (`presHtml(row.raw.presence)`), **names** (prot / `namedIn(win)` when no members; > 5 → folded WHICH_FISH(n)).
5. **ladder** (ladderHtml 1078–1112, `members` undefined): items = `rowSources(row)` (every role of every member's H/W verdict, plus
   `settled.lifted` rules for the row's species as role "lifted"; best role per rule, winners first, then by rank); none → `{r: win, role: "governs"}`;
   `record_retention` winners → role "duty". Sections in LADDER_Q order with roles `[governs,contains,narrows,also]`, `[floor]`, `[limit]`,
   `[season,possession_cap,duty,possession]`; the first carries the result (`RES_*`, or "<daily> a day here…" for a pool row). Steps: items grouped by rank
   high → low, level words = rank 3 → `prov.entry_name || "The region"`, rank 2 → entry name when < 40 chars, else LEVEL_HEAD(k). Line text = the subset
   sentence for a partial fish set (`facts.subsets`) else `say(r)` (`facts.plain`) else `shortLabel(label)`; tag = SAYS[role] (won) / LOSES[role] or
   "Replaced by " + BYNAME (lost); "Record it", "Stop fishing", gate → "Closed"/"Release". Book: the rule's Rule book runs (citeBooks). `lost` = non-winning
   items except `agrees`, any whose sentence a winner already says, and any whose replacing rule has the same short label.
6. **restates** (prototype 1088–1095): a non-week row's `restates[]` add the data's note (`x.says`) under the winning line that it restates.

## Reads
`card.rows[i].{kind, pool, members, allMembers, win, prot, presence, week, restates, scope}`, `card.R[S].{H,W}.roles`, `settled.lifted`,
`RuleView.{facts.{plain, subsets, book}, export.{label, type, fields.record_retention, prov.{rank, entry_name}}, rank}`, `inp.year` (T13), fish manifest.

## Tests
Golden parity lives in `app/conformance/src/regs/plainrow.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
Golden `keep.plainRows[i]` is `lines()` of the article: one string per block element, one space between abutting elements (contracts.md §1).
`plainrow.parity.test.ts` (conformance) builds the expected array from the VM as `[title + " " + pill.text + " ⓘ", sum, release && (names + " " + pictures captions),
presence, DECIDED, ...sections.flatMap(s => [s.q, s.result && "✓ " + s.result, ...steps.flatMap(st => [st.level, ...st.lines.map(l => [l.text, l.tag, plain(l.book)].join(" "))])]),
lost.length && RULES_NOT_APPLY(n), ...lost lines, last line + " " + MORE]` (filter empties) and compares to golden, all 25 states / 31 non-pool rows:
1. cow1_1010 → `["Char ↩ Release ⓘ", "You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Jul 4–Aug 31.",
   "Brook trout · Dolly Varden/bull trout · Lake trout Brook trout › Bull trout › Lake trout ›", "How this was decided", "What you can keep",
   "✓ Release every one here.", "Region 1, excluding Haida Gwaii", "Release every char. Release Rule book p.13 ↗", "Other rules", "Region 1",
   "Possession: 2 days’ quota. Fish at home don’t count. Carry limit Rule book p.13 ↗ See every regulation line ›"]`.
2. iron_720 → sum "You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Dec 1–Jun 30.";
   lost `["In streams, no fishing, Jul 15–Aug 31. Replaced by this river’s rule Rule book p.13 ↗"]` under "Rules that don’t apply here (1)".
3. x15p8_1010 row 2 → `"Brook trout, brown trout, cutthroat trout, Dolly Varden/bull trout, lake trout and rainbow trout ⊘ Closed ⓘ"`,
   sum "Don’t fish for them until Nov 15. If one bites, carefully release it. You can keep them from Nov 16.", step "Fraser River" /
   "No fishing for trout and char, Sep 1–Nov 15. Closed Rule book p.30 ↗", "Rules that don’t apply here (5)".
4. x15p8_1010 row 1 (Steelhead ⊘ Closed) → presence "A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ"; sum "Don’t fish for them. If one bites, carefully release it. Closed Aug 15–Jun 30.".
5. x16p1_1010 → sum "You can fish for them, but release every one. No keeping at any time of year." (no closure run).
core `ladder.test.ts` / `plainrow.test.ts`: unit fixtures for SAYS/LOSES/BYNAME and the "lost" filter; `ladderOf(row, env, members, res)` is the form T36's `other:` sheet uses (members set).

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/plainrow.parity packages/core/src/regs/plainrow packages/core/src/regs/ladder` → 31/31 non-pool plain rows equal; T21's
plainRows exclusion reduced to the one pooled row below (or removed if that decision lands).

## Notes
- **klmb_1010 holds the only pooled plain row** (Rainbow trout, "Separate from your 5 trout and char", "10 a day", "Keep 10 any size · wild or
  hatchery", "20 a year over 50 cm", "Record ones over 50 cm on your licence"): a keep row with no `group.headline`, drawn through page v36's
  legacy `speciesBlock` / `groupBlock` / `sent` / `extraChips` path (1007–1071, 881–935). Porting that path for one row is ~1 day of dead weight;
  proposed decision (decisions.md): ask the data side to give that row a `group` (rows 27) so it becomes a g28 card, and keep it excluded until then.
- page v36 `miniStrip` (a per-species bar when the species' year differs from the water's) sits after the names; check the golden render of each
  plain row in T27 — its text (`shortWindows`, "Keeping allowed only …") does not appear in any v33 plain row.
- The ladder is the same machinery as the `other:` sheet ("How this was decided" there, with `members`); T36 imports `ladderOf` from here.
