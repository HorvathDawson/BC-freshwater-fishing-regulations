# T883 — every fish's "How this was decided": one readable layout, trout and every other fish alike

Status: PARTIAL (app chat) — left (Claude): merge into 0.8.2; pipeline steps[] (main chat) after.

Owner 2026-11-18 ~22:58 (app chat), on the re-sent proposal pictures, verbatim: "all are good ( T878 … T879 … T881 …
T883 … T886 … ) except sunrise/set should be a bit better in same spot" — T883 approved for 0.8.2.

Owner 2026-11-18 ~21:30, two fish sheets on Cultus Lake (Kokanee "Release"; Black crappie "20 a day"), verbatim:
"We need to make the individual sources for each fish be formatted better in the source page for the species … These
pages are super clunky and ideally we want to make it nice and readable showing how the result was achieved. We want it
to pair well with the trout ones too. All generated the same way (sources) and the way they are displayed should be the
same."

Lane: app/t883-fish-sources (worktree, not pushed). Proposal built 2026-11-18 21:50; finished for 0.8.2 at 23:17
(every fish sheet, over today's data); merges into 0.8.2 next.

## What each sheet shows today (r-fc137c4040fa)
- **Other fish** (`other:oN`, core `sheets/other.ts` → decided.ts `ladderOf`, drawn by `LadderView`): the answer
  ("↩ Release ⓘ" / "20 a day" + its sentence), then "How this was decided" as page v36's ladder: a question head
  ("What you can keep"), a ticked result, a step per LEVEL (kicker "CULTUS LAKE"), each rule as its sentence + a role
  word glued on ("Release every kokanee. Release"), the whole "This water’s own row in the 2025–2027 BC Freshwater
  Fishing Regulations Synopsis: Cultus Lake." / "From the Region 2 table — all of Region 2." line, then "Rule book
  p.23 ↗" on its own line; then a second question ("Other rules") in the same form; then "Rules that don’t apply here
  (1)" folded — which is where the rule the answer REPLACED sits (Region 2's "Keep up to 5 kokanee a day").
- **Trout** (`tf:<row>`, core `sheets/trout.ts`, payload `FishSheet {head, here[], src[]}`): bold head, "Here, today"
  (Python's lines), "How to tell it", then "Sources": each rule's title bold, the book's words quoted, "Rule book
  p.21 ↗" — a flat list, no level, no role, cut to 4 by Python.

## Why the other-fish sheet reads as clunky
1. Four lines per rule, three of them boilerplate: the role word repeats the sentence ("Release every kokanee.
   Release"), the "applies" sentence restates the kicker, the page link takes its own line.
2. The story is split: the base rule that was replaced is folded away under "Rules that don’t apply here", so the
   sheet never shows base → this water → answer; the angler sees only the winner.
3. Question heads ("What you can keep", "Other rules") and the result line sit between the section head and the
   steps, so the answer is said three times (value, sentence, ✓ result) before the first rule.
4. It shares nothing with the trout sheet: a different list (ladder vs Sources), different line anatomy, different
   order, so the two fish families look like two apps.

## Design (one layout, every fish)
Answer first (unchanged: the value and its sentence, or the trout head + "Here, today"). Then **How this was
decided** as a rail:
- one step per rule, in reading order: All of B.C. → the region's base rule → an area → this water's own rule →
  what narrows it → federal / part of the water (story.ts's level order; within a level, the stored role order);
- each step: a kicker naming who says it and what kind of rule ("REGION 2 · BASE RULE", "CULTUS LAKE · THIS
  WATER’S RULE") with its page at the right ("p.21 ↗"); the rule's one sentence (opens the rule sheet); a tag only
  when it adds something ("Replaced by Cultus Lake", "Lowers the number", "Counts toward this total"); none on the
  rule that sets the answer;
- a replaced rule stays in its place, quietly: sub colour, hollow dot, "Replaced by …";
- a step under the same kicker as the one before drops the kicker and keeps its page at the right of its line;
- the rail ends in the answer, ticked: "✓ So: Release every one here.";
- **Other rules** (possession, duties, yearly): one line each, "Region 2 · base rule  p.21 ↗" under it;
- rules that changed nothing (says the same, moot, not needed, and a possession rule replaced by another) folded:
  "Rules that don’t apply here (N)"; then "See every regulation line ›" (other fish) / "All sources ›" (trout).
The trout sheet draws the same rail from `FishSheet.src`, moved up under "Here, today" (above "How to tell it") so
both families read answer → how it was decided → the fish.

Built: core `regs/flow.ts` (FlowVM from the stored roles, no decision; `withWeek`, `withNotes`, `partStep`, `restateStep`),
`LadderEnv.flow` / `PlainRowEnv.flow` (only the other: sheet builds it; the Keep tab does not), SheetBlock `flow`,
`sheetLines` reads it, ui-native `sheet/FlowView.tsx`. EVERY fish sheet draws it:
- a plain row (Kokanee, Black crappie, White sturgeon …);
- a group sheet (Whitefish, Bass) — owner 2026-11-18 ~23:25, verbatim: "The white fish repeats a lot at the top. The
  15 quota etc. feels like a waste of space kokanee is better." The card is now the answer once (its kinds, sizes,
  pictures and the examples fold); its head (title, scope, "15 a day per angler across Region 2"), a tier line that is
  only a Rule book link and an origin's book link are dropped (`answerOnly`); then the flow; then "Fishing more than
  one water today? ›" and "See every regulation line ›" last;
- a weekday row (Kootenay Lake — Lower West Arm kokanee): the stored day-parts stand where their rule stood, each
  "Sat–Sun: Keep up to 5"; the part holding on the VIEWED day is tagged ("Today", else its weekday) in the accent; the
  restatement is its own quiet step ("Restates …'s rule", hollow ink ring); the answer is the week's status line;
- a plain row's restates notes sit under the rule they restate;
- trout (`tf:`): the rail from the row's WHOLE list (`row.src`, uncut, Python's order) with each rule's role from that
  fish's rows-frame answer (the row's origin, else the answer's `lead`; a rule only the other origin holds is tagged
  "Wild ones" / "Hatchery ones"); "So:" is the sheet's own head, whole. Where any rule of the list has no stored role
  (or, with no lead, two different ones), the sheet keeps its old Sources list (`FishSheet.src`, a list that claims
  nothing). Release r-fc137c4040fa: 149 of 153 trout sheets on four test waters draw the rail. Mamquam rainbow now
  shows Region 2's "At most 2 hatchery trout and char a day from streams" (Lowers the number) — checked: `row.src`
  holds it, the sheet's `src` (cut to 4) did not. Re-review (23:5x): never one origin's role for both — a rule the
  hatchery and wild answers give different roles, or only one of them holds, is a step per origin tagged "Hatchery
  ones" / "Wild ones" (Lois EB's "2 over 50 cm" reads "Wild ones: A cap inside the total"); a head naming a number no
  step carries ("counts toward the 4 and the 6") keeps the list. Over the 356 vector sheets: 298 rail, 58 list.
  "Replaced by <X>" only when X is not the rule's own source, else "Replaced".

Rail rules (independent review of c38c96e35, each fixed with a test): steps are named against the SHEET's water — a
rule another water's row carries down to its tributaries reads "<that water> · its tributaries" (owner ~23:40, verbatim:
"Like tributary rules (rule from a different water labelled 'this water's rule';) yes label as from tributarie
inheritance."), another water's own row "<X> · listed water", never "this water's rule" (the "all its parts" wording
waits for `part_of` in the inputs, not carried today); replaced steps first, the deciding ones last above "So:";
repeated words drawn once; a step's tag read once by a screen reader.
Desktop (1280, the centred sheet) and the light theme checked: same rail, tokens only. A long kicker wraps and its
page stays on the right (fixed after the Kootenay picture). Tests: `flow.test.ts` (whole strings: kokanee, kicker once,
trout list, week, notes); SheetHost + conformance trout pins moved; the frozen VM fixtures re-cut
(`UPDATE_REGS_FIXTURES=1`, only `other:oN` sheets changed). Gates: vitest core + ui-native + conformance + surfaces,
check-colours, check-platform, boundaries, anchors, knip, `tsc -b`.

## Pictures (real clock Nov 18, staging data)
Scratchpad (session 699dcd1b), `t883-{before,after}-…png`: 390 dark Cultus kokanee, crappie, whitefish (group);
Mamquam rainbow (trout); Kootenay West Arm kokanee (weekday); 390 light Cultus kokanee; 1280 dark Cultus kokanee.

## Data spec for the main chat (so both families are "generated the same way")
Today the app builds the other-fish steps from `rows_decided.roles` + the rule's provenance, and the trout steps from
`FishSheet.src` alone. For the clean version, Python emits ONE list per fish answer, the same shape for a rows-frame
row and a trout row (`FishSheet.decided`, and the rows frame's per-row equivalent):

```
decided: {
  answer:  str                 # "Release every one here." / "Keep 2 a day here, hatchery only." — a sentence, not the head
  steps:   [Step]              # reading order, Python's; the app draws one line per entry, never sorts or cuts
  other:   [Step]              # possession, duties, yearly limit
  aside:   [Step]              # agrees / moot / falls / a replaced possession rule (folded)
}
Step: {
  rule:   int                  # rule index (opens the rule sheet; pages from facts.book)
  role:   "base"|"sets"|"narrows"|"counts"|"caps"|"size"|"also"|"replaced"|"lifted"|"possession"|"duty"|"yearly"|...
  level:  str                  # "Region 2 · base rule", "Mamquam River · this water's rule" (who says it)
  words:  str                  # the rule's one plain sentence for THIS fish (subset sentence where it holds for fewer)
  effect: str | null           # "Replaced by Cultus Lake", "Lowers the number" — null on the rule that sets the answer
  by:     int | null           # the rule that replaced / lifted it
}
```
What improves once that data arrives (the app then draws `decided` as given, and `flow.ts` stops reading roles):
- the app stops pairing `row.src` with the rows frame's roles (`troutFlow`) and the 4 sheets that fall back to the
  plain Sources list draw the rail too;
- (done app-side 23:35 from `row.src`: Mamquam rainbow's "At most 2 hatchery trout and char a day from streams");
- the trout answer reads as a sentence ("So: Keep 2 a day here, hatchery only.") instead of the head;
- the kicker words ("Region 2 · base rule") and the effect words come from Python, so the two families cannot drift;
- a weekday row's parts come as steps with their own rule and "today" mark, not stitched in by `withWeek`.

Gaps this would close: (1) the trout `src` has no role, so no trout step can say "replaced" or "lowers"; (2) the
trout `src` cut to 4 can drop the rule that set the answer — Mamquam's rainbow sheet lists Region 2's "Keep up to 4"
but not Region 2's "At most 2 hatchery trout and char a day from streams", the rule that makes it 2 (it is 6th in
the table's "All sources"); `steps` should hold every step of the answer, uncut; (3) the trout head ("Keep ·
counts toward the 2") is not a sentence, so "So: …" reads oddly — `answer` should be one; (4) one shape lets one
check prove both families (each step's rule holds for the fish; the answer equals the last deciding step's).
