# T883 — every fish's "How this was decided": one readable layout, trout and every other fish alike

Status: PARTIAL (app chat) — left (Claude): pipeline steps[] with the main chat. Merged into 0.8.2 (app/integrate-082, 2026-11-19 ~00:06).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T883 | Every fish's "How this was decided" sheet: one readable layout (answer, then the steps that made it, each with its source), trout and other fish alike (owner) | PARTIAL — left (Claude): pipeline steps[] with the main chat; merged into 0.8.2 | 8b4943387, e4e98a35c, 3e1412fdf, a80a8e6a3 | owner 2026-11-18 ~21:30; approved ~22:58 |

Owner 2026-11-18 ~22:58 (app chat), on the re-sent proposal pictures, verbatim: "all are good ( T878 … T879 … T881 …
T883 … T886 … ) except sunrise/set should be a bit better in same spot" — T883 approved for 0.8.2.

Owner 2026-11-18 ~21:30, two fish sheets on Cultus Lake (Kokanee "Release"; Black crappie "20 a day"), verbatim:
"We need to make the individual sources for each fish be formatted better in the source page for the species … These
pages are super clunky and ideally we want to make it nice and readable showing how the result was achieved. We want it
to pair well with the trout ones too. All generated the same way (sources) and the way they are displayed should be the
same."

Done (app/t883-fish-sources, merged into 0.8.2 via app/integrate-082, 2026-11-19 ~00:06): core `regs/flow.ts` (FlowVM
from the stored roles, no decision; `withWeek`, `withNotes`, `partStep`, `restateStep`), SheetBlock `flow`, ui-native
`sheet/FlowView.tsx`: every fish sheet (plain row, group, weekday row, trout `tf:`) draws one rail, answer → "How this
was decided" (one step per rule in reading order, kicker + page, a tag only when it adds something, replaced rules in
place, "✓ So: …") → the fish. Trout builds the rail from `row.src` (uncut) with roles from the rows-frame answer and
keeps the old Sources list where a rule has no stored role (356 vector sheets: 298 rail, 58 list).
Owner 2026-11-18 ~23:25, verbatim (group sheets, built as `answerOnly`): "The white fish repeats a lot at the top. The
15 quota etc. feels like a waste of space kokanee is better."
Owner ~23:40, verbatim (built: "<that water> · its tributaries", "<X> · listed water"): "Like tributary rules (rule from
a different water labelled 'this water's rule';) yes label as from tributarie inheritance." The "all its parts" wording
waits for `part_of` in the inputs, not carried today.
Independent review of c38c96e35: rail rules fixed, each with a test.
Pictures: scratchpad (session 699dcd1b), `t883-{before,after}-…png`.

## Work (left): data spec for the main chat (so both families are "generated the same way")
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
