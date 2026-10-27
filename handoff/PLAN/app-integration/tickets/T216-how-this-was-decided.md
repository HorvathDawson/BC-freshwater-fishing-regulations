# T216 — "How this was decided", better: explain the interpretation and what overrode what

Status: DONE (4e39f3fb)

Source: owner 2026-10-27. The prototype's "How this was decided" ladder (app/packages/core/src/regs/decided.ts; T190 grouped
two-region headings) explains a day's answer. Make it better and clearer for an angler: for the water + date, show
which rules applied (region base → water row → exceptions), what each one overrode or was overridden by (stricter wins,
closures beat openings, the water's own row beats the region, "on parts", tidal, DFO), and why — in plain words, each
step citing its source (printed page). Collapsed by default, one tap to expand. Tests on real cases (Fraser two
regions, a spring-closure override, a sub-cap, a closed water).

## Done (2026-10-27)

- core `regs/story.ts` (+ words in `regs/decidedWords.ts`, spread into ALL_WORDS): `ladderOf` now carries `story` — the
  row's stored items (rowSources: roles + `by` + lifts; nothing re-decided) in layers: All of B.C. → each region's base
  (T190: one per region) → an area rule → the river it flows into → this water's own row (per region table when in two)
  → exceptions (federal law, a rule on part of the water). Each step: the rule's words, its printed page, what it did
  today (Decides it / Lowers the limit / Caps some fish / Overridden / Lifted here / Says the same / …), why in plain
  words (a closure beats an opening, release beats keep, a water's own rule beats the region's, an area beats the
  region, the region beats B.C., else the stricter wins), and the rule that beat or opened it with its page. Notes: a
  two-region stretch, tidal water (DFO). Ends with today's answer.
- `PlainRow.tsx`: the fold (collapsed by default, one tap) draws the story in place of the old question ladder; the
  ladder's sections stay in the VM for the `other:` sheet and the goldens.
- Tests `app/conformance/src/regs/decidedStory.fixture.test.ts`: fixture — the Fraser part 14 (Regions 3 + 5), the
  Bella Coola sub-cap, Glenn Creek's spring closure lifted (R4); live release — Williams Lake River (listed R5 stream
  lifts the spring closure), Chimney Creek above Brunson Lake (closed water). `edge_unnamed.json` VM fixture refreshed.
- Not done: the `other:` sheet still shows the old ladder (its own SheetBlock); DFO tags show only where a rule carries
  `source: "dfo"` (none in the Keep rows today).
