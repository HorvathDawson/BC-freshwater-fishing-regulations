# T531 — Each calendar sits with its own species (Regulations: What you can keep)

Status: DONE — cb1e01549; owner 2026-11-09 on build 24: fine

Was: IN REVIEW — owner phone look on 0.7.2 (mockup approved 2026-11-09: "i like the new mockup more please add as new patch release")

Owner 2026-11-09 (Vedder River screenshot): salmon should live under the "SALMON (DFO)" calendar and trout/char, kokanee,
whitefish under the "GAME FISH (B.C. RULES)" calendar; "either way we want the text to be easier to have visible".

## Proposal
- Two blocks, each: heading (source) → status ("✓ Open today") → its year calendar + legend → its species rows.
  Salmon (DFO): Salmon. Game fish (B.C. rules): trout and char, kokanee, whitefish, …. No separate "Species and limits"
  list that the reader has to map back to a calendar.
- Legend text full contrast and larger (the B.C. legend's "✓ open" reads disabled today); "fly fishing" → "fly fishing only".
- Which species goes under which calendar comes from the data (the rule's source), never a hand list in the UI; a water with
  no DFO salmon block keeps one block.
- Mockup (light + dark, Vedder and a no-salmon water) for the owner before code.

## Mockup (2026-11-09)
Self-contained HTML (light/dark, before/after, Vedder / one-block toggles; app tokens, 390 px): `shots/T531-mockup.html`.
Shots: `shots/T531-vedder-light.png`, `T531-vedder-dark.png`, `T531-single-light.png`, `T531-single-dark.png`, and today's layout (redrawn
from KeepBlocks/YearStrip, not a live app capture) `T531-before.png`, `T531-before-dark.png`. Limits and dates shown are illustrative.
Species go under a block by the source of the rule that governs them (DFO rule -> Salmon block; B.C. synopsis rule -> Game fish), never a hand list.

## Done (2026-11-09)
- Core `glance.ts`: every folded row carries `source` (`dfo` for the Salmon card row, `bc` for a quota group); `speciesBlocks(rows, dfoBlock)`
  puts a row under the block of its own source (a DFO row with no salmon calendar drawn stays with the rest). Other fish rows are
  the synopsis' own, so B.C. by construction. No hand list of species anywhere. `legendText` (core strip) words a legend entry
  ("✓ Open", "Fly fishing only", "✕ Closed"); the salmon bar legend is capitalised the same way (`SALMON_WORDS.barLegend`).
- UI (`KeepTab`, `KeepBlocks` `SpeciesRows`, `YearStrip`, `type.ts` `TYPE.legend`): each block = heading, status, year bar and legend,
  then its rows (folded groups + Other fish + status key under Game fish (B.C. rules); the Salmon row under Salmon (DFO)). The
  "Species and limits" list is drawn only where no game-fish calendar is (tidal / outside B.C. / no strip). The legend is full
  ink 14 px semibold. A one-block water also shows the "Game fish (B.C. rules)" heading (as the mockup does). Details expanders,
  chevrons, stamps, the status key and the Possession row are unchanged.
- Block order: as the app has it today (Game fish first, Salmon (DFO) second; the mockup's DFO-first was only to match the owner's
  sketch). The in-season line and the caveat / inherited blocks stay after the blocks, where they were.
- Tests: core `glance.test.ts` (source tags; a species only under its own source's block; no DFO calendar), `KeepTab.test.tsx`
  (T531 block: Vedder order, a species only under its own block, one-block water, Details / rows / Possession still work, legend
  words); `salmon.test.ts` legend words and the glance row's `source` updated. No assertion removed.
- Shots (web, 390 px, light + dark): `shots/T531-built-vedder-{light,dark}.png`, `shots/T531-built-single-{light,dark}.png`.
- Owner: phone look on 0.7.2.
- Follow-up (owner 2026-11-12, staging r-0a9930fb8826 Vedder: "Why is salmon under trout table now? Instead of on top when
  salmon rules exist"): the "Block order" call above was the builder's, against the owner's sketch and the approved mockup
  (DFO first). Fixed: whenever the Salmon (DFO) block is drawn it comes first, above the game-fish calendar and its rows
  (`KeepTab`; separator rule moved under the salmon block). Tests: `KeepTab.test.tsx` order pins + a strict whole-block-above
  check across the fixture waters. Store shot 2 (T319, game fish stacked over salmon) no longer matches the app.
- Follow-up (main, 11-09): a DFO row with no DFO calendar (Glenn Creek) now sits under its own "Salmon (DFO)" heading and status ("Closed"), no bar or legend, after the B.C. block; the grouping test is strict across fixture waters. Versions 0.7.2, changelog entry added.
