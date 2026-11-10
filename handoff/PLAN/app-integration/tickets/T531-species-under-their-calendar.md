# T531 — Each calendar sits with its own species (Regulations: What you can keep)

Status: BLOCKED — owner looks at the mockup (owner 2026-11-09 asked for a second opinion; Claude agreed)

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
