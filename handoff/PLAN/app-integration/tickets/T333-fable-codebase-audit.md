# T333 — Big Fable audit: duplicate data types, duplicate pathways, bad practices (NEXT credit cycle)

Status: PLANNED for the next credit cycle (owner 2026-10-30: "a ticket for next credit cycle on doing a big Fable audit
to find duplicate datatypes/pathways/bad practices etc… thoroughly").

## Scope
A thorough multi-agent audit on Claude Fable (claude-fable-5-1), whole repo: pipeline/, app/ (core, data, ui-native,
web, workers), curation-review/, deploy/, scripts/. Find:
- **Duplicate data types:** two shapes for one idea (e.g. DFO `q` vs game-fish quota/lengths — T332), parallel
  enums, copies of the same schema in Python and TS without a parity test.
- **Duplicate pathways:** two code paths producing the same output (status index vs reader before T306), files that
  say the same fact (tidal ×4 before T326), readers bypassing `CURATED`/`GENERATED`, env overrides.
- **Bad practices:** silent fallbacks, checks seeded from their own input (memory checks-seeded-from-input-launder),
  non-determinism, dead code/files, tests that can't fail, guards missing at write time.
- **Drift:** docs/AGENTS rules the code no longer follows.

## Method
Use data-flow.md as the map. Run as a workflow: finders per area and per lens → dedupe → adversarial verify
(≥2 skeptics each) → ranked report with file:line and a proposed fix per finding → cleanup tickets. No fixes in
the audit run itself.

Depends: T324 (map), T331 merged. Size: L (credits). Owner: start it in the new cycle.
