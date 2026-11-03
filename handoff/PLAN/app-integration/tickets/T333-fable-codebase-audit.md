# T333 — Big Fable audit: duplicate data types, duplicate pathways, bad practices (NEXT credit cycle)

Status: POSTPONED — for the next credit cycle (owner 2026-10-30: "a ticket for next credit cycle on doing a big Fable audit
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

## Scope with the owner FIRST (owner 2026-11-03)
"Make sure the agent that eventually works on that will collaborate with me to really nail down what we want out of
the audit." Before ANY audit work: the session that starts T333/T334/T335 sits down with the owner (one batched
question round, then a written scope in this ticket that the owner approves): what counts as a finding, which areas
are in/out, depth vs credits, what the output looks like (report only, or fixes too), and who decides each fix. No
workflow or agent fan-out until that scope is approved here.

## Added 2026-11-03 (owner): failures, fallbacks and reporting, app-wide
"We don't like callbacks generally they hide issues. Fallbacks ... Make sure we get reports on failures like this ...
This and minimal fallbacks should be true universally in app ... generally reusable and the reporting and error handling
is consistent across the app. It should be part of the audit phase later." So the audit also covers:
- every `catch` / silent fallback / swallowed error in app/ (and the pipeline's equivalents): each either rethrows, or is
  SHOWN to the reader and REPORTED, or is listed with why (a net like tools/back-layers.test.ts: a scan that fails on a
  new unlisted one);
- one shared path: `reportFailure` (app/packages/ui/src/crashReport.ts, added for T354) for every failure the app shows;
  one way a failure is shown (a card/face with what went wrong, Retry where it makes sense);
- callbacks that hide outcomes (stored closures, fire-and-forget) → data + named functions.
T354's rule-book open is the first done this way (no pdf-lib or outside-app fallback; viewer failure said + reported).
