# T297 — Build clean-up, done together with T295 (faster full rebuild) and T266 (marks → data exporter)

Status: OPEN (owner 2026-10-29: "the build cleanup should come after all the loose end tickets. then build cleanup with
the optimizing it (requiring big long builds) can happen together").

## Order
After the loose-end tickets (T287 leftover, tidal leftovers, salmon proposals/apply, T282, T292/T291 tails…), ONE
build-focused pass: T266 (builds read exported decided-data files, never marks.jsonl; the ~6 per-scope apply paths collapse
into one exporter) + T295 (stage timings, input-hash caching, parallel stages) + this clean-up (dead stages/flags, duplicate
helpers, one entry point, clear stage graph, docs). One full rebuild at the end proves it: outputs byte-identical (or every
difference explained) against the release before.

Depends: T266, T295. Size: L. Owner: no.
