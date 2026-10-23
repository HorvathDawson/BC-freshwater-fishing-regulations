# T82 — Regs leftovers from the T41 walk: the reader-gate run, the timed caveat box (O3), sheet height (O5)

Status: OPEN. The reader run: owner 2026-10-23 lets Claude run it (running).

Source: T41-walk/REPORT.md — O3: x15p2 "At certain times" has no grey box; O5: sheets open taller on a phone than in
the prototype. `handoff/PLAN/app-integration/reader/` does not exist: the comprehension reader gate (tests.md §4) has
never run. Owner 2026-10-22 delegated the look calls ("the prototype is a reference, not the end-all … Claude decides").

## Work
- O3: draw the timed caveat in the same caveat box the other top caveats use (T15b).
- O5: measure sheet heights vs v38 (`app/tools/regs-heights.mjs`); size sheets to content up to the prototype's cap, or
  record why not.
- Record both calls in decisions.md.
- Reader gate: the human launches compare-regs `--reader` (an agent run) at 390 px and 1280 px, light and dark; file
  the dated result in `reader/`; each finding becomes a fix or a ticket.

## Acceptance
O3/O5: before/after shots, a rhythm/height test pinning each value; census + style gates green. Reader report committed,
every finding has a commit or a ticket id. Then T41 → DONE.

Depends: T56 (fixtures re-cut). Size: S + a human run. Owner: launches the reader run. Default: after T56.
