# T82 — Regs leftovers from the T41 walk: the reader-gate run, the timed caveat box (O3), sheet height (O5)

Status: OPEN — the reader gate PASSED 2026-10-23 (re-run after T137, app be8b4693). Light 390 px: 39/39. Dark 390 px:
39/39. Neither has a confidence drop >= 2, and every answer matches the prototype reader's
(reader/2026-10-23-app-rerun.md; first run reader/2026-10-23-app.md 38/39, fixed by T137). 1280 px: not applicable.
tests.md §4 fixes the gate at 390x844, the prompt is a phone set, and the prototype has no 1280 reader record to compare
with. Finding → T138 (row frame). Left: O3, O5.

Source: T41-walk/REPORT.md — O3: x15p2 "At certain times" has no grey box; O5: sheets open taller on a phone than in
the prototype. `handoff/PLAN/app-integration/reader/` does not exist: the comprehension reader gate (tests.md §4) has
never run. Owner 2026-10-22 delegated the look calls ("the prototype is a reference, not the end-all … Claude decides").

## Work
- O3: draw the timed caveat in the same caveat box the other top caveats use (T15b).
- O5: measure sheet heights vs v38 (`app/tools/regs-heights.mjs`); size sheets to content up to the prototype's cap, or
  record why not.
- Record both calls in decisions.md.
- Reader gate (DONE 2026-10-23, see Status): the human launches compare-regs `--reader` (an agent run) at 390 px and 1280 px, light and dark; file
  the dated result in `reader/`; each finding becomes a fix or a ticket.

## Acceptance
O3/O5: before/after shots, a rhythm/height test pinning each value; census + style gates green. Reader report committed,
every finding has a commit or a ticket id. Then T41 → DONE.

Depends: T56 (fixtures re-cut). Size: S + a human run. Owner: launches the reader run. Default: after T56.
