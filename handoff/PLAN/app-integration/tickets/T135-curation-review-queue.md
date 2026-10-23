# T135 — Curation review phase 4: risk-ordered queue, pre-checks, ticket export, commit marks

Status: OPEN — owner approved PLAN.md 2026-10-23 (defaults Q34–Q41).

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 4: queue, pre-checks and output" (and §5); split out of T97.

## Work
See PLAN.md §4 "Phase 4: queue, pre-checks and output" — built as written there.

**Owner 2026-10-23 — splits mid-review:** "Make sure we could add splits and rebuild then continue review with the new
splits." In fix mode the owner adds or moves a split → the tool prints the rebuild commands (atlas → binding diff →
promote → reach → deliver; it never runs them) → after they run, the tool picks up the new vintage without losing
anything: the new split appears in tier S0 to confirm, entries whose binding moved go stale with reason "binding moved",
every other mark stands, and review continues where it was.

## Acceptance
PLAN.md §4 "Phase 4: queue, pre-checks and output" "Accept when", run unfiltered (not through rtk).
- A split added after review began enters the S0 tier of the queue on the next load; its dependants re-queue only if stale.

Depends: T97, T134. Size: M (about 3 days). Owner: no (plan approved).
