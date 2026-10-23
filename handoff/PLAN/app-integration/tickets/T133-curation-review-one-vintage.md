# T133 — Curation review phase 2: one vintage on screen (entry digests, awaiting build)

Status: OPEN — owner approved PLAN.md 2026-10-23 (defaults Q34–Q41).

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 2: one vintage on screen" (and §5); split out of T97.

## Work
See PLAN.md §4 "Phase 2: one vintage on screen" — built as written there.

**Owner 2026-10-23 — splits mid-review:** "Make sure we could add splits and rebuild then continue review with the new
splits." In fix mode the owner adds or moves a split → the tool prints the rebuild commands (atlas → binding diff →
promote → reach → deliver; it never runs them) → after they run, the tool picks up the new vintage without losing
anything: the new split appears in tier S0 to confirm, entries whose binding moved go stale with reason "binding moved",
every other mark stands, and review continues where it was.

## Acceptance
PLAN.md §4 "Phase 2: one vintage on screen" "Accept when", run unfiltered (not through rtk).
- After a split is added in fix mode and the rebuild has run, the affected entries clear "awaiting build" with no tool
  restart (reload the reach report / bundle / release by digest).

Depends: T97. Size: M (2–3 days). Owner: no (plan approved).
