# T136 — Curation review phase 5: parity gate and pilot before the full review

Status: OPEN — owner approved PLAN.md 2026-10-23 (defaults Q34–Q41).

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 5: gate before full review" (and §5); split out of T97.

## Work
See PLAN.md §4 "Phase 5: gate before full review" — built as written there.

**Owner 2026-10-23 — splits mid-review:** "Make sure we could add splits and rebuild then continue review with the new
splits." In fix mode the owner adds or moves a split → the tool prints the rebuild commands (atlas → binding diff →
promote → reach → deliver; it never runs them) → after they run, the tool picks up the new vintage without losing
anything: the new split appears in tier S0 to confirm, entries whose binding moved go stale with reason "binding moved",
every other mark stands, and review continues where it was.

## Acceptance
PLAN.md §4 "Phase 5: gate before full review" "Accept when", run unfiltered (not through rtk).
- End to end on a scratch copy: mark entries, add a split in fix mode, run the printed rebuild via heavy.sh, reload —
  0 false-stale marks, the moved entries stale as "binding moved", the new split in S0. Then the 30-entry pilot.

Depends: T97, T133, T134, T135. Size: S + build time. Owner: no (plan approved).
