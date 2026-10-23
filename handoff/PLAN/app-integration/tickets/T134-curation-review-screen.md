# T134 — Curation review phase 3: a review screen separate from fixing

Status: OPEN — owner approved PLAN.md 2026-10-23 (defaults Q34–Q41).

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 3: a review screen, separate from fixing" (and §5); split out of T97.

## Work
See PLAN.md §4 "Phase 3: a review screen, separate from fixing" — built as written there.

**Owner 2026-10-23 — splits mid-review:** "Make sure we could add splits and rebuild then continue review with the new
splits." In fix mode the owner adds or moves a split → the tool prints the rebuild commands (atlas → binding diff →
promote → reach → deliver; it never runs them) → after they run, the tool picks up the new vintage without losing
anything: the new split appears in tier S0 to confirm, entries whose binding moved go stale with reason "binding moved",
every other mark stands, and review continues where it was.
Fix mode keeps split add / move / delete / rename (SplitEditor) and shows the pending-build panel (§5).

## Acceptance
PLAN.md §4 "Phase 3: a review screen, separate from fixing" "Accept when", run unfiltered (not through rtk).
- Fix mode: add a split, save → the pending-build panel lists it and the exact commands; review mode never writes it.

Depends: T97, T133. Size: L (about a week). Owner: no (plan approved).
