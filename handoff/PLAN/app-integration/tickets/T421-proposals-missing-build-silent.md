# T421 — Review proposals: a missing build reads as "binding moved", not as a missing build

Status: OPEN (found 2026-11-05 fixing pipeline CI, 897984554).

`pipeline/regs/review/proposals.py` `Sources.item()` returns `{}` when `build` is None or has no `registry.json`. The
class docstring allows that for a new split's anchor (falls back to the mark's stamped point "and says so"), but
`applied_keys` uses the same read SILENTLY: with no build, the two Campbell River DFO marks (cut at "Quinsam River")
read as "binding moved" instead of "no build". That is how the CI failure of `test_no_owner_mark_is_orphaned_or_dropped_live`
looked like a data defect.

## Work
Name the missing build wherever a caller judges a mark from the registry (`applied_keys`, the review app's mark route):
refuse, or return a "no build" state the caller shows. Keep the documented anchor fallback, which already says so.
Rewrite `test_marks_never_orphaned.py`'s `CHEAP_SCOPES` comment ("needs no build"), now half true.

## Acceptance
Pointing `Sources.live` at a missing build gives an error naming the build, never "binding moved"; a mutation test pins it.

Size: S. Owner: no.
