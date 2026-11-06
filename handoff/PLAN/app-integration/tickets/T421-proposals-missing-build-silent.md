# T421 — Review proposals: a missing build reads as "binding moved", not as a missing build

Status: DONE 2026-11-05 — f29410a41 (found fixing pipeline CI, 897984554).

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

## Outcome 2026-11-05
- `Sources` remembers when an item was asked for with no `registry.json` (`_has_reg`, `_asked_without_registry`); the
  anchor fallback in `split_record` is unchanged (it notes itself on the proposal).
- `applied_keys` refuses then: `FileNotFoundError` naming the registry path, never "binding moved". The review app
  shows it as an error (its `lru_cache` keeps no exception, so it recovers once a build exists).
- Tests (`test_marks_never_orphaned.py`): the live marks against a missing build raise; the flag on/off. Mutation:
  with the refusal disabled the first fails. The docstring is fixed (the CHEAP_SCOPES test notes the refusal).
- Ran: marks_never_orphaned 19, review_proposals 11, review_split_types 25, review_write_through 12,
  curation_review_writes 9, curation_review_api (below).
