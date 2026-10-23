"""Curation review (curation-review PLAN §4 phase 1, T97): marks that last and mean something.

    fingerprint   the ONE fingerprint module — text / meaning / binding (+ its bundle half,
                  reach) / answer
    marks         the `Mark` model, the append-only `marks.jsonl` store, typed stale reasons and
                  the one staleness decision (`stale_reasons`, rule identity `match_records`)
    carry         `carry_marks(before, after, ...)` for migrations and normalisation passes
    vintage       phase 2 (T133): is THIS entry built — its digest the same in the reach run, the
                  bundle and the release, on the served atlas (`entry_state`); "awaiting build"
                  allows only T and R (`refuse_unbuilt`); "bundle matches" per rule (`bundle_matches`)

`review.carry_marks` is the name a migration calls.
"""
from pipeline.regs.review.carry import carry_marks  # noqa: F401
