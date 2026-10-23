"""Curation review (curation-review PLAN §4 phase 1, T97): marks that last and mean something.

    fingerprint   the ONE fingerprint module — text / meaning / binding (+ its bundle half,
                  reach) / answer
    marks         the `Mark` model, the append-only `marks.jsonl` store, typed stale reasons and
                  the one staleness decision (`stale_reasons`, rule identity `match_records`)
    carry         `carry_marks(before, after, ...)` for migrations and normalisation passes
    vintage       phase 2 (T133): is THIS entry built — its digest the same in the reach run, the
                  bundle and the release, on the served atlas (`entry_state`); "awaiting build"
                  allows only T and R (`refuse_unbuilt`); "bundle matches" per rule (`bundle_matches`)
    coverage      phase 3 (T134): the coverage strip — which words of the row no rule quotes
    tiers         phase 3 (T134): PLAN §3's tiers (S0 P1 Z P2 P3 P4 sweep) for progress and ETA;
                  phase 4 (T135): the risk factors and THE ORDER of the one queue (`queue_rank`)
    runfacts      phase 4 (T135): what the reach run says of a rule (ambiguous cuts, unresolved,
                  unplaced pieces) — the one reader of the run's diagnostics on the review side
    precheck      phase 4 (T135): the pre-checks (severity A facts, B heuristics) + CLI
    tickets       phase 4 (T135): wrong / unsure marks as ticket text, and back (round trip) + CLI
    commit_marks  phase 4 (T135): `git commit --only` of the marks store, nothing else + CLI

`review.carry_marks` is the name a migration calls.
"""
from pipeline.regs.review.carry import carry_marks  # noqa: F401
