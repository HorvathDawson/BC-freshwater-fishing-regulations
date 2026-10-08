"""THE VERDICTS — the reference reader, run ONCE per delivery (DATAFLOW §1.3 D, P3).

    python -m pipeline.deliver verdicts [--bundle FILE] [--out FILE] [--workers N]

`read.effective_rules_bound(trace=True)` answers, for every rule key of the bundle (`rule_key`),
every distinct reading of its year at every MOMENT of the key (`calendar.moments` + `calendar.moment_segments`: weekday classes, inside / outside an hours window — answers 2.1), every fish asked (every game fish, plus
crayfish, chinook and the protected species a member rule names) and every origin (none, hatchery,
wild), and the answer is stored exactly as returned — speakers and losers, each loser's reason
and `by`, every partial lift — interned, in `verdicts.sqlite` beside `bundle.sqlite` (decision
U1: a sidecar stamped with the bundle's digests, never shipped to the app).

Every later stage LOOKS THE ANSWER UP (`store.VerdictStore`): the status index (a projection of
`reading.closed` and `key_meta.own`), the export's closure scans and cases, the answers' ladder,
rows, gear and display. Nothing else calls the reader (`test_dataflow_gates` gate 1).

  build.py    the stage: one worker task per rule key, the parent interns and writes
  store.py    the file: its schema (typed, CHECKed, enum tables), `create`, `VerdictStore`
  check.py    the structural proof of a written file (`check`)
  project.py  the closed predicate over a verdict (the status index's, applied to stored rows)
"""
