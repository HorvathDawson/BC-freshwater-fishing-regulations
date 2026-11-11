# T628 — Base rules pinned to print: follow-ups from T553

Status: OPEN

Source: T553 report 2026-11-11.

## Work
1. A reader for the fee table on printed p.5, so the fee column assignment is checked against print (T553 did not verify it).
2. `--replace-entry` refuses an entry that carries an owner mark, and names the mark.
3. The ingest ledger keys on `io.entry_digest`. Needs a ledger re-seed: plan it with the next annual run, not before.
4. T539's dead-path guard also covers `handoff/PLAN/runbooks/`.

## Acceptance
- Changing one fee on p.5's transcription fails a test. A replace on a marked entry is refused. The guard fails on a dead runbook path.

Depends: T553. Size: S–M. No parser run.
