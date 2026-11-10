# T606 — Atlas inputs: what T547 left

Status: OPEN

Source: T547's report 2026-11-10 (commits 9c701a58b … 4acd5d0f1). T547's ticket file numbers the sections.

## Work
1. Proof owed (owner: "consistent with the builds now"): one side atlas build with the new code, compared with the
   live atlas (the build is deterministic; no curated file is newer than the live atlas, so every difference must be
   the new `input_hashes` and `COMPLETE.json` or be explained). Command:
   `HEAVY_LOCK=heavy-job-2.lock scripts/heavy.sh t547-side 50 -- python -m pipeline build --side <DIR> --atlas`.
2. T547 §3 second half: lake-boundary clash suffixes numbered by the boundary itself; §5 lake names. Both change the
   atlas, so they ride the next planned atlas rebuild, with a rule-binding diff before any promote.
3. Atlas-only files leave the deliver stage's cache key.
4. A cut's label edit is not caught by the new input check (cuts are fingerprinted by position only): decide whether
   a label edit should count, and say why in `pipeline/atlas/inputs.py`.

## Acceptance
- Item 1's diff is in the ticket; items 2–4 done or each closed with a reason.

Depends: T547. Size: M (one heavy build). Owner: no.
