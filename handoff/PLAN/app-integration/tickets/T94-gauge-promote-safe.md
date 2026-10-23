# T94 — Gauge promote: one reviewed station at a time; demotions shown before writing

Status: OPEN.

Source: memory `gauge-regenerate-drifts`: on 2026-10-10 a full match + promote demoted 25 stations and rewrote 1,042
rows. `pipeline/gauges/generate/promote.py` takes only `--dry-run` / `--out`; `pipeline/tools/check_curated.py` still
prints the full `match … && promote` as the fix.

## Work
`promote --only <station…>`; a full promote refuses to demote without `--accept-demotions` and lists each demotion
first; check_curated suggests the safe command.

## Acceptance
A test: a full promote with a would-be demotion exits non-zero and writes nothing; `--only` changes exactly one row.

Depends: none (not T68, which is the feed). Size: S. Owner: no.
