# T429 — A missing review input can empty a curated file

Status: OPEN — from the audit (T333–T335, 2026-11-05). Severity: **high**. Verified by a second reader.

Source: `handoff/PLAN/audit-2026-11-05/` (findings: pipeline 1, 4; `*.verified.md` has the evidence per finding).

## What happens
`regs/review/names.py:93-99` and `slough.py:104-112` return an empty candidate set when their generated input is
missing; `tools/mined_names.build` / `slough_calls.build` then build `[]` and `review/apply.write_files` (L85-100)
writes it over `found_names.json` / `slough_calls.json` — on every saved mark (write-through). Needs a moved/renamed
input or a per-file override (the inputs are in git), and shows in `git diff`, but nothing refuses it.
Also: the remaining per-file overrides for review INPUTS (`CURATION_NAME_CANDIDATES`, `CURATION_SLOUGH_*`,
`CURATION_TIDAL_*`; `gate.py`/`parity.py` set them) look like the retired curated ones.

## Fix
1. The loaders raise on a missing input (no empty set). 2. `write_files` refuses output with fewer rows than the
file on disk unless `--accept-shrink` (and says how many). 3. One `GENERATED_ROOT` (like T422's `CURATED_ROOT`) or a
loud refusal for the input overrides. Mutation tests for 1 and 2.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: M. Owner: no (picks the order).
