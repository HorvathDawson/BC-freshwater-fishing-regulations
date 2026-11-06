# T429 — A missing review input can empty a curated file

Status: DONE 2026-11-05 (chat 3) — from the audit (T333–T335, 2026-11-05). Severity: **high**. Verified by a second reader.

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

## Result (2026-11-05)
1. The names / sloughs (automark + finder) / tidal candidate loaders raise on a missing input; no empty set.
2. `apply.write_files` refuses a write that drops more than `SHRINK_LIMIT` (10) rows unless `--accept-shrink`, naming
   the count; `apply.run` says it as a refused event (the review app shows it on the item). Not "any fewer rows": an
   un-mark legitimately drops one (the write-through test does exactly that); the live files hold 184 / 811 / 1169.
3. **No** to `GENERATED_ROOT`: the input overrides are test hooks on generated INPUTS, not curated files, and nothing
   but tests sets them (`gate.py`/`parity.py` set only BUILD / CURATED_ROOT / REACH / BUNDLE / RELEASE). With 1 a wrong
   pointer now raises instead of emptying anything.
Tests: `pipeline/tests/test_review_shrink_guard.py` (7 of 12 fail with the fix reverted).
