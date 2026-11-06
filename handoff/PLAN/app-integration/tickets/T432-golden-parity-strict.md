# T432 — The app's golden parity passes green on zero cases; no app skip gate

Status: DONE 2026-11-05 — 5b8fc762d. `test-full.sh` runs the app step with `REGS_STORE_REQUIRED=1` (an unpaired release fails,
naming sync.mjs; release runbook + MAP.md say so); `tools/no-silent-skips.test.ts` allowlists the 59 app skip sites with counts;
`test_smoke_coverage.py` makes every pipeline test file smoke-listed, data/slow-only or in `smoke_pending.txt` (148, shrink-only).

Source: `handoff/PLAN/audit-2026-11-05/` (findings: tests 1, 2, 7; `*.verified.md` has the evidence per finding).

## What happens
`app/conformance/src/regs/golden.ts:285-294` only warns when the paired store is missing or the release on disk is
not the one `taps.json` was cut from (`blob.release.test.ts:30-32`) — so after every release the rules→app parity
skips green until `sync.mjs` runs. `REGS_STORE_REQUIRED=1` is set nowhere; `app/MAP.md:70` says `heavy.sh` runs the
strict pass (it does not). ~60 app skip/todo sites (35 conformance, 15 tools, 5 worker) with no count gate (the
pipeline has `test_no_silent_skips.py`). The pipeline smoke set covers 205 of 385 files with no coverage gate.

## Fix
`test-full.sh` and the release checklist set `REGS_STORE_REQUIRED=1`; a stale pairing FAILS there with the
`sync.mjs` command; an app skip allowlist with counts (like the pipeline's); a smoke-coverage check (every file
listed or marked out); MAP.md corrected.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: M. Owner: no (picks the order).
