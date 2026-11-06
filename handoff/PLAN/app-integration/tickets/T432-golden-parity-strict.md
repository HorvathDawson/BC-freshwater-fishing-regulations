# T432 — The app's golden parity passes green on zero cases; no app skip gate

Status: OPEN — from the audit (T333–T335, 2026-11-05). Severity: **high**. Verified by a second reader.

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
