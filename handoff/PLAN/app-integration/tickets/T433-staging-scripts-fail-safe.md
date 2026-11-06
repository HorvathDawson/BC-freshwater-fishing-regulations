# T433 — Staging scripts: upload can take staging dark; sync and HYDAT mask failures

Status: OPEN — from the audit (T333–T335, 2026-11-05). Severity: **high**. Verified by a second reader.

Source: `handoff/PLAN/audit-2026-11-05/` (findings: review-deploy 1, 2, 3, 12, 14; `*.verified.md` has the evidence per finding).

## What happens
`deploy/staging/upload.sh:38` deletes `release/manifest.json` and the live release before steps :40-90 can fail —
any exit there leaves staging with no manifest. `sync-inputs.sh:22` `| grep … || true` hides rclone failures and
prints "in sync". `scripts/update-hydat.sh:52` uses `fetch_data.fetch_hydat` (exits 0 on a failed listing) instead of
the checked `scripts/hydat-fetch.sh`. `check-public.mjs:45,51,57-63`: a failed secret read drops that secret from the
scan without failing. `build-ios-testflight.sh --production` (in deploy/staging) has no CONFIRM_PROD guard.

## Fix
Clear AFTER the pointer flip (or keep the manifest + live release until then); `pipefail` + checked exits in
sync-inputs; one HYDAT downloader (the checked one); check-public fails on an unreadable secret; a CONFIRM guard for
the production TestFlight path. Dry-run each against staging.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: M. Owner: no (picks the order).
