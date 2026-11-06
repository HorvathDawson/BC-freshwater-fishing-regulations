# T433 — Staging scripts: upload can take staging dark; sync and HYDAT mask failures

Status: DONE 2026-11-05 (chat 3, 4d8b72626) — from the audit (T333–T335, 2026-11-05). Severity: **high**. Verified by a second reader.

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

## Result (2026-11-05)
- upload.sh: the clear runs AFTER the pointer flip and spares what the new release names (manifest, its tiles,
  release folder, basemap, synopsis, map-assets/v1, the wasm) plus feeds/ private/ runtiming/ test/ app/android/.
  A failure above it now leaves the old manifest + release live. No dry-run mode exists; pinned by a test on the order.
- sync-inputs.sh: a failed copy prints its log and exits 1 (nothing after it sent); dry-run against staging: 12
  objects, clean.
- HYDAT: fetch_data's hydat layer now downloads through `scripts/hydat-fetch.sh` (the checked one, as CI) and raises
  on an unreadable listing; update-hydat.sh keeps its own listing check. The zip stays in `data/source/hydat-download/`
  (ignored) so a rerun resumes.
- check-public.mjs: a failed private read or `rclone config dump` throws; only a missing `.run-token` is a skip.
  Not added: "fail on 0 emails" — the reporter email is optional. Run against staging: PASS.
- build-ios-testflight.sh `--production` needs `CONFIRM_PROD=canifishthis.ca`, checked before the heavy queue.
- Finding 14: the two `curation-review/frontend/*.tsbuildinfo` are untracked (gitignored already).
Tests: `pipeline/tests/test_staging_scripts_fail_safe.py` (6 of 7 fail with the fixes reverted).
