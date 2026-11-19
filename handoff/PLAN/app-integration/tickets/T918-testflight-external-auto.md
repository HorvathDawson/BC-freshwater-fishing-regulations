# T918 — TestFlight: a new iPhone build goes to the External group by itself

Status: IN PROGRESS (owner 2026-11-19 ~15:49; built on lane app/t918-testflight-external, tested with App Store Connect
mocked; left (Claude/main chat): the first real run against App Store Connect, then merge).

Owner 2026-11-19 (main chat), verbatim: "when we push new apple one will it update the version automatically for testers"
-> told: no, an External group gets each build only when it is added (and the first build of a version waits for Beta
App Review); "can we make it auto update it for the group too?" -> yes, built here.

## What
After `deploy/staging/build-ios-testflight.sh` uploads, a new step (also runnable alone, for a build already uploaded):
wait until App Store Connect has processed the build, add it to the External group (by name), and submit it for Beta App
Review when Apple asks for one (the first build of a version). It uses the API key the script already reads
(~/.canifishthis/asc-api-key.json -> its .p8), signed in Node (ES256 JWT, no new dependency). Claude never opens those
files; only the script reads them at run time. Production and staging builds both; an internal group is left alone.

Testers' phones: TestFlight's per-app "Automatic Updates" (on by default, as far as known) then installs it.

## Built (2026-11-19, lane app/t918-testflight-external; not yet run against the real App Store Connect)
- `deploy/staging/testflight-distribute.mjs` (no dependency; ES256 JWT with node:crypto, 15-min tokens, aud
  `appstoreconnect-v1`). Steps: the app by bundle id -> the external group (named, else the only one; several refused by
  name; an internal group never touched) -> the build by app + version + build number, waited for every 30 s up to 45 min
  (a build not listed yet counts as waiting; FAILED/INVALID stop) -> export compliance from app.json
  `ios.config.usesNonExemptEncryption` when the build has none (app.json without it: refused) -> "What to Test" from the
  version's iPhone changelog bullets (core `notesOn`) when empty -> add to the group (already in: no-op) -> Beta App Review
  submitted when `buildBetaDetail.externalBuildState` is READY_FOR_BETA_SUBMISSION (an existing submission is reported,
  never resubmitted). Last line: waiting for review / in review / approved / testers can install. Any failure: Apple's own
  error text, exit 1. `--dry-run` reads only (GET) and prints each write it would make.
- Review fixes (2026-11-19): the external state is allow-listed (READY_FOR_BETA_SUBMISSION submits; WAITING/IN_BETA_REVIEW,
  BETA_APPROVED, IN_BETA_TESTING, READY_FOR_BETA_TESTING are reported; PROCESSING / IN_EXPORT_COMPLIANCE_REVIEW /
  MISSING_EXPORT_COMPLIANCE are re-read every 30 s up to 5 min, also covering Apple's lag after the compliance answer;
  any other state, or none, stops it, named). `--max-wait-min` / `--poll-s` must be positive numbers. The "What to Test"
  text used is logged, with a warning naming why when it fell back to the plain line.
- `deploy/staging/build-ios-testflight.sh` runs it after the upload and the version files (`--no-distribute` skips;
  `--no-upload`/`--unsigned`/`--dry-run` never run it; the dry run prints the step). A failure exits 1 after the upload is
  recorded, printing the rerun command.
- Tests: `node --test deploy/staging/testflight-distribute.test.mjs` (16, HTTP mocked, throwaway key), run from pytest by
  `pipeline/tests/test_deploy_node_scripts.py`. Documented in the script headers and `handoff/PLAN/ios/START-HERE-ios.md`
  ("Commands you will need").

Commands:
- distribute an uploaded build (main chat; build = `~/.canifishthis/testflight-last-build` when `--build` is left out):
  `node deploy/staging/testflight-distribute.mjs --version 0.8.3 --dry-run` then the same without `--dry-run`
- name the group once (optional; needed only if the app has several external groups):
  `echo "<External group name>" > ~/.canifishthis/testflight-external-group`

Unsure of Apple's behaviour (to confirm on the first real run): whether the app-level Beta App Review details (contact,
"Beta App Information") are already filled (a submission without them is refused with Apple's message); whether Apple
creates the build's en-CA beta localization itself (the script patches it, or creates one in the app's primary locale).
