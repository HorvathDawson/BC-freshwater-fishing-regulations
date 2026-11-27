# The app chat: start here (iOS page; cut to open items and restart info in T1081)

Live state: `docs/ios/RESTART-ios-release-chat.md` (the iOS full-release chat, T1071), `docs/ROADMAP-after-0.9.0.md`,
and the ticket ledger (`tickets/README.md`). This page used to hold 1,150 lines of the 2026-11-14/16
unattended-day and restart notes, the per-lane branch lists and the IOS-1..10 work list; those tickets shipped or live in the
ledger now, and git history (before T1081) holds the pages. What is left is what still holds.

### The rules that bit (the rest is in AGENTS.md and the memory index)
- Never run the LLM parser. Never production (no `CONFIRM_PROD`; production deploys are the owner's).
- Original handoff rule, the owner's words: "Nothing merges into redesign/stream-sections without the Android benches
  and a message to the main chat; never production; never run the parser."
- Shell: prefix with `rtk`; `/usr/bin/git`, `/usr/bin/grep` for raw output; `graphify query` before reading source.
  Kill only your own PIDs. Heavy jobs (emulator, Gradle, `pnpm check`, the pipeline tier) through `scripts/heavy.sh`;
  lane 2 is yours (`HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock`), lanes 1 and 3 are the main chat's. A background
  shell for a long job carries no `timeout`.
- Commits: only your own paths (`git commit -- <paths>`); `node app/tools/check-tickets.mjs >/dev/null` first (it
  reads commit subjects too; a branch cut before a ticket existed must merge the shared head first). Never stash or
  reset in the main checkout. Never `--no-fast-fail` / `--no-ci-check`.
- Subagents: always pass a model, Opus or Sonnet. Owner 2026-11-16: "make sure you limit fable use on aubagents for
  now", then "only use when really needed". A fork runs on Fable, so it counts.
- Read `date` before writing any time. Owner answers go into the ticket's `## Owner` block (tickets/README.md) the
  same turn. He wants questions and pictures IN CHAT, in batches (dialog), with the picture in the same turn
  (SendUserFile); he cannot open repo paths.
- Your ticket numbers: T769 is free, then T820 onward (the main chat uses T799 to T806 and on).

### Commands you will need
(T1003, 2026-11-22: R8 is on in every release build; `CANIFISHTHIS_R8` is removed and refused, so the commands below no
longer carry it. A second emulator lane needs its own AVD and console port: `TESTBED_AVD=n1_pixel_b TESTBED_PORT=5584`.)
- Six benches (R8), from a worktree W with `M=<main checkout>`:
  `export HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock TESTBED_APK_TAG=<tag> MLN_LOCAL_MAVEN=$M/cache/maplibre/$(sed -n s/^MLN_VERSION=//p $M/app/tools/maplibre-android/pin.env); $M/scripts/heavy.sh <label> 30 -- bash -c "cd $W && bash app/tools/testbed/session.sh --build --crash-stub --max-min 80 -- node app/tools/testbed/run.mjs --no-r2 install crash-bed bg-install regs-open tap-stalls memory > <log> 2>&1"`
  (drop `--build` and add `--allow-stale "<reason>"` to reuse the APK when only bench files changed). A worktree needs
  `pnpm install` in `app/` and a `.venv` symlink to the main checkout's for the worker and pipeline tests.
- Live-release app tests in a worktree: `RELEASE_DIR=$M/data/release/$(cat $M/data/release/LATEST) pnpm exec vitest run <file>`
  (without it they are skipped, by name). Live staging release: r-46e4355a7277. Staging answers 429 when runs are
  less than about 60 s apart.
- App tests fill `$TMPDIR` over time (T764 fixed most): if the disk fills, look there first.
- (T918, 2026-11-19) **TestFlight External group, automatic.** `deploy/staging/build-ios-testflight.sh` now ends, after the
  upload and the version files, with `deploy/staging/testflight-distribute.mjs`: waits for Apple's processing (every
  30 s, up to 45 min), answers export compliance from app.json, sets "What to Test" from the version's iPhone notes, adds
  the build to the External group, submits Beta App Review when Apple asks (first build of a version) and prints where
  review stands. `--no-distribute` skips it. Alone, for a build already uploaded (default build = `~/.canifishthis/testflight-last-build`,
  group = `~/.canifishthis/testflight-external-group` or the only external group):
  `node deploy/staging/testflight-distribute.mjs --version 0.8.3 [--build N] [--group "<name>"] [--dry-run]`
  (`--dry-run` only reads App Store Connect). Rerunning is safe: every step is a no-op once done. Tests (HTTP mocked):
  `node --test deploy/staging/testflight-distribute.test.mjs`.


## What needs the owner physically (kept to the minimum)

- Apple ID with 2FA to enrol, pay and accept agreements (only after decision 1).
- The Team ID and an App Store Connect API key (.p8, role App Manager), saved as `~/.canifishthis/ios-team-id` and
  `~/.canifishthis/asc-api-key.json`; Claude must not make or hold them. Create the app record (bundle id
  `ca.canifishthis.app`) in App Store Connect.
- One real iPhone for the TestFlight check (and a note of its model and iOS version); an older iPhone if one exists.
- Entering App Privacy answers and the age rating in App Store Connect (Claude supplies the text).
- Nothing else until IOS-11: IOS-1 to IOS-10 run on the simulator without the owner.
