# Everything outside git that setup, build, test, release or deploy still needs (T1120 step 1)

Written 2026-11-25 for T1120 step 1 (the repo move, chat 3). It lists what is git-ignored or lives outside the repo on
the owner's Mac, and how each thing comes back on a fresh machine. Names and places only: no secret was opened or
copied; the repo is public. Sources: the repo's scripts and docs read from `main`, and `ls` of the Mac (names only).
"Written down" = a doc or script a stranger could follow today. Finds are collected in the last two sections.

## 1. Tools on the machine

| What | Where | Needed for | How it comes back | Written down today? |
|---|---|---|---|---|
| Homebrew | `/opt/homebrew` | every `brew install` below | brew.sh | NO (no doc says it) |
| Python 3.13 | brew or system; `.venv` is 3.13.12 | venv, pipeline, hooks | `PYTHON=python3.13`; install not said | partly: bootstrap.sh, README |
| tippecanoe 2.79, tile-join | `/opt/homebrew/bin` | tiles stage, bootstrap | `brew install tippecanoe pmtiles` | yes: bootstrap.sh checks it |
| pmtiles | `/opt/homebrew/bin` | basemap, tiles, bootstrap | same line | yes: bootstrap.sh checks it |
| curl, git | `/usr/bin` | bootstrap fetches, everything | macOS | yes: bootstrap.sh checks it |
| Node (version in `app/.nvmrc`; 25.9 here) | `/opt/homebrew/bin` | app, workers, hooks, builds | `brew install node`, match `.nvmrc` | partly: README; bootstrap only warns |
| pnpm 9.12.0 | `/opt/homebrew/bin` | app install | `packageManager` in `app/package.json` | partly: README; bootstrap silent |
| rclone | `/opt/homebrew/bin` | `upload.sh`, `sync-inputs.sh`, `backup.sh` | `brew install rclone` + remote `r2:` | partly: bootstrap warns, no install |
| jq | `/usr/bin/jq` | `sync-inputs.sh`, input formats, CI status | macOS, or `brew install jq` | yes: scripts say it when missing |
| sqlite3 | `/usr/bin/sqlite3` | `hydat-fetch.sh`, `update-hydat.sh` | macOS | NO (not in any setup doc) |
| gh (GitHub CLI) | `/opt/homebrew/bin` | CI status in builds, `start-workflow.sh`, pushes | `brew install gh`, then `gh auth login` | NO (only secrets.md: "login") |
| shellcheck | not installed here | `fast-fail.sh`, `sync-inputs.sh` | `brew install shellcheck` | NO (see finds) |
| wrangler | not global: `npx --prefix app wrangler` | worker and site deploys, token rotation | comes with `app/node_modules` | yes: secrets.md |
| claude CLI | `/opt/homebrew/bin/claude` | the parser (owner only) | install Claude Code, log in | yes: secrets.md (login only) |
| JDK | Android Studio's JBR, `JAVA_HOME` default | Android builds, keytool | install Android Studio | yes: build-apk.sh precheck says it |
| Android Studio | `/Applications` | JDK, SDK manager | developer.android.com | partly: precheck text only |
| Android SDK | `~/Library/Android/sdk` (`ANDROID_HOME`) | Android builds, emulator | SDK Manager | yes: build-apk.sh precheck |
| NDK 28.2.13676358 + CMake 3.31.6 | in that SDK | the MapLibre AAR build | `sdkmanager --install` (maplibre README) | yes: maplibre-android/README.md |
| NDK 27.0, 27.1; build-tools; platforms | in that SDK | the app's Gradle build, llvm-readelf | Expo/Gradle fetch what the app asks | partly: no list of versions |
| Emulator AVDs `n1_pixel`, `n1_pixel_b` | `~/.android/avd` | test bed, emulator checks | made once in Android Studio (T50/T172) | partly: testbed README names, no steps |
| Maestro | not installed here | test bed UI scenarios | testbed README says "Maestro" | partly: no install line |
| Xcode 27.2, iOS 26.5 simulators | `/Applications`, CoreSimulator | iPhone sim build, TestFlight build | App Store; Xcode, then simulators | NO (no version, no steps) |
| CocoaPods 1.17 | `/opt/homebrew/bin/pod` | iOS prebuild | `brew install cocoapods` | yes: TestFlight precheck says it |
| xcodegen | `/opt/homebrew/bin` | not called by any script found | none | not needed (see section 7) |
| Ruby, bundle | `/usr/bin` | not called by any script found | none | not needed |
| ffmpeg, zstd | `/opt/homebrew/bin` | no build script found calls them (test-bed helpers at most) | `brew install` | not needed |
| rtk, graphify | `/opt/homebrew/bin`, `~/.local/bin` | the owner's agent tooling | owner's install | not needed to build |

## 2. Installed by a command

| What | Where | Needed for | How it comes back | Written down today? |
|---|---|---|---|---|
| Python venv | `.venv/` (904 MB) | pipeline, tests, hooks, builds | `bash scripts/bootstrap.sh` | yes: fresh-machine.md |
| Worktree venv link | `.venv` (a link, ignored) | any worktree: pre-push, fast-fail | `ln -s <main>/.venv .venv` by hand | NO: T1112 (open) |
| JS install, app | `app/node_modules` (1 GB), pnpm store | tests, web app, builds, hooks | `(cd app && pnpm install --frozen-lockfile)` | partly: build scripts print it |
| Workspace `node_modules` | `app/{packages,conformance,workers}/*/node_modules` | same | same install, one command | yes: same line |
| Review app frontend deps | `tools/curation-review/frontend/node_modules` | fast-fail typecheck of the review app | `(cd tools/curation-review/frontend && npm ci)` | yes: build-links.sh prints it |
| Root `node_modules` | `./node_modules` | unknown (a stray, see finds) | unknown | NO |
| Git hooks setting | `.git/config` `core.hooksPath` | commit-msg, pre-push, ledger views | `git config core.hooksPath .githooks` | yes: AGENTS.md; bootstrap does not |
| Ledger and owner to-do views | `handoff/PLAN/views/` | reading the ledger | hooks run `node app/tools/ledger.mjs` | yes: .gitignore note |
| Expo prebuild output | `app/apps/mobile/android/`, `ios/` | every phone build | build scripts run `expo prebuild` | yes: mobile `.gitignore` |
| Gradle caches, daemons | `~/.gradle/{caches,daemon,wrapper}` | Android builds | Gradle refills them | not needed |
| `~/.gradle/gradle.properties` | exists (181 B, not opened) | optional machine-wide Gradle settings | none needed | yes: gradle-watch.sh says it overrides |
| Pods, Xcode DerivedData | `app/apps/mobile/ios/Pods`, `~/Library/Developer` | iOS builds | prebuild + `pod install` in the scripts | yes: build-ios scripts |
| Tool caches | `.pytest_cache`, `.ruff_cache`, `__pycache__`, `.vite` | nothing | regenerate | not needed |

## 3. Fetched by a command

| What | Where | Needed for | How it comes back | Written down today? |
|---|---|---|---|---|
| FWA `bc_fisheries_data.gpkg` (10.2 GB) | `data/source/` | atlas | bootstrap, sha256 in `data/source/pins.json` | yes: fresh-machine.md |
| `hydat.sqlite3` (1.3 GB) | `data/source/` | gauges, flow stage | bootstrap, pinned | yes: fresh-machine.md |
| `fish_obstacles/fish_obstacles.gpkg` | `data/source/` | atlas | bootstrap, pinned | yes: fresh-machine.md |
| `bc.pmtiles` (4.4 GB) | `data/source/` | basemap build only | bootstrap, pinned | yes: fresh-machine.md |
| `basemap.pmtiles` (225 MB, the released one) | `data/source/` | tiles, release | bootstrap, pinned, then released | yes: fresh-machine.md |
| Synopsis extraction, row boxes | `data/generated/regs/extraction/` | PDF highlight of every rule | bootstrap, pinned | yes: fresh-machine.md |
| `fwa_coast.gpkg` (559 MB) | `data/source/` | atlas coast cut (T678) | `fetch_data.py` (fwa_rivers, land layers) | NO: not pinned, not in bootstrap |
| DFO salmon pages, typed | `data/generated/` (ignored) | bundle | bootstrap runs `pipeline.regs.dfo_salmon.live` | yes: bootstrap.sh |
| Map glyphs and sprites | `data/source/map-assets/` | tiles stage | the tiles stage, pinned to a commit | yes: fresh-machine.md |
| Pinned MapLibre AAR (423 MB) | `cache/maplibre/13.7.1-main.3a6b955/` | every Android build | `bash app/tools/maplibre-android/build.sh <dir>` downloads it when `maplibre-android.lock.json` has `"published": true`; until then `scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh --compile <dir>` (12 min) | yes: maplibre README, precheck |
| Steelhead model (6.6 GB) | `data/source/steelhead/` | `known_waters` proposals only | the script re-fetches when missing | yes: fresh-machine.md (not mirrored) |
| Official region PDFs, print check | `data/source/official/` | quota print tests (they skip) | gov.bc.ca; no fetch command found | partly: fresh-machine.md says nothing reads them |
| Run-timing pages and tiles | `data/source/runtiming/*.html`, `cu_tiles/` | run-timing refresh only | `pipeline.runtiming.fetch` | yes: data/.gitignore |
| DFO NuSEDS CSV | `data/source/runtiming/dfo/` | run-timing refresh only | `pipeline.runtiming.fetch.nuseds` | yes: data/.gitignore |
| Bathymetry PDFs | `data/source/bathymetry_pdfs/` | nothing in the build | served from R2 already | yes: fresh-machine.md |
| `temp_bc_data/`, old `hydat.20260717.sqlite3` | `data/source/` | nothing (leftovers) | none | not needed |
| BC outline cache | `data/source/bc_boundary.exact.*` | atlas splits | rebuilt on demand from the gpkg | yes: data/.gitignore |
| HYDAT zip download | `data/source/hydat-download/` | `update-hydat.sh` only | `scripts/hydat-fetch.sh` | yes: data/.gitignore |
| Release set (4 GB) | `data/release/` (+ `LATEST`) | release baseline, staging uploads | R2 staging, or the first build | partly: fresh-machine.md ("fetch or accept first diff") |

## 4. Built by a command

| What | Where | Needed for | How it comes back | Written down today? |
|---|---|---|---|---|
| Atlas (11 GB) | `data/generated/atlas/` | every later stage | `heavy.sh first-build 50 -- … pipeline build --atlas --promote` | yes: fresh-machine.md |
| Tiles, reach, bundle, deliver cache | `data/generated/{tiles,reaches,bundle,deliver-cache}` | release | later stages of `pipeline build` | yes: fresh-machine.md |
| Municipal, added streams, runtiming, side | `data/generated/*` | atlas inputs | regenerable (data/generated/README.md) | yes: that README |
| Build and promote logs | `data/generated/*.log`, `build-logs/` | nothing | none | not needed |
| Source maps per build | `data/sourcemaps/` | crash symbolication of past builds | made by each app build, "kept on this machine" | partly: data/.gitignore |
| `app/dist/` (APKs, web, `last-sha`, `last-play-sha`) | `app/dist/` | fast-fail base of the next build | each build writes it; a loss falls back to the default | yes: build-apk.sh header |
| Test-bed output | `app/tools/testbed/out/` | emulator runs | `app/tools/testbed/session.sh` | yes: testbed README |
| Review app build | `tools/curation-review/frontend/dist/` | the review app | `vite build` | yes: .gitignore note |
| Store raw screenshots (28 MB) | `handoff/PLAN/app-integration/store/graphics/raw/` | store listing images | `app/tools/store-shots.mjs` re-shoots (heavy) | yes: T319 |
| Knowledge graph | `graphify-out/`, `app/graphify-out/` | the owner's agent tooling | `graphify update .` | not needed to build |
| Worker local state | `app/workers/feeds/{fixtures-full,.wrangler}` | worst-case timing test | `fixtures-full/` kept local; no recipe found | NO |

## 5. Secrets and logins (names and places only)

| What | Where | Needed for | How it comes back | Written down today? |
|---|---|---|---|---|
| Play upload keystore, properties, certificate | `~/.canifishthis/upload-keystore.*`, `upload_certificate.pem` | `build-apk.sh --play` | secret: restored from the owner's backup; else upload-key reset | yes: secrets.md |
| Play service account key | `~/.canifishthis/play-service-account.json` | `play-upload.py`, listing pushes | secret: Google Cloud, new JSON key | yes: secrets.md |
| App Store Connect key | `~/.canifishthis/asc-api-key.json` + `AuthKey_W35H7S7X3F.p8` | TestFlight build, distribute | secret: ASC, new Admin key (p8 downloads once) | yes: secrets.md |
| Two other `.p8` keys | `~/.canifishthis/AuthKey_6T3DBFF92H.p8`, `AuthKey_843F3K6BSH.p8` | no script (one is the APNs key) | secret: Apple Developer, Keys | partly: secrets.md says "probably" |
| iOS team id, store url, state files | `~/.canifishthis/{ios-team-id,app-store-url,play-*,testflight-last-build}` | iOS and Play builds | team id: Apple membership page; the rest: scripts write | partly: `app-store-url` not in secrets.md |
| Debug keystore | `app/apps/mobile/android/app/debug.keystore` | staging APK signing, same key each time | secret: owner's backup (a new one means a reinstall) | yes: secrets.md |
| Staging run token | `app/workers/feeds/.run-token` | reports, emails, workflow starts | `deploy/staging/rotate-run-token.sh` | yes: secrets.md |
| Production run token | `app/workers/feeds/.run-token.prod` | prod reports, prod builds | secret: owner's backup, else rotate prod | partly: secrets.md says "not on this Mac yet" |
| Worker secrets (RUN_TOKEN, REPORT_SALT, …) | Cloudflare, per worker | feeds workers | `wrangler secret put`; names in secrets.md | yes: secrets.md |
| GitHub Actions secrets | GitHub repo settings | CI jobs | `gh secret set`; they stay with the repo | yes: secrets.md; PREP.md for the move |
| `.env` (repo root, 1 KB, not opened) | main checkout, ignored | unknown (nothing in code or docs reads it) | owner adds its variable names | NO: secrets.md says so itself |
| rclone remote `r2:` | `~/.config/rclone/rclone.conf` | uploads, backups, test bed | secret: new R2 token, `rclone config` | yes: secrets.md |
| wrangler login | wrangler's own config | deploys, token rotation | `npx --prefix app wrangler login` | yes: secrets.md |
| gh login | keychain | CI status, pushes, workflows | `gh auth login` | yes: secrets.md |
| Claude CLI login | Claude's own config | parser (owner only) | `claude`, log in | yes: secrets.md |
| ssh keys and host alias | `~/.ssh/config`, `id_ed25519_personal` | `git@github.com-personal:` remote | secret: owner's backup, or a new key on GitHub | NO: PREP.md names the alias only |
| Apple and Play accounts | Play Console, App Store Connect | store releases | the owner's logins | yes: secrets.md |

## 6. Owner's uncommitted work

| What | Where | Needed for | How it comes back | Written down today? |
|---|---|---|---|---|
| Review marks, curation edits | `data/curated/**` | the data build | in git; `git status` of the main checkout was clean | yes: T246 inventory |
| Curated backup tarball | `~/canifishthis-backups/curated-20261029…tar.gz` | disaster recovery | `deploy/staging/backup.sh curated` | yes: T246 |
| Review refused-saves, write-through log | `data/generated/review/*.jsonl` | nothing (runtime state) | none | not needed |
| Worktrees | 41 listed by git; `~/bcfish-*` (15 folders) | in-flight lanes | `git worktree list`; uncommitted work there is lost with them | partly: PREP.md section 3 |

## 7. Machine state that is not needed

| What | Where | Needed for | How it comes back | Written down today? |
|---|---|---|---|---|
| Heavy-job locks and queues | `~/.claude/heavy-job*.lock*` | `scripts/heavy.sh` makes them itself | made on first use | yes: heavy.sh header |
| Claude memory (154 notes) | `~/.claude/projects/…/memory/` | the owner's chats only | not needed to build | not needed |
| Claude settings, project `.claude/` | `.claude/` (ignored), `~/.claude/` | the owner's chats only | not needed to build | not needed |
| Proof folders of the T416 run | `~/t416-fresh`, `~/t416-*.log` | nothing now | delete | not needed |
| Stray tools: xcodegen, Ruby, bundle | system | no script found calls them | none | not needed |
| Local branch `redesign/stream-sections` | main checkout | nothing | none | not needed |

## Unknown or unwritten (finds)

1. **`CLAUDE.md` (root) is git-ignored** (`.gitignore` line 53). It holds the rule "parser runs are HUMAN-ONLY" and the
   RTK notes. A fresh clone has no `CLAUDE.md`, so an agent there does not see the parser rule. AGENTS.md rule A1 says
   the same, so the rule survives, but the file itself exists only on this Mac. `.claude/CLAUDE.md` likewise.
2. **`data/source/fwa_coast.gpkg` (559 MB) is not pinned or fetched by bootstrap.** The atlas reads it
   (`config.yaml` `coast_gpkg_path`, `pipeline/atlas/inputs.py`); `pins.json` has no entry and `bootstrap.sh` skips it.
   The T416 proof (2026-11-05) pre-dates T678, so a fresh atlas build may stop here. Needs a try (T1120 step 2).
3. **Installs bootstrap does not do:** no `pnpm install`, no `core.hooksPath`, no Node or pnpm version check. A fresh
   clone's first push fails `pre-push` (`.githooks/pre-push` needs `.venv`) until hooks and installs are done by hand.
   T1112 covers worktrees; the fresh-clone case is the same missing step.
4. **`fresh-machine.md` says "Secrets: none are needed to build"**, true for data, but a phone build needs the
   Android SDK/NDK/JDK or Xcode, which the page never lists (it stops at the data build). No page lists the
   mobile toolchain: JDK path default, SDK, NDK 28.2 + CMake 3.31.6 for MapLibre, AVD names, Xcode version, CocoaPods.
5. **Pinned MapLibre AAR (`cache/`, ignored) is built, not fetched**: 12 minutes plus clone, on a heavy lane. Nothing
   mirrors it. It is written down (maplibre README), but `fresh-machine.md` does not mention it.
6. **`gh` is needed by every build gate** (`scripts/ci_status.py`, `start-workflow.sh`, pre-push) and its install and
   login are named in no setup doc; only secrets.md lists the login.
7. **`shellcheck` is not installed on this Mac** though `scripts/fast-fail.sh` and `sync-inputs.sh` call it; unknown
   whether fast-fail skips or fails without it (not run: a build is in progress). Check in step 2.
8. **secrets.md is stale in places:** it says `.run-token.prod` is "not on this Mac yet" (it is there now); it
   does not list `~/.canifishthis/app-store-url`; it says `.env` "exists, not read" with no variable names
   (nothing in the code or docs reads a root `.env` at all: unknown purpose, kept only as a name); it says the APNs
   code "is not on `redesign/stream-sections` yet", but that branch is not the trunk any more.
9. **The two extra `.p8` keys** (`AuthKey_6T3DBFF92H`, `AuthKey_843F3K6BSH`): no script reads them and secrets.md
   only guesses ("probably"); the owner must say which is the APNs key (the worker's `APNS_KEY`).
10. **`fresh-machine.md` has no iPhone, Android or deploy section:** step 2 of T1120 has to be written from the scripts.
    It names the old clone URL (PREP.md section 2 already lists it for the slug flip).
11. **Local-only state:** `~/.canifishthis/play-last-version-code`, `testflight-last-build` and `app/dist/apk/last-*sha`
    steer version codes and the fast-fail base. A new machine starts without them: Play's own highest versionCode is
    read first (so safe), but `last-sha` falls back to the default base, which is said, not guessed (build-apk.sh).
12. **`app/workers/feeds/fixtures-full/`** (worst-case timing recordings, "tens of MB, kept local"): no recipe says how
    to record them again.
13. **`data/source/official/` (region PDFs, 18 MB):** the print cross-check tests skip without it, but no command
    fetches it, so a fresh clone silently runs fewer tests (they name the file when skipping).
14. **Root `./node_modules` and `.test-scratch/`** exist in the main checkout; nothing found says what makes the
    root `node_modules` (a stray `npm install`?). Probably not needed; confirm in step 2.
15. **Maestro** (test bed UI runs) is named in the testbed README but not installed here; not a build need.
16. **SSH:** the remote is `git@github.com-personal:…`, which works only through the alias in `~/.ssh/config` and the
    personal key; `fresh-machine.md` clones `git@github.com:…`. A fresh clone on another machine needs its own key.
17. **Worktree builds:** `build-links.sh` prints fixes that link or copy from the main checkout; on a machine with no
    main checkout the `.run-token*`, debug keystore and `cache/maplibre` come only from secrets or the build commands.

## Secrets for the owner to confirm he can restore

Names only. For each: can you get it back if this Mac is gone?

- `~/.canifishthis/`: `upload-keystore.jks`, `upload-keystore.properties`, `upload_certificate.pem`,
  `play-service-account.json`, `asc-api-key.json`, the three `AuthKey_*.p8` files.
- `app/apps/mobile/android/app/debug.keystore` (without it every phone must uninstall the staging app once).
- `app/workers/feeds/.run-token` (rotatable) and `.run-token.prod` (restore, or rotate the prod worker secret).
- Cloudflare worker secrets: `REPORT_SALT` (cannot be read back; never rotate), `NOTIFY_EMAIL_TO`,
  `GITHUB_DISPATCH_TOKEN` (expires), `APNS_KEY`, `APNS_KEY_ID`, `APNS_TEAM_ID`.
- GitHub Actions secrets: `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`.
- rclone remote `r2:` (R2 token), wrangler login, `gh` login, Claude CLI login.
- `~/.ssh/id_ed25519_personal` and the `github.com-personal` entry in `~/.ssh/config`.
- Root `.env`: what is in it, and does anything still read it?
- Accounts: Play Console, App Store Connect and Apple Developer, Cloudflare, Google Cloud, GitHub, the registrar.

## Fresh clone, light half (2026-11-25): what happened

T1120 lane, step 2 of the ticket, the half that needs little disk. A new folder (`canifishthis-fresh`, beside the main
checkout, which was not touched: no link, copy or read from it to make a step pass). Branch `docs/T1120-fresh-clone-setup`.
Nothing from `~/.canifishthis/`, `.env` or a token file was opened. Not run (heavy half, see the list in
`fresh-machine.md` sections 5-10): bootstrap's fetch, the data build, Android, iPhone, deploy dry runs.

| step | doc line followed | command | result | time | missing or wrong |
|---|---|---|---|---|---|
| 1 clone | fresh-machine.md: `git clone --branch main git@github.com:canifishthis/canifishthis.git` | that, then (after 8 minutes at about 100 KB/s, 20 MB of 1.7 GB) https | ssh: too slow to finish, stopped; https PASS | https 3 min 18 s; 2.8 GB with the checkout | the page said nothing of the size (1.7 GB history, 2.8 GB on disk); the ssh clone from this Mac crawled while https and a plain download ran at 20 MB/s (cause unknown; the alias form timed out at 40 s with nothing) |
| 1 hooks | AGENTS.md / README: `git config core.hooksPath .githooks` | that | PASS | 0 s | fresh-machine.md did not say it; `bootstrap.sh` did not do it (now does) |
| 2 Python | bootstrap.sh: `PYTHON=python3.13`, venv, `pip install -r requirements.txt -c constraints.txt` | (by hand, then the new `bootstrap.sh --setup-only`) | PASS | 30 s | no doc says where Python 3.13 comes from (here: miniconda's, the only one; no brew python); the shell had the main checkout's `VIRTUAL_ENV` and `.venv/bin` first on PATH, so `python3.13` was that checkout's (a clean PATH was needed: now said in section 2) |
| 3 JS | README: `pnpm -C app install --frozen-lockfile`; `app/.nvmrc` 25.9.0, `packageManager` pnpm 9.12.0, engines node >=22.18 | same, inside bootstrap | PASS (node 25.9.0, pnpm 9.12.0 were already here) | 4 s | bootstrap did not install or check; pnpm reused 692 of 716 packages from the machine-wide pnpm store, so this was not a cold install (a find: a machine with an empty store downloads them) |
| 4 commit and push | AGENTS.md: hooks | commit with the hooks on | commit PASS (commit-msg ran); the push is in the PR section below | 2 s | none |
| 5 tests | scripts/before_pr.sh names the two suites | `heavy.sh -- pnpm -C app exec vitest run`; `heavy.sh -- bash scripts/pytest-chunked.sh -- -q -m "$markers"` (one at a time, not before_pr.sh, which runs both at once) | pipeline no-data tier PASS (94 chunks, 14 min, peak 1.4 GB); app suite: 826 files and 8818 tests passed, 20 files and 1005 tests skipped, but exit code 1: one unhandled `[vitest-worker]: Timeout calling "onTaskUpdate"` (the Mac was also running another chat's heavy job and my dev server); a rerun was not possible (disk fell to 22 GB, under the 25 GB floor) | 4 min 55 s; 14 min | the skips are "no release on this machine (data/release/r-*)" and the LIVE gates; pytest -q hides pytest skip reasons |
| 6 web app | app/README: `pnpm -C app dev:web` | that; `curl http://localhost:39218/` and the web bundle; stopped by PID | PASS: 200, 1266 bytes of HTML; bundle 200, 10.7 MB, "Web Bundled 7663 ms (1295 modules)" | 45 s start | what the app shows with no data release was not looked at |
| 7 heavy half | | read only | the command list is fresh-machine.md sections 5-10 | | see the finds below |

Disk: the clone is 4.2 GB after the installs and the two suites (`.venv` 0.9 GB, `app/node_modules` 1 GB).

### The 17 finds

- Fixed here: 3 (installs, hooks, versions: `bootstrap.sh --setup-only`, fresh-machine.md sections 1-2), 4 and 5 (the mobile
  toolchain and the MapLibre AAR are in sections 8-9), 6 (gh install and login, section 0), 8 (secrets.md: `.run-token.prod`,
  `app-store-url`, the APNs line, the `.env` wording), 10 (the page now has Android, iPhone and deploy sections and the https
  clone URL), 16 (the clone command says the ssh alias and the https route).
- Confirmed, still open: 1 (`CLAUDE.md` and `.claude/` are absent in the fresh clone, so the human-only parser rule lives in
  AGENTS.md A1 only), 2 (`pins.json` has no coast entry, bootstrap fetches no `fwa_coast.gpkg`), 9 (the two extra `.p8` keys:
  the owner says which is APNs), 13 (`data/source/official/` has no fetch command; the app suite skips with a message
  naming the release, and `pytest -q` hides the pipeline's), 14 (root `node_modules` was not made by anything in setup,
  tests or the web app: it is a stray; `.test-scratch/` is made by the tests and is ignored), 7 (shellcheck: nothing runs
  it, only `# shellcheck` comments exist: not needed, closed), 17 (a worktree build still copies from a main checkout).
- Not reached (heavy half): 11 (local-only state), 12 (`fixtures-full`), 15 (Maestro).

### New finds for the planning chat (not numbered)

- The ssh clone from this Mac ran at about 100 KB/s (https 20 MB/s): cause not known; the doc's `git@github.com:` URL did not finish.
- A clone is 2.8 GB on disk (1.7 GB of history): worth saying on the page, or a smaller history.
- `python3.13` on PATH was the main checkout's venv (the shell inherited `VIRTUAL_ENV`); a worktree or second clone run
  from such a shell borrows the other venv silently. `bootstrap.sh` should refuse a `VIRTUAL_ENV` that is not its own.
- The pnpm store is machine-wide: a fresh clone here was not cold, so the install time and the `--offline` builds are not
  proven for a machine with an empty store.
- The app vitest suite exited 1 with every test passed (one worker RPC timeout under load): is it flaky in CI too?
- `pre-push` finds `.venv` through the git common dir, which works for a clone and for worktrees (T1112), but nothing makes
  it for a worktree's own installs.

