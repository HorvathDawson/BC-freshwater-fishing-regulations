# Runbook: set up and build from a fresh clone

The one setup page. A new machine with only the git repo can run the tests, the web app and a data build, and (with the
accounts' files) the phone builds and the deploys. Sections 1-4 are cheap (a few GB, minutes); sections 5 and up are
heavy and each names its prerequisites. Timings are from the owner's Mac (M-series, 64 GB). What a fresh-clone run found
that is not fixed here: `handoff/PLAN/repo-move/OUTSIDE-GIT.md`.

## 0. What the machine needs first

| tool | for | how |
|---|---|---|
| Homebrew | every install below | brew.sh |
| Python 3.13 | the venv, the pipeline, the hooks | `brew install python@3.13` (bootstrap looks for `python3.13`; `PYTHON=/path/to/python3.13` names another) |
| Node (the version in `app/.nvmrc`) and pnpm 9 (`app/package.json` `packageManager`) | the app, the workers, the commit-msg hook | `brew install node pnpm` (match `.nvmrc`; Node 22.18 or newer is the floor in `engines`) |
| gh (GitHub CLI) | CI status in every build gate, `scripts/start-workflow.sh`, PRs | `brew install gh`, then `gh auth login` |
| tippecanoe (with tile-join), pmtiles | the tiles stage, bootstrap's fetch | `brew install tippecanoe pmtiles` |
| rclone | `upload.sh`, backups, `sync-inputs.sh` (uploads only) | `brew install rclone`, then a remote `r2:` (secrets.md) |
The phone toolchains are in sections 8 and 9. `CLAUDE.md` and `.claude/` are git-ignored, so a fresh clone has only
`AGENTS.md` (rule A1 holds the parser rule).

## 1. Clone and hooks

```bash
git clone --branch main https://github.com/canifishthis/canifishthis.git     # about 2.8 GB with the checkout, 3 minutes
cd canifishthis
git config core.hooksPath .githooks       # the versioned commit-msg and pre-push hooks (`bootstrap.sh` does this too)
```

With a push key (the owner's Mac uses the ssh alias `github.com-personal`), use
`git@github.com-personal:canifishthis/canifishthis.git`; any other machine needs its own ssh key added to GitHub, then
`git@github.com:canifishthis/canifishthis.git`. The repo is public: clone over https and set the push URL afterwards.

## 2. Installs (cheap): Python venv and the app's node_modules

```bash
bash scripts/bootstrap.sh --setup-only    # tools check, core.hooksPath, .venv (requirements + constraints), pnpm -C app install --frozen-lockfile; fetches nothing, 35 s
```

Leave any other checkout's venv first (`unset VIRTUAL_ENV`, its `.venv/bin` out of `PATH`): a `python3.13` found there
is that checkout's. A worktree of this clone shares the hooks
setting but needs its own installs, or `PIPELINE_PY` for the pre-push hook.

## 3. Tests

```bash
git commit …                                     # commit-msg: Conventional Commits `type(Tnnn): subject`, commitlint from app/node_modules
git push -u origin HEAD                          # pre-push: scripts/before_push.py --changed (a docs push is seconds)
bash scripts/heavy.sh app-suite 30 -- pnpm -C app exec vitest run                                 # the whole app suite (heavy; 5 min)
. scripts/test-env.sh && test_env_pycache; markers=$(no_data_markers)
bash scripts/heavy.sh tier 30 -- bash scripts/pytest-chunked.sh -- -q -m "$markers"                # the pipeline's no-data tier (heavy; 14 min, 94 chunks)
```

`scripts/before_pr.sh --title "<the PR title>"` checks the title with its ` (#N)` suffix, then runs the last two at once on two heavy lanes, and is what to run before opening a PR. Tests that
need a data release are marked `needs_bundle`. The region PDFs are not kept: the book is
`data/source/fishing_synopsis.pdf`; validate against nothing else.

## 4. The web app

`pnpm -C app dev:web` serves Expo web on http://localhost:39218 and needs only Node and no data release; the map and
the waters need one (section 6, or fetch one from staging R2).

## 5. The data: fetch the pinned sources (heavy: about 16 GB)

`bash scripts/bootstrap.sh` (the installs of section 2 first, then the fetches; `PYTHON=` names another Python)
`bootstrap.sh` checks the tools (tippecanoe, tile-join, pmtiles, curl, git; rclone and node are needed only to upload
or for the app), makes `.venv`, then fetches:

| what | from | size | how it is checked |
|---|---|---|---|
| `bc_fisheries_data.gpkg` (FWA 2024 + live layers as of the Mac's fetch) | `build.canifishthis.ca/source/<sha16>/` | 10.2 GB | sha256 in `data/source/pins.json` |
| `hydat.sqlite3` (HYDAT 20261019) | same bucket | 1.3 GB | sha256 |
| `fish_obstacles/fish_obstacles.gpkg` | same bucket | 33 MB | sha256 |
| `bc.pmtiles` (the raw basemap, Protomaps 2026-03-05, pruned upstream; only a basemap rebuild, the tidal audit tools and the manifest's basemap date read it) | `data.canifishthis.ca/bc.pmtiles`: **404 since 2026-11-25, so the fetch exits 1 here** (T1120 find; copy the file from a machine that has it until its ticket lands) | 4.4 GB | sha256 |
| `basemap.pmtiles` (the RELEASED basemap, basemap-6960ed933a82: the live release's ground) | `build.canifishthis.ca/source/<sha16>/` | 225 MB | sha256; bootstrap then releases it (`pipeline.deliver.tiles.basemap --release-only`) |
| `generated/regs/extraction/{synopsis_raw_data,row_boxes}.json` (the synopsis extraction and its row boxes: the PDF highlight of every rule) | `build.canifishthis.ca/source/<sha16>/` | 0.8 MB | sha256 (the old `extraction/` backup was stale; a release without the row boxes is refused) |
| DFO salmon pages, typed | DFO's live pages (`pipeline.regs.dfo_salmon.live`) | small | today's pages |
| `fwa_coast.gpkg` (the atlas's coast cut, `config.yaml` `coast_gpkg_path`; without it the atlas fails in one second) | same bucket (pinned 2026-11-26; upstream: `data/fetch_data.py` layers `fwa_rivers`, `fwa_land`) | 0.6 GB | sha256 |

Downloads resume (`curl -C -`); rerun the script after a drop (the message says "rerun to resume" for a 404 too). A file whose sha256 differs is deleted and the run fails.
About 7 minutes at ~24 MB/s; on 2026-11-25 (3-8 MB/s, a shared link) the 10 GB file dropped three times and took four runs.

Everything else the build reads is in git (curated data, places, stations, the synopsis PDF, municipal layers, DFO
tidal pages, run-timing parse). Map fonts and sprites are fetched by the tiles stage itself, pinned to a commit.

## 6. The data build

```bash
python -m pipeline build --dry-run           # on a fresh clone: "no atlas … run with --atlas --promote first"
bash scripts/heavy.sh first-build 50 -- .venv/bin/python -m pipeline build --atlas --promote --jobs 2
```

Stages and peak memory: atlas 26 min, 23 GB (the machine needs about 24 GB free); clim 24 s; tiles 9 min, 11 GB; reach
11 min, 17 GB; deliver 12 min, 14 GB. About 1h15 in all.

**The basemap is not a build stage**: bootstrap releases the pinned one, so the release names the same ground as the live
one, and a release without a basemap is refused. For a newer OSM ground, build it from `bc.pmtiles`:
`python -m pipeline.deliver.tiles.basemap` (then pin it again). After the first build, `python -m pipeline release
--staging` works as in `release.md` (the first release has no live baseline: fetch `data/release/` from R2 or accept the
first binding diff).

## 7. Refreshing the inputs (not part of a rebuild)

The pins are the exact bytes the live release was built from. To move to newer upstream data:
`PYTHONPATH=. .venv/bin/python data/fetch_data.py --missing-only` (without the flag, everything, about 55 min), then
`.venv/bin/python -m pipeline.tools.source_pins pin data/source/bc_fisheries_data.gpkg --upstream "…"`. Live layers drift
between fetches, so a refreshed gpkg gives a slightly different release: a data update, reviewed through the binding diff.

Not mirrored on purpose: `data/source/steelhead/` (6.6 GB; only `known_waters` reads it, to propose the curated list),
the region PDFs (no longer kept; nothing reads them),
`data/source/runtiming/cu_tiles/` (the run-timing workflow fetches its own), `bathymetry_pdfs/` (served from R2).

## 8. Android debug build (heavy; prerequisites written from the scripts, T1120)

| prerequisite | where it is checked or said | how |
|---|---|---|
| JDK | `deploy/staging/build-apk.sh` precheck (`JAVA_HOME`, default Android Studio's JBR) | install Android Studio |
| Android SDK (`ANDROID_HOME`, default `~/Library/Android/sdk`) with platforms, build-tools, and NDK 27.x for the app's Gradle build | the precheck | Android Studio, SDK Manager; Gradle fetches what the app asks |
| NDK 28.2.13676358 + CMake 3.31.6 | `app/tools/maplibre-android/README.md` | `sdkmanager --install "ndk;28.2.13676358" "cmake;3.31.6"` (JAVA_HOME set as in that README) |
| the pinned MapLibre AAR in `cache/maplibre/<MLN_VERSION>/` (git-ignored) | the precheck, `pin.env` | `bash app/tools/maplibre-android/build.sh <work dir outside the repo>` downloads it when `app/tools/maplibre-android/maplibre-android.lock.json` has `"published": true`; until then it stops, so build it: `bash scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh --compile <work dir outside the repo>` (12 min; 33 on 2026-11-26, 20 of them the clone and its submodules) |
| `llvm-readelf` | the precheck (under the SDK's `ndk/`) | comes with the NDK |
| `app/apps/mobile/android/app/debug.keystore` (git-ignored) | the precheck; secrets.md | restored from the owner's backup, never a fresh key (the build refuses to sign with one): an update would not install over the last APK |
| `app/workers/feeds/.run-token` | `build-apk.sh` emails with it; `--no-email` skips | `deploy/staging/rotate-run-token.sh` (rotates the STAGING worker's token: the owner's) |
| Cloudflare login | uploading builds | `npx --prefix app wrangler login`; `--no-upload` skips |

```bash
bash deploy/staging/build-apk.sh --precheck                       # lists every missing item with its fix, builds nothing
bash deploy/staging/build-apk.sh --no-upload --no-email           # the APK, kept in app/dist/apk/ (heavy.sh wraps it)
```

A build is of COMMITTED HEAD in a temporary worktree; `pnpm install --offline` there reads pnpm's store (filled in
section 2). With no other checkout, `deploy/lib/build-links.sh` cannot offer a copy: make each missing file with the
command in its message.

## 9. iPhone simulator build (heavy)

Prerequisites: Xcode (the App Store; run it once for the licence and the simulator runtime), CocoaPods
(`brew install cocoapods`), Node. No Apple account is needed for the simulator.

`bash deploy/staging/build-ios-sim.sh --dry-run` prints the steps and builds nothing; without `--dry-run` it builds
`app/dist/ios-sim/canifishthis.app` (heavy.sh wraps it).

The TestFlight build (`build-ios-testflight.sh --precheck` lists what it needs) is the owner's: it needs the App Store
Connect key files in `~/.canifishthis/` (secrets.md).

## 10. Deploys and secrets

No secret is needed to build. Every deploy script (`deploy/staging/*.sh`, `deploy/prod/*.sh`) needs a login or key that
is not in git: `handoff/PLAN/runbooks/secrets.md` names each, what it does and how to replace it (uploads: the rclone
remote `r2:` and a wrangler login; Play: the files under `~/.canifishthis/`). Production steps need `CONFIRM_PROD`
typed by the owner.
