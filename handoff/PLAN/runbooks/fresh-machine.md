# Runbook: set up and build from a fresh clone (T416, T1120)

The one setup page. A new machine with only the git repo can run the tests, the web app and a data build, and (with the
accounts' files) the phone builds and the deploys. Sections 1-4 are cheap (a few GB, minutes); sections 5 and up are
heavy and each names its prerequisites. Proven on 2026-11-05 (data build, `~/t416-fresh`) and on 2026-11-25 (sections 1-4
and the prerequisite lists below, T1120, `canifishthis-fresh` beside the old checkout); the timings are from those runs
on the owner's Mac (M-series, 64 GB). What T1120 tried and found: `handoff/PLAN/repo-move/OUTSIDE-GIT.md`.

## 0. What the machine needs first

| tool | for | how |
|---|---|---|
| Homebrew | every install below | brew.sh |
| Python 3.13 | the venv, the pipeline, the hooks | `brew install python@3.13` (bootstrap looks for `python3.13`; `PYTHON=/path/to/python3.13` names another) |
| Node (the version in `app/.nvmrc`) and pnpm 9 (`app/package.json` `packageManager`) | the app, the workers, the commit-msg hook | `brew install node pnpm` (match `.nvmrc`; Node 22.18 or newer is the floor in `engines`) |
| git, curl, jq, sqlite3 | everything | macOS ships them |
| gh (GitHub CLI) | CI status in every build gate, `scripts/start-workflow.sh`, PRs | `brew install gh`, then `gh auth login` |
| tippecanoe (with tile-join), pmtiles | the tiles stage, bootstrap's fetch | `brew install tippecanoe pmtiles` |
| rclone | `upload.sh`, backups, `sync-inputs.sh` (uploads only) | `brew install rclone`, then a remote `r2:` (secrets.md) |

`shellcheck` is not needed (nothing runs it; only `# shellcheck` comments exist). The phone toolchains are in sections 8 and 9.

## 1. Clone and hooks

```bash
git clone --branch main https://github.com/canifishthis/canifishthis.git     # about 2.8 GB with the checkout, 3 minutes
cd canifishthis
git config core.hooksPath .githooks       # the versioned commit-msg and pre-push hooks (`bootstrap.sh` does this too)
```

With a push key (the owner's Mac reaches the personal account through the ssh alias `github.com-personal`), use
`git@github.com-personal:canifishthis/canifishthis.git`; any other machine needs its own ssh key added to GitHub, then
`git@github.com:canifishthis/canifishthis.git`. (2026-11-25: the ssh clone ran at about 100 KB/s on the owner's Mac
and https at 20 MB/s; the repo is public, so clone over https and set the push URL afterwards.)

## 2. Installs (cheap): Python venv and the app's node_modules

```bash
bash scripts/bootstrap.sh --setup-only    # tools check, core.hooksPath, .venv (requirements + constraints), pnpm -C app install --frozen-lockfile; fetches nothing, 35 s
```

Not in a clean shell? Leave any other checkout's venv first (`unset VIRTUAL_ENV`, and take its `.venv/bin` out of
`PATH`): a `python3.13` found there is that checkout's, not this clone's. A worktree of this clone shares the hooks
setting but needs its own installs (T1112), or `PIPELINE_PY` for the pre-push hook.

## 3. Tests

```bash
git switch -c docs/something && git commit …     # commit-msg: Conventional Commits `type(Tnnn): subject`, commitlint from app/node_modules
git push -u origin HEAD                          # pre-push: scripts/before_push.py --changed (only the areas the push touches; a docs push is seconds)
bash scripts/heavy.sh app-suite 30 -- pnpm -C app exec vitest run                                 # the whole app suite (heavy; 5 min)
. scripts/test-env.sh && test_env_pycache; markers=$(no_data_markers)
bash scripts/heavy.sh tier 30 -- bash scripts/pytest-chunked.sh -- -q -m "$markers"                # the pipeline's no-data tier (heavy; 14 min, 94 chunks)
```

`scripts/before_pr.sh --title "<the PR title>"` checks the title with its ` (#N)` suffix, then runs the last two at once on two heavy lanes, and is what to run before opening a PR. Tests that
need a file outside git skip with a message naming it (`data/source/official/` region PDFs for the quota print checks; a
data release for the ones marked `needs_bundle`).

## 4. The web app

```bash
pnpm -C app dev:web          # Expo web on http://localhost:39218 (the phone UI in a browser); needs only Node
```

Proven 2026-11-25 without a data release: the page answers 200 and the web bundle builds (1295 modules, 8 s). What the
app shows with no `data/release/` was not looked at; the map and the waters need a release (section 6, or fetch one from
staging R2). Stop it with Ctrl-C.

## 5. The data: fetch the pinned sources (heavy: about 16 GB)

```bash
bash scripts/bootstrap.sh            # PYTHON=python3.13 by default; the installs of section 2 first, then the fetches
```

`bootstrap.sh` checks the tools (tippecanoe, tile-join, pmtiles, curl, git; rclone and node are needed only to upload
or for the app), makes `.venv`, then fetches:

| what | from | size | how it is checked |
|---|---|---|---|
| `bc_fisheries_data.gpkg` (FWA 2024 + live layers as of the Mac's fetch) | `build.canifishthis.ca/source/<sha16>/` | 10.2 GB | sha256 in `data/source/pins.json` |
| `hydat.sqlite3` (HYDAT 20261019) | same bucket | 1.3 GB | sha256 |
| `fish_obstacles/fish_obstacles.gpkg` | same bucket | 33 MB | sha256 |
| `bc.pmtiles` (basemap, Protomaps 2026-03-05, pruned upstream) | `data.canifishthis.ca/bc.pmtiles` | 4.4 GB | sha256 |
| `basemap.pmtiles` (the RELEASED basemap, basemap-6960ed933a82: the live release's ground) | `build.canifishthis.ca/source/<sha16>/` | 225 MB | sha256; bootstrap then releases it (`pipeline.deliver.tiles.basemap --release-only`) |
| `generated/regs/extraction/{synopsis_raw_data,row_boxes}.json` (the synopsis extraction and its row boxes: the PDF highlight of every rule) | `build.canifishthis.ca/source/<sha16>/` | 0.8 MB | sha256 (the old `extraction/` backup was stale; a release without the row boxes is refused) |
| DFO salmon pages, typed | DFO's live pages (`pipeline.regs.dfo_salmon.live`) | small | today's pages |

Downloads resume (`curl -C -`); rerun the script after a drop. A file whose sha256 differs is deleted and the run fails.
Time: about 7 minutes at ~24 MB/s; a rerun with everything present takes seconds.

Everything else the build reads is in git (curated data, places, stations, the synopsis PDF, municipal layers, DFO
tidal pages, run-timing parse). Map fonts and sprites are fetched by the tiles stage itself, pinned to a commit.

## 6. The data build

```bash
python -m pipeline build --dry-run           # on a fresh clone: "no atlas … run with --atlas --promote first"
bash scripts/heavy.sh first-build 50 -- .venv/bin/python -m pipeline build --atlas --promote --jobs 2
```

| stage | time | peak memory |
|---|---|---|
| atlas (+ parity, promote) | 25m56s | 23.2 GB |
| clim | 24 s | 0.9 GB |
| tiles | 8m54s | 10.7 GB |
| reach | 11m24s | 17.3 GB |
| deliver | 11m52s | 13.8 GB |

The machine needs about 24 GB of free memory for the atlas. About 1h15 in all.

**The basemap is not a build stage**; bootstrap releases the pinned one (above), so the release names the same
ground as the live one. **A release without a basemap is refused** (`basemap_block`: no `basemap.meta.json` beside the
tiles is an error, T416 — it used to ship `basemap: null` silently). To move to a newer OSM ground instead, build it
from `bc.pmtiles`: `python -m pipeline.deliver.tiles.basemap` (then pin it again). After this first build, `python -m pipeline release
--staging` works as in `release.md` (the first release has no live baseline; fetch `data/release/` from R2 or accept the
first binding diff).

## 7. Refreshing the inputs (not part of a rebuild)

The pins are the exact bytes the live release was built from. To move to newer upstream data:

```bash
PYTHONPATH=. .venv/bin/python data/fetch_data.py --missing-only   # or without the flag for everything (~55 min)
.venv/bin/python -m pipeline.tools.source_pins pin data/source/bc_fisheries_data.gpkg --upstream "…"
```

`fetch_data.py` fails (exit 1, listing each layer) when any layer fails. Live layers drift between fetches
(forest_service_roads, water_access_points, waterfalls, historic_sites, land_access, wma), so a refreshed gpkg gives a
slightly different release; that is a data update, reviewed through the binding diff like any other.

## Secrets

None are needed to build. Uploading needs the rclone remote `r2:` (an R2 S3 token) and a wrangler login; Play uploads
need the files under `~/.canifishthis/` (the owner's).

## Not mirrored on purpose

- `data/source/steelhead/` (6.6 GB accessibility model): only `pipeline.regs.steelhead.known_waters` reads it, to
  propose the curated steelhead list; the build reads the curated list in git. It re-fetches from the province.
- `data/source/official/` (region PDFs): nothing reads them.
- `data/source/runtiming/cu_tiles/`: the run-timing workflow fetches its own; the build does not read them.
- `bathymetry_pdfs/`: served from R2 already; the build does not read them.

## 8. Android debug build (heavy; prerequisites written from the scripts, T1120)

| prerequisite | where it is checked or said | how |
|---|---|---|
| JDK | `deploy/staging/build-apk.sh` precheck (`JAVA_HOME`, default Android Studio's JBR) | install Android Studio |
| Android SDK (`ANDROID_HOME`, default `~/Library/Android/sdk`) with platforms, build-tools, and NDK 27.x for the app's Gradle build | the precheck | Android Studio, SDK Manager; Gradle fetches what the app asks |
| NDK 28.2.13676358 + CMake 3.31.6 | `app/tools/maplibre-android/README.md` | `sdkmanager --install "ndk;28.2.13676358" "cmake;3.31.6"` (JAVA_HOME set as in that README) |
| the pinned MapLibre AAR in `cache/maplibre/<MLN_VERSION>/` (git-ignored; built, not fetched) | the precheck, `pin.env` | `bash scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh <work dir outside the repo>` (12 min) |
| `llvm-readelf` | the precheck (under the SDK's `ndk/`) | comes with the NDK |
| `android/app/debug.keystore` (git-ignored) | secrets.md | made by `expo prebuild` (Expo's standard one); a new one means each phone uninstalls the staging app once |
| `app/workers/feeds/.run-token` | `build-apk.sh` emails with it; `--no-email` skips | `deploy/staging/rotate-run-token.sh` (rotates the STAGING worker's token: the owner's) |
| Cloudflare login | uploading builds | `npx --prefix app wrangler login`; `--no-upload` skips |

```bash
bash deploy/staging/build-apk.sh --precheck                       # lists every missing item with its fix, builds nothing
bash deploy/staging/build-apk.sh --no-upload --no-email           # the APK, kept in app/dist/apk/ (heavy.sh wraps it)
```

A build is of COMMITTED HEAD in a temporary worktree; `pnpm install --offline` there reads pnpm's store, which `pnpm -C app
install` filled in section 2. On a machine with no other checkout, `deploy/lib/build-links.sh` cannot offer a copy: make
each missing file with the command in its message.

## 9. iPhone simulator build (heavy)

Prerequisites: Xcode (the App Store; run it once for the licence and the simulator runtime), CocoaPods
(`brew install cocoapods`), Node. No Apple account is needed for the simulator.

```bash
bash deploy/staging/build-ios-sim.sh --dry-run     # prints the steps, builds nothing, needs no account
bash deploy/staging/build-ios-sim.sh               # heavy.sh wraps it; app/dist/ios-sim/canifishthis.app
```

The TestFlight build (`build-ios-testflight.sh --precheck` lists what it needs) is the owner's: it needs the App Store
Connect key files in `~/.canifishthis/` (secrets.md).

## 10. Deploys and secrets

Every deploy script (`deploy/staging/*.sh`, `deploy/prod/*.sh`) needs a login or key that is not in git:
`handoff/PLAN/runbooks/secrets.md` names each one, what it does and how to replace it. Production steps need
`CONFIRM_PROD=canifishthis.ca` typed by the owner. A `--dry-run` of a deploy script that reads production or a login is
not part of a setup check.
