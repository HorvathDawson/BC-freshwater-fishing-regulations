# App test bed: the real app on the Android emulator

The real app (a release APK reading staging on the `test` update channel) on the `n1_pixel` emulator, driven by
[Maestro](https://maestro.mobile.dev) flows (YAML, no app code). The network is switched by adb and the data
origin moved by `deploy/staging/test-updates.mjs` (writes `test/` in the staging bucket only). Every step is
screenshotted and recorded; the report is `out/runs/<stamp>/index.html` (plus `report.md`, `report.json`).

## Scenarios

Every scenario, in run order, is in `scenarios/index.mjs`; each file's header says what it proves and its knobs
(environment variables such as `NEAR_COLD_RUNS`, `PINCH_ROUNDS`, `BACK_CASES`). By group:

| group | scenarios |
|---|---|
| install and data | `install` (first, leaves the app holding the current staging release; the rest build on it), `feeds`, `big-update`, `bg-update`, `bg-install`, `update-freeze`, `retire-swap`, `old-app-gate` |
| offline and sync | `offline`, `spots`, `saved-spots`, `report`, `gauge-offline`, `gauge-level` |
| crashes and notifications | `crash`, `crash-bed` (needs `session.sh --crash-stub`, run with `--no-r2`), `notify-fire`, `push-absent` |
| map and gestures | `pan`, `gestures`, `glide`, `zoom-flick`, `zoom-pinch`, `satellite`, `photo-zoom`, `part-steps`, `route-scroll` |
| water page and navigation | `regs-open`, `regs-switch`, `water-nav`, `back-flash`, `two-days`, `start-land`, `mode-switch`, `trout-table` |
| performance and memory | `perf`, `near-cold`, `tap-stalls`, `memory`, `heapprof`, `open-race`, `pane` |

Gesture scenarios send
raw touch events as root (`adb root`, put back after). `pan` takes `PAN_AT=lon,lat,zoom`, `PAN_COLD=1`, `PAN_MS`.

`run.mjs [all | <id>...] [--sync local|account] [--keep-going] [--out <dir>] [--no-r2]`: `--no-r2` writes nothing to
the staging bucket (no test-channel reset at the end), and `install` then only checks that `test/` already names the
live release and stops if not. No scenario clears the app's data again: a fresh install is about 500 MB.

## Run it locally

One-time: the Android SDK, AVD `n1_pixel`, Android Studio's JBR, an `rclone` remote `r2:` (staging) and Maestro:

```bash
curl -sSL -o /tmp/maestro.zip https://github.com/mobile-dev-inc/maestro/releases/latest/download/maestro.zip
mkdir -p ~/.maestro && unzip -qo /tmp/maestro.zip -d ~/.maestro     # ~/.maestro/maestro/bin/maestro
```

The emulator and Gradle are heavy: always through `scripts/heavy.sh`, one bounded batch per job.

```bash
scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build -- node app/tools/testbed/run.mjs install
scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh -- node app/tools/testbed/run.mjs feeds report
```

`session.sh` boots `n1_pixel` headless on port 5580, installs `out/app-testbed.apk`, runs the command under a
wall-clock cap (`--max-min`, default 45), then shuts down its own emulator and Gradle daemon. The emulator's data
persists between batches, so the release installed by `install` stays. Several batches can write one report: pass the
same `--out <dir>` to each.

- Parallel lanes need their own AVD and console port (`TESTBED_AVD=n1_pixel_b TESTBED_PORT=5584`): the heavy lock
  covers the machine, not the emulator, and a second lane on the default port would reinstall the app under the first.
  `bg-install` belongs on `n1_pixel` (the second AVD does not reach mobile data or the bench proxy).
- R8 is on in every test-bed APK, as in every shipped build; `session.sh --no-r8="<reason>"` builds a comparison
  APK (`app-testbed-nor8.apk`).
- Writing a flow: `node app/tools/testbed/explore.mjs` (inside a session) saves a screenshot and every word of each
  screen to `out/explore/`; an RN `testID` is Maestro's `id:`.

## The build under test is named

`session.sh` writes `out/<apk>.stamp.json` beside every APK: the commit, whether `app/` was dirty, and the
`libmaplibre.so` build id. A kept APK is refused before the heavy lock if its commit is not HEAD, its tree was dirty
or it has no stamp, unless `--allow-stale "<reason>"`. The default build carries the pinned upstream MapLibre
(`app/tools/maplibre-android/README.md`); an experiment AAR needs `MLN_LOCAL_MAVEN=<repo> TESTBED_APK_TAG=<tag>` and
is stamped LOCAL; `--stock-maplibre="<reason>"` builds the stock SDK as `app-testbed-stock.apk`. `run.mjs` prints the
stamp at the top of the console and of `report.md`: quote that line in the STATUS entry.

## Pieces

- `session.sh`: the heavy batch (APK build, emulator, install, the capped command, shutdown).
- `run.mjs`: the runner: `ctx.step` (run, screenshot, record), `ctx.flow` (Maestro), `ctx.waitText`, `ctx.updates`
  (the test channel), `ctx.sync` (the sync adapter).
- `lib/device.mjs`: adb (`net('wifi'|'cellular'|'offline')`, `link(speed, delay)`, launch/stop, screenshots, photos,
  geo). `lib/screen.mjs`: the screen as uiautomator nodes (where an element is, whether a label fits its chip,
  `scrollTo`, `blankShare`); a selector is written `resource-id="<testID>"` so `app/tools/testbed-static.test.ts`
  checks the id against the app source. Unit tests: `node --test app/tools/testbed/lib/*.test.mjs`.

## Accounts

Spots are device-local today. The `spots` scenario asks the sync adapter (`sync/adapter.mjs`: `expectStored`,
`expectPending`, `expectSynced`). `--sync local` answers from the device; `--sync account` starts
`sync/fake-server.mjs` on the host (the emulator reaches it at `10.0.2.2:39255`) and SKIPs until the app has
accounts. Then: point the app's sync client at the fake server in `session.sh`'s build line, add a `sign-in` flow
called from `accountAdapter.setup`, and add account-only cases as `scenarios/account-*.mjs`.
