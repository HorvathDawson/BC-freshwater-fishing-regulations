# App test bed (T255): downloads, sync and offline on the Android emulator

The real app (a release APK reading **staging** on the T251 **test channel**) on the `n1_pixel` emulator, driven by
[Maestro](https://maestro.mobile.dev) flows (YAML, no app code), with the network switched by adb and the data origin
moved by `deploy/staging/test-updates.mjs` (writes `test/` in the STAGING bucket only). Every step is screenshotted
and recorded; the report is `out/runs/<stamp>/index.html` (plus `report.md`, `report.json`).

## Scenarios

The main ones below; every scenario (perf, pan, gesture and pane probes too) is in `scenarios/index.mjs`, in run order.

| id | what it proves |
|---|---|
| `install` | a clean app downloads the whole release on Wi-Fi and opens the map; every source checked on start |
| `feeds` | a feed bumped in R2 is seen on a foreground return (no tap) and on Check now; the Updates log rows and the sync button say so; a 404 feed badges the sync button and the bell; Retry clears it |
| `big-update` | a release over the metered line (basemap id moved, ~270 MB) waits for Wi-Fi on mobile data (sync button, More, the bell), downloads on Wi-Fi with progress, is applied at the next pause ("Updated to …"); `minAppVersion 99` → "Update the app"; a corrupt release → `sha-mismatch`, the set in use kept |
| `offline` | cold start in airplane mode on the installed set (map, search, a water's regulations, Conditions); the banner; offline mid-session; satellite says it is online-only; Check now offline is `offline`; a slow line; reconnect clears the banner and checks at once |
| `spots` | a spot made offline (At my location) with a photo is saved, survives a cold start offline, says what it is missing; back online it fills in its weather/gauge context by itself; an edit made offline is kept |
| `saved-spots` | (T603) a spot saved through the UI on a gauged river (the Fraser at Hope, online, the day before) shows everything a saved spot holds: the Spots row and its lines, the visit chip and rating stars, the week-before strip (weather columns, the gauge's daily means with their short "N in 10 days higher" place, the trend line); then the alert editor on a gauge card (its chips, the five level-for-the-day presets and Custom, the one line saying what will happen), one alert saved, read whole in More → Notifications and removed. Fails on a label that is missing, off the screen, past its chip or squeezed (`SAVED_AT`, `ALERT_Q` / `ALERT_ROW`) |
| `two-days` | (T603, T582) the regulations day set on the map's day strip to today, tomorrow and 11 days on: Conditions says nothing / "Regulations day: <day>, marked on the chart." / "<day> is past the forecast." (the whole sentence, B.C.'s day), and the chart carries the regulations-day mark only inside the window |
| `route-scroll` | (T603) the Conditions route map after ten flings down the pane and back: photographed the moment it returns and again at rest; fails if it is one flat colour at rest (the blank map seen once on build 25), a finding if only the first frame was (`ROUTE_ROUNDS`) |
| `report` | Report a problem offline → "saved on this device"; survives a restart; flushed on reconnect; the staging feeds worker holds it (`GET /reports`, read only) |
| `near-cold` | (T264) ten cold starts (offline/online alternating), a town tapped FIRST: its waters are listed every time and no failed near read is logged (`NEAR_COLD_RUNS=n`) |
| `part-steps` | (T411) Coquitlam → Map → ▼ to Part 1 → ▲ once: no section of the water drawn by the water copy AND the atlas at once (`[copydraw]`); each kept section's `copy_until` in `T411-copy-log.txt` (`STEPS_VIA=tap` for the map-tap way in) |
| `photo-zoom` | (T454) a spot with two photos: the viewer double-tap zooms in, a drag pans, a sideways swipe while zoomed keeps the photo, a pinch zooms, double-tap returns to the fit, a swipe at the fit changes the photo; screenshots + a 30 s recording in `out/t454/` |
| `crash-bed` | (T242) an unhandled promise rejection, an uncaught JS error (fatal: kept, sent at the next start, its exit record not sent again) and a native crash (SIGSEGV via `adb root`, Android's CRASH_NATIVE record) each reach the feeds worker's OWN `POST /crash` handler, run locally by `crash-stub/server.mjs` over an in-memory bucket. Needs `session.sh --crash-stub`; run with `run.mjs --no-r2`. Nothing goes to staging |

Stress knobs (T420): `TESTBED_EMU_ECORES=1` runs the emulator on the Mac's efficiency cores (`taskpolicy -b`), `TESTBED_EMU_ARGS="-cores 2"`
adds emulator flags; `pan` takes `PAN_AT=lon,lat,zoom`, `PAN_COLD=1` (page cache dropped first), `PAN_MS` (120 = a fling) and
reports the pan probe's `[drawn]` lines (frames missing tiles while moving; camera at rest → first fully drawn frame).
`run.mjs --no-r2` writes nothing to the staging bucket (no test-channel reset at the end); `install` then only checks that
test/ already names the live release and stops if not (T442: a stale test/ names cleared files, so a fresh install 404s).

`install` runs first and leaves the app holding the current staging release; the others build on it (none clears the
app again: a fresh install is ~500 MB).

## Last run (2026-11-09, build 22 code, r-d14bdb180f55 on the test channel)

Reports under `out/runs/` (gitignored, `index.html` + `report.md` each): `2026-11-09T22-54-43` install PASS 4/4 (first
run downloaded and up in 84 s), offline 13/15 (the two flow steps "Check now offline" / "slow line"); after the flow fix
(e816e82f3) `2026-11-09T23-09-33` offline PASS 15/15, feeds 7/8 (Retry step: the app had re-checked on its own, so no
Retry button was left); with the Retry step race-tolerant `2026-11-10T00-09-58` feeds PASS 8/8; big-update 12/13 until its bell step opened the bell (T263's Data update row), then PASS 13/13 (next run). Not rerun since 2026-10-29: spots, report. A step tagged with a ticket is a known app gap: it turns green when that ticket
lands.

## The build under test is named (T534)

`session.sh` writes `out/<apk>.stamp.json` beside every APK it builds: commit, whether `app/` was dirty, and the
`libmaplibre.so` build id (read from the APK; checked against the patched AAR's when patched). A kept APK is refused
**before the heavy lock** if its commit is not HEAD, its tree was dirty, or it has no stamp, unless
`--allow-stale "<reason>"` (printed). The default build carries the patched MapLibre (T420 lean AAR in
`cache/maplibre/fslean-m2`, as `deploy/staging/build-apk.sh`); `--stock-maplibre="<reason>"` builds the stock SDK under
its own name (`app-testbed-stock.apk`). `run.mjs` prints the stamp at the top of the console and of `report.md` /
`index.html` ("Build: commit … · libmaplibre build id … (patched, T420 lean)"): quote that line in the STATUS entry.
Existing APKs built before T534 have no stamp: rebuild with `--build`. The build still reads the live checkout (the
stamp says if it was dirty); a clean-worktree build like `build-apk.sh` is not done here.

## Run it locally

One-time: Android SDK + AVD `n1_pixel` (T50/T172), Android Studio's JBR, `rclone` remote `r2:` (staging), Maestro:

```bash
curl -sSL -o /tmp/maestro.zip https://github.com/mobile-dev-inc/maestro/releases/latest/download/maestro.zip
mkdir -p ~/.maestro && unzip -qo /tmp/maestro.zip -d ~/.maestro     # → ~/.maestro/maestro/bin/maestro (2.12.0 used)
```

The emulator and Gradle are HEAVY: always through `scripts/heavy.sh`, **one bounded batch per job** (the lock is
released between batches; never hold the emulator idle):

```bash
# build the APK (release, staging, channel test, env switch; arm64) + the first-run install
scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build -- node app/tools/testbed/run.mjs install
# then batches, e.g.
scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh -- node app/tools/testbed/run.mjs feeds report
scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --max-min 60 -- node app/tools/testbed/run.mjs big-update
scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh -- node app/tools/testbed/run.mjs offline spots
```

`session.sh` boots `n1_pixel` headless on its own port (5580), installs `out/app-testbed.apk`, runs the command under a
wall-clock cap (`--max-min`, default 45), then shuts down its own emulator and Gradle daemon. The emulator's data
persists between batches (`-no-snapshot-save` keeps the userdata image), so the release installed by `install` stays.
`run.mjs` resets the test channel at the end (`test-updates.mjs reset`). Several batches can write ONE report:
pass the same `--out <dir>` to each (a scenario run again replaces its earlier result).

Writing a flow: `node app/tools/testbed/explore.mjs` (inside a session) saves a screenshot and every word of each
screen to `out/explore/`; RN `testID` is Maestro's `id:`; `maestro --device emulator-5580 studio` is the interactive
inspector (inside a session).

## Pieces

- `session.sh` — the heavy batch: APK build, emulator, install, the capped command, shutdown.
- `run.mjs` — the runner: `ctx.step` (run → screenshot → record), `ctx.flow` (Maestro), `ctx.waitText` (screen words,
  uiautomator, falling back to `maestro hierarchy` while a spinner turns), `ctx.updates` (the test channel), `ctx.sync`
  (the sync adapter).
- `lib/device.mjs` — adb: `net('wifi' | 'cellular' | 'offline')` (Wi-Fi + data / data only = METERED / airplane mode
  with both radios off — Android keeps Wi-Fi up in airplane mode once it was turned on during one),
  `link(speed, delay)` (emulator console), launch/stop/background, screenshots, `addPhoto`, `geo`.
- `lib/screen.mjs` (T603) — the screen as nodes (uiautomator): where an element is, whether it is drawn, whether a label
  fits its chip or is squeezed (`labelProblem`, `chipProblem`), `scrollTo` an id (dragging in the page's gutter: a drag
  that starts on a map pans the map), `blankShare` (is a map's box one flat colour), `bcDay`. A selector is written
  `resource-id="<testID>"` so `app/tools/testbed-static.test.ts` checks the id against the app source; an id the app
  builds from a variable is its fixed part plus `{ ends }`. Unit tests: `node --test app/tools/testbed/lib/*.test.mjs`.
- `lib/maestro.mjs` (each flow's Maestro log under `out/<run>/maestro/<scenario>/`), `lib/report.mjs`, `flows/*.yaml`, `scenarios/*.mjs`, `media/` (the test photo).
- `deploy/staging/test-updates.mjs big` (added here): the large release.

## Accounts (T223): adding sync scenarios

Spots are device-local today. The spot scenario never asks "is it on the device?" directly — it asks the **sync
adapter** (`sync/adapter.mjs`): `expectStored`, `expectPending`, `expectSynced`. `--sync local` (default) answers from
the device (the Spots list across a cold start, the "missing" line gone after reconnect). `--sync account` starts
`sync/fake-server.mjs` on the host (the emulator reaches it at `10.0.2.2:39255`, and `adb reverse` makes it
`localhost:39255` on the device) and answers from the server's state; until the app has accounts it SKIPs.

When T223 lands:
1. Point the app's sync client at the fake server for the test bed (`EXPO_PUBLIC_ACCOUNT_URL=http://localhost:39255`
   in `session.sh`'s build line) and keep its API and `fake-server.mjs`'s in step (the file states v0 of it).
2. Add a `sign-in` flow; call it from `accountAdapter.setup`; drop the `notYet()` guard.
3. Run `run.mjs spots --sync account`: the same steps now prove the spot (and its photo) reached the server only after
   reconnect. New account-only cases (two devices, a conflict → 409, server outage via `POST /__testbed/down`) go in
   a new `scenarios/account-*.mjs` added to `scenarios/index.mjs`.
