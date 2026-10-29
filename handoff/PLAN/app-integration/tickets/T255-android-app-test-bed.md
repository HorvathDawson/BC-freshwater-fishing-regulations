# T255 — Android app test bed: downloads, sync and offline (Maestro on the emulator)

Status: DONE — 657f44e33, d37f77f29, 47a8c3449, 43d22b03c, b700a3608, b6ac5f2af (+ this close)

Source: owner 2026-10-29 — a reusable APP TEST BED in the Android emulator (reuse T50's AVD/tooling) proving the app
behaves with downloads, sync and offline: new feeds, a bigger update (Wi-Fi/metered, update needed), offline launch /
mid-session / reconnect, spots offline (photos, captured weather/gauge context, refresh on reconnect; a sync-adapter
seam for T223), report-a-problem outbox (T237).

## Built — `app/tools/testbed/` (README there: how to run, how T223 adds scenarios)
- **Runner** `run.mjs`: scenarios of steps; each step = host work (adb network, the staging test channel) and/or a
  Maestro flow, then a screenshot + the screen's words, pass/fail; the crash buffer read after every step (a crash
  fails it); report `out/runs/<stamp>/` (index.html, report.md, report.json); batches can append to one report.
- **Heavy batch** `session.sh` (only via `scripts/heavy.sh`, BOUNDED: `--max-min`, never an idle hold): APK build
  (release, staging, channel test, env switch, arm64; the JS bundle forced fresh), `n1_pixel` headless on :5580,
  install, the command, shutdown of its own emulator + Gradle daemon.
- **Maestro 2.12.0** (~/.maestro; YAML flows in `flows/`, RN testID = `id:`), **adb network**: Wi-Fi / mobile data
  (metered) / airplane with radios off; emulator link speed/latency; GPS fix; a gallery photo.
- **Scenarios**: install, feeds, big-update, offline, spots, report. `deploy/staging/test-updates.mjs big` added (the
  basemap id moves: a real ~270 MB download over the metered line; `test/` only).
- **Sync seam** `sync/`: adapter contract (expectStored / expectPending / expectSynced), `local` (today: device-local
  spots), `account` + `fake-server.mjs` (v0 API proposal; SKIPs until T223 has accounts).
- `explore.mjs`: a tour saving every screen's words + shot, for writing flows.

## Ran (2026-10-29; report + shots `../T255-shots/run-final/`)
| scenario | result |
|---|---|
| install | FAIL — first install dies of OutOfMemoryError in expo fetch, crash-loops and resumes → **T262** |
| feeds | PASS 8/8 — bump seen on foreground return (no tap) + Check now; 404 → red sync badge + bell "1 update failed" → Retry clears |
| big-update | FAIL 11/13 — waiting-for-Wi-Fi on mobile data (sync label, More, log) ✓; bell ✗ **T263**; Wi-Fi back does not start it ✗ **T265** (Check now does: 274.5 MB in 40–65 s, staged, applied); minAppVersion 99 → "Update the app" ✓; corrupt → sha-mismatch, set kept ✓ |
| offline | PASS 14/14 — cold start in airplane mode, banner, search, regs, town near, Conditions, satellite "needs a connection", Check now = offline, reconnect clears banner + checks |
| spots | PASS 14/14 — At my location offline + gallery photo + name; "No weather was recorded"; survives cold start; notes edited offline kept; back online the weather fills in by itself |
| report | PASS 6/6 — queued offline, survives restart, flushed on reconnect, on the staging worker (`GET /reports`) |

## App bugs found
Fixed here: (1) spots made offline never filled in on reconnect — `useRefreshOnReconnect` (ui/spots.ts, settle delay +
retries); (2) the cause it exposed: `netInfoConnectivity` shared one `last` word across subscribers, so only the FIRST
subscriber ever heard offline → online (every later one — imagery, the spots, anything subscribing after the update
service — was deaf). Both render/unit-tested; verified on the emulator.
Ticketed: T262 (first-install OOM crash-loop + the failed first-run screen's dead end, dev hint shown), T263 (bell has
no release notices), T264 (intermittent "Could not read what is near this town", first read after a cold start),
T265 (held big update not started on Wi-Fi return).
Not judged: the slow-line step (EDGE) shows nothing within 2 s because the installed set answers locally — a web
concern (T254's CDP harness covers it).

Depends: T50, T251, T237. Size: L. Owner: no.
