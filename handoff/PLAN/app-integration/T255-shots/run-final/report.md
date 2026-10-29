# Test bed run 2026-10-29T13-00-11

emulator-5580 · sdk_gphone64_arm64 · Android 15 · APK app/tools/testbed/out/app-testbed.apk (release, staging, channel test, env switch on) · channel test (staging test/)

| scenario | result | steps | time |
|---|---|---|---|
| install — First run downloads the release and opens the map | FAIL | 2/3 | 920 s |
| feeds — New feeds in R2 are detected, downloaded and shown | PASS | 8/8 | 243 s |
| big-update — A big release update: Wi-Fi hold, download, apply; app-too-old; corrupt | FAIL | 11/13 | 595 s |
| offline — Offline launch, offline mid-session, slow line, reconnect | PASS | 14/14 | 437 s |
| spots — Spots offline: create with a photo, persist, fill in on reconnect | PASS | 14/14 | 300 s |
| report — Report a problem offline → queued → sent on reconnect | PASS | 6/6 | 86 s |

## install — First run downloads the release and opens the map: FAIL

1. PASS test channel reset to the live release — test/ = r-0fa7927b16e5
2. PASS clean app on Wi-Fi ([shot](001-install-clean-app-on-wi-fi.jpg))
3. FAIL first-run download, then the map — **no text /^More$/ on screen in 900 s (saw: At a glance | Thu, Oct 29 | Thu, Oct 29 | Gmail | Gmail | Photos | Photos | YouTube | YouTube | Home | Phone | Phone); APP CRASHED: FATAL EXCEPTION: expo.modules.AsyncFunctionQueue / Process: ca.canifishthis.app, PID: 3281 / java.lang.OutOfMemoryError: Failed to allocate a 8388627 byte allocation with 965872 free bytes and 943KB until OOM, target footprint 201326592, growth limit 201326592 / at dalvik.system.VMRuntime.newNonMovableArray(Native Method)** ([shot](002-install-first-run-download-then-the-map.jpg))

Findings:
- crash in "first-run download, then the map": FATAL EXCEPTION: expo.modules.AsyncFunctionQueue / Process: ca.canifishthis.app, PID: 3281 / java.lang.OutOfMemoryError: Failed to allocate a 8388627 byte allocation with 965872 free bytes and 943KB until OOM, target footprint 201326592, growth limit 201326592 / at dalvik.system.VMRuntime.newNonMovableArray(Native Method)

## feeds — New feeds in R2 are detected, downloaded and shown: PASS

1. PASS test channel reset; app up on Wi-Fi ([shot](030-feeds-test-channel-reset-app-up-on-wi-fi.jpg))
2. PASS Check now: every feed unchanged (304) — Gauges: ok · DFO salmon: ok · In-season: ok · Release: ok ([shot](031-feeds-check-now-every-feed-unchanged-304-.jpg))
3. PASS gauges bumped in R2 → detected on return to the foreground (no tap) — 06:47:13, Gauges, changed, 200 · 272.5 kB · 1325 ms, 24525c56 → dda8b09e ([shot](032-feeds-gauges-bumped-in-r2-detected-on-return-t.jpg))
4. PASS DFO + in-season bumped → Check now downloads both — 06:47:24, In-season, changed, 200 · 16.1 kB · 380 ms · manual, b911cbfb → 339de21b ([shot](033-feeds-dfo-in-season-bumped-check-now-downloads.jpg))
5. PASS sync button: up to date after the check — Refresh data: up to date ([shot](034-feeds-sync-button-up-to-date-after-the-check.jpg))
6. PASS DFO index removed (404) → sync error badge + bell — Refresh data: could not update the DFO salmon feed · bell: 1 update failed ([shot](035-feeds-dfo-index-removed-404-sync-error-badge-b.jpg))
7. PASS the bell opens what failed, with Retry — DFO salmon: The server did not answer. Last succeeded just now. ([shot](036-feeds-the-bell-opens-what-failed-with-retry.jpg))
8. PASS feed back → Retry clears the error — Refresh data: up to date ([shot](037-feeds-feed-back-retry-clears-the-error.jpg))

## big-update — A big release update: Wi-Fi hold, download, apply; app-too-old; corrupt: FAIL

1. PASS test channel reset; app up on Wi-Fi, current ([shot](108-big-update-test-channel-reset-app-up-on-wi-fi-curre.jpg))
2. PASS a BIG release in R2; the phone on mobile data (metered) — r-0fa7927b16e5-big69017 · network MOBILE ([shot](109-big-update-a-big-release-in-r2-the-phone-on-mobile-.jpg))
3. PASS Check now → "waiting for Wi-Fi" (nothing downloaded) — Release: ok · waiting-wifi r-0fa7927b16e5-big69017 ([shot](110-big-update-check-now-waiting-for-wi-fi-nothing-down.jpg))
4. PASS top bar says it: the sync button — Refresh data: an update is waiting for Wi-Fi ([shot](111-big-update-top-bar-says-it-the-sync-button.jpg))
5. FAIL the bell lists the update waiting — **no bell entry for an update waiting for Wi-Fi (the bell lists in-season changes and failed updates only) — ticket T263** ([shot](112-big-update-the-bell-lists-the-update-waiting.jpg))
6. PASS More → Data says it — Updates: An update is waiting for Wi-Fi. ([shot](113-big-update-more-data-says-it.jpg))
7. FAIL Wi-Fi back → the download starts by itself, with progress — **no text /^Refresh data: (downloading an update, \d+%|an update is downloaded)/ on screen in 120 s — still held for Wi-Fi: ticket T265** ([shot](114-big-update-wi-fi-back-the-download-starts-by-itself.jpg))
8. PASS (T265 until fixed) Check now on Wi-Fi → it downloads and is staged — 07:29:14, Release, staged, 274.5 MB · 39651 ms, r-0fa7927b16e5-big68225 → r-0fa7927b16e5-big69017 ([shot](115-big-update--t265-until-fixed-check-now-on-wi-fi-it-.jpg))
9. PASS next pause (background → foreground) → applied; the new release in use — no toast seen · data staging · data-staging.canifishthis.ca · channel test · release r-0fa7927b16e5-big69017 ([shot](116-big-update-next-pause-background-foreground-applied.jpg))
10. PASS a release that needs app 99.0.0 → "Update the app", data kept — Release: ok · app-too-old r-0fa7927b16e5-needs-99.0.0 (needs 99.0.0) ([shot](117-big-update-a-release-that-needs-app-99-0-0-update-t.jpg))
11. PASS More says "Update the app to get new regulations" — Updates: Update the app to get new regulations. ([shot](118-big-update-more-says-update-the-app-to-get-new-regu.jpg))
12. PASS a corrupt release → sha-mismatch, the set in use kept — 07:31:42, Release, failed, 0 B · 2027 ms · stage, r-0fa7927b16e5-big69017 → r-0fa7927b16e5-corrupt, sha-mismatch: https://data-staging.canifishthis.ca/release/r-0fa7927b16e5/paint-f27598e208c2.bin.gz: sha256 f27598e208c2…, the manifest says 000000000000… ([shot](119-big-update-a-corrupt-release-sha-mismatch-the-set-i.jpg))
13. PASS reset: back to the live release (its basemap id differs again: a second big download, on Wi-Fi) — Gauges: ok · DFO salmon: ok · In-season: ok · Release: ok ([shot](120-big-update-reset-back-to-the-live-release-its-basem.jpg))

## offline — Offline launch, offline mid-session, slow line, reconnect: PASS

1. PASS airplane mode, then a cold start: the map opens from the device — none ([shot](148-offline-airplane-mode-then-a-cold-start-the-map-.jpg))
2. PASS the offline banner is shown — Offline — showing what’s saved on this device ([shot](149-offline-the-offline-banner-is-shown.jpg))
3. PASS search works offline ("Cultus") — Cultus Lake ([shot](150-offline-search-works-offline-cultus-.jpg))
4. PASS a water's regulations open offline — ✓Open ([shot](151-offline-a-water-s-regulations-open-offline.jpg))
5. PASS a town offline: the waters near it (read from the installed set) — listed ([shot](152-offline-a-town-offline-the-waters-near-it-read-f.jpg))
6. PASS back to the water: Conditions view ([shot](153-offline-back-to-the-water-conditions-view.jpg))
7. PASS Conditions offline: last captured readings or "not available offline", never a spinner — No gauge on this water is close enough in size to speak for it. ([shot](154-offline-conditions-offline-last-captured-reading.jpg))
8. PASS online again: the banner goes and the service checks at once — 07:50:23, Gauges, changed, 200 · 270.1 kB · 455 ms, 3f00a22c → 04610e74 ([shot](155-offline-online-again-the-banner-goes-and-the-ser.jpg))
9. PASS the same town online: the waters near it — listed ([shot](156-offline-the-same-town-online-the-waters-near-it.jpg))
10. PASS mid-session: offline while using the app → the banner, the app keeps working ([shot](157-offline-mid-session-offline-while-using-the-app-.jpg))
11. PASS online-only part offline: satellite imagery says so — Satellite needs a connection. Showing the map until you are back online; satellite returns by itself. ([shot](158-offline-online-only-part-offline-satellite-image.jpg))
12. PASS Check now offline: "offline", nothing requested — Gauges: failed (offline) · DFO salmon: failed (offline) · In-season: failed (offline) · Release: failed (offline) ([shot](159-offline-check-now-offline-offline-nothing-reques.jpg))
13. PASS a slow line (EDGE speed, GPRS latency) → slow banner or "still loading" — nothing said within 2 s (the installed set answers before the line matters) ([shot](160-offline-a-slow-line-edge-speed-gprs-latency-slow.jpg))
14. PASS back to a full line: no error left on screen — Refresh data: up to date ([shot](161-offline-back-to-a-full-line-no-error-left-on-scr.jpg))

## spots — Spots offline: create with a photo, persist, fill in on reconnect: PASS

1. PASS setup: local sync adapter, a photo in the gallery, GPS on Cultus Lake
2. PASS offline (airplane mode) ([shot](135-spots-offline-airplane-mode-.jpg))
3. PASS Spots → At my location → the capture, offline ([shot](136-spots-spots-at-my-location-the-capture-offline.jpg))
4. PASS add a photo from the gallery (the system photo picker) — PHOTOGRAPHS ([shot](137-spots-add-a-photo-from-the-gallery-the-system-.jpg))
5. PASS name it "Testbed njv3u" and save ([shot](138-spots-name-it-testbed-njv3u-and-save.jpg))
6. PASS it is stored (adapter: expectStored) — Testbed njv3u ([shot](139-spots-it-is-stored-adapter-expectstored-.jpg))
7. PASS it says what it could not record offline (adapter: expectPending) — No weather was recorded — it can be filled in from the day this was pinned. ([shot](140-spots-it-says-what-it-could-not-record-offline.jpg))
8. PASS cold start, still offline: the spot and its photo are still there — listed with its photo · Testbed njv3u ([shot](141-spots-cold-start-still-offline-the-spot-and-it.jpg))
9. PASS edit offline: notes typed (keyboard up: is "Save changes" above it?) — "Save changes" visible above the keyboard ([shot](142-spots-edit-offline-notes-typed-keyboard-up-is-.jpg))
10. PASS system back closes the keyboard only (the edit stays) ([shot](143-spots-system-back-closes-the-keyboard-only-the.jpg))
11. PASS Save changes; cold start offline; the notes are kept — kept: "edited offline by the test bed" ([shot](144-spots-save-changes-cold-start-offline-the-note.jpg))
12. PASS back online → its weather fills in by itself (adapter: expectSynced) — filled in: nothing missing ([shot](145-spots-back-online-its-weather-fills-in-by-itse.jpg))
13. PASS the spot after reconnect — No gauge is entitled to speak for this water, so no reading was recorded. A station draining a far larger watershed would have given a number, and the number would have been wrong. · HOW THIS SPOT REACHES THE GAUGE · AT THE GAUGE · WEATHER ([shot](146-spots-the-spot-after-reconnect.jpg))
14. PASS clean up: delete the spot ([shot](147-spots-clean-up-delete-the-spot.jpg))

## report — Report a problem offline → queued → sent on reconnect: PASS

1. PASS offline, app up ([shot](076-report-offline-app-up.jpg))
2. PASS send a report offline → "saved on this device" — You are offline. Your report is saved on this device and will be sent when you are back online. ([shot](077-report-send-a-report-offline-saved-on-this-devi.jpg))
3. PASS not on the server yet — absent, as it should be ([shot](078-report-not-on-the-server-yet.jpg))
4. PASS closed and reopened offline (the outbox is on disk) ([shot](079-report-closed-and-reopened-offline-the-outbox-i.jpg))
5. PASS online → flushed by itself; the worker holds it — stored 58e8e30b56bb30a8 2026-10-29T14:07:55.579Z ([shot](080-report-online-flushed-by-itself-the-worker-hold.jpg))
6. PASS back to the map ([shot](081-report-back-to-the-map.jpg))
