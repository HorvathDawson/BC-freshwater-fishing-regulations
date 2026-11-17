# T814 — iOS: after the app is closed and reopened mid-download, the fish spinner stops moving

Status: OPEN (app, iOS; app chat)

Source: owner 2026-11-16 ~21:22, testing iPhone TestFlight build 36 (app chat; pictures in the chat). Verbatim: "a-d worked good except after closing app and opening again the fish icon stopped moving....".
The first-install checks (a) lock, (b) switch apps, (c) swipe away and reopen, (d) a steady bar all passed; only the
swimming fish (`fish-sprite.generated.ts`, the download screen's spinner) stood still after the reopen.

## Look at
The spinner's animation start on a cold start that resumes a download (the bar already part-way): an animation started
before the view is laid out, a JS-driven loop paused by the app state, or the T746 "applying" screen's swap. Reproduce on
the iOS simulator (`deploy/staging/build-ios-sim.sh`): start the first download, kill the app, reopen.
