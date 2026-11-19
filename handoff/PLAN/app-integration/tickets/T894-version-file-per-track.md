# T894 — The app's version files per track and per version (after 0.8.2)

Status: IN PROGRESS (app chat; lane app/t894-version-files; in 0.8.2: owner 2026-11-19 ~00:40 "we want all the in flight tickets in 0.8.2 and small bugs found").

From the second independent review of T888 (2026-11-19 ~00:27):
1. Medium, Play: play.json names whatever was uploaded last, on any track (appUpdater.native.ts ~L89). With 0.8.3 in
   closed testing and 0.8.2 offered to a production user, the popup says "Version 0.8.3 is out", Update installs 0.8.2,
   and `popped=0.8.3` stops the real 0.8.3 from popping. Fix: compare play.json's `build` with the version code Play
   offers (if the in-app update module reports it), or write the file per track.
2. Low, iPhone: production appstore.json is written at the TestFlight upload; a newer upload ahead of the store's version
   leaves store users with "no notes published". Fix: one file per version (`app/ios/<ver>.json`).
3. Low, tests: the App Store port's version-equality guard; the iOS dry-run's bucket (staging by default,
   bc-fishing-regulations with --production).
