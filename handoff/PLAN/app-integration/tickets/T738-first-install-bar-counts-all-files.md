# T738 — The first-install bar counts every file that is downloading, not one at a time

Status: OPEN (after closed testing starts; shared code, so it changes Android's bar too: owner pick first).

Source: T722's simulator proof, 2026-11-14 (branch `ios/t722-install`, run on build 347281f96). After the app was
force-closed at 104.3 MB on disk and reopened, the bar read "38.8 of 479.3 MB" while 116.3 MB were on disk. An earlier
run showed 27.3 MB with about 180 MB down. Shot: sent to the owner 2026-11-14 (`proof4/relaunch-4s.png` in the iOS
chat's scratchpad).

## Cause (read in the code by the T722 lane)
The system downloads the four big files at once (tiles, mask, basemap, rule book), on Android (DownloadManager) and, on
the T722 branch, on iOS (a background URLSession). The first-install progress adds the files that are finished plus only
the ONE file whose turn it is; bytes already down for the other three do not show until their turn. So the bar reads low
for most of the download and jumps when a turn changes. T722's iOS module already reports each job's bytes at once after
a relaunch (commit 347281f96); the under-count is in the shared JS that sums them.

## Work
1. The first install's progress sums the bytes of every queued job while the big files download (one total, both
   platforms). The words under the bar stay as they are.
2. Tests: a whole-sentence test of the bar's line with two jobs part-way; a mutation that counts one job turns it red.
3. Android: `regs-open` and `tap-stalls` benches, plus the Android `bg-install` scenario with a shot mid-download; iOS:
   the T722 first-install run with a relaunch at about 100 MB (the first shot shows at least what was on disk).

## Acceptance
On both platforms the bar's figure is within a few MB of what is on disk at any moment of the first install, also right
after a relaunch; it never goes down; no change to when the install finishes.

Depends: T722 merged (the iOS half). Size: S. Owner: yes (it changes how Android's bar moves; default: do it).

## Results
(filled in as the work runs)
