# T88 — iOS build

Status: PARTIAL 2026-11-05 — eb2405de0 (`deploy/staging/build-ios-testflight.sh`); running it needs the owner's Apple Developer account (OWNER-TODO "iOS"). Ships after Android 0.5.0.
Re-scoped (owner 2026-11-03): a signed device build and a TestFlight upload script; needs the $99 Apple Developer account (owner).

Source: owner deferred iOS at N1 ("code kept platform-neutral so iOS is a build later"); N2 status: "Device run, iOS:
open". CocoaPods not installed; EdgeBack is iOS-only and has never run.

## Work
Install CocoaPods; build the dev client on the simulator; run the T50-style parity and system-bar walk.
(The simulator half is T313: done there.)

## Acceptance
Simulator walk filed; edge-back verified.

Depends: T50, T74. Heavy, alone. Size: L. Owner: yes (when). Default: after the Android ship.

## Done (2026-11-05)
`deploy/staging/build-ios-testflight.sh [--production] [--no-upload | --unsigned] [--dry-run]`: committed HEAD in a temp
worktree → prebuild → pods → `xcodebuild archive` (generic iOS, Release) with AUTOMATIC signing driven by an App Store
Connect API key (`-allowProvisioningUpdates`: Xcode makes the distribution certificate and App Store profile) →
`-exportArchive` method app-store-connect, destination upload (TestFlight) or export (.ipa). Staging data + switch by
default (the internal-tester build, like Play internal), `--production` for the review build. Build number =
max(app.json, ~/.canifishthis/testflight-last-build) + 1, set in the worktree only. dSYMs kept under data/sourcemaps.
Secrets: ~/.canifishthis/ios-team-id, asc-api-key.json (key_id, issuer_id, key_path to the .p8). `--dry-run` lists what is
missing (both, today). `--unsigned` builds the device archive with no signing: Claude's proof that the device build
compiles and links without the account (result below).
- `--unsigned` run 2026-11-05 at eb2405de0 (0.5.0): the device (iphoneos, arm64) Release archive compiled and linked in 80 s,
  build number 9 (worktree only). Nothing signed, installed or uploaded. Left: the owner's account + the signed run.


## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T363, T364, T365 — read each file's open items and finish them here; each closes pointing here.
