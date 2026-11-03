# T88 — iOS build

Status: OPEN — re-scoped (owner 2026-11-03): a signed device build and a TestFlight upload script; needs the $99 Apple Developer account (owner). Ships after Android 0.5.0. Was: OPEN. Owner timing.

Source: owner deferred iOS at N1 ("code kept platform-neutral so iOS is a build later"); N2 status: "Device run, iOS:
open". CocoaPods not installed; EdgeBack is iOS-only and has never run.

## Work
Install CocoaPods; build the dev client on the simulator; run the T50-style parity and system-bar walk.

## Acceptance
Simulator walk filed; edge-back verified.

Depends: T50, T74. Heavy, alone. Size: L. Owner: yes (when). Default: after the Android ship.
