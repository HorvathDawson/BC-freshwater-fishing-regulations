# T313 — iOS: build for the simulator and run the basic tests

Status: OPEN (owner 2026-10-31: "Xcode is installed. Could you set it up and do basic iOS tests and make sure it works as expected?").

## Work
1. `expo prebuild --platform ios` in a temp worktree (like build-apk.sh), pods, build a Debug/Release app for the iOS
   Simulator (no Apple developer account needed). MapLibre RN iOS build (see T217 binding patch).
2. iOS fallbacks for Android-only pieces: exit-info (crash reason) → no-op on iOS; bg-download → iOS background URLSession
   path (expo-file-system) already used; notification permission; location (expo-location iOS Info.plist strings).
3. Test bed on iOS: Maestro on the simulator (or simctl + screenshots): install/first open, map + tap a water → regs,
   offline, satellite, location (simctl location set), walkthrough, PDF open. Screenshots side by side with Android.
4. Script it (e.g. `deploy/staging/build-ios-sim.sh`, `app/tools/testbed` iOS target) so it repeats; heavy jobs via heavy.sh.

## Acceptance
iOS simulator build launches against staging; the basic scenarios pass or each failure is ticketed; screenshot pairs.

Depends: T217, T88. Size: M–L. Owner: no (App Store needs a $99 account later).
