# T1073 — The "app update" notice shows only when this phone can actually install the update

Status: IN REVIEW — owner: see ## Owner (built 0e10879e8; proof pending: the device check on Play Internal one build behind, at the next phone build).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1073 | The "app update" notice shows only when this phone can actually install the update | IN REVIEW | 0e10879e8 | proof pending: device check on Play Internal one build behind, at the next phone build |

Source: owner 2026-11-24 (planning chat), verbatim: "inside the app (for someone on closed and internal) got a
notification for the app update when it went onto internal but it wont let install happen then so the notification is
fake. should only show if thge user can actually download (maybe for 0.9.2)". Seen during the 0.9.0 release: the build
was on Play Internal, not yet promoted to Closed. Related: T390 (the in-app update prompt), T867 (inbox app-update
row), T234 (new-app-version notice).

## Work
1. Find where the notice's "a newer version exists" comes from (our own version file / feed, T1014, or Play). The
   likely cause: our file names the newest build as soon as it exists, while Play offers it to this install only once
   it is on a track the account can get, and after Play's own delay.
2. The notice, the inbox row and the Update button show only when the store says this install can update now:
   Android = Play in-app update availability (`UPDATE_AVAILABLE` for this install); iPhone = the App Store lookup (or
   TestFlight for a TestFlight install: no notice we cannot act on). Our own version file may say "a release needs a
   newer app" (the strong prompt), but even then the words must not promise an install the store will not give.
3. No silent drop (`no-silent-fallbacks`): a newer version that the store does not offer yet is not an error and not
   a notice; it is rechecked on the next check.
4. Tests: version file newer + store says not available → no notice, no inbox row, no button; store says available →
   all three. Device check on Play Internal one build behind (T390's test bed).

## Acceptance
The tests above fail without the change (mutation); on a phone in both Internal and Closed, a build that is on a
track the phone cannot install from yet raises nothing; when Play offers it, the notice appears and the install works.

Depends: T390. Severity: medium (a notice the tester cannot act on; testers are the only users today). Release: 0.9.2
(owner: "maybe for 0.9.2"). Model: Opus (Android Play Core + notification path). Size: S–M. Owner: no.

## Built (hardening lane, 2026-11-26)
Found: the gentle prompt was already store-gated on Play and the App Store since T390/T894 (the store's answer is the
only thing that sets `check.available`; `appUpdater.native.ts` uses `expo-in-app-updates`, no new module). The holes:
the strong prompt ("New data needs the latest app", with an Update button) stood whatever the store said; a failed
store check drew a "Could not check" row; the "no background notice for a store build" rule was a string compare in
`notify.native.ts`; a TestFlight install had no way to say it has no channel.
Now one decision in `core/src/appUpdate.ts` (`appUpdateVM`, `updatePopupVM`, `appUpdateBackgroundNotice`,
`appUpdateChannel`): store channel + store does not offer (or check failed) = no row/popup/button for a newer version;
the strong prompt then reads "New data needs a newer app. It is not available for your phone yet." with no button;
a failed store check is reported once by `useAppUpdate` and shows nothing. `EXPO_PUBLIC_APP_DISTRIBUTION=testflight`
on iOS gives no channel (nothing sets it yet: build-ios-testflight.sh should).

## Owner
- [ ] 2026-11-26 — Device check on Play Internal one build behind (T390's test bed): a build on a track the phone cannot
  install from raises nothing; when Play offers it, the notice appears and Update installs. Not run: it needs a phone build.
