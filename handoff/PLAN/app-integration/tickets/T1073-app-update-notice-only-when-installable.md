# T1073 — The "app update" notice shows only when this phone can actually install the update

Status: PARTIAL — owner: see ## Owner (built, store-gated notice; left: run-time TestFlight detection (follow-up from the planning chat) and the owner's device check on Play Internal one build behind).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1073 | The "app update" notice shows only when this phone can actually install the update | PARTIAL | 0e10879e8, 3ab7492ec | TestFlight install cannot be told at run time (needs native + phone build; follow-up); owner: device check on Play Internal one build behind |

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
`notify.native.ts`; a kept "available" answer outlived an update made through the Play Store app.
Now one decision in `core/src/appUpdate.ts` (`appUpdateVM`, `updatePopupVM`, `appUpdateBackgroundNotice`,
`appUpdateChannel`). On a store channel (Play, App Store):
- the store offers it: row, popup and Update button, as before;
- the store ANSWERED no: the strong prompt reads "New data needs a newer app. It is not available for your phone yet."
  with no button, and its Not now (key `needed-wait`) never hides the later popup with Update;
- no answer yet (not asked, offline, failed check): "New data needs a newer app.", no button, no popup; no Google Play:
  the noPlay words; a failed store check is reported once by `useAppUpdate` and shows nothing else;
- the staging APK's own failed check keeps its "Could not check" row (the ticket covers store checks only);
- the saved check keeps the app version that asked (`installed`) and is dropped when the installed version differs.
An iOS build has no distribution word: one that is set throws, like Android's.

## What holds today for iPhone, and what is left
The iPhone asks the App Store lookup, so a TestFlight build that is AHEAD of the store gets no notice; a TestFlight
build BEHIND the store would be told of the store's version. Telling a TestFlight install at run time needs a native
capability and a phone build: left, a find for the planning chat (it numbers the follow-up). Setting a build-time word
is wrong: the TestFlight binary is the one promoted to the App Store.

## Owner
- [ ] 2026-11-26 — Device check on Play Internal one build behind (T390's test bed): a build on a track the phone cannot
  install from raises nothing; when Play offers it, the notice appears and Update installs. Not run: it needs a phone build.
