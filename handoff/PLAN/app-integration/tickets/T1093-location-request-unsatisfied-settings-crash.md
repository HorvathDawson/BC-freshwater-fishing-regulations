# T1093 — "Location request failed due to unsatisfied device settings" is an unhandled crash on 0.9.0

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1093 | "Location request failed due to unsatisfied device settings" is an unhandled crash on 0.9.0 (kind `f7cb406a97ce7436`) | OPEN — 0.9.2 (missed the 0.9.1 cut) | | planning chat 2026-11-24 |

Source: production feeds worker, crash kind `f7cb406a97ce7436`, 1 report, 2026-11-24 23:08Z, app 0.9.0 (the first
crash kind seen on 0.9.0). The message is the Android location library's refusal when the phone's Location setting
(or high-accuracy mode) is off although the app holds the permission. Read by `pipeline reports`, planning chat
2026-11-24 22:20 MST. Related: T982 #6 (the location ask), T1005 (crash kinds table).

## Work
1. Read the kind's stack (`GET /crashes`, read-only) and find the call that is not caught: the locate button, the
   start-at-my-location step, or a saved-spot / alert flow.
2. Every location request catches this refusal and says what to do ("Location is off on this phone: turn it on in
   Settings"), with the same words on every way in (`one-load-path-per-screen`). No crash, no silent nothing.
3. Class the kind `expected` in the worker (a device setting), keep the shown failure reported.
4. Check iPhone and web for the same hole (location services off, permission granted earlier).
   iPhone, read 2026-11-24 by the iOS chat at 65f84020c: no twin found, every iOS location caller catches the
   refusal (`app/packages/ui/src/myLocation.ts`:93–104, 171; `mapRail.ts`:251–263; `SpotHost.tsx`:118–121;
   `locate.native.ts`:46–49). Web is still to check.

## Acceptance
A test makes the location call reject with this error on each caller: the message shows, nothing is thrown past the
caller. Emulator check with Location switched off in quick settings. Fails without the change.

Severity: medium (a tap on a core button can end in an unhandled error on the newest build). Release: 0.9.2 (0.9.1 if
the 0.9.1 chat has a free app lane before its build; it decides). Model: Sonnet. Size: S. Owner: no.
