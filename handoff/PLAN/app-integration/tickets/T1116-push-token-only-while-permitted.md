# T1116 — A push token is registered only while the phone's notification permission is on; the worker drops stale tokens

Status: OPEN.

Source: T1098 item g (iOS release chat's read of build 45). Owner 2026-11-25 ~00:50 MST, in the 0.9.1 chat (relayed),
asked "say it on the page, or change the behaviour?": "that behaviour change can move to 0.9.1".

## Facts
- `app/packages/ui/src/notify.ts` lines 147–153 write the state before the system ask, and the registrar syncs on
  `notifyActive(state)` only, so a token is registered even when the reader taps Don't Allow (silent pushes need no
  permission).
- `app/workers/feeds/src/push.ts` keeps a stored token with no age limit.

## Work
1. App: register a push token only while the phone's notification permission is granted. Not after Don't Allow;
   when the permission is later revoked, unregister (tell the worker to drop the token) at the next start or resume.
2. Worker: drop a token that has not been re-registered for about 30 days (the app re-registers every 7); the drop
   is logged, as dead tokens are (T996).
3. The privacy page (T1098) then says the token is sent only while the phone's permission is on, plus the age line.
4. Tests: Don't Allow → no register call; grant → register; revoke → unregister; the worker's age rule on a clock.
   Emulator / simulator check of the three paths.
5. Needs the feeds worker deploy (in the 0.9.1 release) and the app build.

## Acceptance
The tests fail without the change; after Don't Allow the worker holds no token for that install; a 31-day-old token
is gone after the worker's next sweep.

Depends: T1098 (the words). Severity: medium (a privacy statement made true by behaviour). Release: 0.9.1 (owner).
Model: Opus. Size: S–M. Owner: no (he decided).
