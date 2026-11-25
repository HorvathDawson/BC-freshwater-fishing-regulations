# T1116 — A push token is registered only while the phone's notification permission is on; the worker drops stale tokens

Status: DONE (code) — f4ef89f4c (app + feeds worker). Ships with the 0.9.1 app build AND a feeds worker deploy; the
simulator walk of the three paths is not done (see As built).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1116 | A push token is registered only while the phone's notification permission is on; the worker drops tokens not re-registered for ~30 days | DONE — 0.10.0 (owner 2026-11-25); needs the app build + feeds worker deploy | f4ef89f4c | owner via the 0.9.1 chat, T1098 item g |

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

## As built (lane L27, 2026-11-25)
- App, one path (`app/packages/ui/src/notify.ts`): `pushWanted(state, permission)` = a notification on AND the phone's
  permission `granted`. `NotifyRoot` holds the app's one permission answer (read at start, on every return to the
  front through the new `NotifyPlatform.onResume`, when the page opens, and set by the system dialog's answer) and
  sends the token only once that answer is read. "Don't Allow" → no register; revoked in Settings → unregister at the
  next start or return; allowed again → register. "Try again" follows the same rule. A permission that cannot be read
  is reported and nothing is sent. `apps/mobile/src/notify.native.ts` `onResume` = AppState back to `active`.
  Android has no push (owner 2026-11-16): the same code runs, nothing is registered either way.
- The switch keeps the reader's choice; the existing line under it says "Notifications are blocked for this app in the
  phone's settings, so none can be shown." with "Open settings". No words or look changed.
- Without a token there is no silent wake: the app finds new data itself (update service on start, every 3 h while
  open, on a return to the front after 10 min; the periodic background check reads the manifest).
- Worker (`app/workers/feeds/src/push.ts`): `PUSH_TOKEN_MAX_AGE_DAYS = 30` since the record's `at` (each register
  writes it): exactly 30 days kept, any more deleted. Daily sweep `purgePushTokens` in the "25 9 * * *" purge
  (`report-retention.json` `pushTokens`, /status Daily jobs line, a failure told as `push-token-purge:<day>`);
  `sendWake` never wakes a stale token (deletes it, `stale` count on /status). Drops logged by record id, never the
  token. Migration: every record since T739 (2026-11-14) has `at`, so none is older than ~11 days at deploy; a record
  with no readable `at` is dated by the sweep (kept, ages from then), never dropped on sight.
- Tests: `app/packages/ui-native/src/menu/Notifications.test.tsx` "(T1116) …" (7), `app/workers/feeds/test/push.age.test.ts`
  (9); mutations (permission check removed, no resume re-read, ask answer not kept, retry ungated, push before the
  read; `>=` and `+1 day` on the age, undated dropped, undated not dated, send check off, sweep not wired) each fail.
- Not verified: the iPhone simulator / a real iPhone walk of the three paths (`scenarios-ios/push.mjs` covers Allow only).
