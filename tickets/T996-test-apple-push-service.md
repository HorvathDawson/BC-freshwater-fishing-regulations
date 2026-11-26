# T996 — Test the Apple push service end to end (next release)

Status: IN PROGRESS (worker half done 2026-11-22, lane L5, `fix/T996-apns-check`: APNs accepts the staging worker's provider token on both hosts, the token is reused ~50 min then made again, a dead device token is dropped and logged; left (Claude): the 0.9.0 TestFlight build, then the owner's phone steps below, then production's APNs secrets and its first send).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T996 | Test the Apple push service end to end (registration, wake-up delivery, token reuse over 1 h+) | IN PROGRESS (worker half proved on staging 2026-11-22, 6be78401b: token accepted both hosts, reused 50 min then remade, dead tokens dropped + logged; production keys set 2026-11-24, but the proof failed: the owner's iPhone on 0.9.0 (45) gets no push token, cause open; owner 2026-11-25: after the iPhone release, fix as a patch; the phone's log 11-25 points to the phone (its push service holds no token); next: a second, healthy iPhone) | T994 | owner 2026-11-21, next release |

Source: owner 2026-11-21, verbatim: "Add ticket to test apple push service in next release. (The next chat)." Owner
2026-11-25 ~00:20 MST (iOS release chat, relayed), verbatim: "we should move this to after iphone release I think. we
could fix with a patch?" The iOS chat's reading, recorded in T1071: production push no longer gates the App Store
submission (this replaces his "Soon, on build 45" pick as a gate); the proof and any fix come after the release, a fix
shipping as an ordinary iPhone update.

## What
iPhone push "wake-ups" from the feeds worker (APNs, iPhone only; T739 / T822 close with this): the phone registers, a
wake-up reaches it, the worker's provider token is reused across 1 h+ with no `TooManyProviderTokenUpdates` /
`ExpiredProviderToken`, a refused device token is dropped and logged. Known cost, told to him: until it works no server
wake-up reaches an iPhone, so "new data" and flow notices rely on iOS's own background check and can be late. To watch
(the planning chat): the store listing and the first-load notification ask (T1110) must not promise iPhone notices the app
cannot deliver yet.

Done:
- Worker half on staging (lane L5, PR #39, `fix/T996-apns-check`, head 3c186213a; logging 6be78401b: every send's line
  carries `apns: {id, iat, kid, minted}`, `/status` too; dead tokens dropped with their own line; `apnsTokenRefused`).
  Provider token from key `6T3DBFF92H` accepted on `api.sandbox.push.apple.com` and `api.push.apple.com` (every probe
  `400 BadDeviceToken`, never 403; Cloudflare's fetch speaks HTTP/2 to APNs). Reuse: 70 minutes, 8 sends, 2 provider
  tokens (the first used 49 m 57 s, a new one at 59 m 56 s, `APNS_JWT_REUSE_S`); 0 token-error lines; the kept token
  lives in R2 and outlives a deploy. Hosts: the worker picks the host from the token's registered `env`; a TestFlight
  build (`EXPO_PUBLIC_APNS_ENV=production`) registers `production` → `api.push.apple.com`; staging build → staging
  worker, production → `feeds.canifishthis.ca` (`env.ts` `REPORT_URLS`).
- Production secrets set by the owner 2026-11-24 (status.json: push.missing.ios = [], configured true).
- Production proof attempt 2026-11-24 23:30–23:50 MST: NOT proven. His iPhone (XR, iOS 18.5, TestFlight 0.9.0 (45)) never
  registers: "push register: the system gave no push token in 30 s (ios: the request for one was never answered)", on
  Wi-Fi and cellular, after "Try again" (same silence as build 36, T822); two POST /run/push: tokens 0, sent 0. Ruled
  out: signing (builds 41, 43, 44, 45: aps-environment production; the App ID has PUSH_NOTIFICATIONS); expo-notifications
  58.1.6 / expo 58.0.19 / the build 45 log (no defect; the same code returned tokens twice on Release simulator builds,
  T739). The phone's Console (filter apsd, 25 s while retrying): only timer housekeeping, no connection, no registration;
  identityservicesd "asked for publicToken, got (null)": the phone's push service holds no APNs token at all. No app
  change indicated, no new ticket. The owner's two TestFlight feedback items of 2026-11-25 06:37Z and 06:40Z, ids
  `ADcTz8stYyx3IE-aFrsdMMY` and `ACeTORpC4_17LbsatW-MECg` (no comment, one screenshot each), are his own pictures of the
  Notifications page for this proof, not tester feedback. Cited here so `pipeline reports` does not list them as new.

## Work (left)
1. The second-iPhone test (owner, below), with the phone steps: install the TestFlight build; More → Notifications →
   turn on **"New regulations data"**, allow notifications; the page should say **"Server wake-ups: this phone is
   registered (…)"**. Claude checks `/status.json` (`push.last.ios.tokens` ≥ 1) and sends `POST
   /run/push?reason=owner-phone-test` (APNs accepted it when `ios.sent` is 1). App in the background a minute, then More →
   Notifications → "Last background checks": the newest line should say **"woken by our server"** (a screenshot). Leave
   notifications on for an hour or more; Claude reads the worker log for the token's reuse and token errors. Staging
   build first, then production. If the cause is in the app it needs a new iPhone build and its own ticket from the
   planning chat.
2. Watch production's first hour for a 429 (`TooManyProviderTokenUpdates`: staging and production both mint from the same
   key, one each per ~50 min).
3. (d) iOS simulator end to end, not done (a simulator on Apple silicon can register against the APNs sandbox and receive
   a real push, which `simctl push` cannot): a fresh Release simulator build (the one on disk predates the push merge
   f9c2e42b5): `HEAVY_LOCK=~/.claude/heavy-job-3.lock deploy/staging/build-ios-sim.sh`, install, turn on "New regulations
   data", then `POST /run/push?reason=sim-test` on staging and read the app's run log for "woken by our server".

## Acceptance
Owner phone receives a wake-up from staging and from production; worker logs over 1 h+ show no token errors.

Depends: T994 F1 merged; a TestFlight build. Size: S. Owner: yes (the phone test; Claude runs the rest).


## 2026-11-25: a crash kind from the owner's push proof attempt
Crash `67a5a3a112caf554` (0.9.0 iPhone, 2026-11-25 06:39Z): no push token arrived within 30 s. It is the owner's own
push proof attempt on TestFlight, not a tester's. It is cited here so `pipeline reports` no longer lists it as
NEW; the push proof on a second iPhone is still owed after the App Store release (the iOS chat was retired
2026-11-25; the planning chat holds this). Feedback log row F46.

## Owner

- [ ] 2026-11-25 — After the iPhone release: install the same TestFlight build on a second iPhone and try there (your spare iPhone's push service gives no token; signing and the app's push code were checked). Your words: "we should move this to after iphone release I think. we could fix with a patch?"
