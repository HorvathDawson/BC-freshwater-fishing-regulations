# T996 — Test the Apple push service end to end (next release)

Status: IN PROGRESS (worker half done 2026-11-22, lane L5, `fix/T996-apns-check`: APNs accepts the staging worker's provider token on both hosts, the token is reused ~50 min then made again, a dead device token is dropped and logged; left (Claude): the 0.9.0 TestFlight build, then the owner's phone steps below, then production's APNs secrets and its first send).

Source: owner 2026-11-21, verbatim: "Add ticket to test apple push service in next release. (The next chat)."

## Context
- T739 / T822: iPhone push "wake-ups" from the feeds worker (APNs, iPhone only); both still wait on a TestFlight build
  and the owner phone test.
- T994 lane F1 (`lane/t994-f1-worker`, finding 23): the worker now keeps its APNs auth token and reuses it for 50
  minutes instead of minting one per wake. Not verified against Apple's live service (Apple refuses a token older than
  an hour and throttles one refreshed more often than every 20 minutes). Finding 25 caps new push tokens at 1,000 a day.

## Work
1. With the next release's TestFlight build: the phone registers with the worker (staging, then production).
2. A wake-up push reaches the phone; the app checks and shows the alert/notice on time.
3. Across more than one hour of worker runs: the token is reused, refreshed before expiry, and Apple returns no
   `TooManyProviderTokenUpdates` / `ExpiredProviderToken`; the worker log says which token it used.
4. A refused token (bad device token, 410 Unregistered) is dropped and said in the log.

## Acceptance
Owner phone receives a wake-up from staging and from production; worker logs over 1 h+ show no token errors.

Depends: T994 F1 merged; a TestFlight build. Size: S. Owner: yes (the phone test; Claude runs the rest).

## Worker half (2026-11-22/23, lane L5, staging; branch `fix/T996-apns-check`)
**Logging added first (6be78401b)**, so the log answers "which token": every send's line carries `apns: {id, iat, kid,
minted}` (`id` = the first 12 hex of the provider token's SHA-256; the token itself is never logged), the run's push
summary and `/status` carry it too ("provider token <id> (key …), made <iat>: reused / new for this send"); each dead
device token is dropped with its own line `{job: "push", dropped: <record id>, env, why: "APNs 400 BadDeviceToken"}`
and listed in the send (`ios.dropped`), never the token; a refused provider token logs `apnsTokenRefused` with its id.
Tests: `test/push.test.ts` (the dead-token and reuse tests now assert the log lines and the /status words).

**(a) Apple accepts the worker's token, both hosts.** Control from this Mac first (curl over HTTP/2, a made-up device
token): a garbage JWT is answered `403 InvalidProviderToken` and no JWT `403 MissingProviderToken` on BOTH
`api.sandbox.push.apple.com` and `api.push.apple.com`, so APNs checks the provider token BEFORE the device token (and
HTTP/1.1 gets no HTTP answer at all). Then the staging worker, a deliberately invalid device token registered as
`sandbox` and one as `production`, `POST /run/push`: both answered **`400 BadDeviceToken`** (never 403): the provider
token from key `6T3DBFF92H` is accepted on both hosts, and Cloudflare's outbound fetch speaks HTTP/2 to APNs (open
question in T739 "What needs a real phone", now settled).
**Hosts:** the worker picks the host from the token's registered `env`, not from which worker it is. A TestFlight build
(staging or production, `deploy/staging/build-ios-testflight.sh` exports `EXPO_PUBLIC_APNS_ENV=production`) registers as
`production` → `api.push.apple.com`, which is what a distribution-signed (TestFlight / App Store) build needs; a
simulator / development build registers `sandbox` → `api.sandbox.push.apple.com`. A staging TestFlight build registers
with the staging worker, a production one with `feeds.canifishthis.ca` (`env.ts` `REPORT_URLS`).
**Production cannot send yet:** `GET https://feeds.canifishthis.ca/status.json` → `push.missing.ios` = `APNS_KEY,
APNS_KEY_ID, APNS_TEAM_ID` (read 2026-11-22/23): the secrets were only ever set on staging (`runbooks/secrets.md`).

**(c) A dead token is dropped and said.** Every probe: both records `dead`, deleted (`tokens` back to 0 at the next
cron run: `{"job":"push","reason":"gauges","ios":{"tokens":0,…}}` at 01:35:20Z), one `dropped` line each in the log.

**(b) Reuse over more than an hour** (`wrangler tail` of the staging worker + a probe script in the lane's scratchpad: every 10 min, two
invalid tokens registered then one `POST /run/push`; staging deploys in between (6be78401b first, then e11978f9e and
3452aae26): the kept token lives in R2 and outlives a deploy):
| probe | sent at | token id | token made | | device tokens | failed |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 01:27:35Z | bc3c618317dc | 01:27:35Z | new | 2 dead: APNs 400 BadDeviceToken, APNs 400 BadDeviceToken | 0 |
| 1 | 01:37:34Z | bc3c618317dc | 01:27:35Z | reused | 2 dead: APNs 400 BadDeviceToken, APNs 400 BadDeviceToken | 0 |
| 2 | 01:47:33Z | bc3c618317dc | 01:27:35Z | reused | 2 dead: APNs 400 BadDeviceToken, APNs 400 BadDeviceToken | 0 |
| 3 | 01:57:31Z | bc3c618317dc | 01:27:35Z | reused | 2 dead: APNs 400 BadDeviceToken, APNs 400 BadDeviceToken | 0 |
| 4 | 02:07:31Z | bc3c618317dc | 01:27:35Z | reused | 2 dead: APNs 400 BadDeviceToken, APNs 400 BadDeviceToken | 0 |
| 5 | 02:17:31Z | bc3c618317dc | 01:27:35Z | reused | 2 dead: APNs 400 BadDeviceToken, APNs 400 BadDeviceToken | 0 |
| 6 | 02:27:31Z | 00f6aa1ba232 | 02:27:31Z | new | 2 dead: APNs 400 BadDeviceToken, APNs 400 BadDeviceToken | 0 |
| 7 | 02:37:32Z | 00f6aa1ba232 | 02:27:31Z | reused | 2 dead: APNs 400 BadDeviceToken, APNs 400 BadDeviceToken | 0 |
70 minutes, 8 sends, 2 provider tokens: the first used 49 m 57 s, then a new one at 59 m 56 s (the first send past
`APNS_JWT_REUSE_S`), both accepted (every answer 400 BadDeviceToken, never 403). No `TooManyProviderTokenUpdates`,
`ExpiredProviderToken` or `InvalidProviderToken` anywhere in the tail (0 lines). The worker log (`wrangler tail`) carried
the same `apns` id on each send's line. The cron gauge runs between had no phones (`tokens: 0`) and made no token.

**(d) iOS simulator end to end: not done.** A simulator on Apple silicon can register against the APNs sandbox and
receive a real push (that would prove APNs → apsd → `didReceiveRemoteNotification` → our task, which `simctl push`
cannot, T739 "Why the silent push did not run on the simulator"). It needs a fresh Release simulator build: the only one
on disk (`app/dist/ios-sim`, 0.8.3 at 1f989bb48) predates the push merge (f9c2e42b5; no `/push/register` in its bundle),
and all three heavy lanes were held by other lanes for the whole run (lane 3 by L6's atlas build). Follow-up when a lane
is free: `HEAVY_LOCK=~/.claude/heavy-job-3.lock deploy/staging/build-ios-sim.sh`, install, turn on "New regulations
data", then `POST /run/push?reason=sim-test` on staging and read the app's run log for "woken by our server".

**Not verified here:** a 200 from APNs to a real device token; delivery to a phone and the push task running; Apple's
`TooManyProviderTokenUpdates` with staging AND production both minting from the same key (production has no secrets
yet, so only staging minted; Apple's 20-minute rule is per provider token stream, and two workers make one each per
~50 min: watch production's first hour for a 429).

## Owner phone steps (with the 0.9.0 TestFlight build)
1. Production only (do once, before the production test): set the three secrets on the production feeds worker, the
   same values as staging (`runbooks/secrets.md`; the key file is `AuthKey_6T3DBFF92H.p8`):
   `npx --prefix app wrangler secret put APNS_KEY -c app/workers/feeds/wrangler.prod.toml` (paste the .p8 text), then
   `APNS_KEY_ID` (6T3DBFF92H) and `APNS_TEAM_ID` (your team id) the same way. Then deploy the production feeds worker
   from main once this branch is merged (`deploy/prod/feeds.sh`, your CONFIRM_PROD step), so its log names the token.
2. Install 0.9.0 from TestFlight. Open it; More → Notifications → turn on **"New regulations data"**, allow
   notifications when iOS asks. The page should then say **"Server wake-ups: this phone is registered (…)"** (or what failed, with "Try again").
3. Tell Claude. Claude checks `/status.json` (`push.last.ios.tokens` ≥ 1 after a send) and sends one wake:
   `POST /run/push?reason=owner-phone-test`. APNs accepted it when the answer's `ios.sent` is 1.
4. Put the app in the background (do not swipe it away), wait a minute, open it: More → Notifications → "Last
   background checks". The newest line should say **"woken by our server"**. Tell Claude what it says (a screenshot).
5. Leave notifications on for an hour or more; nothing else to do. Claude reads the worker log for the token's reuse
   and for any `TooManyProviderTokenUpdates` / `ExpiredProviderToken`.
Steps 2-5 on the staging build first, then on the production build.
