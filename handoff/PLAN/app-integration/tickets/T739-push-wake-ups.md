# T739 — Push "wake-ups" so alerts and notices arrive on time (iPhone first, Android as a second step)

Status: IN PROGRESS (both steps approved by the owner 2026-11-14: iPhone, then "for android too."; iOS chat, branch `ios/t739-push`; ANDROID PUSH IS OFF THE TABLE FOR NOW, owner 2026-11-16 12:08 on Firebase: "we dont want this right now"; iPhone push unchanged). 2026-11-20 09:07: lane/l5-push merged (f9c2e42b5, reviewed twice), staging feeds worker deployed (push.missing.ios == [], 0 tokens); 2026-11-20 10:35: review leftovers L2 + L3 built on lane/la-feeds-leftovers (683042a46: every token listed; one run tells at most 1,500 phones and carries the rest to the next run, under the subrequest limit, said on /status); left (Claude): main chat review + merge, the staging feeds worker deploy, the TestFlight build with this release, then the owner phone test.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T739 | Push "wake-ups" so alerts and notices arrive on time: a content-free "check now" push from the feeds worker, iPhone only (APNs; no Android push, owner 2026-11-16) | IN PROGRESS (merged with T822 on `lane/l5-push` 2026-11-20: iPhone only, Android push removed (owner 2026-11-16, "we dont want this right now"), the approved privacy line in the page source; merged f9c2e42b5 + staging feeds worker deployed 09:07 (push.missing.ios == []); left (Claude): TestFlight build with this release, then the owner phone test; privacy wording question in old owner to-do 2026-11-20 07:33; 11-20 review leftovers L2+L3 (token list uncapped, 1,500 phones a run + carry-over) on lane/la-feeds-leftovers, left (Claude): main chat review + merge) | 683042a46 | owner 2026-11-14 ("okay i ticked push notifications for app yes build for iphone.", "for android too."); audit F3/G3 |

Source: owner 2026-11-14, verbatim: "dont we want to use it for the notifications thought?" (on the Push Notifications
box of the App ID), "there are also time sensitive notifications?" and "can we do this same push notification style to
android?". Audit findings F3 / G3 (git history, `handoff/PLAN/ios/parity-audit.md`, deleted 2026-11-25): the app's own words say checks run "about every
15 minutes at best. Android may check less often while the phone is idle" (`packages/core/src/alerts.ts`), and on iPhone
the background check runs only when iOS allows, often hours late.

## Owner answers
- 2026-11-14 (late), verbatim: "okay i ticked push notifications for app yes build for iphone." / "dont need this (Closure notices marked
  time-sensitive on iPhone: off for now / on.)" / "i guess if new data is there we could send a notification that the user
  needs to download it." So: Push ticked on the App ID, `aps-environment` stays (audit G12's removal dropped), no
  Time-sensitive notices; the wake-up also fires when a new release is published (Claude's reading). Android: "for android too."
- 2026-11-15 evening (the old owner to-do page "owner answers, round 6", verbatim: "Yes to both (Recommended by it)"): the
  privacy-page line and the App Store privacy answer (Identifiers → Device ID: collected, App Functionality, not linked to
  the user, not used for tracking) approved (To-Do row `t739-privacy-wording`). Putting the page live is the owner's, with
  the production release.
- 2026-11-16 12:08, asked whether "for android too" was his exception to "self-host everything" for push delivery through
  Google's Firebase: "we dont want this right now". Android gets no push and needs no Firebase project.
- 2026-11-20 09:39 (dialog), privacy line: "Claude's edit (Recommended)": Apple only, "I/my", push token in "the short version", Apple (push only) in the services list, the one-day salted IP hash and stored environment + time said, effective date = go-live day; the owner sees the text before it goes live.
- 2026-11-20 09:48 (dialog): privacy text "Looks good" (merged); and "Fix the sentence": "This website" no longer says "runs no analytics" (it said so beside the T910 visit counter); now "runs no advertising or tracking scripts; the only script is the visit counter described above". Pinned in site.test.ts.

## What
A content-free "check now" push. When a feed changes (a new in-season notice, a DFO salmon change, a gauge update
round) or a new `data_release` is published, the feeds worker sends a silent push to every registered iPhone; the phone
runs the SAME check it runs today and makes the same local notification if a rule of that reader's fires. The periodic
background check stays as the fallback. What our server learns: a device push token and its APNs env, nothing else; a
token is dropped when APNs says it is dead. iOS throttles silent pushes (a few an hour), does not wake an app the reader
swiped away, and holds them in Low Power Mode.

Done (merged f9c2e42b5 via `lane/l5-push`, reviewed twice; staging feeds worker deployed 2026-11-20 09:07,
`push.missing.ios == []`):
- Worker `app/workers/feeds/src/push.ts`: `POST /push/register` / `/push/unregister` (iOS only; Android refused 400 "no
  push on Android", 2102890de), rate-limited per address (2 s apart, `PUSH_PER_DAY` 200), store
  `private/push/tokens/<sha256(token)>.json`, never backed up; APNs HTTP/2 ES256 from `APNS_KEY` / `APNS_KEY_ID` /
  `APNS_TEAM_ID`, topic `ca.canifishthis.app`, payload `{"aps":{"content-available":1},"k":"check"}`; `wakePhones`
  (a release told after `sent > 0` or `RELEASE_WAKE_TRIES` 3), `dfoAnswersChanged` (M1); failures on /status (M2);
  `POST /run/push` by hand; `/status.json` `push.missing` / `push.last`.
- App: `apps/mobile/src/pushToken.ios.ts` (`pushToken.android.ts` null always), `packages/data/src/notify/push.ts`
  registrar (30 s token deadline `PUSH_TOKEN_WAIT_MS`, 429 waited out once, re-register when token / env / address
  changed or after 7 days `PUSH_REFRESH_MS`), `notify/wake.ts` `runWake`, the "Server wake-ups" line + "Try again" on More →
  Notifications (T822 fix, `app-fix/t822-push-register` f4598b112). Words `NOTIFY_WORDS` pinned whole in `alerts.test.ts`.
  Android build config byte-identical before / after the push branch (introspect sha256 `9ca64761c5bd2706…`).
- Privacy page "Push token" section in `app/tools/seo/html.ts` (pinned whole in `tools/seo/site.test.ts`; shots
  `../T739-shots/privacy-*`), not live until a production release.
- Simulator (2026-11-20, 2102890de, `scenarios-ios/push.mjs`): register / unregister PASS; Android benches 3/3 PASS
  (`scenarios/push-absent.mjs`). `simctl push` cannot start the push task: the simulator hands the push over as a
  background FETCH, and `NotificationsBackgroundTaskConsumer.swift:7-8` takes only the RemoteNotification launch reason;
  nothing of ours to fix.
- Review leftovers: L2 + L3 (683042a46, `lane/la-feeds-leftovers`): every token listed page by page; ONE RUN TELLS AT MOST
  `PUSH_SENDS_PER_RUN` (1,500) phones and carries the rest in `private/push/carry.json`, said on /status; tests
  `push.scale.test.ts`. f538f437f: the carry is looked for with `BUCKET.head(PUSH_CARRY_KEY)`. Two feed runs at once
  both reading the carry: lease design in T977.

## Work (left)
1. Main chat review + merge of `lane/la-feeds-leftovers`, then the staging feeds worker deploy.
2. TestFlight build with this release (`deploy/staging/build-ios-testflight.sh` sets `EXPO_PUBLIC_APNS_ENV=production`,
   so the worker sends to `api.push.apple.com`; the APNs key must allow Production).
3. **The owner's phone test** (FIRST: that a real silent push starts the push task at all):
   1. More → Notifications → turn on **"New regulations data"** (the "Notifications" switch alone does not register:
      `data/src/notify/settings.ts` `notifyActive`). The "Server wake-ups" line says "registered (time)" or "not
      registered: <why>".
   2. `GET https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status.json` → `push.missing.ios` must be `[]`.
   3. One wake by hand (from the repo root; the token is never printed):
      `curl -sS -X POST -H @<(printf 'Authorization: Bearer %s\n' "$(cat app/workers/feeds/.run-token)") 'https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/run/push?reason=owner-phone-test'`
      **APNs accepted it when `ios.sent` ≥ 1**; `ios.dead` ≥ 1 with "BadDeviceToken" in `ios.errors` means the wrong
      host for that token.
   4. With the app in the background (not swiped away): More → Notifications → **"Last background checks"**: the newest
      line reads `<date> at <time> PT · <n> s · woken by our server, …`. The periodic task's lines never say it. A trace
      without the phone: `wrangler tail bc-fishing-r2-staging` shows the app's manifest reads seconds after the push.
   Then: APNs end to end, iOS's throttling ("two or three per hour"), Low Power Mode, a swiped-away app, the wake-to-
   notice time against the 2 minutes, a token turning over, the production host.
4. At go-live: **the page's effective date (`PRIVACY_EFFECTIVE`, still 2026-11-19) moves to the go-live day**; prod APNs
   secrets (the same three with `-c app/workers/feeds/wrangler.prod.toml`) + prod worker are the owner's.
- Seen, not changed: with notifications off the intro still reads in the present tense, "our server sends this phone a
  silent signal"; the line under it says off.

## Acceptance (if picked)
On a real iPhone, with the app in the background, a new in-season notice for a watched water notifies within 2 minutes
of the feed changing (today: hours); the worker's store holds tokens only; turning notifications off in the app removes
the token; a failed push is reported and the periodic check still catches the notice.

Depends: T88 (a signed build on a real iPhone), the feeds worker. Size: M (iPhone), M (Android). Owner: yes (the pick,
the APNs key, the App ID box; for Android the Firebase project and the self-host exception).
