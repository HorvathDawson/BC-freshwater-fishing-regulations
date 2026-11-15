# T739 — Push "wake-ups" so alerts and notices arrive on time (iPhone first, Android as a second step)

Status: IN PROGRESS (both steps approved by the owner 2026-11-14: iPhone, then "for android too."; iOS chat, branch `ios/t739-push`; Android needs the owner's Firebase project file before it can be built).

Source: owner 2026-11-14, verbatim: "dont we want to use it for the notifications thought?" (on the Push Notifications
box of the App ID), "there are also time sensitive notifications?" and "can we do this same push notification style to
android?". Audit findings F3 / G3 (`handoff/PLAN/ios/parity-audit.md`): the app's own words say checks run "about every
15 minutes at best. Android may check less often while the phone is idle" (`packages/core/src/alerts.ts`), and on iPhone
the background check runs only when iOS allows, often hours late.

## Owner answers (2026-11-14, late)
Verbatim: "okay i ticked push notifications for app yes build for iphone." / "dont need this (Closure notices marked
time-sensitive on iPhone: off for now / on.)" / "i guess if new data is there we could send a notification that the user
needs to download it."
So: step 1 (iPhone, APNs) is approved and Push is ticked on the App ID; the `aps-environment` entitlement stays (audit
G12's removal is dropped). Time-sensitive notices are NOT wanted. New scope from the third sentence (Claude's reading):
the wake-up also fires when a new release is published; the phone then starts the download itself when its settings
allow, and otherwise makes a local notification saying new data is ready to download. Android (FCM): answered a minute later, verbatim: "for android too."
So the Android step is approved as well, said after being told it means Google's Firebase inside the app: Claude reads
that as the owner's exception to "self-host everything" for push delivery only (no Firebase Analytics, nothing else of
Firebase). The owner makes the Firebase project and hands over `google-services.json`.

## Today (read in the code)
No notification on either platform uses push. The phone wakes in the background (`apps/mobile/src/notify.native.ts`,
`packages/data/src/notify/check.ts`), reads our feeds, and makes a local notification itself. The alert rules (which
waters, which thresholds) never leave the phone. There is no Firebase and no push token anywhere in the app.

## Proposal
A content-free "check now" push. When a feed changes (a new in-season notice, a DFO salmon change, a gauge update
round), the feeds worker sends a silent push to every registered phone; the phone runs the SAME check it runs today and
makes the same local notification if a rule of that reader's fires. The periodic background check stays as the fallback
and is reported when it is the only thing that ran (no silent fallback).
- **What our server learns:** a device push token and the platform, nothing else. Not which waters are watched, not the
  alert rules, not a location. A token is dropped when the push service says it is dead.
- **iPhone (step 1):** Apple's push service (APNs) straight from the worker with an APNs key. No third-party SDK. Needs
  Push Notifications ticked on the App ID (owner), an APNs key saved as a worker secret (owner makes it), the
  `aps-environment` entitlement kept (audit G12's removal is on hold for this), Background Modes "remote-notification".
  Limits to state in the app: iOS throttles silent pushes (a few an hour), does not deliver them to an app the reader
  swiped away, and holds them in Low Power Mode.
- **Android (step 2, only if phones show late alerts):** the only push channel is Google's Firebase Cloud Messaging: a
  Firebase project, `google-services.json`, the FCM library in the app (analytics off). That is a Google service in the
  app, so it needs the owner's exception to "self-host everything" (FCM is to Android what APNs is to iPhone). Android's
  15-minute check mostly works today; Doze on an idle phone is the case push would fix.
- **Time Sensitive (separate pick):** a closure notice for a watched water could be marked time-sensitive on iPhone so
  it breaks through Focus and the scheduled summary (capability ticked on the App ID, one field on the notification).
  Off by default in this proposal.
- **Privacy:** one new sentence on the privacy page and in both stores' answers (a push token, for delivering
  notifications, not linked to an account). The Android chat owns the privacy page and `play/data-safety.md`.

## Acceptance (if picked)
On a real iPhone, with the app in the background, a new in-season notice for a watched water notifies within 2 minutes
of the feed changing (today: hours); the worker's store holds tokens only; turning notifications off in the app removes
the token; a failed push is reported and the periodic check still catches the notice.

Depends: T88 (a signed build on a real iPhone), the feeds worker. Size: M (iPhone), M (Android). Owner: yes (the pick,
the APNs key, the App ID box; for Android the Firebase project and the self-host exception).

## Results

### What was built (branch `ios/t739-push`)
**A. Feeds worker** (`app/workers/feeds/src/push.ts`, wired in `src/index.ts`, `src/status.ts`):
- `POST /push/register` / `POST /push/unregister` with `{platform: "ios"|"android", token, env: "sandbox"|"production"}`.
  Validated (an iOS token is 64–200 hex, an Android one 32–4096 url-safe characters, Android env is production, any other
  field refused), rate-limited per address exactly like `POST /crash` (R2 counter, salted address hash, 2 s apart, 20 a day,
  counters deleted with the others by the daily purge), needs `REPORT_SALT` (503 without). Stored in R2 at
  `private/push/tokens/<sha256(token)>.json` = `{format, platform, token, env, at}`: no address, no install id, nothing else.
  Never backed up (`backup.ts personalKind` → "never"): a lost store is rebuilt by the phones, which register at every start.
- The sender `sendWake`: one content-free wake to every token. iPhone: APNs HTTP/2 with an ES256 provider token made with
  WebCrypto from `APNS_KEY` (the .p8 text), `APNS_KEY_ID`, `APNS_TEAM_ID`; topic `ca.canifishthis.app`, `apns-push-type:
  background`, `apns-priority: 5`, `apns-expiration` now + 1 h, payload `{"aps":{"content-available":1},"k":"check"}`, the
  sandbox host for a token registered as sandbox. Android: FCM HTTP v1, an OAuth token from `FCM_SERVICE_ACCOUNT` (RS256
  assertion, WebCrypto), then a data-only message `{k: "check"}`, priority HIGH, ttl 3600 s, collapse key "check".
- Dead tokens are deleted: APNs 410, 400 BadDeviceToken / DeviceTokenNotForTopic; FCM UNREGISTERED.
- When: after a feed run that changed what the phones read (gauges: a run whose stations were not all unchanged; DFO: a
  region rewritten or a notice; in-season: a new / changed / removed row) and, on the gauge run (every 30–60 min), when
  the live `release/manifest.json` names a new `data_release` (the first look only records it). Never on a dry run.
  `POST /run/push` (Bearer RUN_TOKEN) sends one by hand.
- A platform with a secret missing sends nothing and says so: `GET /status.json` → `push.missing` (per platform, the
  secret names) and `push.last` (the last send: per platform tokens / sent / dead / failed / first errors); the HTML
  status page has a "Push wake-ups" table ("not configured: nothing is sent: … not set"). A failed send is a run step
  error (printed) and in the status; the console names a platform that had tokens it could not send to.
- Tests: `test/push.test.ts` (15): validation, store content (no address), unregister, 400/405/503, the per-address
  limit, the APNs JWT (header, claims, signature verified with the public key), the APNs request whole, the FCM assertion
  (verified) and message whole, missing-secret state, APNs sent/dead/failed with deletion, FCM UNREGISTERED deletion,
  a bad key failing every token said, wake reasons, the release watch, `/status(.json)`. Worker suite 462 + 15 green.
- Scale note: the sends run inside the feed job's invocation (20 at a time); past a few hundred phones the subrequest
  budget calls for a Cloudflare Queue. Not needed for closed testing.

**B. App, iPhone** (built and run on the simulator, below):
- `apps/mobile/src/pushToken.ios.ts` (+ `.android.ts`, typed by `pushToken.d.ts`): the device token through
  expo-notifications' `getDevicePushTokenAsync` and `addPushTokenListener` (no native code needed: SDK 58's
  expo-notifications does the token, the background task and the Info.plist key). APNs env "sandbox", or "production"
  when the build says `EXPO_PUBLIC_APNS_ENV=production` (`deploy/staging/build-ios-testflight.sh` now does).
- `packages/data/src/notify/push.ts` `createPushRegistrar`: while notifications are on (`notifyActive`, the same switch
  that schedules the periodic check, `@app/ui` `NotifyRoot`) the token is registered with `PUSH_URL` (the report worker's
  `/push`, `env.ts pushUrlFor`), re-sent when it changes (the old one withdrawn), withdrawn when they are turned off. What
  was sent is kept in `push-token.json`; a failed send throws (reported; offline is the environment's) and the next start
  tries again.
- `packages/data/src/notify/wake.ts` `runWake`: the push task (`PUSH_TASK`, expo-notifications `registerTaskAsync`)
  runs the update service's check in this runtime (App hands it over, `setWakeUpdates`) and then the SAME
  `runBackgroundCheck` the periodic task runs (which stays). A newer release whose download that check started (T722's
  system downloader, under the reader's mobile-data setting) is not announced; one held (Wi-Fi only on mobile data, or no
  runtime to download in) gets ONE "New regulations data is ready to download. Open the app to download it." (once per
  release, `toldRelease`; only with "New regulations data" on). The run log says "woken by our server" (and "new data
  downloading").
- `app.json`: expo-notifications `enableBackgroundRemoteNotifications: true` → `UIBackgroundModes` gains
  `remote-notification` (fetch, processing, remote-notification); `aps-environment` stays (development; Xcode's App
  Store export makes it production).

**C. App, Android** (written, unit-tested, not buildable yet): the same seam. `pushToken.android.ts` is null unless the
build says `EXPO_PUBLIC_FCM=1`: then the FCM token, env production. With it null, no token is asked, no push task is
defined or registered, `NotifyControls.platform.push` is absent, and the intro keeps today's words. `app.config.js` adds
`android.googleServicesFile` and the plugin `plugins/with-fcm-no-analytics.js` only with `EXPO_PUBLIC_FCM=1`, and
refuses that flag without `app/apps/mobile/google-services.json`.
- Firebase Analytics stays off: the app links only `firebase-messaging` (expo-notifications' own dependency since
  long before this ticket, inert without a Firebase project); no `firebase-analytics` dependency is added; and with FCM on
  the manifest carries `firebase_analytics_collection_deactivated=true` and `google_analytics_adid_collection_enabled=false`
  (checked in the introspected manifest with a placeholder file).
- **Today's Android build is unchanged:** `npx expo config --type introspect --json` before / after this branch's config
  changes: the whole Android mod result (manifest, gradle properties, strings, styles, colours) is byte-identical (sha256
  of the JSON `9ca64761c5bd2706…` both), `android` config identical; the only differences are iOS's `UIBackgroundModes` +
  `remote-notification` and the plugin option. The WorkManager check (`TASK`, `expo-background-task`) is untouched.

**Steps when `google-services.json` arrives (Android):**
1. Put it at `app/apps/mobile/google-services.json` (it is not a secret; commit it or keep it local and copy it in the
   build script).
2. Build with `EXPO_PUBLIC_FCM=1` (add `export EXPO_PUBLIC_FCM=1` to `deploy/staging/build-apk.sh` next to the other
   EXPO_PUBLIC lines). Prebuild then applies the Google Services Gradle plugin itself (Expo adds the
   `com.google.gms:google-services` classpath and `apply plugin` when `googleServicesFile` is set).
3. Set the worker secret (below) and send `POST /run/push`; on an Android phone with notifications on, the run log in
   More → Notifications says "woken by our server".

**D. Words** (`packages/core/src/alerts.ts`, pinned whole in `alerts.test.ts`):
- `NOTIFY_WORDS.introPushIos` (shown on iPhone when the build has push): "Notifications are made on this phone. When our
  gauge, salmon or in-season data changes, or new regulations data is out, our server sends this phone a silent signal to
  check now; the app then checks the gauges and the data itself. The server keeps only a push token for this phone, never
  your alerts, your waters or where you are. iOS may still hold a check back, often in Low Power Mode, and wakes no app
  that was swiped away until you open it again."
- `NOTIFY_WORDS.introPushAndroid` (shown once FCM is in): the same, "…and still checks about every 15 minutes at best on
  its own. The server keeps only a push token…".
- `UPDATE_READY_NOTE`: "New regulations data is ready to download" / "Open the app to download it."
- Run log words: "woken by our server", "new data downloading".

**For the privacy page and the stores (another chat's files; not edited here):**
- Privacy page, a new line under what the app sends: "Push token. If you turn notifications on, the app sends our server
  a push token for this phone (an address Apple or Google issue so a message can reach the app) and whether the phone is
  an iPhone or Android. We use it only to send a silent signal telling the app to check for new data; the signal carries
  no words, no waters and no location. We do not receive your alerts, the waters you watch or where you are. The token is
  deleted when you turn notifications off, or when Apple or Google tell us it no longer works. It is not linked to you or
  to an account and is not backed up."
- Same page, Android, once FCM is on: "On Android the signal is delivered by Google's Firebase Cloud Messaging. The app
  uses Firebase only for this; Firebase Analytics is turned off."
- App Store privacy (App Privacy): Identifiers → Device ID: collected, used for App Functionality, not linked to the
  user, not used for tracking. (The manifest already declares `NSPrivacyCollectedDataTypeDeviceID` with exactly that.)
- Google Play Data safety (once FCM is on): Device or other IDs — collected: yes; shared: no; processed ephemerally:
  no; required or optional: optional (sent only when the user turns notifications on); purpose: App functionality;
  encrypted in transit: yes; users can request deletion: yes (turning notifications off deletes it).

**Owner steps (plainly):**
1. APNs key: developer.apple.com → Certificates, Identifiers & Profiles → Keys → "+" → name it ("Can I Fish push") →
   tick "Apple Push Notifications service (APNs)" → Continue → Register → Download the .p8 file (Apple lets you download it
   ONCE; keep it safe) and note the Key ID shown on that page. The Team ID is the one already in hand (Membership).
2. For staging, in a terminal at the repo (each command asks for the value; paste it):
   - `npx --prefix app wrangler secret put APNS_KEY -c app/workers/feeds/wrangler.toml` (paste the whole .p8 text,
     BEGIN/END lines included)
   - `npx --prefix app wrangler secret put APNS_KEY_ID -c app/workers/feeds/wrangler.toml`
   - `npx --prefix app wrangler secret put APNS_TEAM_ID -c app/workers/feeds/wrangler.toml`
   then the feeds worker is deployed with this branch (main chat), and `/status` shows "Push wake-ups: ios ok".
3. Android (when you want it): console.firebase.google.com → Add project (Analytics: OFF when it asks) → Add app →
   Android → package name `ca.canifishthis.app` → download `google-services.json` and hand it over. Then Project settings
   → Service accounts → "Generate new private key" (a JSON file) and run
   `npx --prefix app wrangler secret put FCM_SERVICE_ACCOUNT -c app/workers/feeds/wrangler.toml` (paste the whole JSON).

### What needs a real phone
APNs end to end (the worker → Apple → phone; the simulator cannot receive from APNs, and Cloudflare Workers' outbound
fetch speaking HTTP/2 to APNs is proved only by the first real send — `/status` will show it), iOS's throttling of silent
pushes (Apple: "two or three per hour"), Low Power Mode holding them, a swiped-away app not being woken, the wake-to-
notice time against the acceptance's 2 minutes, a token turning over, and the production host with a TestFlight build.
