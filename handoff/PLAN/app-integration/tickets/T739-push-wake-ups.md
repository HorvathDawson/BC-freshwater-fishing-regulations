# T739 — Push "wake-ups" so alerts and notices arrive on time (iPhone first, Android as a second step)

Status: IN PROGRESS (both steps approved by the owner 2026-11-14: iPhone, then "for android too."; iOS chat, branch `ios/t739-push`; ANDROID PUSH IS OFF THE TABLE FOR NOW, owner 2026-11-16 12:08 on Firebase: "we dont want this right now"; iPhone push unchanged).

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

## Owner answer, 2026-11-15 evening (OWNER-TODO.md "owner answers, round 6", verbatim: "Yes to both (Recommended by it)")
The proposed privacy-page line ("Push token. If you turn notifications on, the app sends our server a push token for
this phone …") and the App Store privacy answer (Identifiers → Device ID: collected, App Functionality, not linked to
the user, not used for tracking) are approved as written (To-Do row `t739-privacy-wording`). Left (Claude): the line
into the privacy page's source on the push branch; the Android / Firebase line and the Play Data safety answer still
wait for the Firebase decision. Putting the page live is the owner's, with the production release. This removes the
wording block on merging the push code; the merge order (push last) stands.

## Owner, 2026-11-16 12:08 (question dialog in the main chat): no Firebase, no Android push for now
Asked whether "for android too" was his exception to "self-host everything" for push delivery through Google's
Firebase, he typed: "we dont want this right now". So Android gets no push and needs no Firebase project. To check
when the push branch comes up for merge (it is the last of the held merges): the Android app must not register a
token or need `google-services.json`; the worker's FCM sender may stay as dead code only if nothing can call it
(else it comes out). iPhone push is unchanged. The 200 registrations a day per address stand as built.

## Results

### What was built (branch `ios/t739-push`)
**A. Feeds worker** (`app/workers/feeds/src/push.ts`, wired in `src/index.ts`, `src/status.ts`):
- `POST /push/register` / `POST /push/unregister` with `{platform: "ios"|"android", token, env: "sandbox"|"production"}`.
  Validated (an iOS token is 64–200 hex, an Android one 32–4096 url-safe characters, Android env is production, any other
  field refused), rate-limited per address exactly like `POST /crash` (R2 counter, salted address hash, 2 s apart, 200 a day (follow-up 2: was 20),
  counters deleted with the others by the daily purge), needs `REPORT_SALT` (503 without). Stored in R2 at
  `private/push/tokens/<sha256(token)>.json` = `{format, platform, token, env, at}`: no address, no install id, nothing else.
  Never backed up (`backup.ts personalKind` → "never"): a lost store is rebuilt by the phones, because a phone with
  notifications on registers its token again at every app start (follow-up 2; before it, an unchanged token was never
  re-sent, so a token the worker had deleted stayed gone until the token changed).
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
  `/push`, `env.ts pushUrlFor`) ONCE PER APP START even when unchanged, again within the run only when it changes (the
  old one withdrawn), withdrawn when they are turned off. What was sent is kept in `push-token.json`; a failed send throws
  (reported; offline and the worker's 429 limit are the environment's) and the next start tries again.
  Follow-up 2 (2026-11-15): "once per app start" is new. A start is a cold start or a background launch (a push or the
  periodic task launching the app); `NotifyRoot` mounts on each (`App.tsx:611`) and syncs once. Test:
  `notify/push.test.ts` "an unchanged token is registered once per app start, not once per sync". The worker's
  per-address limit went from 20 to 200 a day (`PUSH_PER_DAY`): a normal day for one phone is a few to a few dozen
  starts (iOS throttles background pushes to a few an hour), and several phones can share one address (a home, a
  carrier's NAT).
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
- The new-data notices (follow-up 2026-11-14): the EXISTING notice of the periodic check is unchanged —
  `UPDATE_READY_NOTE` "New regulations data is ready" / "Open the app to update." (the first commit had changed it; restored
  in the follow-up). The PUSH path has its own, `UPDATE_DOWNLOAD_NOTE`: "New regulations data is ready to download" /
  "Open the app to download it.", raised only by a run a push woke (`CheckDeps.woke`). Android has no push yet, so it
  never says the new one. Both share the once-per-release memory (`toldRelease`): a release told by either is not told
  again by the other. Pinned whole in `alerts.test.ts`, `check.test.ts`, `push.test.ts`.
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
   The bundle id is NOT a secret: it is in the code (`APNS_TOPIC = "ca.canifishthis.app"`, worker `src/push.ts`).
   Production later: the same three (and `FCM_SERVICE_ACCOUNT`) with `-c app/workers/feeds/wrangler.prod.toml`.
3. Android (when you want it): console.firebase.google.com → Add project (Analytics: OFF when it asks) → Add app →
   Android → package name `ca.canifishthis.app` → download `google-services.json` and hand it over. Then Project settings
   → Service accounts → "Generate new private key" (a JSON file) and run
   `npx --prefix app wrangler secret put FCM_SERVICE_ACCOUNT -c app/workers/feeds/wrangler.toml` (paste the whole JSON).

**The catch sites listed in FINE (`app/tools/failures.test.ts`), each one line (none hides a failure the reader could see):**
- `workers/feeds/src/push.ts:196` an APNs answer without a JSON body: its HTTP status alone decides (non-200 is still counted failed).
- `workers/feeds/src/push.ts:205` the same for an FCM answer.
- `workers/feeds/src/push.ts:280` a send that never reached APNs/FCM: counted `failed` with its reason in the send's result and `/status`, and the run prints it as a step error.
- (follow-up) Two former FINE sites now REPORT and are out of the list (`push.ts` FINE n 5 → 3): a stored token record that
  does not parse is counted (`unreadable` on the send's result and `/status`: "N stored token record(s) did not parse
  (deleted)"), makes the send not ok, is logged and is a run step error; a "release last seen" record that exists but does
  not parse is replaced with the release now published and THROWS ("private/push/state.json did not parse (…); replaced with
  r-…, so no wake was sent for it"), which the run records as a step error. A missing record stays a first look. Tests in
  `test/push.test.ts`.
- `packages/data/src/notify/push.ts:80` the sync queue's own link (`chain.then(f, f)`): each sync's failure goes to its caller's promise, which `NotifyRoot` reports; the queue only goes on.
(Also: two credential failures in `sendWake` now log with `console.error` and are counted, `push.ts` APNs key / FCM credential.)

### Simulator proof (2026-11-14, build `1f6272410`, `deploy/staging/build-ios-sim.sh --testbed` via heavy.sh lane 3, device `t739-17` iPhone 17 / iOS 26.5)
Set-up: the build pointed at local stand-ins through the test bed's own switches: `EXPO_PUBLIC_REPORT_URL=http://127.0.0.1:39257/report`
(the app derives `/push` from it) served by `app/tools/testbed/crash-stub/server.mjs` (now also the worker's own `handlePush`),
and `EXPO_PUBLIC_DATA_BASE=http://127.0.0.1:39258`, a scratch proxy to data-staging that can answer the test-channel manifest
with another `data_release` (a "newer release" without any R2 write). The built binary's `__TEXT,__entitlements` holds
`aps-environment = development` (and associated domains); Info.plist `UIBackgroundModes` = fetch, processing, remote-notification.
- **(a) register — PASS.** Turning on "New regulations data" (notifications on, iOS permission allowed) sent the token: the
  stub held `{platform: "ios", env: "sandbox", token: 160 hex}`; nothing else in the body. Shot `03-notifications-on.png`
  (the push intro shown, whole). Re-registered after a later re-enable (stub log 05:54:42 201).
- **(b) silent push — NOT PROVABLE ON THIS SIMULATOR; handler to prove on a phone.** `xcrun simctl push <udid>
  ca.canifishthis.app` with `{"aps":{"content-available":1},"k":"check"}` and with an alert + content-available payload:
  - app in front: the simulator log shows SpringBoard's `SBBackgroundFetchTask` → `UIFetchContentInBackgroundAction`, "Finished
    … with result: NoNewDataAvailable" 4 ms later, "Not delivering user visible notification … because it has no alert";
    no request reached the data proxy, the check's run log stayed empty (`notify.json runs: []`).
  - app in background (suspended): the same fetch action, NoNewData after 46 ms, suspended again 1.2 s later; no request.
    The alert-type push showed its banner (shot `bg-alert-1.png`: "T739 test"), so delivery to the app works; the
    JS task still did not run.
  - app terminated: the push cold-launched the app in the background; the app's own boot and update service then read the
    manifest (proxy: `obs.json`, `test/release/manifest.json` → the "newer" release, the feed indexes) and staged and applied
    it (`held` became that release; its files are the same as the installed set, so 0 bytes were fetched). That is the
    ordinary start, not the push task: the run log stayed empty.
  - The task IS registered: the app's preferences hold `EXTaskService.mainApplication.tasks."canifishthis-push-wake"` with
    consumer `ExpoNotifications.NotificationsBackgroundTaskConsumer`, and no failure was reported to the stub. The
    consumer runs only for the remote-notification launch reason; the simulator hands the push over as a background
    FETCH (expo-task-manager's `performFetch` path), which the consumer does not take. So on this simulator the push task
    cannot be reached by `simctl push`; there is no test-bed call that starts it directly (none was invented). What the
    task runs (`runWake`: update check, then the same check; one notice for a held release, none for one downloading,
    none twice) is proved by unit tests (`packages/data/src/notify/push.test.ts`), not on a device.
- **(c) unregister — PASS.** Turning Notifications off sent `POST /push/unregister`; the stub held 0 tokens (shot
  `10-notifications-off.png`).
Shots and logs: the agent's scratchpad `shots/` (03, 05, 06, 09, 10, bg-alert-*), `session.log`, `session2.log`,
`proxy-*.log`, `simlog.txt` (not committed).

### Why the silent push did not run on the simulator (follow-up, settled by reading the code and the log)
**(c) The payload pushed** (`xcrun simctl push <udid> ca.canifishthis.app <file>`), whole:
`{"Simulator Target Bundle":"ca.canifishthis.app","aps":{"content-available":1},"k":"check"}` — the same `aps` and `k`
the worker sends (`APNS_PAYLOAD`); the alert trials added `"alert":{"title":"T739 test","body":"alert-type push with content-available"}`.

**The delegate chain (node_modules, SDK 58):**
- `expo/ios/Expo/ExpoAppDelegate.swift:147-150` forwards `application(_:didReceiveRemoteNotification:fetchCompletionHandler:)`,
  and `:209-211` forwards `application(_:performFetchWithCompletionHandler:)`, each to
  `expo-modules-core/ios/AppDelegates/ExpoAppDelegateSubscriberManager.swift` (`:213-251` and `:398-433`), which calls every
  subscriber that implements it and aggregates the results.
- `expo-task-manager/ios/TaskManagerAppDelegateSubscriber.swift:9-10`: `performFetch…` → `runTasks(with:
  EXTaskLaunchReasonBackgroundFetch, userInfo: nil …)`; `:13-14`: `didReceiveRemoteNotification…` → `runTasks(with:
  EXTaskLaunchReasonRemoteNotification, userInfo: userInfo …)`.
- `expo-task-manager/ios/EXTaskManager/EXTaskService.m:571-597`: only tasks whose consumer `supportsLaunchReason:` the reason
  are added; with none, the request is evaluated at once (NoData). `:708-717` `_addTask` calls the consumer's
  `didBecomeReadyToExecuteWithData:`, and `:313-326` `executeTask` NSLogs "EXTaskService: Executing task '…' for app '…'."
  `:776-777`: a cold launch whose launch options carry `UIApplicationLaunchOptionsRemoteNotificationKey` is the
  RemoteNotification reason too.
- `expo-notifications/ios/ExpoNotifications/Notifications/Background/NotificationsBackgroundTaskConsumer.swift:7-8`:
  `supportsLaunchReason` is `launchReason == EXTaskLaunchReasonRemoteNotification` ONLY; `:31-34` it then executes the task
  with the payload. No condition on the payload's shape (alert / sound absent is fine).
- `expo-notifications/ios/ExpoNotifications/Notifications/NotificationsAppDelegateSubscriber.swift:21-26` also takes
  `didReceiveRemoteNotification` (it answers NoData when no JS delegate claims it; the aggregator still waits for the task).

**(a) The call the simulator made:** the app's own log at each push (`log show --predicate 'process == "CanIFish"'`) has
exactly one line: `Received action(s) in scene-update: <UIFetchContentInBackgroundAction …>`; SpringBoard's log says
`Executing <SBBackgroundFetchTask: app<ca.canifishthis.app>…>` then `Finished … with result: NoNewDataAvailable` (4 ms in
front, 46 ms in the background); and the notification itself was added by `CoreSimulatorBridge` ("Adding notification
request … to destinations: Default"), not by `apsd` (no apsd line at the push times). So `simctl push` injects the
notification through the simulator bridge and pokes a background-FETCH task.

**(b) Did it reach expo-task-manager, and why no task ran:** a remote-notification call would have run our task: the task
was registered and restored (the app's preferences: `EXTaskService.mainApplication.tasks."canifishthis-push-wake"`,
consumer `ExpoNotifications.NotificationsBackgroundTaskConsumer`; the cold-launch log: "EXTaskService: Restoring tasks
configuration"), the consumer has no payload condition, and `executeTask` would have logged "EXTaskService: Executing
task" — that line appears NOWHERE in the app's log for any of the four pushes. The 4–46 ms NoData answer is the fetch path
with no fetch consumer (`TaskManagerAppDelegateSubscriber.swift:9-10` → reason BackgroundFetch → our consumer refuses it
at `NotificationsBackgroundTaskConsumer.swift:7-8`). The cold launch (terminated app) carried a fetch action at scene
creation (`scene-create: <UIFetchContentInBackgroundAction>`), not a remote-notification launch option. Not the payload
(it is the documented shape), not registration timing (the task was restored before the call), not missing wiring (both
delegate methods are forwarded). **Nothing of ours to fix.**

**What a real phone does differently:** there APNs → `apsd` → UIKit calls `application(_:didReceiveRemoteNotification:
fetchCompletionHandler:)` for a background push (`apns-push-type: background`, `content-available: 1`), which is the path
quoted above (`ExpoAppDelegate.swift:147-150` → `TaskManagerAppDelegateSubscriber.swift:13-14` → reason
RemoteNotification → `NotificationsBackgroundTaskConsumer.swift:7-8,31-34` → our JS task). The simulator's bridge never
takes that path, so the first real-phone check is still: one push → the run log says "woken by our server".

### Real-phone test: steps and what to read (TestFlight build 35, staging worker)
Build 35 was made by `deploy/staging/build-ios-testflight.sh`, which sets `EXPO_PUBLIC_APNS_ENV=production` (`:57`), so the
phone registers as "production" (`apps/mobile/src/pushToken.ios.ts:9`) and the worker sends to `api.push.apple.com`
(`workers/feeds/src/push.ts` `APNS_HOSTS`, the request URL in `apnsRequest`). The APNs key must allow Production.
NOTE: build 35 is from `650665c61`, BEFORE follow-up 2: it re-sends an unchanged token only when the token changes, so
if its first register succeeded and the worker later lost the token, turn Notifications off and "New regulations data"
on again (a build from this branch's later head registers at every start by itself).
1. **Register (the owner, in the app).** More → Notifications → turn on **"New regulations data"** ("Tell me when a
   newer regulations release is ready to download."). The "Notifications" switch alone does not register: something to
   check must be on as well (`data/src/notify/settings.ts` `notifyActive`); turning "New regulations data" on also turns
   Notifications on (`ui/src/notify.ts` `setUpdateReady`). The iPhone's permission prompt ("… Would Like to Send You
   Notifications") appears at that moment if it was never answered (`allow` → `ask`); the token is sent at the same time
   and does not wait for the answer (a silent push needs no permission). If the worker was not deployed yet, the next
   app start registers (nothing is stored after a failed send).
2. **Check it arrived (Claude / the coordinator).** `GET https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status.json`
   → `push.missing.ios` must be `[]`. After step 3, `push.last.ios.tokens` ≥ 1.
3. **Send one wake by hand** (from the repo root; the token is never printed):
   `curl -sS -X POST -H @<(printf 'Authorization: Bearer %s\n' "$(cat app/workers/feeds/.run-token)") 'https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/run/push?reason=owner-phone-test'`
   The answer (HTTP 200 if ok, 502 if not) is the send: `{at, reason, ok, unreadable, ios:{configured, missing, tokens,
   sent, dead, failed, errors[]}}` (no android block since 2026-11-20); a send that threw answers 500 `{ok: false, error}`. **APNs accepted it when `ios.sent` ≥ 1** (APNs answered 200).
   `ios.dead` ≥ 1 with "BadDeviceToken" in `ios.errors` would mean the wrong host for that token (it is then deleted);
   any other APNs reason is in `ios.errors`. The same object is `push.last` on `/status.json`, and the HTML `/status` has
   a "Push wake-ups" table. `wrangler tail` on the feeds worker prints `{"job":"push","reason":…,"ios":{…}}`.
4. **What the owner sees.** With the app in the background (not swiped away), a few seconds after the wake: More →
   Notifications → tap **"Last background checks"** to open it. The newest line reads
   `<date> at <time> PT · <n> s · woken by our server, …` — `nothing to tell`, or `new data downloading`, or `new data
   ready` (`core/src/alerts.ts` `NOTIFY_WORDS.run.woke` / `.downloading`; `ui-native/src/menu/NotificationsPage.tsx`, the
   `notify-runs` card, shown on every build). The periodic task's lines never say "woken by our server". Leave the page
   and open it again to see a new line.
5. **A trace without the phone.** A successful wake sends nothing to the feeds worker. It reads the data host:
   `wrangler tail bc-fishing-r2-staging` (`deploy/staging/data.wrangler.toml`) prints `release/manifest.json app=<version>`
   and `…/index.json app=<version>` for app requests (`deploy/staging/data-worker.js:34-35`); those lines seconds after
   the push, with the app in the background, are the wake (an app start looks the same: read them by time).
6. **Off.** Turning Notifications off sends `POST /push/unregister`; the next `/run/push` shows `ios.tokens` one fewer.

### What needs a real phone
FIRST: that a real silent push starts the push task at all (run log "woken by our server"; the simulator could not show
it, above). Then APNs end to end (the worker → Apple → phone; the simulator cannot receive from APNs, and Cloudflare Workers' outbound
fetch speaking HTTP/2 to APNs is proved only by the first real send — `/status` will show it), iOS's throttling of silent
pushes (Apple: "two or three per hour"), Low Power Mode holding them, a swiped-away app not being woken, the wake-to-
notice time against the acceptance's 2 minutes, a token turning over, and the production host with a TestFlight build.

### Follow-up 3 (2026-11-16): iPhone build 36 never registered: the failure is now said and reported (ledger row T822)
**What happened.** The owner, on TestFlight build 36 (`ios/next` d658f88ac, staging, `EXPO_PUBLIC_APNS_ENV=production`),
turned on More → Notifications → "New regulations data", allowed notifications, restarted the app; two `POST /run/push`
on the staging worker (21:34, 21:37) found `ios.tokens: 0`, and a `wrangler tail` from ~21:40 saw no request from the phone.

**What was ruled out (evidence).**
- Address: build 36 is a staging build, so `PUSH_URL` = `https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/push`
  (`env.ts` `pushUrlFor(reportUrlFor("staging"))`), the worker that answers. A run-time switch to production (About;
  `EXPO_PUBLIC_ENV_SWITCH=1`) is not it: `feeds.canifishthis.ca` is NXDOMAIN and `data.canifishthis.ca` has no manifest
  (404), so the app would not have worked at all.
- Entitlement: build 36's own log (the TestFlight worktree's `app/dist/ios/build.log`) signs `aps-environment` into the
  archive and `** EXPORT SUCCEEDED **` with App Store signing (which makes it production); token call cannot fail for a
  missing entitlement without the export failing first.
- Wiring: `notify.native.ts` makes the registrar (`pushSeam && PUSH_URL`); `NotifyRoot` calls `push(notifyActive)` at
  every start and on every change; "New regulations data" sets `on` + `updateReady`, so `notifyActive` is true.
- A refusal from the worker (400/404/503): it throws a plain Error, which `reportFailure` sends to `/crash`. The staging
  worker's `/crashes` (since 11-01, all classes, `test=1`, `background=1`) holds NO report from a real iPhone at all
  (the only iOS kinds are the simulator's test runs), so no such error was raised on the phone.

**What is left: two paths that said nothing (the cause is one of them).**
1. The token call had no deadline. expo-notifications 58.1.6 `PushTokenModule.getDevicePushTokenAsync` calls
   `registerForRemoteNotifications()` and settles only from iOS's `didRegister…` / `didFailToRegister…` callback; when iOS
   answers neither, the promise never settles: no request, no error, and the registrar's queue is held for the whole
   run, so turning the switch off and on again queues behind it and sends nothing either.
2. Offline and the worker's 429 were marked "the environment's": thrown, never reported, never shown. The worker's
   per-address gap is 2 s (`PUSH_GAP_MS`), so a quick off → on (unregister, then register) lost the register to a 429
   and nothing was kept; only the next cold start tried again.
The tail at ~21:40 does not separate them: the registrar sends once per app START (a cold start, or a background
launch), not while the app is in use, and the restarts came before the tail began. The worker has Workers Logs on
(`[observability]` in wrangler.toml): the dashboard's logs for `bc-fishing-feeds-staging`, 2026-11-16 21:00–21:40 local (04:00–04:40 UTC 11-17),
filtered on `/push`, would say whether a register (and its status) arrived before the tail.

**The fix (branch `app-fix/t822-push-register`, from `ios/t739-push`).**
- `packages/data/src/notify/push.ts`: the token has a deadline (`PUSH_TOKEN_WAIT_MS`, 30 s), then a REPORTED error
  ("push register: the system gave no push token in 30 s (ios: the request for one was never answered)") and the queue
  goes on; a token iOS hands over later still registers through `onToken`. A 429 is waited out once (`PUSH_RETRY_MS`,
  3 s) and a second one is a reported defect ("push register: HTTP 429 twice (the worker's per-address limit)").
  Offline stays unreported (failure.ts) but is shown. Every sync's outcome is `status()` (idle / sending / registered /
  withdrawn / failed with why) with `subscribe`.
- `packages/ui/src/notify.ts` (`NotifyPlatform.pushStatus`, `NotificationsVM.pushStatus` / `retryPush`),
  `packages/ui-native/src/menu/NotificationsPage.tsx`: a line under the intro on More → Notifications, words in
  `NOTIFY_WORDS.push` (pinned whole in `alerts.test.ts`): "Server wake-ups: this phone is registered (2026-11-16 at 21:52
  PDT)." / "Server wake-ups: this phone is not registered: <why>. It tries again when the app next starts." with
  "Try again" (sends the token again now).
- `apps/mobile/src/notify.native.ts`: hands the registrar over as `pushStatus`.
- Tests: `push.test.ts` (429 waited out, 429 twice reported, a token that never comes fails at the deadline, frees the
  queue and a late token still registers, offline shown), `Notifications.test.tsx` (the line, "Try again", no line
  without push), `alerts.test.ts` (the words whole). `tools/failures.test.ts` FINE count unchanged.

**To test again (owner / app chat).** Needs a NEW TestFlight build from a branch carrying this (the iOS line, e.g.
`ios/next` + this branch); no worker or DNS change. On the phone: More → Notifications → the "Server wake-ups" line.
"registered (time)" → `POST /run/push` should count `ios.tokens: 1`. "not registered: …" → the words say which path
(no token from iOS; HTTP n; no connection), and a defect also lands in the staging `/crashes`.

### Merge into the shared branch (2026-11-20, lane L5, branch `lane/l5-push`)
- Pushed `app-fix/t822-push-register` (f4598b112, the T822 fix on top of `ios/t739-push`) to origin; branched
  `lane/l5-push` from it and merged redesign/stream-sections 27e98e4eb INTO it (485dcb2fd). Conflicts: `notify.native.ts`
  (the one `CHECK` for the periodic task and the push wake now uses T741's conditional GETs + kept copies and T676's
  per-run feed gate), `alerts.ts` NOTIFY_WORDS (both word sets), `notify/check.ts`, `notify/index.ts`,
  `spots/index.ts` (both import / export sets), `workers/feeds/src/index.ts` (`/status.json` carries `push` and T759's
  `dfo` / `dfoIssues`), the ledger (stream-sections' copy) and this ticket (results kept, the owner's 11-15 / 11-16
  answers placed above them). App.tsx, config / env, app.config.js, failures.test.ts and build-ios-testflight.sh merged
  clean.
- **Android push removed** (owner 2026-11-16, above), 2102890de: the worker refuses `platform: "android"` (400 "no push
  on Android"), the FCM sender / OAuth assertion / `FCM_SERVICE_ACCOUNT` / the android status row are gone, and a stored
  record that is not an iPhone's is a counted fault (deleted, never sent to). In the app `pushToken.android.ts` is
  `null` always (it stays as a file only because `check-platform` wants a whole ios + android set), `EXPO_PUBLIC_FCM`,
  `google-services.json` and `plugins/with-fcm-no-analytics.js` are gone, and `introPushAndroid` with them. So the
  "What was built" Android lines above are history.
- **Privacy line**: the owner-approved paragraph, as written, is a "Push token" section of `app/tools/seo/html.ts`
  (pinned whole in `site.test.ts`); it goes live with a production release. Its words still name Google / Android: a
  wording question is in OWNER-TODO (2026-11-20 07:33), default "leave as approved".
- Checks: `pnpm -C app check` green (763 files, 9004 tests); worker suite 555 green; the push / notify / alerts /
  Notifications / failures / site tests green.
- **iOS simulator** (iPhone 17, iOS 26.5, `build-ios-sim.sh --testbed` of 2102890de with `EXPO_PUBLIC_REPORT_URL` on the
  crash stub, heavy lane 2; scenario `scenarios-ios/push.mjs`, run-ios `push`): PASS. "New regulations data" on → the
  stub held one `{platform: ios, env: sandbox, token: 160 hex}` (body: platform, token, env, nothing else), the line
  "Server wake-ups: this phone is registered (2026-11-20 at 07:50 PT)"; a cold start in dark registered the same token
  again (still one record; once per app start); Notifications off → `/push/unregister`, the stub empty, the line "Server
  wake-ups: off (no notification is on); our server keeps no push token for this phone.", and a restart brought no token
  back. Shots `../T739-shots/ios-notifications-{1-on-light,2-on-dark,3-off-dark,4-off-light}.png`. (Seen: with
  notifications off the intro still reads in the present tense, "our server sends this phone a silent signal"; the line
  under it says off. Not changed.)
- **Android benches** (emulator, test-bed APK of ae7fc72db with `--crash-stub`, patched MapLibre, `run.mjs --no-r2
  install crash push-absent`): 3/3 PASS. install: first-run download and map in 70 s, every source ok; crash: one exit
  record at the next start, none after a normal stop; push-absent (new, `scenarios/push-absent.mjs`): with "New
  regulations data" on, the stub held no token and saw no push call after 15 s, no "Server wake-ups" line, the periodic
  intro ("… Nothing is sent to a server … about every 15 minutes at best.").
- Left (main chat): review + merge `lane/l5-push` into redesign/stream-sections, deploy the STAGING feeds worker and
  check `/status.json` `push.missing.ios == []` (no `android` key any more), TestFlight build (`EXPO_PUBLIC_APNS_ENV=
  production`, already in build-ios-testflight.sh). Then the owner's iPhone test (T822's "To test again"); prod APNs
  secrets + prod worker are his.

### Independent review of `lane/l5-push` 0e6d2bf94 (2026-11-20): fixed on the lane before the merge
- **M1, DFO woke nearly every run.** `wakeReason` woke on `notices > 0`, and an unchanged region's notices are re-offered
  every run (dfo/live.ts, T759 makes them common); a T851 one-region retry woke too (`regions` counted the whole index,
  `unchangedRegions` only that run's region). Now a DFO run wakes only when `dfoAnswersChanged(last feed, new feed)`
  (`push.ts`): every place's answers, the verdict / gear tables, the issues of the regions read this run, and each
  region's `asof` / `stale` / `held`, compared with keys sorted; never the run's bookkeeping (`at`, `checked`,
  `failures`, the page hash, `issues.unread`) or its notices. The run reads the last `feeds/dfo/index.json` first (one
  more R2 get; unreadable: a step error and the run counts as changed). Tests `test/push.dfo.test.ts` on real runs over
  the saved pages: an unchanged run with carried notices does not wake, a one-region retry does not, a dropped place does.
- **M2, a thrown or lost wake left no trace.** A wake that throws is written to the push status as
  `{at, reason, ok: false, error}` (`writePushFailure`; the run and `POST /run/push`), shown on /status ("not sent: …");
  `sendWake` whose own status write fails throws after sending; a status that cannot be written at all fails the run.
  `newRelease` no longer records a new release: the run records it (`markReleaseSeen`) only after its wake went out ok,
  else the next gauge run wakes for it again. Tests `test/push.run.test.ts` (real in-season runs over a bucket failing on
  cue) and `push.test.ts`.
- **Test gaps (mutations that survived):** a token over 200 hex refused (and 63), unregister rate-limited, a body over
  1 KB answered 413 (`push.test.ts`); the check's sources moved out of `notify.native.ts` into `@app/data`
  `createCheckSources` with `sources.test.ts`: a new feed gate per run, each file's own kept copy and its validator.
- **L1, register on every start met the shared-address limit.** The registrar now registers only when the token, its
  environment or our address changed, or the last register is over 7 days old (`PUSH_REFRESH_MS`, the time kept on the
  phone in `push-token.json`); a second 429 is no longer a reported defect: the line says "Server wake-ups:
  registration pending: our server is busy for this connection. It tries again when the app next starts." with "Try
  again". Tests `notify/push.test.ts`, `Notifications.test.tsx`, `alerts.test.ts`.
- **Leftovers (not fixed here):**
  - **L2**: `sendWake` lists tokens with `listKeys(…, 100_000)`, a silent cap: past 100k registered phones the rest get no
    wake and nothing says so (`push.ts`, the sender's list).
  - **L3**: one R2 get plus one APNs fetch per token, inside the gauge run's invocation (20 at a time): past a few
    hundred phones the subrequest budget wants a Cloudflare Queue (the scale note above).
- The simulator run and the Android benches above are of ae7fc72db, before these fixes: the iPhone line "a cold start
  registered the same token again" is the old once-per-start rule (L1 now skips it within 7 days; `scenarios-ios/push.mjs`
  passes either way). Not re-run on a device for the fixes (unit and run tests only); the TestFlight build will carry them.
