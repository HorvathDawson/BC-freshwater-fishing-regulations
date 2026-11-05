# T234 — Push notifications: regulation changes and condition alerts

Status: PARTIAL 2026-11-05 — phone-only v1 built (13b963619, 77790b907, 8b7b45866, dd0a49dde); phone checks pending (next APK, checklist below).

Source: owner 2026-10-28: "add ability to have push notifications for regulation changes too, or set push
notifications for specific condition changes or forecasted changes."

## What
1. **Regulation changes:** a push when something changes on a water the user follows (saved spot / followed water):
   in-season changes (T230 feed), DFO salmon openings/closures (T211 feed), a new release that changes that water's
   answer. Wording comes from the feeds verbatim; never a computed claim the page wouldn't make.
2. **Condition alerts:** user-set rules per water/gauge, e.g. "flow below X / above Y", "level rising", "forecast says
   flow will cross X in the next N days" (T225 gauge feed + forecasts), "opens tomorrow".
3. **Settings:** per-water follow, per-alert rules, quiet hours, a global off; everything opt-in.

## Owner direction 2026-10-28
- Native phone push (APNs / FCM via Expo Notifications), configured by the user inside the app: which waters/systems,
  which kinds, thresholds and reasons. (Web: optional later.)
- Regulation changes: notify that new data is available, with a notification action to download it right away
  (background download), or download automatically (user setting); otherwise it downloads on next open (T231).

## Proposed design (hybrid, rules stay on the device)
- The feeds worker sends a small push ("data changed: gauges / DFO region 2 / in-season / release r-…") to devices that
  follow anything affected — topic-style, only device token + followed water ids stored server-side (KV/D1).
- The device wakes (silent/background push where the OS allows), fetches the changed feed (T231 update service),
  evaluates the user's own alert rules LOCALLY (thresholds, forecasts, followed waters), and shows a local notification
  only when a rule fires. Rules never leave the phone; the server never computes per-user conditions.
- Release/regulation updates: notification with an "Update now" action (background download + apply) — or auto per
  setting; on iOS background time is limited, so large downloads continue on next open with progress.
- Why not purely on-device background fetch: iOS/Android schedule it loosely (hours), too slow for "opens tomorrow"
  or a flow threshold; why not purely server-side rules: needs every user's thresholds on our server.

## Shape (earlier defaults, superseded where they conflict)
- Native: Expo Notifications (APNs/FCM). Web: Web Push (service worker) where supported.
- Delivery: the T225 feeds worker evaluates changes after each run and sends pushes; device tokens + follows +
  alert rules stored server-side (Cloudflare D1/KV) — this needs a minimal per-device record, no account (T223) required;
  if T223 accounts exist later, follows sync across devices.
- Dedup + rate limit per device; every push deep-links to the water/part/view (T221 URL scheme).
- Privacy: device token + followed water ids only; delete on unsubscribe/uninstall.

## Owner questions
- Q-T234a: device-only (no account) for v1? Default yes.
- Q-T234b: which alert kinds in v1? Default: in-season + DFO changes on followed waters, flow thresholds.
- Q-T234c: timing relative to first release. Default: after first native release.

Depends: T225 (feeds worker, staging), T230 (in-season feed), T231 (update service), T223 (optional accounts).
Size: L. Owner: questions above.

## Owner 2026-11-05
- Wants: gauge alerts (a reading rises/falls past a threshold) and "regulation changes need downloading".
- **New data (release / regulation updates): phone only** — no server push. The phone's background check finds the new
  release and raises a local notification (download now / it downloads on next open).
- Asked whether phone-only can be more real-time for gauge alerts. Claude's answer (below); owner to confirm the route
  for gauge alerts.

### How real-time can phone-only be (Android)
- Background work (WorkManager via expo-background-task): every 15 min at best, while the phone is awake/charging;
  Doze defers it to maintenance windows when the phone sits idle — roughly every 1–2 h at first, longer in deep idle.
- Faster on-device options are closed or costly: exact alarms need SCHEDULE_EXACT_ALARM (Play allows it only for alarm/
  calendar apps); a foreground service polls freely but shows a permanent notification, drains battery, and Android 15
  caps dataSync services at 6 h/day.
- The data's own pace: ECCC gauges publish hourly (about an hour behind), RFC forecasts once a day, DFO and in-season a
  few times a day. So a 15–60 min phone check loses little against the data itself; only deep idle (hours) does.
- Server push (FCM high priority) is the only way to wake an idle phone within minutes.
- **Owner 2026-11-05, decided: phone-only v1 for gauge alerts too.** Background check every ~15 min (expo-background-task
  / WorkManager), local notifications (expo-notifications, local only — no push token, no server). Measure real delays
  on the owner's phone; FCM push only if alerts arrive too late. Q-T234a answered (device-only, no account, no server
  record at all); Q-T234b: v1 = gauge thresholds (rise/fall past a value) + new data/regulation update ready; Q-T234c: now.

### Owner 2026-11-05 (later): settings live in More
- "we should have a section in More where we configure the notifications." → **More → Notifications** (its own page,
  listed on the More tab only on a phone): the global switch, "New regulations data", the permission state (refused:
  said, with "Open the phone's settings"), the gauge alerts with Change and Delete, and "Last background checks". A gauge's
  **Alert me** (on its card in a water's Conditions) stays the one way to ADD an alert.

## Built (v1, 2026-11-05)
- **Packages** (deps.md): `expo-notifications` 57.0.24 (local only — no push token, no server), `expo-background-task`
  57.0.21 + its peer `expo-task-manager` 57.0.21 (WorkManager / BGTaskScheduler, `minimumInterval` 15 min). Native-only:
  `apps/mobile/src/notify.native.ts`; `notify.web.ts` is empty (web bundle unchanged; More lists no Notifications there,
  and no gauge offers Alert me).
- **Rules** (`@app/core` `alerts.ts`): per gauge, flow or level (the gauge's quantity, m³/s / m), "rises above" / "falls
  below". Once per crossing: the first reading only sets the side (a rule set while already past does not ring);
  inside → past fires; it re-arms only once back inside by 2 % of the line (min 0.05 m³/s / 0.01 m); a reading older
  than 6 h, missing, or the same reading again = nothing. Bound to the station + the water's durable item id.
- **Background check** (`@app/data` `notify/check.ts`): reads `obs.json` through the same `fetchObs` the app's kept week
  uses (only when an alert is set), and the release manifest (only when "new data" is on); "new data ready" once per
  data release, never for a release this app is too old for, never with nothing noted as in use. Offline = silent
  (logged "offline", nothing reported); any other failure reported + logged. A notification that could not be raised is
  raised by the next run. It never downloads a release: tapping "New regulations data is ready" opens the app, which
  takes the update as it always does (T231) under the mobile-data setting (T298).
- **Stored on the phone** (`spots/notify.json`, web key unused): switches, rules, each rule's side, the release in use
  (noted on every open), the last release told, the last 20 background runs (time, duration, result).
- **Permission:** read without a dialog on the page; ASKED only when the reader turns a switch on or saves an alert.
- **Tap:** a gauge alert opens its water's Conditions (`Shell` `openRequest` → `opener.item(item, "conditions")`; an
  unnamed reach's alert opens the Conditions map); a cold start from the tap works the same (`getLastNotificationResponse`).
- **Test channel:** `node deploy/staging/test-updates.mjs gauge <station> <flow|level> <value>` writes
  `test/feeds/live/obs.json` (the background check reads `ENDPOINTS.alertFeed`, under `test/` on the test channel);
  `reset` copies the live obs.json there; `release [tag]` makes a "new" release for the update-ready notice.

## Emulator / phone checklist (next APK — the APK is held for the batch)
Build on the staging **test** update channel (Updates log → channel), Android 13+ image; `reset` first.
1. Fresh install: More shows **Notifications**; the page says everything Off and "The phone will ask you…"; no system
   dialog appeared on open (POST_NOTIFICATIONS not asked at launch).
2. Turn **New regulations data** on → the system dialog appears once; Allow → the page says "This phone lets the app
   notify you." `adb shell dumpsys jobscheduler | grep -A3 canifishthis` (or `adb shell dumpsys activity service
   WorkManager`) shows the periodic work registered, ~15 min.
3. `test-updates.mjs release a1`, background the app, force the job: `adb shell cmd jobscheduler run -f
   ca.canifishthis.app <job id from step 2>` → one "New regulations data is ready" notification; force again → none
   (once per release); `release a2` + force → one more. Tap it → the app opens and applies the update ("Updated to …"),
   on mobile data only as the Data setting allows.
4. Open a gauged water (e.g. Chilliwack River) → Conditions → **Alert me** → "Rises above" 5 % over the reading → Save;
   "1 alert on this gauge"; More → Notifications lists it. `test-updates.mjs gauge <its station> flow <below the line>`,
   force the job (sets the side, no notification); `gauge … flow <above>` + force → ONE notification "Chilliwack River:
   flow above …" on the "Gauge alerts" channel; force again with the same reading → none; a reading just under the line
   (inside the 2 % margin) then above again → none; well under, then above → one more.
5. Tap the gauge notification with the app (a) in the background and (b) swiped away → it opens on that water's
   Conditions both times; it does not re-open after an update is applied.
6. Deny path: clear app data, save an alert, Don't allow → the card says notifications are blocked with "Open the phone's
   settings" (opens the app's settings page); More → Notifications says the same.
7. Offline (airplane mode) + force → no notification, "Last background checks" shows "… · offline"; no crash report sent.
8. Turn the global switch Off → the periodic work is cancelled (dumpsys); On again → registered again.
9. Logcat at start: FirebaseApp says it could not initialise (no google-services.json) and nothing else; no crash.
10. Leave the phone idle overnight with an alert set: "Last background checks" gives the real cadence under Doze (the
    owner's measurement for the push-or-not decision below).

## Left
- The phone checks above (needs the batch APK).
- Measure real delays on the owner's phone (the runs log); FCM push only if alerts arrive too late (owner 2026-11-05).
- Later kinds (not v1): forecast crossings, in-season / DFO changes on followed waters, quiet hours; web push.

