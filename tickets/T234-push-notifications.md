# T234 — Push notifications: regulation changes and condition alerts

Status: POSTPONED (deferred, owner 2026-11-05; T437 aligned with the ledger) — what was built: PARTIAL 2026-11-05 — phone-only v1 built (13b963619, 77790b907, 8b7b45866, dd0a49dde); v1.1 reader-configured alerts, cadence, new-app-version notice and changelog built (665dcb49b, 3dbc91380, 981c60cf5); phone checks pending (next APK, checklist below).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T234 | notifications, phone-only: v1 gauge alerts + new data ready; v1.1 reader-set alerts (above/below/±% in 24 h), cadence (once per water per day default, own gap), More → Notifications cards + editor, new-app-version notice, changelog (About → What's new, bell bullets) | POSTPONED (deferred, owner 2026-11-05) | 13b963619, 77790b907, 8b7b45866, dd0a49dde, 665dcb49b, 3dbc91380, 981c60cf5 | phone checks on the next APK (checklist 1–17 in the ticket); owner: changelog draft, cadence readings; measure real delays, FCM only if late |

Source: owner 2026-10-28: "add ability to have push notifications for regulation changes too, or set push
notifications for specific condition changes or forecasted changes."

## What
Phone-only notifications (owner 2026-11-05: no server push, no push token, no account): gauge alerts and "new
regulations data is ready", plus a new-app-version notice and a changelog. Later kinds (not v1): forecast crossings,
in-season / DFO changes on followed waters, quiet hours; web push. Server push (FCM high priority) only if measured
alerts arrive too late.

## Owner calls (answered)
- 2026-10-28: native push configured in the app; new data: notify with download now, or auto, else on next open (T231).
- 2026-11-05: phone-only v1 for gauge alerts too; background check every ~15 min (expo-background-task / WorkManager),
  local notifications. Q-T234a device-only, no account, no server record at all; Q-T234b v1 = gauge thresholds + new
  data ready; Q-T234c: now. Measure real delays on the owner's phone; FCM push only if alerts arrive too late.
- 2026-11-05 (later): "we should have a section in More where we configure the notifications." → More → Notifications.
- 2026-11-05 (v1.1): "on x gauge if it rises to y point or z times then send notifications … they wanna know when a river rises or falls past
  a point. Only should notify once on a river per day, or user can set max cadence … Make the UI for this nice please, and
  in a menu in More." Also a changelog: "What's new in <version>" in the update prompt, every version on a page in More.
- 2026-11-05: blocked crossings are dropped, not queued: "if we get a rise then fall in a short time we don't want that queueing a message — until it happens again after it is
  open for notifications again." (`pending` removed; `dropped` count in the run log.)

Why phone-only is close enough (Android): background work every 15 min at best; Doze defers it to roughly every 1-2 h
idle, longer in deep idle; exact alarms need SCHEDULE_EXACT_ALARM (alarm/calendar apps only); a foreground service
shows a permanent notification and Android 15 caps dataSync at 6 h/day. ECCC gauges publish hourly, RFC forecasts
daily, DFO / in-season a few times a day.

Depends: T225 (feeds worker, staging), T230 (in-season feed), T231 (update service), T223 (optional accounts).
Size: L. Owner: questions above.

## Built
- v1 (13b963619, 77790b907, 8b7b45866, dd0a49dde): `expo-notifications` 57.0.24 (local only), `expo-background-task`
  57.0.21 + `expo-task-manager` 57.0.21 (`minimumInterval` 15 min); native-only `apps/mobile/src/notify.native.ts`
  (`notify.web.ts` empty). Background check `@app/data` `notify/check.ts` (obs.json via `fetchObs` only when an alert
  is set; release manifest only when "new data" is on; once per data release; offline silent, other failures
  reported; never downloads, the tap opens the app and T231/T298 apply). Stored in `spots/notify.json` (switches,
  rules, last release told, last 20 runs). Permission asked only when a switch is turned on or an alert saved. Tap
  opens the water's Conditions (cold start too, `getLastNotificationResponse`). Test channel:
  `node deploy/staging/test-updates.mjs gauge <station> <flow|level> <value>` / `reset` / `release [tag]`.
- v1.1 (665dcb49b core + data, 3dbc91380 UI + phone glue, 981c60cf5 scripts): More → Notifications cards + editor
  (alert kinds and cadence since replaced by T527, below); notify.json `v: 2` migration (unreadable alert reported,
  `reportOnce`). New app version (opt-in): reads our version file for the channel (core `appVersionFileUrl`):
  sideload `app/android/latest.json`; Play `app/android/play.json` on the data origin, written by `build-apk.sh --play`
  (production needs the same file on prod R2: owner's); once per version (`toldAppVersion`); tap opens the bell on App
  update. Changelog: one source `app/packages/core/src/changelog.ts`; About → "What's new in each version"; entries
  0.1.0-0.6.0 are a DRAFT for the owner (`draft: true`); build-apk.sh warns when a release's entry is missing.

## Emulator / phone checklist (next APK)
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
4. (superseded by T527's checklist)
5. Tap the gauge notification with the app (a) in the background and (b) swiped away → it opens on that water's
   Conditions both times; it does not re-open after an update is applied.
6. Deny path: clear app data, save an alert, Don't allow → the card says notifications are blocked with "Open the phone's
   settings" (opens the app's settings page); More → Notifications says the same.
7. Offline (airplane mode) + force → no notification, "Last background checks" shows "… · offline"; no crash report sent.
8. Turn the global switch Off → the periodic work is cancelled (dumpsys); On again → registered again.
9. Logcat at start: FirebaseApp says it could not initialise (no google-services.json) and nothing else; no crash.
10. Leave the phone idle overnight with an alert set: "Last background checks" gives the real cadence under Doze (the
    owner's measurement for the push-or-not decision below).
11-15. (superseded by T527's checklist)
16. New app version: More → Notifications → "New app version" On (permission once). `test-updates.mjs app-version 0.9.0
    99 "Alerts" "Changelog"` + force → ONE "A new version of the app is out / Version 0.9.0 is ready…"; force again →
    none; `app-version 0.9.1 100` → one more. Tap → the app opens with the bell on App update, "What's new in 0.9.0"
    and its bullets, Update (the APK page). A local build (no channel): no "New app version" switch at all.
17. About → "What's new in each version" → every version newest first, the installed one marked "Installed".

## Left
- The phone checks above (needs the batch APK).
- Owner: review the changelog draft; confirm the cadence readings (old owner to-do); production `app/android/play.json`
  (prod R2) once a Play build reads production data.
- Measure real delays on the owner's phone (the runs log); FCM push only if alerts arrive too late (owner 2026-11-05).
- Later kinds (not v1): forecast crossings, in-season / DFO changes on followed waters, quiet hours; web push.

## Owner 2026-11-09: the alert model replaced (T527)
"a clean alerting system like you would have for stocks" + the percentile for the day → T527: one model, crosses above /
below a value or a percentile for the day, once per crossing with a 2 % re-arm; the four kinds, the cadence and Edit are
gone; a saved ±% alert is refused and said. Checklist items 4, 11–15 above are superseded by T527's checklist.
