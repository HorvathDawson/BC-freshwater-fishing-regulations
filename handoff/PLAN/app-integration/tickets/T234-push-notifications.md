# T234 — Push notifications: regulation changes and condition alerts

Status: OPEN — started 2026-11-05 (owner: phone-only v1, gauge thresholds + update-ready notices).

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
