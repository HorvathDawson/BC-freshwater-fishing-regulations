# T234 — Push notifications: regulation changes and condition alerts

Status: OPEN — not started (owner 2026-10-28: "another ticket").

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
