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

## Shape (defaults, owner to confirm)
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
