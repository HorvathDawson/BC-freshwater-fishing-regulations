# T514 — Share page "Open in the app": link the Play Store listing once it is public

Status: PARTIAL — the switch is built 2026-11-11 (one value, `OPEN_IN_APP_URL` in `app/packages/core/src/share.ts`, default null = web app + greyed "coming soon"); left (Claude): set it to the Play listing address when the listing is public (T495 step 5). Earlier: owner 2026-11-08: "Open on the website" + a greyed "Open in the app · coming soon" now (3b36d3959); the live store link ships with the production app release (T495 step 5).

Owner 2026-11-08: "link can open website for now but have a ticket in production app release to link to it as well".

## What
- Today (T487): a shared gauge/water page's "Open in the app" opens the web app.
- When the Play listing is public (T244/T371): Android visitors without the app get the Play Store listing; with the app
  installed, the app opens at that gauge/water. iOS gets its App Store link after TestFlight/App Store (T88).
- Proof: on a phone without the app, the link reaches the listing; with the app, it opens the same gauge.
