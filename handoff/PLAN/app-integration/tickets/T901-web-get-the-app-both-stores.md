# T901 — The web's "Get the app" offers Google Play and the App Store, each its own button

Status: DONE (2d3e89299, merged into 0.8.2; review: nothing blocking, lows fixed in 4a2e1af; owner saw the redone pictures). Launch: set PLAY_PUBLIC / APP_STORE_PUBLIC in core storeLinks.ts (pnpm run store-pages checks them).

Owner 2026-11-19 ~01:14 (app chat), verbatim: "the web app get the app should link to both google play and apple depending
on button you click".

Today: the web's Spots notice (ui-native WebSpotsNotice.tsx, words core/spotWords.ts `getApp` / `appUrl`) has one "Get
the app" button with one link. Build: two buttons, "Google Play" → the Play listing, "App Store" → the App Store page
(https://apps.apple.com/app/id<Apple ID>; the Apple ID read from App Store Connect, the same number build-ios-testflight.sh's
`~/.canifishthis/app-store-url` names), every other "get the app" place on the web and the website (app/tools/seo) the
same. Self-hosted art only (self-host rule); words in the words files; tests. If the App Store page is not public yet
(the app is in TestFlight only), the App Store button says so plainly instead of linking to a 404 — ask nothing, show
both states in the pictures.

## What was built (2026-11-19)

- **Apple ID 6835000455**, read-only from App Store Connect (GET /v1/apps, bundle id ca.canifishthis.app). The App Store page
  `https://apps.apple.com/app/id6835000455` answers **404** (TestFlight only), so the App Store button is a disabled
  "iPhone app coming soon", not a link.
- `app/packages/core/src/storeLinks.ts` (new, exported by @app/core): `PLAY_STORE_URL`, `APP_STORE_URL`, `APP_STORE_PUBLIC`
  (false), `appStoreLink()`. **To flip it on: set `APP_STORE_PUBLIC = true` (one line) once the page loads (curl -> 200).**
- Web Spots notice (`WebSpotsNotice.tsx`): "Get the app" with two buttons, Google Play (links the Play listing) and App Store
  (or the coming-soon state). Words in `WEB_SPOTS_WORDS` (`playButton`, `appStoreButton`, `appStoreSoon`); old `appUrl` removed.
- Website (tools/seo `gauges.ts` `openInAppControl`): the gauge pages' single "Open in the app" is now "Google Play" and
  "App Store", each greyed "coming soon" until its page is public (Play: `OPEN_IN_APP_URL` null as before; App Store: `APP_STORE_PUBLIC`).
  No other "get the app" place exists on the web app or the site.
- No remote art: plain buttons in the app's button style.
- `~/.canifishthis/app-store-url` did not exist, so it was written with the URL above (the iPhone build's update check will
  now be baked in by build-ios-testflight.sh; note the page 404s until public).
- Tests: WebSpotsNotice.test.tsx (Play URL, coming state disabled and no open, App Store live state), tools/seo/appLinks.test.ts
  (both controls, addresses), gauges.test.ts updated. Pictures: scratchpad/t901-shots/ (390 and 1440, light, coming + live).

### Follow-up (coordinator, 2026-11-19): Play 404s too

The Play listing also answers 404 (closed testing only). `storeLinks.ts` now has `PLAY_PUBLIC` (false) beside `APP_STORE_PUBLIC`
(false); each button is a disabled "Android app coming soon" / "iPhone app coming soon" until its flag is true. The old
`OPEN_IN_APP_URL` (share.ts) is gone: the gauge pages read `playStoreLink()`. **At launch flip both: `PLAY_PUBLIC = true` and
`APP_STORE_PUBLIC = true`.** `pnpm run store-pages` (app/tools/check-store-pages.mjs, by hand, needs the network) curls both pages
and fails if a flag is true over a non-200 page, and reports a page that is live while its flag is false.

### Owner addition 2026-11-19 ~01:22, verbatim: "we could add a contact to get into testing maybe?"

Built: under the two coming-soon buttons, "Want to try it early? Ask to join the test." and a press per store, "Join the
Android test" / "Join the iPhone test" (words: `WEB_SPOTS_WORDS.join`, core spotWords.ts). Each opens an email to the app's
contact (More → Contact: `menuInfo.contact`, EXPO_PUBLIC_CONTACT; `joinTestAsk` in core joinTest.ts) with that subject and a
body asking for the email on the Google account (Android) / Apple ID (iPhone). With no contact address (staging) the press opens
the in-app Report form with the subject and ask already in "What happened" (`ReportWhere.ask`); where neither exists (the capture
notice with no contact) no press is drawn, never a dead one. When `PLAY_PUBLIC` / `APP_STORE_PUBLIC` is true that store's join
press goes away and its store button is a real link. The website's gauge pages carry the same line as plain mailto links
(site contact), the "coming soon" spans are plain text (no aria-disabled), their words moved to `WEB_SPOTS_WORDS.site`.
Reviewer notes folded in: the website reads `playStoreLink()` (the flag), not OPEN_IN_APP_URL (removed).

### Redo, owner 2026-11-19 ~01:32: "the buttons for join test suck"

The block is now: "Get the app", one plain line naming only the stores still coming ("Coming soon to Google Play and the App Store."),
"Want to try it early?", then one primary button per platform, equal width (side by side where they fit, stacked full width on a
phone). No disabled buttons. When a flag goes true that platform's join button becomes its store button ("Google Play" /
"App Store") in the same spot and style; both public: no line, no lead. Decided once in core `getAppBlock` (joinTest.ts) for the
app and the website (gauge pages: plain text + two `a.app` links in `.stores`). Words: `WEB_SPOTS_WORDS.coming`, `.join.lead`.
