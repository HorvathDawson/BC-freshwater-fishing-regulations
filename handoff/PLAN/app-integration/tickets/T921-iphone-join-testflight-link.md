# T921 — "Join the iPhone test" opens the public TestFlight link

Status: DONE 2026-11-19 (82673b6f2; landed at d28344298).

Source: owner 2026-11-19 (main chat), verbatim: "in the webapp where it says join apple trial we could have this link??
https://testflight.apple.com/join/x7uVRt8j"

## Built
- `core/storeLinks.ts` `TESTFLIGHT_URL` (the one place the invite is spelled; null = back to the email ask).
- `core/joinTest.ts` `getAppBlock(…, testflight)`: while the App Store page is not public, the iPhone slot is a link
  (`kind: "store"`, label "Join the iPhone test", `early: true` so "Want to try it early?" stays above it). A public App
  Store page still wins. Android unchanged (email card, T911).
- Both renderers read it: the web app's `WebSpotsNotice` (prop `testflight`, default the constant) and the website's
  gauge pages (`tools/seo/gauges.ts` `openInAppControl`, 4th argument). The iPhone email card stays as the fallback.
- Tests: `joinTest.test.ts` (invite, lead, App Store wins, no-contact still a live press, the shipped link),
  `JoinTestCard.test.tsx` (press opens the link, no card), `appLinks.test.ts` (the site's plain link, no iPhone dialog);
  the email-path tests pass `null` explicitly. 188 passed; `tsc -b` clean.
## Acceptance
On staging: the web app's Spots "Get the app" and a gauge page's "Join the iPhone test" open the TestFlight page.
Depends: none. Size: S. Owner: no.
