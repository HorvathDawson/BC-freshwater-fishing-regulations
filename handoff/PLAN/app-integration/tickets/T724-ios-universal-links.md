# T724 — iOS: shared links open the app as universal links (IOS-7)

Status: PARTIAL (built: 9af44b246, ffd44b943, f9d47f0d9; the Team ID is in place since 2026-11-14 (4cc85a914), so every site build writes `apple-app-site-association`; left (Claude): the simulator `openurl` run, then the verified hand-off on a signed build (needs the App ID with Associated Domains on, T88)).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T724 | iOS: shared links open the app as universal links (IOS-7) | PARTIAL (left (Claude): the simulator `openurl` run; the verified hand-off needs a signed build with Associated Domains, T88) | 9af44b246, ffd44b943, f9d47f0d9, 4cc85a914 | owner 2026-11-14; `associatedDomains` for both hosts; the site build writes `apple-app-site-association` only when a Team ID is configured (`CIFT_IOS_TEAM_ID` or … (detail: ticket file) |

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-7 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-7. Shared links as universal links.** Goal: a shared `canifishthis.ca/water/...` link opens the app on that water.
Do: `associatedDomains` in app config for both hosts, site build serves `/.well-known/apple-app-site-association` (mirror
the T625 `assetlinks.json` work and its site test), Team ID from the owner. Acceptance: simulator `xcrun simctl openurl`
opens the water; link without the app opens the web. Size: M (final proof needs a signed build).
  Sub-steps: copy the T625 site test for `/.well-known/apple-app-site-association` (Team ID as a placeholder read from config,
  never invented); `associatedDomains` via app config for both hosts; check `appLink.native.ts` and `resolveAppLink` handle
  the iOS launch URL; `xcrun simctl openurl` runs; no-app case = web. Real proof needs a signed build (IOS-11, parked).
  Mirrors: T625 items 2-4.

## Acceptance
simulator `xcrun simctl openurl` opens the water; link without the app opens the web.

Depends: T625; the Team ID (owner) for the final proof. Size: M. Owner: yes (Team ID, after the Apple account). Default: a placeholder read from config, never invented.

## Results
Built (code and config only; nothing run on a simulator, nothing deployed):
- `app/tools/seo/hosting.ts` `appSiteAssociationFile(teamId)`; `build.ts` writes `/.well-known/apple-app-site-association` when a Team
  ID is configured; `_headers` serves it `application/json`. Paths: `/water/*` and `/` (the same pages as the Android filter), appID
  `<TeamID>.ca.canifishthis.app`. The same text serves from both hosts (canifishthis.ca, staging.canifishthis.ca).
- `app/apps/mobile/app.json` `ios.associatedDomains`: `applinks:canifishthis.ca`, `applinks:staging.canifishthis.ca`
  (`APP_LINK_HOSTS`; a test pins file hosts = associatedDomains).
- No app-code gap: `appLink.native.ts` uses `Linking.getInitialURL` + the `url` event, which iOS fills for universal links exactly as
  Android does; `resolveAppLink` is platform-free. No new unit test needed.
- Android untouched: `assetlinks.json` and the intent filters are unchanged (the T625 test still passes and the new test asserts
  assetlinks.json equals `assetLinksFile()` in the iOS build).
- Tests: `tools/seo/appLinks.test.ts` (with Team ID, without, bad id, read from env/file, associatedDomains).

PROPOSAL (Team ID behaviour, owner to confirm): the Team ID is read from env `CIFT_IOS_TEAM_ID`, else `~/.canifishthis/ios-team-id`
(the file T88 reads). None -> the file is NOT written (no bogus appID) and `build-seo` prints one warning line saying so; a
malformed value throws. `node app/tools/build-seo.mjs ... --require-ios-links` turns the missing id into an error: use it for the
production build once the account exists. Until then iOS links just open the web page (the correct no-app behaviour).

Simulator proof (main lane, after the next simulator build; the simulator build has the associatedDomains entitlement but, with
no Team ID, no verified association, so the first run is the "no app / unverified" case):
1. `xcrun simctl openurl booted "https://staging.canifishthis.ca/water/<a real slug>"` : expect Safari to open the web page
   (acceptance: a link without the verified app opens the web).
2. After T88 (signed build, file deployed): `curl -sI https://staging.canifishthis.ca/.well-known/apple-app-site-association`
   (200, `application/json`), install the build, repeat step 1: the app opens on that water.
Waits for T88: the Apple account and Team ID, a build with the Associated Domains entitlement signed by that team, the site deploy
carrying the file (rebuild with the Team ID set, `--require-ios-links` for prod), and the on-device/simulator verified proof.
Apple fetches the file through its CDN: a change can take hours to be seen.

## Team ID in place (2026-11-14 evening)
The owner's Apple Developer account was approved (individual) and he saved the Team ID in `~/.canifishthis/ios-team-id`
himself. Local check, no deploy: the site build's own reader (`readIosTeamId`) accepts the file and
`appSiteAssociationFile` gives `{"applinks":{"details":[{"appIDs":["<team>.ca.canifishthis.app"],"components":[{"/":
"/water/*"},{"/":"/"}]}]}}`; `tools/seo/appLinks.test.ts` 8 of 8. From now on every site build writes the file. Still
left: the simulator `openurl` run; the real hand-off needs the App ID registered with Associated Domains on (owner) and
a signed build (T88).

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-14; `associatedDomains` for both hosts; the site build writes `apple-app-site-association` only when a Team ID is configured (`CIFT_IOS_TEAM_ID` or `~/.canifishthis/ios-team-id`); Android output unchanged
