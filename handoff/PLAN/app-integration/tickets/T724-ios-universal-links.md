# T724 — iOS: shared links open the app as universal links (IOS-7)

Status: PARTIAL (built: 9af44b246, ffd44b943, f9d47f0d9; the Team ID is in place since 2026-11-14 (4cc85a914), so every site build writes `apple-app-site-association`; left (Claude): the simulator `openurl` run, then the verified hand-off on a signed build (needs the App ID with Associated Domains on, T88)).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T724 | iOS: shared links open the app as universal links (IOS-7) | PARTIAL (left (Claude): the simulator `openurl` run; the verified hand-off needs a signed build with Associated Domains, T88) | 9af44b246, ffd44b943, f9d47f0d9, 4cc85a914 | owner 2026-11-14; `associatedDomains` for both hosts; the site build writes `apple-app-site-association` only when a Team ID is configured (`CIFT_IOS_TEAM_ID` or … (detail: ticket file) |

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-7 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## What
**IOS-7.** A shared `canifishthis.ca/water/...` link opens the app on that water (mirrors T625 items 2-4).

## Done (9af44b246, ffd44b943, f9d47f0d9; Team ID 4cc85a914)
- `app/tools/seo/hosting.ts` `appSiteAssociationFile(teamId)`; `build.ts` writes `/.well-known/apple-app-site-association`
  (`_headers`: `application/json`), paths `/water/*` and `/`, appID `<TeamID>.ca.canifishthis.app`, both hosts.
- `app/apps/mobile/app.json` `ios.associatedDomains`: `applinks:canifishthis.ca`, `applinks:staging.canifishthis.ca`
  (`APP_LINK_HOSTS`; a test pins file hosts = associatedDomains). `appLink.native.ts` (`Linking.getInitialURL` + the
  `url` event) and `resolveAppLink` need no change. Android output unchanged. Tests: `tools/seo/appLinks.test.ts` (8 of 8).
- Team ID: env `CIFT_IOS_TEAM_ID`, else `~/.canifishthis/ios-team-id` (the owner saved it 2026-11-14 evening); none ->
  the file is NOT written and `build-seo` warns; a malformed value throws; `node app/tools/build-seo.mjs ...
  --require-ios-links` makes the missing id an error (production build). PROPOSAL, owner to confirm.

## Work (left)
1. Simulator: `xcrun simctl openurl booted "https://staging.canifishthis.ca/water/<a real slug>"`; with no verified
   association, expect Safari to open the web page.
2. After T88 (App ID registered with Associated Domains on (owner), a signed build, the file deployed):
   `curl -sI https://staging.canifishthis.ca/.well-known/apple-app-site-association` (200, `application/json`), install
   the build, repeat step 1: the app opens on that water. Apple fetches the file through its CDN: a change can take hours.

## Acceptance
simulator `xcrun simctl openurl` opens the water; link without the app opens the web.

Depends: T625; the Team ID (owner) for the final proof. Size: M. Owner: yes (Team ID, after the Apple account). Default: a placeholder read from config, never invented.


## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-14; `associatedDomains` for both hosts; the site build writes `apple-app-site-association` only when a Team ID is configured (`CIFT_IOS_TEAM_ID` or `~/.canifishthis/ios-team-id`); Android output unchanged
