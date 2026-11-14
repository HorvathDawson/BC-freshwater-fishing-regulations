# T724 — iOS: shared links open the app as universal links (IOS-7)

Status: IN PROGRESS (iOS chat, links lane).

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
(filled in as the work runs)
