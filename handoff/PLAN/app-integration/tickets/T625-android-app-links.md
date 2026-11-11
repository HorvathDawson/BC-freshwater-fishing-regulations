# T625 — A shared link opens the installed app (Android App Links)

Status: PARTIAL — code built 2026-11-11 (items 2–3): site build writes `/.well-known/assetlinks.json`, app.json intent filter (`autoVerify`) for canifishthis.ca + staging.canifishthis.ca, the phone reads a tapped link (`appLink.native.ts` → `resolveAppLink` → the Shell's open request); left (Claude): after the next site deploy and APK, item 4 on the emulator (`adb shell pm get-app-links ca.canifishthis.app` says verified; a tapped link opens that water) and a phone check from Play.

Source: owner 2026-11-10: "but we can't set up app link until we are in closed testing stage? unless you know how."
It does not need closed testing. It needs two things that exist already on internal testing.

## How it works
- The app declares, in its Android manifest, that it handles `https://<our domain>/water/…` (and the map start link),
  with `autoVerify`.
- The website serves `/.well-known/assetlinks.json` naming the app's package (`ca.canifishthis.app`) and the SHA-256
  fingerprint of the key the installed app is signed with. For a build installed from Play that is the **Play app
  signing key**, shown in Play Console → Test and release → Setup → App signing. It is the same key on internal,
  closed and production tracks.
- Android checks that file when the app is installed. If it matches, a tap on such a link opens the app; if not, the
  browser, as today.

## Work
1. Owner copies the "SHA-256 certificate fingerprint" of the app signing key from Play Console (a fingerprint is
   public information, not a secret). Add the upload/debug key's fingerprint too only if side-loaded test builds
   should also open links.
2. Site build writes `/.well-known/assetlinks.json` (staging and production hosts each serve their own; both list the
   same package and fingerprint), served as `application/json`, no redirect. A site test pins it.
3. Expo config (`app.json` `android.intentFilters`, `autoVerify: true`) for the hosts we use; the phone's start-link
   reader (`apps/mobile/src/startLink.native.ts` reads no links today) parses the same addresses as the web through
   the shared parser.
4. Verify on the emulator with a Play-signed or locally re-signed build:
   `adb shell pm get-app-links ca.canifishthis.app` says `verified`; a tapped link opens the app on that water.
5. iOS universal links are the same idea (`apple-app-site-association`); out of scope while iOS is benched.

## Acceptance
- On a phone with the app from Play, a shared water link opens the app on that water; without the app, the website.

Depends: T624. Size: M. Owner: the fingerprint from Play Console.

## Owner 2026-11-11: the fingerprint
Play app signing key, SHA-256 (public), copied by the owner from Play Console (Test and release → App integrity →
Protected with Play → Play Store protection → Play app signing):
`A0:73:C5:A9:63:D3:20:2E:9D:18:FA:95:A8:11:AD:51:1F:6F:C4:BB:74:CF:84:27:71:23:EC:7E:24:BF:A8:DC`
Work item 1 is done; items 2–4 can start.

## Built 2026-11-11 (items 2–3)
- `@app/core` share.ts owns `APP_LINK_HOSTS`, `APP_PACKAGE`, `PLAY_SIGNING_SHA256`; `tools/seo/hosting.ts` `assetLinksFile()` is
  written by the site build to `.well-known/assetlinks.json` (the same text on staging and production; `_headers` pins
  `Content-Type: application/json`); `tools/seo/appLinks.test.ts` pins file, headers and the app.json filter against each other.
- app.json `android.intentFilters`: https, `/water/` prefix and `/` exactly, on both hosts, `autoVerify: true`.
- Link reading: `@app/ui` `appLink.ts` (`parseAppLink`, `resolveAppLink`): `/water/<slug>` takes its item from the page's own
  `cift:item` (the web start link's authority; no second slug table), `/?regs=<item>` carries it, view from `?view=`. The phone
  opens it through the Shell's `openRequest` (the notification tap's path) on that view. Web/native split by file
  (`apps/mobile/src/appLink.{web,native}.ts`).
- Not read yet (reported, never guessed): place links (`blk`+`m`), `show`, `browse`, `spot`; the part, day and horizon of a
  link are not applied on the phone (the water opens whole). A follow-up if the owner wants them.
- Needs the next site deploy (the file) AND the next APK (the filter + reader). Staging-built APKs are side-loaded, signed
  with the upload key, so Android will not verify them; only a Play install verifies.
