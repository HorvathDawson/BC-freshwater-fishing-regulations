# T625 — A shared link opens the installed app (Android App Links)

Status: PARTIAL — 8399427db (code built 2026-11-11, items 2–3: site build writes `/.well-known/assetlinks.json`, app.json intent filter (`autoVerify`) for canifishthis.ca + staging.canifishthis.ca, the phone reads a tapped link via `appLink.native.ts` → `resolveAppLink` → the Shell's open request); left (Claude): item 4 on the emulator (`adb shell pm get-app-links ca.canifishthis.app` says verified; a tapped link opens that water) on a Play-installed build; owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T625 | A shared link opens the installed app (Android App Links: assetlinks.json + intent filter) | PARTIAL | 8399427db | owner 2026-11-10; needs the app signing fingerprint from Play Console; site file + intent filter + link reader built; left (Claude): emulator `pm get-app-links` verified + phone … (detail: ticket file) |

Source: owner 2026-11-10: "but we can't set up app link until we are in closed testing stage? unless you know how."
It does not need closed testing. Android checks the site's `assetlinks.json` (package `ca.canifishthis.app` + the
**Play app signing key** SHA-256, same key on internal, closed and production) when the app is installed; if it
matches, a tap on a link opens the app, if not, the browser.

## Done
- Item 1 (owner 2026-11-11): the Play app signing key, SHA-256 (public), from Play Console (Test and release → App
  integrity → Protected with Play → Play Store protection → Play app signing):
  `A0:73:C5:A9:63:D3:20:2E:9D:18:FA:95:A8:11:AD:51:1F:6F:C4:BB:74:CF:84:27:71:23:EC:7E:24:BF:A8:DC`
- Items 2–3 (8399427db): `@app/core` share.ts owns `APP_LINK_HOSTS`, `APP_PACKAGE`, `PLAY_SIGNING_SHA256`;
  `tools/seo/hosting.ts` `assetLinksFile()` → `.well-known/assetlinks.json` (`_headers` pins `Content-Type:
  application/json`); `tools/seo/appLinks.test.ts` pins file, headers and the app.json filter against each other.
  app.json `android.intentFilters`: https, `/water/` prefix and `/` exactly, both hosts, `autoVerify: true`.
  `@app/ui` `appLink.ts` (`parseAppLink`, `resolveAppLink`): `/water/<slug>` takes its item from the page's `cift:item`,
  `/?regs=<item>` carries it, view from `?view=`; opened through the Shell's `openRequest`
  (`apps/mobile/src/appLink.{web,native}.ts`).
- Item 5: iOS universal links are T724.

## Work (left)
4. Verify with a Play-installed build (staging-built APKs are side-loaded, signed with the upload key, so Android will not
   verify them): `adb shell pm get-app-links ca.canifishthis.app` says `verified`; a tapped link opens the app on that water.
   Needs the next site deploy (the file) AND the next APK (the filter + reader).
- Not read yet (reported, never guessed): place links (`blk`+`m`), `show`, `browse`, `spot`; the part, day and horizon of a
  link are not applied on the phone (the water opens whole). A follow-up if the owner wants them.

## Acceptance
- On a phone with the app from Play, a shared water link opens the app on that water; without the app, the website.

Depends: T624. Size: M. Owner: the fingerprint from Play Console.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-10; needs the app signing fingerprint from Play Console; site file + intent filter + link reader built; left (Claude): emulator `pm get-app-links` verified + phone check after the next site deploy and APK

## Owner

- [ ] 2026-11-12 — On the phone, open a link someone sent you: it should land on the right water (Android App Links).
