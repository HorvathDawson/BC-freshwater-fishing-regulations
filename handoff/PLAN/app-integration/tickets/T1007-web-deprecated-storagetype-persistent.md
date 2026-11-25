# T1007 — Website: Chrome warns "`StorageType.persistent` is deprecated" (first party)

Status: DONE — df4c72f0f: not our code; Cloudflare's injected bot script (lane L2, 2026-11-22).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1007 | Website: Chrome warns "StorageType.persistent is deprecated" (first party): Cloudflare's injected JavaScript Detections script, not our code; guard test | DONE | df4c72f0f | owner 2026-11-22 (Lighthouse) |

Source: owner 2026-11-22 (Chrome DevTools / Lighthouse on https://canifishthis.ca), verbatim: "`StorageType.persistent`
is deprecated. Please use standardized `navigator.storage` instead." Source column: "canifishthis.ca 1st party".

## Found so far (2026-11-22)
- None of our code names the old API (`git grep` over `app/packages`, `app/apps` for `webkitPersistentStorage`,
  `webkitStorageInfo`, `StorageType`, `requestQuota`: no hit).
- Neither of the two served bundles names it either (`/_expo/static/js/web/index-76f27ada….js`, 3.86 MB, and
  `later-82c4390b….js`, 347 KB, searched as fetched).
- So it comes from code loaded another way: a worker (MapLibre's, the SQLite / wasm loader), a dependency that probes
  `navigator` / `window.PERSISTENT`, or a browser extension that Chrome attributes to the page (the same report listed
  React DevTools' scripts). Check in a clean Chrome profile with no extensions first.

## Work
1. Reproduce in a clean profile; take the warning's stack (DevTools → Issues → the affected resource) to the file.
2. Ours → move it to `navigator.storage` (`persist()` / `estimate()`); a dependency's → update it or report upstream,
   with the version that fixes it named here. An extension's → close as not ours, saying how that was shown.
3. A test that fails if the old API comes back in our code.

Depends: none. Size: S. Owner: no.

## Done 2026-11-22 (lane L2, df4c72f0f)
Reproduced in a clean Chromium (Playwright, no extensions) on https://canifishthis.ca/ with the DevTools protocol `Audits`
domain: the `DeprecationIssue` of type `PersistentQuotaType` (and a `FencedFrame` one, and a `document.cookie` performance
issue) all point at `https://canifishthis.ca/cdn-cgi/challenge-platform/scripts/jsd/main.js?ray=…` column 4237. That is
Cloudflare's **JavaScript Detections** script, injected by Cloudflare into every HTML page of the zone (seen in the raw
HTML of `/` and `/region/4`): same origin, so Chrome and Lighthouse call it first party. Not ours, not in our bundles.
- To silence it: the zone's Security -> Bots setting (JavaScript Detections / Bot Fight Mode) off, an owner choice in the
  Cloudflare dashboard; or wait for Cloudflare to update the script. Nothing to change in our code.
- Guard: `app/tools/storage-api.test.ts` fails if our code (packages, apps, workers, tools) names the old quota API
  (mutation-checked with a planted `navigator.webkitPersistentStorage`).
- Side find, same run: a `FormEmptyIdAndNameAttributesForInputError` issue (an input with no id or name) on `/` - the app's
  own, likely the search field; not ticketed here.
