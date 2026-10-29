# T251 — testing data updates (Updates log + staging test channel) and the app version

Status: DONE (fe10a015)

Source: owner 2026-10-28: (1) "we need a way to test the app data updates — it syncing with R2 when in service and
tracking updates"; (2) "we need an app version too". Add-ons the same day: the web app's version tracks the release it
ships with (web and data deploy together; store builds can lag, which `minAppVersion` handles); "Regulations as of
<date>" is the release's PUBLISHED day, stamped at publish (never by the deterministic build); a stamp never forces a
tile rebuild or re-upload.

## Built

**Updates log** (dev + staging builds only: `__DEV__`, `EXPO_PUBLIC_ENV_SWITCH=1`, or any build not reading production;
About → "Updates log ›"). Every check per source: time, unchanged 304 / changed / failed + class (`http-404`, `http-5xx`,
`sha-mismatch`, `size-mismatch`, `parse`, `apply`, `offline`) / offline (no request), bytes, sha or release before →
after, ms, manual; the release's staged / applied / app-too-old steps; tester actions. Ring buffer of the last 200 on
the device (`openUpdateLogStore`: localStorage `canifishthis.updateLog.v1` / `Documents/spots/update-log.json`).
Buttons: Check now, Force re-download (forget ETags + shas → each object whole), Simulate offline, Clear cache (validators;
on the web also Cache Storage `release-v1`), Apply the staged release, Copy log (web clipboard / phone share sheet; the
export opens with the version line, env, origin, channel, release).
- core `updateLog.ts` (event type, `errorClassOf`, `updateLogVM`, `updateLogText`); data `update/log.ts`
  (`createUpdateLog`, `versionedFetch`); service `onEvent`, `forget()`, `applied(r, {from, ms})`; checkObject returns
  `status` + `got`; ui-native `menu/UpdatesLogPage.tsx`; menu page key `updates`.

**Staging test channel.** `endpointsFor(base, "test")` moves only what the update service CHECKS under `test/` (the
manifest and the three feed indexes, `ENDPOINTS.checks`); release files and feed folders stay live. Picked by
`EXPO_PUBLIC_UPDATE_CHANNEL=test` or, in a switchable build, on the Updates log page (kept, next start).
`deploy/staging/test-updates.mjs` (rclone `r2:`, staging bucket hardcoded, writes `test/` only): `reset`,
`feed-bump [gauges|dfo|inseason]`, `release [tag]`, `corrupt` (paint sha wrong), `missing` (paint 404), `feed-404 [feed]`,
`min-app <semver>`, `status`, `clean`. upload.sh's clear also clears `test/` (its manifest would name deleted files).

**End-to-end run** `app/tools/updates-e2e.mjs` (headless Chrome over regs-web/cdp.mjs — the repo's driver; Playwright is
not a dependency): boot, 304s, feed bump, force, new release staged + applied, corrupt (sha-mismatch), missing (404),
feed 404, minAppVersion 99.0.0 (app too old, More says "Update the app to get new regulations"), simulate offline, export
copied, About's version; resets the channel at the end. **All scenarios pass on staging (2026-10-28).**

**App version.** One source: `apps/mobile/app.json` `expo.version` (0.1.0; package.json repeats it) + build number
`android.versionCode` = `ios.buildNumber` (1). `apps/mobile/version-stamp.js` (run by metro.config.js) inlines
`EXPO_PUBLIC_APP_VERSION / _APP_BUILD_NUMBER / _GIT_SHA` (EAS's `EAS_BUILD_GIT_COMMIT_HASH` first) `/ _BUILD_DATE`;
core `appVersion.ts` resolves (malformed throws), `versionLine` → About "Version 0.1.0 (build 1, 2ea061c, 2026-10-29)"
(and the problem report's "App version"). Requests: phone — `X-App-Version` + `User-Agent: canifishthis/<tag> (<os>)` on
every data request; web — `X-App-Version` on the update checks only (CORS preflight; not sent to `local`). Staging data
worker allows the header and logs `app=<tag>` on manifest/index reads (Workers Logs on); deployed.
**Gate:** manifest `minAppVersion` (pack contract); a phone older than it keeps its data (`phase: "app-too-old"`, More's
Updates line + the log say "Update the app to get new regulations"); a first run too old installs nothing and says so.
The web is never gated (its code ships with its data) — `EXPO_PUBLIC_RELEASE_GATE=1` turns it on for the browser test.

**Published day.** `pipeline/deliver/release/stamp.py` stamps `published` (ISO day, kept once set; `--restamp`) and
optional `minAppVersion` into the manifest at publish: `publish.py --yes [--min-app-version x.y.z]`,
`deploy/staging/upload.sh` (`MIN_APP_VERSION=x.y.z`). Nothing the app compares moves (files, tiles, data_release), so no
tile rebuild / re-upload. The regs foot (`regsAsOfDay`) reads `published`, falling back to the newest input day.

## Owner: the same scenarios on your phone

1. Dev build reading staging: `cd app/apps/mobile && EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_ENV_SWITCH=1 npx expo run:android --variant release`
   (or `EXPO_PUBLIC_UPDATE_CHANNEL=test` on that line to start on the test channel).
2. On the phone: More → About → Updates log → Update channel **Test** → close and reopen the app.
3. On the Mac: `node deploy/staging/test-updates.mjs reset`, then one of `feed-bump gauges`, `release 1`, `corrupt`,
   `missing`, `feed-404 dfo`, `min-app 99.0.0`.
4. On the phone: Check now (or wait / background and return) and read the row; "Copy log" shares the export.
5. Done: `node deploy/staging/test-updates.mjs reset` (or `clean`), and switch the channel back to Live.
Browser run of everything: start Expo with `EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_UPDATE_CHANNEL=test
EXPO_PUBLIC_RELEASE_GATE=1 npx expo start --web --port 39251`, then `node app/tools/updates-e2e.mjs`.

## Left

- Production data worker (T86/T240): allow `X-App-Version` in `Access-Control-Allow-Headers` before the web app reads
  production, or every web update check fails CORS. Same fix as staging's `deploy/staging/data-worker.js`.
- The local dev server (`:39217`, serve-tiles.mjs) allows the header from its next restart (not sent to local anyway).

## Acceptance

Tests: core `appVersion.test.ts` (resolution, semver order, gate, log words/export), `menu/legal.test.ts` (published
wins, fallback), data `update/service.test.ts` (events per check, sha-mismatch class, forget, gate incl. seedRelease, ring
buffer, versioned fetch), `tools/update-service.test.ts` (boot gate: first run refused, later launch keeps held set),
`apps/mobile/src/version.test.ts` (app.json = package.json, build numbers agree, stamp), `env.test.ts` (channel),
`pipeline/tests/test_release_stamp.py`. Shots: `handoff/PLAN/app-integration/T251-shots/` (Updates log phone top +
rows, desktop; About version phone + desktop). e2e: all scenarios pass.

Depends: T231, T232. Size: M. Owner: phone run (above); prod worker header at T86.
