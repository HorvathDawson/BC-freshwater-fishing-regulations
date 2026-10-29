# T232 — app environments: local / staging / production, chosen at build time

Status: DONE (3d171c8f) — native device run is the owner's (build commands below)

Source: owner 2026-10-28: app builds need selectable environments (local / staging / production) so a dev build on
the owner's phone reads STAGING data (data-staging.canifishthis.ca, feeds there; site staging.canifishthis.ca).

## What picks an origin (audit)

Everything the app fetches from us hangs off ONE base, which was already `app/apps/mobile/src/config.ts` (P10):
the release manifest + files (`ENDPOINTS.manifest/release`), tiles/basemap/outside (the dev fallback, `releaseMapInput`;
the manifest's tiles are relative to `release/`), sql.js wasm (`sqlWasm`), the feeds (`feed` live gauges, `dfoFeed`,
`inSeason` + matches), the synopsis PDF copy (`${ENDPOINTS.release}${manifest.synopsis.url}`, App.tsx) and the T231
update service (App.tsx hands it `ENDPOINTS.*` URLs; `@app/data` `update/` takes URLs injected, no origin of its own).
Third-party origins (Esri imagery, Open-Meteo, GeoMet, gov.bc.ca, DFO) are the same in every environment.
SEO "Open in the app" links are built absolute to production by `tools/seo` (canonical); `deploy/staging/site.sh`
rewrites them to the staging site — it now also rewrites the bare "Open the app" link (it only did `?regs=`).

## Built

- `apps/mobile/src/env.ts` — `ORIGINS` (local `http://localhost:39217`, staging `https://data-staging.canifishthis.ca`,
  production `https://data.canifishthis.ca` — unused until launch, T86 confirms the name), `resolveEnv`, `endpointsFor`.
  - `EXPO_PUBLIC_APP_ENV` = local | staging | production (a typo throws at start).
  - unset: a known `EXPO_PUBLIC_DATA_BASE` names its environment; else web = local (the dev loop + browser walks),
    phone = **staging**. Never production by default.
  - `EXPO_PUBLIC_DATA_BASE` still overrides the build's own origin (a LAN address for a phone aimed at this machine).
- `config.ts` exports `APP_ENV` and `ENDPOINTS = endpointsFor(APP_ENV.origin)` (same shape as before — T231 and App.tsx
  unchanged). **The one config to import is `apps/mobile/src/config.ts` (`APP_ENV`, `ENDPOINTS`)**; packages keep taking
  URLs injected (layers.json: a package cannot import the app).
- Dev switch: About (More → About) in a switchable build (`__DEV__`, or `EXPO_PUBLIC_ENV_SWITCH=1`) shows Local /
  Staging / Production; the choice is persisted (`envStore.web.ts` localStorage `canifishthis.appEnv.v1`;
  `envStore.native.ts` `Documents/settings/app-env.txt`, read synchronously at start) and applied on the next start
  (web reloads; a `__DEV__` build reloads its JS; an installed internal build says "close and reopen").
  Put on About, not More → Data, because MoreScreen/BrowseHost/Shell were mid-edit by T231.
- Badge: About shows a small `STAGING` / `LOCAL` badge and a "Data source" row (origin) unless the build is production
  reading production with no switch (`core/menu/about.ts` `aboutEnvVM`, words in `MENU_WORDS.about.env`).
- `apps/mobile/eas.json`: `development` (internal APK, staging, switch on), `preview` (internal APK, staging),
  `production` (production). `apps/mobile` script `export:web:staging` → `app/dist/web-staging`.
- No app.config needed: `EXPO_PUBLIC_*` are inlined by Metro at bundle time; app.json is unchanged (same bundle id).
- Native: installed tiles are `pmtiles://file:///…` (no network at draw); staging is https → no ATS/cleartext change.
  Local on a phone is http to a LAN IP: Android debug builds allow cleartext, iOS ATS exempts IP addresses; release
  builds never aim at local.
- layers.json: `apps/mobile` may import `expo-file-system` (only for the sync env read; already a dependency).

## Build commands (owner)

```bash
# Android, no EAS account needed — a release APK on a USB-connected phone, reading STAGING, with the switch:
cd app/apps/mobile
EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_ENV_SWITCH=1 npx expo run:android --variant release
# the dev loop on a phone (Metro), staging data (the default for a phone):
npx expo run:android            # or: npx expo run:ios   (iOS needs Xcode + CocoaPods)
# aimed at this machine's tile server instead (phone and Mac on one Wi-Fi):
EXPO_PUBLIC_APP_ENV=local EXPO_PUBLIC_DATA_BASE=http://<mac LAN IP>:39217 npx expo run:android

# EAS (cloud; first run asks to log in and writes extra.eas.projectId into app.json — commit that):
npm i -g eas-cli && eas login
eas build --profile development --platform android   # installable APK, staging, switch on About
eas build --profile preview --platform android       # installable APK, staging
eas build --profile development --platform ios       # needs an Apple developer account + device registration

# web pointed at staging (the staging site's app half; then deploy/staging/site.sh):
pnpm -C app/apps/mobile export:web:staging
```

## Acceptance

- `apps/mobile/src/env.test.ts` (7): defaults (web local, phone staging), named env, typo throws, DATA_BASE naming /
  overriding, the switch only when switchable, every endpoint off one origin. `core/menu.test.ts` +3 (badge, production
  silent, the switch's pending note, whole strings); `ui-native/menu/Menu.test.tsx` +2 (About renders badge/origin,
  switch press → persisted pick + note; production shows nothing). `tools/no-localhost.test.ts` allows `env.ts`.
- tsc: no new errors (the tree's remaining errors are T231's uncommitted `updates.test.ts` / `SyncButton.test.tsx`).
- Playwright on a local `expo export` with `EXPO_PUBLIC_APP_ENV=staging`, served from a static server: 31 requests to
  data-staging.canifishthis.ca (manifest, release files, tiles, basemap, map assets, sql wasm, live/dfo/inseason feeds),
  0 to localhost; About shows `STAGING` and "Staging · data-staging.canifishthis.ca"; the map and in-season sheet drew.

Depends: T231 (reads `ENDPOINTS`). Size: M. Owner: yes (run the builds; confirm the production data host in T86).
Default: phone builds read staging; production stays unused until launch.
