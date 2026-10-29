/**
 * THE ENDPOINTS (P10): every URL the app reads, from ONE base, and the only files a `localhost` may appear in (this and
 * `env.ts`; `tools/no-localhost.test.ts`).
 *
 * (T232) THE BASE IS THE ENVIRONMENT'S: local / staging / production (`env.ts`), chosen at build time by
 * `EXPO_PUBLIC_APP_ENV` (eas.json's profiles set it), a development build's switch on About aside. Unset, the web is
 * local (the dev server on this machine, `pnpm tiles`) and a phone is staging. `EXPO_PUBLIC_DATA_BASE` still sets the
 * base outright for the build's environment — a LAN address such as http://192.168.1.20:39217 for a device build aimed
 * at this machine, where "localhost" is the phone itself.
 *
 * ONE SET, ONE BASE. The release (`release/manifest.json`, P08/P11) is the app's data; the tiles below are drawn while
 * the manifest names no tile set of its own, and the feed is the live gauges. They must be one vintage: tiles, release
 * and feed join on section and station ids, and a mixed set colours nothing.
 */
import { Platform } from "react-native";
import { resolveAppVersion, versionLine, type AppEnv } from "@app/core";
import { imageryTiles } from "@app/map";
import { endpointsFor, reportUrlFor, resolveChannel, resolveEnv } from "./env";
import { channelStore, envStore, restart } from "./envStore";

const SWITCHABLE = __DEV__ || process.env.EXPO_PUBLIC_ENV_SWITCH === "1";

/** (T232) This run's environment and data origin. Read once, at start: a switch applies on the next start. */
export const APP_ENV = resolveEnv({
  web: Platform.OS === "web",
  appEnv: process.env.EXPO_PUBLIC_APP_ENV,
  dataBase: process.env.EXPO_PUBLIC_DATA_BASE,
  switchable: SWITCHABLE,
  override: SWITCHABLE ? envStore.read() : null,
});

/**
 * (T251) THE APP'S VERSION: app.json's semver and build number, the commit and the build day, stamped into the bundle
 * when Metro starts (`../version-stamp.js`). Said on About, sent with data requests, written into the Updates log's export.
 */
export const APP_VERSION = resolveAppVersion({
  version: process.env.EXPO_PUBLIC_APP_VERSION,
  build: process.env.EXPO_PUBLIC_APP_BUILD_NUMBER,
  sha: process.env.EXPO_PUBLIC_GIT_SHA,
  date: process.env.EXPO_PUBLIC_BUILD_DATE,
});

/** (T251) Live, or the staging test channel (`test/`) a tester opts into. Read once at start, like the environment. */
export const UPDATE_CHANNEL = resolveChannel({
  named: process.env.EXPO_PUBLIC_UPDATE_CHANNEL,
  switchable: SWITCHABLE,
  override: SWITCHABLE ? channelStore.read() : null,
});

/**
 * (T251) THE UPDATES LOG is a development and staging tool: kept and shown in a switchable build, or any build reading
 * anything but production (an internal staging APK). A production build reading production data keeps none.
 */
export const UPDATES_LOG = SWITCHABLE || APP_ENV.env !== "production";

/**
 * THE MENU'S BUILD FACTS (owner 2026-10-21). `CONTACT` is THE ONE contact value the menu's Contact and Report a problem
 * pages read: an email address (`name@domain`) or a web link (`https://…`). Unset, the menu says contact details are
 * coming and a report goes out through the share sheet only — no address is ever written into the code; the owner sets
 * `EXPO_PUBLIC_CONTACT` for a build. `APP_BUILD` names the build in About and in a report ("1.2.0 (34)"), set by the
 * build (`EXPO_PUBLIC_APP_BUILD`); unset, the menu says it is not stated. (T251) It now leads with the version
 * (`versionLine`): "0.1.0 (build 1, a1b2c3d, 2026-10-28)", the profile's label after it ("· dev (staging)").
 */
export const MENU_INFO = {
  appBuild: [versionLine(APP_VERSION), process.env.EXPO_PUBLIC_APP_BUILD?.trim() || null].filter(Boolean).join(" · "),
  contact: process.env.EXPO_PUBLIC_CONTACT?.trim() || null,
  // (T232) About's data-source badge and, in a development build, its switch (kept; applied on the next start)
  env: {
    about: { env: APP_ENV.env, built: APP_ENV.built, origin: APP_ENV.origin, switchable: APP_ENV.switchable },
    pending: null,
    pick: (env: AppEnv) => { envStore.write(env); if (env !== APP_ENV.env) restart(); },
  },
} as const;

/**
 * THE SATELLITE IMAGERY'S TILES (owner 2026-10-21: Esri World Imagery, online-only). KEYLESS BY DEFAULT — the owner's
 * decision (2026-10-21 late: "we used esri free without a key in original app lets do that again. this is a non profit
 * app"): unset, the template is Esri's own ArcGIS Online World_Imagery MapServer, as the original app used, and that is
 * what ships. `EXPO_PUBLIC_ARCGIS_API_KEY` is OPTIONAL config: an ArcGIS Location Platform key (restricted by referrer /
 * app in the ArcGIS dashboard) switches the tiles to Esri's keyed endpoint, counted against that account's free tier.
 * Esri's terms on the keyless path: handoff/PLAN/app-data/satellite/esri/README.md. Never a key in the code.
 */
export const IMAGERY_TILES = imageryTiles(process.env.EXPO_PUBLIC_ARCGIS_API_KEY?.trim() || null);

/** Every URL the app reads (the release, tiles, feeds, sql.js), from `APP_ENV.origin`. T231's update service reads it. */
export const ENDPOINTS = endpointsFor(APP_ENV.origin, UPDATE_CHANNEL);

/** (T237) Where "Send report" posts (null: email / share only). */
export const REPORT_URL = reportUrlFor(APP_ENV.env, process.env.EXPO_PUBLIC_REPORT_URL);

/** (T251) Pick the update channel (a switchable build): kept, applied on the next start. */
export function pickChannel(c: string): void {
  channelStore.write(c);
  if (c !== UPDATE_CHANNEL) restart();
}
