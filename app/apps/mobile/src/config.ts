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
import type { AppEnv } from "@app/core";
import { imageryTiles } from "@app/map";
import { endpointsFor, resolveEnv } from "./env";
import { envStore, restart } from "./envStore";

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
 * THE MENU'S BUILD FACTS (owner 2026-10-21). `CONTACT` is THE ONE contact value the menu's Contact and Report a problem
 * pages read: an email address (`name@domain`) or a web link (`https://…`). Unset, the menu says contact details are
 * coming and a report goes out through the share sheet only — no address is ever written into the code; the owner sets
 * `EXPO_PUBLIC_CONTACT` for a build. `APP_BUILD` names the build in About and in a report ("1.2.0 (34)"), set by the
 * build (`EXPO_PUBLIC_APP_BUILD`); unset, the menu says it is not stated.
 */
export const MENU_INFO = {
  appBuild: process.env.EXPO_PUBLIC_APP_BUILD?.trim() || null,
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
export const ENDPOINTS = endpointsFor(APP_ENV.origin);
