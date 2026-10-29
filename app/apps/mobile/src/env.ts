/**
 * (T232) THE APP'S ENVIRONMENTS — where a build reads its data: the release, the tiles, the three feeds (gauges, DFO
 * salmon, in-season), the synopsis copy and sql.js's engine all hang off ONE data origin (P10), so choosing the
 * environment is choosing that origin. Pure: `config.ts` hands in the build's variables, the platform and the
 * persisted development override; tests call `resolveEnv` directly.
 *
 *   local       the dev server on this machine (`pnpm tiles`, :39217). On a phone "localhost" is the phone itself:
 *               set `EXPO_PUBLIC_DATA_BASE=http://<this machine's LAN address>:39217` for a device build.
 *   staging     data-staging.canifishthis.ca (R2 bucket bc-fishing-regulations-staging, `deploy/staging/`);
 *               the staging site (the web app + SEO pages) is staging.canifishthis.ca.
 *   production  data.canifishthis.ca — UNUSED until production launches (T86 confirms the name); a build only reads it
 *               when made with `EXPO_PUBLIC_APP_ENV=production`.
 *
 * Chosen at BUILD time by `EXPO_PUBLIC_APP_ENV` (Metro inlines `EXPO_PUBLIC_*` into the bundle; eas.json's profiles set
 * it). Unset: a known `EXPO_PUBLIC_DATA_BASE` names its environment; otherwise the web (the dev loop and the browser
 * walks) is local and a phone is STAGING — a phone cannot reach localhost, and a build never defaults to production.
 * A development build (`__DEV__`, or `EXPO_PUBLIC_ENV_SWITCH=1` for an installable internal build) may switch at run
 * time from About; the choice is kept on the device and applied on the next start.
 */
import { APP_ENVS, type AppEnv } from "@app/core";

/** Each environment's data origin (no trailing slash). */
export const ORIGINS: Readonly<Record<AppEnv, string>> = Object.freeze({
  local: "http://localhost:39217",
  staging: "https://data-staging.canifishthis.ca",
  production: "https://data.canifishthis.ca",
});

const isAppEnv = (v: unknown): v is AppEnv => typeof v === "string" && (APP_ENVS as readonly string[]).includes(v);

const trimBase = (u: string) => u.trim().replace(/\/+$/, "");

export interface EnvInputs {
  /** `Platform.OS === "web"` */
  web: boolean;
  /** `EXPO_PUBLIC_APP_ENV` */
  appEnv?: string | undefined;
  /** `EXPO_PUBLIC_DATA_BASE`: an explicit origin for the build's own environment (a LAN address, a test server) */
  dataBase?: string | undefined;
  /** may this build switch at run time (`__DEV__ || EXPO_PUBLIC_ENV_SWITCH === "1"`) */
  switchable: boolean;
  /** the persisted run-time choice (`envStore`); ignored unless `switchable` */
  override?: string | null;
}

export interface ResolvedEnv {
  /** the environment this run reads */
  env: AppEnv;
  /** the one the build was made for */
  built: AppEnv;
  /** the data origin, no trailing slash */
  origin: string;
  switchable: boolean;
}

/** Which environment a known origin belongs to, or null. */
export function envOfOrigin(base: string | undefined): AppEnv | null {
  if (!base?.trim()) return null;
  const b = trimBase(base);
  return (Object.keys(ORIGINS) as AppEnv[]).find((k) => ORIGINS[k] === b) ?? null;
}

/**
 * The environment and origin for this run. An invalid `EXPO_PUBLIC_APP_ENV` throws: a build aimed at a typo must fail
 * at start, not quietly read some other environment's data.
 */
export function resolveEnv(i: EnvInputs): ResolvedEnv {
  const named = i.appEnv?.trim();
  if (named && !isAppEnv(named)) {
    throw new Error(`EXPO_PUBLIC_APP_ENV="${named}" is not one of ${APP_ENVS.join(", ")}`);
  }
  const built: AppEnv = (named as AppEnv | undefined) || envOfOrigin(i.dataBase) || (i.web ? "local" : "staging");
  const env: AppEnv = i.switchable && isAppEnv(i.override) ? i.override : built;
  // an explicit base is the BUILD's environment's origin (a phone's LAN address for local); a switch elsewhere uses the table
  const origin = i.dataBase?.trim() && env === built ? trimBase(i.dataBase) : ORIGINS[env];
  return { env, built, origin, switchable: i.switchable };
}

/** Every URL the app reads, from one origin (P10). The shape `ENDPOINTS` has always had; T231's service reads it. */
export function endpointsFor(base: string) {
  return {
    base,
    /** The release manifest (app-manifest/2); every release file's url is relative to `${base}/release/`. */
    manifest: `${base}/release/manifest.json`,
    release: `${base}/release/`,
    tiles: {
      atlas: `${base}/atlas.pmtiles`,
      basemap: `${base}/basemap.pmtiles`,
      outside: `${base}/bc_outside.geojson`,
    },
    /** The live gauge feed: real ECCC data from `python -m pipeline.gauges.feed.publish`, matching the release. */
    feed: `${base}/feeds/live`,
    /** (T211) The live DFO salmon feed (`python -m pipeline.regs.dfo_salmon.live`; R2 `feeds/dfo/`). */
    dfoFeed: `${base}/feeds/dfo`,
    /** (T230) The provincial in-season changes (T225 worker's index.json) + their curated links (R2 `feeds/inseason/`). */
    inSeason: `${base}/feeds/inseason`,
    /** Where sql.js finds its engine on the web: the same origin as the release files it opens. */
    sqlWasm: `${base}/`,
  } as const;
}
