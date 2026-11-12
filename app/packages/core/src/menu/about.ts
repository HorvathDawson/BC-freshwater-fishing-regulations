/** The About page: what the app is, the authority disclaimer, the official synopsis, the versions, the data source. */
import { DEV_WORDS, MENU_WORDS } from "./words";
import { SITE_ORIGIN } from "../share";

/**
 * (T232) WHERE A BUILD READS ITS DATA: the local dev server, staging or production. The origins themselves are the
 * app's config (`apps/mobile/src/env.ts`, T404: the one module that spells them); core names the three and says them.
 */
/** (owner 2026-10-29) the privacy policy: a static page of the site (Google Play asks for this URL; production). */
export const PRIVACY_URL = `${SITE_ORIGIN}/privacy`;

export type AppEnv = "local" | "staging" | "production";
export const APP_ENVS: readonly AppEnv[] = ["local", "staging", "production"];

/** (T232) The data source in use, as the config resolved it (`apps/mobile/src/env.ts` `resolveEnv`). */
export interface AboutEnvInput {
  /** the environment this run reads */
  env: AppEnv;
  /** the one the build was made for (differs when a development build's switch chose another) */
  built: AppEnv;
  /** the data origin in use (`env.ts` ORIGINS) */
  origin: string;
  /** a development / internal build: the reader may pick another source (applied on the next start) */
  switchable: boolean;
  /** a source picked here that the next start will use; null: none pending */
  pending: AppEnv | null;
  /** (T423) the source whose choice could not be saved; null: none */
  failed?: AppEnv | null;
}

export interface AboutInput {
  /** The app build (app config); null when the build states none. */
  appBuild: string | null;
  /** The data release id (the manifest's `data_release`); null on the development bundle. */
  release: string | null;
  /** The tile version the map draws (`<handles16>.<tilebuild12>`); null when none is known. */
  tiles: string | null;
  /** The printed edition the regulations were read from (the manifest's `sources.synopsis.title`). */
  edition: string | null;
  /** (T232) The data source; omitted / null: a production build, which says nothing about it. */
  dataEnv?: AboutEnvInput | null;
}

/** (T232) The data-source block: a badge, the origin, and (a development build) the switch. */
export interface AboutEnvVM {
  /** the small badge, e.g. "STAGING" */
  readonly badge: string;
  readonly label: string;
  /** e.g. "Staging · <the staging data host>" */
  readonly value: string;
  /** the switch's title and choices; null when the build has none */
  readonly choose: { readonly title: string;
                     readonly choices: readonly { readonly k: AppEnv; readonly label: string; readonly on: boolean }[] } | null;
  /** a sentence under the switch: the pending change, or that this run differs from the build; null: nothing to say */
  readonly note: string | null;
  /** (T423) a choice that could not be kept, said under the switch; null: none */
  readonly failed: string | null;
}

export interface AboutVM {
  readonly what: string;
  readonly notOfficial: string;
  readonly official: { readonly label: string; readonly url: string };
  readonly versionsTitle: string;
  readonly versions: readonly { readonly label: string; readonly value: string }[];
  /** (T232) null in a production build reading production data: the reader is never told about environments */
  readonly env: AboutEnvVM | null;
}

/**
 * (T585 F12, F13) WHICH DATA THIS SCREEN ANSWERS FROM, on the Conditions gauge card's foot in a development or staging
 * build: a phone can run an older release than the web for hours (it applies a staged set at a safe moment), and a
 * switchable build can keep another environment or channel — two devices showing different donors for one water were
 * each right about their own release, and nothing on the screen said which. So the release is named, and the data
 * source and the update channel when they are not what this build was made for. Null in a production build reading
 * production data (`aboutEnvVM`'s rule): the reader is never told about releases or environments.
 */
export interface DataProvenanceInput {
  readonly release: string | null;
  readonly env: AppEnv;
  readonly built: AppEnv;
  readonly switchable: boolean;
  /** the update channel in use, and the one this build names ("live" unless built for another) */
  readonly channel: string;
  readonly builtChannel: string;
}
export function dataProvenanceLine(i: DataProvenanceInput): string | null {
  if (i.env === "production" && i.built === "production" && !i.switchable) return null;
  const a = MENU_WORDS.about, d = DEV_WORDS.build;
  const parts = [`${a.release} ${i.release ?? a.notStated}`];
  if (i.env !== i.built) parts.push(`${a.env.label} ${a.env.names[i.env]}`);
  if (i.channel !== i.builtChannel) parts.push(`${d.channel} ${i.channel}`);
  return parts.join(" · ");
}

/** (T232) The data-source block, or null when there is nothing to say (production, built for production, no switch). */
export function aboutEnvVM(e: AboutEnvInput | null | undefined): AboutEnvVM | null {
  if (!e || (e.env === "production" && e.built === "production" && !e.switchable)) return null;
  const w = MENU_WORDS.about.env;
  const host = e.origin.replace(/^https?:\/\//, "");
  const target = e.pending ?? e.env;
  const note = e.pending && e.pending !== e.env ? w.pending(w.names[e.pending])
    : e.env !== e.built ? w.overridden(w.names[e.built]) : null;
  return {
    badge: w.names[e.env].toUpperCase(), label: w.label, value: `${w.names[e.env]} · ${host}`,
    choose: e.switchable
      ? { title: w.choose, choices: APP_ENVS.map((k) => ({ k, label: w.names[k], on: k === target })) }
      : null,
    note,
    failed: e.failed ? w.failed(w.names[e.failed]) : null,
  };
}

export function aboutVM(a: AboutInput, synopsisUrl: string): AboutVM {
  const w = MENU_WORDS.about;
  return {
    what: w.what, notOfficial: w.notOfficial, official: { label: w.official, url: synopsisUrl },
    versionsTitle: w.versions,
    versions: [
      { label: w.app, value: a.appBuild ?? w.notStated },
      { label: w.release, value: a.release ?? w.devBundle },
      { label: w.tiles, value: a.tiles ?? w.notStated },
      { label: w.regulations, value: a.edition ?? w.notStated },
    ],
    env: aboutEnvVM(a.dataEnv),
  };
}
