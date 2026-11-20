/**
 * (review 2026-11-20) WHAT THE BACKGROUND CHECK READS, made once for the periodic task and the push wake-up
 * (`apps/mobile` notify.native.ts): the gauges' `obs.json`, this build's version file and the release manifest, each by
 * conditional GET with its last copy kept (T741), and the gauge file held to a feed gate made FOR EACH RUN (T676 item 2:
 * a need never outlives the run that met it). Here, not in the native file, so a test can hold both: a gate made once at
 * module level, or a kept copy dropped, failed nobody before.
 */
import { createFeedGate, type AppVersion, type FeedGate } from "@app/core";
import { fetchObs } from "../feed/http";
import { parseManifest } from "../manifest";
import { conditionalJson, type HeldCopy } from "./conditional";

export interface CheckSourcesOptions {
  fetch: typeof fetch;
  /** the gauges' feed base (`<base>/obs.json`) */
  alertFeed: string;
  manifestUrl: string;
  /** this build's version file, or null (no channel: no new-version notice) */
  versionFile: string | null;
  appVersion: AppVersion | null;
  /** the last copy of each file, with its ETag */
  held: { obs: HeldCopy; manifest: HeldCopy; version: HeldCopy };
  /** tests: the gate a run is held to (default `createFeedGate(appVersion)`, a new one each call) */
  gate?: () => FeedGate;
}

export function createCheckSources(o: CheckSourcesOptions) {
  const gate = o.gate ?? (() => createFeedGate(o.appVersion));
  return {
    obs: () => fetchObs(o.alertFeed, o.fetch, undefined, o.held.obs, gate()),
    latestApp: o.versionFile ? () => conditionalJson(o.versionFile!, o.fetch, o.held.version) : null,
    // the kept copy is held to the same reader as a fresh one (`parseManifest` throws on a manifest it refuses)
    manifest: async () => parseManifest(await conditionalJson(o.manifestUrl, o.fetch, o.held.manifest, (v) => { parseManifest(v); })),
  };
}
