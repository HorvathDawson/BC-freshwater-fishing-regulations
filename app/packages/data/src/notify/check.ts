/**
 * (T234) THE BACKGROUND CHECK — what the phone runs when the system wakes the app (Android WorkManager through
 * expo-background-task, ~15 min at best; iOS when it chooses). Platform-free: the app hands in how to read the feed and
 * the manifest (the SAME reads the open app makes: `fetchObs`, the release manifest), how to raise a notification, and
 * the settings file. Nothing here is shown on a screen; the run is logged in the settings (`runs`).
 *
 *   gauges    only when an alert is on: the feed-wide newest rows (`obs.json`), each alert against its gauge's newest
 *             reading through `@app/core` `evaluateAlerts` (T527: once per crossing of a value or of a percentile's line
 *             for the reading's day, re-armed past a small margin; stale, missing or a day with no line = nothing).
 *             (T554 item 1) a skip is kept on the alert with since-when (its row says "gauge not reporting since …"),
 *             the run line counts the silent ones, and a gauge silent for ALERT_SILENT_TELL_MS is told once.
 *   release   only when "new data ready" is on: the manifest's data release against the one in use (noted by the app
 *             on every open); a release this app is too old to apply is not announced (the app-update prompt says
 *             that, T390). Once per release (`toldRelease`).
 *   app       (v1.1) only when "new app version" is on: the version file our server publishes for this build's channel
 *             (`latest.json` beside the staging APK page, `play.json` for a Play build — deploy/staging/build-apk.sh),
 *             newer than this install → once per version (`appVersionToTell`). Play's own in-app update check needs
 *             the app in the foreground (an Activity), so the background check never asks Play.
 *
 * OFFLINE IS SILENT: no notification, nothing reported, the run logged "offline". Any other failure (an HTTP status, a
 * file that is not the feed's) is reported (`reportFailure`) and logged; the next run tries again. A notification that
 * cannot be raised is reported, and the rule's state is not advanced, so the next run raises it.
 */
import {
  alertNotification, alertSilentNotification, APP_VERSION_NOTE, appVersionToTell, evaluateAlerts, isEnvironment, newestReading, NOTIFY_WORDS,
  releaseAllowed, reportFailure, UPDATE_READY_NOTE, type AppVersion,
} from "@app/core";
import type { Manifest } from "../manifest";
import type { ObsFile } from "../feed/history";
import { noteRun, type NotifySettings, type NotifyState } from "./settings";

/** What a notification carries for its tap. */
export type NotifyData =
  | { kind: "gauge"; rule: string; station: string; item: string | null }
  | { kind: "update"; release: string }
  | { kind: "app"; version: string };

export interface Notice { channel: "gauges" | "updates" | "app"; title: string; body: string; data: NotifyData }

export interface CheckDeps {
  settings: NotifySettings;
  /** the feed-wide newest rows (`fetchObs` over the app's feed base); throws offline */
  obs(): Promise<ObsFile>;
  /** the release manifest (the update service's url); throws offline */
  manifest(): Promise<Manifest>;
  /** (v1.1) the version file our server publishes for this build (`latest.json` / `play.json`); absent: no channel */
  latestApp?: (() => Promise<unknown>) | null;
  /** raise a local notification */
  notify(n: Notice): Promise<void>;
  /** this app's version: a release asking for a newer one is not announced; null: no gate */
  appVersion?: AppVersion | null;
  now(): number;
}

/**
 * Should "new data ready" be told for `m`? Only when the switch is on, the app has noted a release in use, `m`'s data
 * release differs from it, this app can apply it, and it was not told before.
 */
export function updateReadyFor(s: Pick<NotifyState, "on" | "updateReady" | "held" | "toldRelease">, m: Manifest,
                               appVersion: AppVersion | null | undefined): boolean {
  if (!s.on || !s.updateReady || s.held === null) return false;
  if (m.data_release === s.held || m.data_release === s.toldRelease) return false;
  return appVersion ? releaseAllowed(m.minAppVersion, appVersion).ok : true;
}

const said = (e: unknown) => (e instanceof Error ? e.message : String(e));

export async function runBackgroundCheck(d: CheckDeps): Promise<BgResult> {
  const started = d.now();
  const s0 = d.settings.read();
  const parts: string[] = [];
  let offline = false;
  const failed: string[] = [];
  /** logged; `reportFailure` (at the catch) says nothing of the environment's */
  const fail = (e: unknown) => {
    if (isEnvironment(e)) offline = true;
    else failed.push(said(e));
  };
  let fired = 0;
  let silent = 0;
  let told = false;

  const live = s0.on ? s0.rules.filter((r) => r.on) : [];
  if (live.length) {
    try {
      const obs = await d.obs();
      const cur = d.settings.read();
      const e = evaluateAlerts(live, cur.ruleState, (r) => newestReading(obs.stations[r.station], r.quantity), d.now());
      const states = { ...e.states };
      for (const f of e.fire) {
        try {
          await d.notify({ channel: "gauges", ...alertNotification(f.rule, f.reading),
                           data: { kind: "gauge", rule: f.rule.id, station: f.rule.station, item: f.rule.water?.item ?? null } });
          fired++;
        } catch (err) {
          // not raised: its state stays where it was, so the next run raises it
          reportFailure(err);
          failed.push(said(err));
          const prev = cur.ruleState[f.rule.id];
          if (prev) states[f.rule.id] = prev; else delete states[f.rule.id];
        }
      }
      // (T554 item 1) a gauge silent for a day: told once; not raised, it is told on the next run
      for (const r of e.silent) {
        try {
          await d.notify({ channel: "gauges", ...alertSilentNotification(r, states[r.id]!),
                           data: { kind: "gauge", rule: r.id, station: r.station, item: r.water?.item ?? null } });
        } catch (err) {
          reportFailure(err);
          failed.push(said(err));
          states[r.id] = { ...states[r.id]!, silentTold: null };
        }
      }
      silent = e.skipped.filter((k) => k === "missing" || k === "stale").length;
      // only the alerts still there (one deleted while the check ran stays deleted)
      d.settings.update((s) => ({ ...s, ruleState: { ...s.ruleState,
        ...Object.fromEntries(Object.entries(states).filter(([id]) => s.rules.some((r) => r.id === id))) } }));
    } catch (e) { reportFailure(e); fail(e); }
  }

  if (s0.on && s0.updateReady && s0.held !== null) {
    try {
      const m = await d.manifest();
      if (updateReadyFor(d.settings.read(), m, d.appVersion)) {
        await d.notify({ channel: "updates", title: UPDATE_READY_NOTE.title, body: UPDATE_READY_NOTE.body,
                         data: { kind: "update", release: m.data_release } });
        d.settings.update((s) => ({ ...s, toldRelease: m.data_release }));
        told = true;
      }
    } catch (e) { reportFailure(e); fail(e); }
  }

  let toldApp = false;
  if (s0.on && s0.appVersion && d.latestApp && d.appVersion) {
    try {
      const latest = await d.latestApp();
      const key = appVersionToTell(d.settings.read(), latest, d.appVersion);
      if (key) {
        const v = key.split("+")[0]!;
        await d.notify({ channel: "app", title: APP_VERSION_NOTE.title, body: APP_VERSION_NOTE.body(v),
                         data: { kind: "app", version: v } });
        d.settings.update((s) => ({ ...s, toldAppVersion: key }));
        toldApp = true;
      }
    } catch (e) { reportFailure(e); fail(e); }
  }

  const W = NOTIFY_WORDS.run;
  if (!s0.on || (!live.length && !s0.updateReady && !s0.appVersion)) parts.push(W.noRules);
  if (fired) parts.push(W.fired(fired));
  if (silent) parts.push(W.silent(silent));
  if (told) parts.push(W.updateReady);
  if (toldApp) parts.push(W.appVersion);
  if (offline) parts.push(W.offline);
  for (const f of failed) parts.push(W.failed(f));
  if (!parts.length) parts.push(W.quiet);
  const result: BgResult = { fired, told, offline, failed, line: parts.join(", ") };
  try {
    d.settings.update(noteRun({ at: started, ms: d.now() - started, result: result.line }));
  } catch (e) { reportFailure(e); }
  return result;
}

export interface BgResult {
  readonly fired: number;
  readonly told: boolean;
  readonly offline: boolean;
  readonly failed: readonly string[];
  /** the run in words, as logged */
  readonly line: string;
}
