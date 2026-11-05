/**
 * (T234) THE BACKGROUND CHECK — what the phone runs when the system wakes the app (Android WorkManager through
 * expo-background-task, ~15 min at best; iOS when it chooses). Platform-free: the app hands in how to read the feed and
 * the manifest (the SAME reads the open app makes: `fetchObs`, the release manifest), how to raise a notification, and
 * the settings file. Nothing here is shown on a screen; the run is logged in the settings (`runs`).
 *
 *   gauges    only when an alert is set: the feed-wide newest rows (`obs.json`), each rule against its gauge's newest
 *             reading of its quantity (`@app/core` `evaluateAlert`: once per crossing, stale or missing = nothing).
 *   release   only when "new data ready" is on: the manifest's data release against the one in use (noted by the app
 *             on every open); a release this app is too old to apply is not announced (the app-update prompt says
 *             that, T390). Once per release (`toldRelease`).
 *
 * OFFLINE IS SILENT: no notification, nothing reported, the run logged "offline". Any other failure (an HTTP status, a
 * file that is not the feed's) is reported (`reportFailure`) and logged; the next run tries again. A notification that
 * cannot be raised is reported, and the rule's state is not advanced, so the next run raises it.
 */
import {
  alertNotification, evaluateAlert, isEnvironment, newestReading, NOTIFY_WORDS, releaseAllowed, reportFailure,
  UPDATE_READY_NOTE, type AlertRuleState, type AppVersion,
} from "@app/core";
import type { Manifest } from "../manifest";
import type { ObsFile } from "../feed/history";
import { noteRun, type NotifySettings, type NotifyState } from "./settings";

/** What a notification carries for its tap. */
export type NotifyData =
  | { kind: "gauge"; rule: string; station: string; item: string | null }
  | { kind: "update"; release: string };

export interface Notice { channel: "gauges" | "updates"; title: string; body: string; data: NotifyData }

export interface CheckDeps {
  settings: NotifySettings;
  /** the feed-wide newest rows (`fetchObs` over the app's feed base); throws offline */
  obs(): Promise<ObsFile>;
  /** the release manifest (the update service's url); throws offline */
  manifest(): Promise<Manifest>;
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
  const fail = (e: unknown) => {
    if (isEnvironment(e)) { offline = true; return; }
    reportFailure(e);
    failed.push(said(e));
  };
  let fired = 0;
  let told = false;

  if (s0.on && s0.rules.length) {
    try {
      const obs = await d.obs();
      const at = d.now();
      const next: Record<string, AlertRuleState> = {};
      for (const r of s0.rules) {
        const e = evaluateAlert(r, s0.ruleState[r.id] ?? null, newestReading(obs.stations[r.station], r.quantity), at);
        if (e.fire) {
          const reading = { at: e.state.at!, value: e.state.value! };
          try {
            await d.notify({ channel: "gauges", ...alertNotification(r, reading),
                             data: { kind: "gauge", rule: r.id, station: r.station, item: r.water?.item ?? null } });
            fired++;
          } catch (err) {
            // not raised: the state stays where it was, so the next run raises it
            reportFailure(err);
            failed.push(said(err));
            continue;
          }
        }
        next[r.id] = e.state;
      }
      // only the rules still there (a rule deleted while the check ran stays deleted)
      d.settings.update((s) => ({ ...s, ruleState: { ...s.ruleState,
        ...Object.fromEntries(Object.entries(next).filter(([id]) => s.rules.some((r) => r.id === id))) } }));
    } catch (e) { fail(e); }
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
    } catch (e) { fail(e); }
  }

  const W = NOTIFY_WORDS.run;
  if (!s0.on || (!s0.rules.length && !s0.updateReady)) parts.push(W.noRules);
  if (fired) parts.push(W.fired(fired));
  if (told) parts.push(W.updateReady);
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
