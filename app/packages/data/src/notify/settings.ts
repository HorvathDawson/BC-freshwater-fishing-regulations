/**
 * (T234) THE NOTIFICATION SETTINGS, KEPT ON THE PHONE (one small file of the app's own, never inside a release, never
 * sent anywhere): the global switch, the "new data ready" switch, the reader's gauge alerts, what the background check
 * remembers between runs (each rule's side of its line, the release in use and the last release told), and the last
 * background runs (to measure how late the phone runs them).
 *
 * EVERYTHING OPT-IN: a fresh install has every switch off and no rule. The background check is wanted only while
 * `active` says so.
 *
 * Every change re-reads the file first and writes it whole: the background check (a headless start, or the same runtime
 * with the app in the background) and the settings page write the same file, and neither may undo the other's change.
 * A write that fails throws to whoever asked (the page shows it).
 *
 * (v1.1) THE FILE IS `v: 2`: alerts carry a kind (above/below/rise/fall), a cadence and their own switch; the state adds
 * when each water was last told (the default cadence), the "new app version" switch and the last version told. A v1
 * file reads into v2 whole (`migrateAlertRule`: direction → kind, default cadence, on), so no reader's alert is lost; an
 * alert that cannot be read is reported (a lost alert is a defect), never dropped quietly.
 */
import { migrateAlertRule, reportOnce, type AlertRuleState, type GaugeAlertRule } from "@app/core";

export interface BgRun {
  /** when it started (epoch ms, this device's clock) */
  readonly at: number;
  readonly ms: number;
  /** what it found, in words (`NOTIFY_WORDS.run`) */
  readonly result: string;
}

export interface NotifyState {
  readonly v: 2;
  /** the global switch */
  readonly on: boolean;
  /** tell me when newer regulations data is ready */
  readonly updateReady: boolean;
  /** (v1.1) tell me when a newer app version is out */
  readonly appVersion: boolean;
  readonly rules: readonly GaugeAlertRule[];
  readonly ruleState: Readonly<Record<string, AlertRuleState>>;
  /** (v1.1) when each water was last told (epoch ms), for the default once-per-water-per-day cadence */
  readonly waterFired: Readonly<Record<string, number>>;
  /** the data release in use (the app notes it on every open), null before the first */
  readonly held: string | null;
  /** the newest release already told (once per release) */
  readonly toldRelease: string | null;
  /** (v1.1) the newest app version already told ("0.6.0+14"), once per version */
  readonly toldAppVersion: string | null;
  /** the last background runs, oldest first */
  readonly runs: readonly BgRun[];
}

export const NOTIFY_DEFAULT: NotifyState = Object.freeze({
  v: 2, on: false, updateReady: false, appVersion: false, rules: [], ruleState: {}, waterFired: {}, held: null,
  toldRelease: null, toldAppVersion: null, runs: [],
});

/** How many background runs the log keeps. */
export const RUNS_KEPT = 20;

export interface NotifyText { read(): string | null; write(text: string): void }

/** Is there anything for the background check to do? */
export function notifyActive(s: Pick<NotifyState, "on" | "updateReady" | "appVersion" | "rules">): boolean {
  return s.on && (s.updateReady || s.appVersion || s.rules.some((r) => r.on));
}

function parse(text: string | null): NotifyState {
  if (!text) return NOTIFY_DEFAULT;
  let j: (Omit<Partial<NotifyState>, "v" | "rules"> & { v?: number; rules?: unknown }) | null;
  // unreadable: the defaults (nothing on), and said — the reader's alerts were lost, which is a defect
  try { j = JSON.parse(text) as typeof j; } catch (e) { reportOnce("notify-settings", e); return NOTIFY_DEFAULT; }
  if (!j || (j.v !== 1 && j.v !== 2)) {
    reportOnce("notify-settings-version", new Error(`notify.json: unknown version ${String(j?.v)}`));
    return NOTIFY_DEFAULT;
  }
  // v1 and v2 alike: every alert through the one reader (a v1 alert takes its direction as its kind)
  const raw = Array.isArray(j.rules) ? j.rules : [];
  const rules = raw.map(migrateAlertRule).filter((r): r is GaugeAlertRule => r !== null);
  if (rules.length < raw.length)
    reportOnce("notify-rules-unreadable", new Error(`notify.json: ${raw.length - rules.length} alert(s) could not be read`));
  return {
    v: 2,
    on: j.on === true,
    updateReady: j.updateReady === true,
    appVersion: j.appVersion === true,
    rules,
    ruleState: j.ruleState && typeof j.ruleState === "object" ? j.ruleState : {},
    waterFired: j.waterFired && typeof j.waterFired === "object" ? j.waterFired : {},
    held: typeof j.held === "string" ? j.held : null,
    toldRelease: typeof j.toldRelease === "string" ? j.toldRelease : null,
    toldAppVersion: typeof j.toldAppVersion === "string" ? j.toldAppVersion : null,
    runs: Array.isArray(j.runs) ? j.runs.filter((r) => r && typeof r.at === "number").slice(-RUNS_KEPT) : [],
  };
}

export interface NotifySettings {
  /** the state as the file holds it now */
  read(): NotifyState;
  /** read, change, write whole; throws when the write fails. Listeners are told. */
  update(fn: (s: NotifyState) => NotifyState): NotifyState;
  subscribe(on: () => void): () => void;
}

export function createNotifySettings(store: NotifyText): NotifySettings {
  const listeners = new Set<() => void>();
  return {
    read: () => parse(store.read()),
    update(fn) {
      const cur = parse(store.read());
      const next = fn(cur);
      if (next === cur) return cur;
      store.write(JSON.stringify(next));
      for (const l of [...listeners]) l();
      return next;
    },
    subscribe(on) { listeners.add(on); return () => { listeners.delete(on); }; },
  };
}

// ── the changes the page and the app make ───────────────────────────────────────────────────────────────────────────

export const setNotifyOn = (on: boolean) => (s: NotifyState): NotifyState => ({ ...s, on });
export const setUpdateReady = (on: boolean) => (s: NotifyState): NotifyState => ({ ...s, updateReady: on });
export const setAppVersionNotice = (on: boolean) => (s: NotifyState): NotifyState => ({ ...s, appVersion: on });

/** Add a rule (or replace the one with its id); adding one turns notifications on — that is what the reader asked. */
export const putRule = (r: GaugeAlertRule) => (s: NotifyState): NotifyState => {
  const had = s.rules.some((x) => x.id === r.id);
  // a changed rule starts again: its side of the new line is read on the next check; when it last fired is kept, so an
  // edit never resets its cadence
  const { [r.id]: old, ...rest } = s.ruleState;
  const ruleState = old?.firedAt ? { ...rest, [r.id]: { past: null, firedAt: old.firedAt } } : rest;
  return { ...s, on: true, rules: had ? s.rules.map((x) => (x.id === r.id ? r : x)) : [...s.rules, r], ruleState };
};

/** One alert's own switch; on again, it starts again (its side read on the next check). */
export const setRuleOn = (id: string, on: boolean) => (s: NotifyState): NotifyState => {
  const r = s.rules.find((x) => x.id === id);
  if (!r || r.on === on) return s;
  // the master switch is the reader's own: one alert's switch never moves it
  return { ...putRule({ ...r, on })(s), on: s.on };
};

export const removeRule = (id: string) => (s: NotifyState): NotifyState => {
  const { [id]: _gone, ...ruleState } = s.ruleState;
  return { ...s, rules: s.rules.filter((r) => r.id !== id), ruleState };
};

/** The app opened on this data release: the release in use. */
export const noteHeld = (release: string) => (s: NotifyState): NotifyState =>
  (s.held === release ? s : { ...s, held: release });

export const noteRun = (r: BgRun) => (s: NotifyState): NotifyState => ({ ...s, runs: [...s.runs, r].slice(-RUNS_KEPT) });
