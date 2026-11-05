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
 */
import type { AlertRuleState, GaugeAlertRule } from "@app/core";

export interface BgRun {
  /** when it started (epoch ms, this device's clock) */
  readonly at: number;
  readonly ms: number;
  /** what it found, in words (`NOTIFY_WORDS.run`) */
  readonly result: string;
}

export interface NotifyState {
  readonly v: 1;
  /** the global switch */
  readonly on: boolean;
  /** tell me when newer regulations data is ready */
  readonly updateReady: boolean;
  readonly rules: readonly GaugeAlertRule[];
  readonly ruleState: Readonly<Record<string, AlertRuleState>>;
  /** the data release in use (the app notes it on every open), null before the first */
  readonly held: string | null;
  /** the newest release already told (once per release) */
  readonly toldRelease: string | null;
  /** the last background runs, oldest first */
  readonly runs: readonly BgRun[];
}

export const NOTIFY_DEFAULT: NotifyState = Object.freeze({
  v: 1, on: false, updateReady: false, rules: [], ruleState: {}, held: null, toldRelease: null, runs: [],
});

/** How many background runs the log keeps. */
export const RUNS_KEPT = 20;

export interface NotifyText { read(): string | null; write(text: string): void }

/** Is there anything for the background check to do? */
export function notifyActive(s: Pick<NotifyState, "on" | "updateReady" | "rules">): boolean {
  return s.on && (s.updateReady || s.rules.length > 0);
}

function parse(text: string | null): NotifyState {
  if (!text) return NOTIFY_DEFAULT;
  let j: Partial<NotifyState> | null;
  try { j = JSON.parse(text) as Partial<NotifyState>; } catch { return NOTIFY_DEFAULT; }
  if (!j || j.v !== 1) return NOTIFY_DEFAULT;
  return {
    v: 1,
    on: j.on === true,
    updateReady: j.updateReady === true,
    rules: Array.isArray(j.rules) ? j.rules.filter((r) => r && typeof r.id === "string" && typeof r.station === "string"
                                                          && Number.isFinite(r.value)) : [],
    ruleState: j.ruleState && typeof j.ruleState === "object" ? j.ruleState : {},
    held: typeof j.held === "string" ? j.held : null,
    toldRelease: typeof j.toldRelease === "string" ? j.toldRelease : null,
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

/** Add a rule (or replace the one with its id); adding one turns notifications on — that is what the reader asked. */
export const putRule = (r: GaugeAlertRule) => (s: NotifyState): NotifyState => {
  const had = s.rules.some((x) => x.id === r.id);
  // a changed rule starts again: its side of the new line is read on the next check
  const { [r.id]: _old, ...ruleState } = s.ruleState;
  return { ...s, on: true, rules: had ? s.rules.map((x) => (x.id === r.id ? r : x)) : [...s.rules, r], ruleState };
};

export const removeRule = (id: string) => (s: NotifyState): NotifyState => {
  const { [id]: _gone, ...ruleState } = s.ruleState;
  return { ...s, rules: s.rules.filter((r) => r.id !== id), ruleState };
};

/** The app opened on this data release: the release in use. */
export const noteHeld = (release: string) => (s: NotifyState): NotifyState =>
  (s.held === release ? s : { ...s, held: release });

export const noteRun = (r: BgRun) => (s: NotifyState): NotifyState => ({ ...s, runs: [...s.runs, r].slice(-RUNS_KEPT) });
