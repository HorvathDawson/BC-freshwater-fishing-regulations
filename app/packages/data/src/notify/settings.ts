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
 * (T527) THE FILE IS `v: 3`: one alert model, stock-style (a gauge crossing above / below a value or a percentile for
 * the day, once per crossing). A v1 or v2 file reads into v3 through the one reader (`migrateAlertRule`): above / below a
 * value is kept with its switch and state; a ±% in 24 h alert (v1.1) cannot become a crossing, so it is REFUSED — kept in
 * `refused` with its words, shown on More → Notifications until the reader dismisses it, and reported. Nothing is dropped
 * quietly.
 */
import { migrateAlertRule, NOTIFY_WORDS, reportOnce, type AlertRuleState, type GaugeAlertRule, type RefusedAlert } from "@app/core";

export interface BgRun {
  /** when it started (epoch ms, this device's clock) */
  readonly at: number;
  readonly ms: number;
  /** what it found, in words (`NOTIFY_WORDS.run`) */
  readonly result: string;
}

export interface NotifyState {
  readonly v: 3;
  /** the global switch */
  readonly on: boolean;
  /** tell me when newer regulations data is ready */
  readonly updateReady: boolean;
  /** (v1.1) tell me when a newer app version is out */
  readonly appVersion: boolean;
  readonly rules: readonly GaugeAlertRule[];
  readonly ruleState: Readonly<Record<string, AlertRuleState>>;
  /** (T527) saved alerts this version could not keep, said on the page until dismissed */
  readonly refused: readonly RefusedAlert[];
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
  v: 3, on: false, updateReady: false, appVersion: false, rules: [], ruleState: {}, refused: [], held: null,
  toldRelease: null, toldAppVersion: null, runs: [],
});

/** How many background runs the log keeps. */
export const RUNS_KEPT = 20;

/**
 * The settings file as whole text. (T554 item 2) `write` is atomic (a temp file moved over it, store.native.ts);
 * `keepAside` copies a file that cannot be read beside it (`notify.json.corrupt-<ms>`) before anything overwrites it —
 * a store without it refuses to overwrite an unreadable file (the reader's alerts are never lost unkept).
 */
export interface NotifyText { read(): string | null; write(text: string): void; keepAside?(text: string): void }

/** (T554 item 2) What an unreadable file reads as: nothing on, and a note on More → Notifications that says so. */
const UNREADABLE: NotifyState = Object.freeze({
  ...NOTIFY_DEFAULT, refused: [{ line: NOTIFY_WORDS.refused.fileLine, why: NOTIFY_WORDS.refused.file }],
});

/** Is there anything for the background check to do? */
export function notifyActive(s: Pick<NotifyState, "on" | "updateReady" | "appVersion" | "rules">): boolean {
  return s.on && (s.updateReady || s.appVersion || s.rules.some((r) => r.on));
}

const isRefused = (r: unknown): r is RefusedAlert =>
  !!r && typeof (r as RefusedAlert).line === "string" && typeof (r as RefusedAlert).why === "string";

function parse(text: string | null): NotifyState {
  if (text === null) return NOTIFY_DEFAULT;        // never written
  let j: (Omit<Partial<NotifyState>, "v" | "rules" | "refused"> & { v?: number; rules?: unknown; refused?: unknown }) | null;
  // (T554 item 2) unreadable — empty (a torn write), not JSON, or no version this app knows: nothing on, SAID on the
  // page and reported; `update` keeps the file aside before it writes over it
  if (!text) { reportOnce("notify-settings-empty", new Error("notify.json is empty")); return UNREADABLE; }
  try { j = JSON.parse(text) as typeof j; } catch (e) { reportOnce("notify-settings", e); return UNREADABLE; }
  if (!j || (j.v !== 1 && j.v !== 2 && j.v !== 3)) {
    reportOnce("notify-settings-version", new Error(`notify.json: unknown version ${String(j?.v)}`));
    return UNREADABLE;
  }
  // every version alike: every alert through the one reader; what it cannot keep is refused, said and reported
  const raw = Array.isArray(j.rules) ? j.rules : [];
  const rules: GaugeAlertRule[] = [];
  const refused: RefusedAlert[] = Array.isArray(j.refused) ? j.refused.filter(isRefused) : [];
  let lost = 0;
  for (const m of raw.map(migrateAlertRule)) {
    if ("rule" in m) rules.push(m.rule);
    else { refused.push(m.refused); lost++; }
  }
  if (lost) reportOnce("notify-rules-refused", new Error(`notify.json v${j.v}: ${lost} alert(s) could not be kept as a crossing alert`));
  const states = j.ruleState && typeof j.ruleState === "object" ? j.ruleState : {};
  return {
    v: 3,
    on: j.on === true,
    updateReady: j.updateReady === true,
    appVersion: j.appVersion === true,
    rules,
    // only the kept alerts' state; what a state means is unchanged (its side of the line, the reading, when it fired)
    ruleState: Object.fromEntries(Object.entries(states).filter(([id]) => rules.some((r) => r.id === id))),
    refused,
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
      const text = store.read();
      const cur = parse(text);
      const next = fn(cur);
      if (next === cur) return cur;
      // (T554 item 2) an unreadable file is kept aside before it is written over (empty: nothing to keep)
      if (cur === UNREADABLE && text) {
        if (!store.keepAside) throw new Error("notify.json could not be read and this store cannot keep it aside; not overwritten");
        store.keepAside(text);
      }
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

/** Add an alert; adding one turns notifications on — that is what the reader asked. Its side is read on the next check. */
export const putRule = (r: GaugeAlertRule) => (s: NotifyState): NotifyState => {
  const { [r.id]: _old, ...ruleState } = s.ruleState;
  return { ...s, on: true, rules: [...s.rules.filter((x) => x.id !== r.id), r], ruleState };
};

/** One alert's own switch; on again, it starts again (its side read on the next check, when it last fired kept). */
export const setRuleOn = (id: string, on: boolean) => (s: NotifyState): NotifyState => {
  const r = s.rules.find((x) => x.id === id);
  if (!r || r.on === on) return s;
  const firedAt = s.ruleState[id]?.firedAt ?? null;
  // the master switch is the reader's own: one alert's switch never moves it
  return { ...s, rules: s.rules.map((x) => (x.id === id ? { ...x, on } : x)),
           ruleState: { ...s.ruleState, [id]: firedAt ? { past: null, firedAt } : { past: null } } };
};

export const removeRule = (id: string) => (s: NotifyState): NotifyState => {
  const { [id]: _gone, ...ruleState } = s.ruleState;
  return { ...s, rules: s.rules.filter((r) => r.id !== id), ruleState };
};

/** (T527) The reader has read the note on the alerts that were not kept. */
export const dismissRefused = (s: NotifyState): NotifyState => (s.refused.length ? { ...s, refused: [] } : s);

/** The app opened on this data release: the release in use. */
export const noteHeld = (release: string) => (s: NotifyState): NotifyState =>
  (s.held === release ? s : { ...s, held: release });

export const noteRun = (r: BgRun) => (s: NotifyState): NotifyState => ({ ...s, runs: [...s.runs, r].slice(-RUNS_KEPT) });

/** (T563) The percentile alerts' lines worked out again from the record the phone holds now (one write, only when one moved). */
export const setRuleLines = (rules: readonly GaugeAlertRule[]) => (s: NotifyState): NotifyState => {
  const by = new Map(rules.map((r) => [r.id, r] as const));
  let changed = false;
  const next = s.rules.map((r) => {
    const n = by.get(r.id);
    if (!n || n.threshold === r.threshold || r.threshold.kind !== "percentile") return r;
    changed = true;
    return { ...r, threshold: n.threshold };
  });
  return changed ? { ...s, rules: next } : s;
};
