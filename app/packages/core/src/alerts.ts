/**
 * (T234) NOTIFICATIONS, PHONE ONLY (owner 2026-11-05): no server, no push token, no account. Two kinds, both raised as
 * LOCAL notifications by the phone's own background check (`@app/data` `notify/`, every ~15 min at best):
 *
 *   gauge alerts   the reader's own lines, per gauge: "flow rises above X" / "level falls below Y" (the quantity the
 *                  gauge has, in the units the app shows). Evaluated ON THE PHONE against the feed's newest reading.
 *                  ONCE PER CROSSING: a rule fires when the reading goes from inside the line to past it, and is not
 *                  told again while it stays past; it re-arms only once the reading has come back inside by a margin
 *                  (`REARM_FRACTION`), so a reading wobbling on the line does not ring every check. A reading missing,
 *                  older than `ALERT_STALE_MS`, or the phone offline: nothing fires and nothing changes.
 *   update ready   a newer data release than the one in use: once per release.
 *
 * Every word the notifications and their settings draw is here (AGENTS 33: tests pin the finished strings).
 */
import { CONDITIONS_WORDS, chartFigure } from "./conditions";
import { bcClockTime } from "./clock";
import { localStamp } from "./day";

export type AlertQuantity = "discharge" | "level";
export type AlertDirection = "above" | "below";

/** One gauge alert, as the reader set it. Binds to the STATION (and the water's durable item id), never a section. */
export interface GaugeAlertRule {
  readonly id: string;
  readonly station: string;
  /** the gauge's name as the chart titled it */
  readonly stationName: string;
  readonly quantity: AlertQuantity;
  readonly direction: AlertDirection;
  /** in the app's units: m³/s for flow, m for level */
  readonly value: number;
  /** the water it was set from (a tap opens its Conditions); item null: an unnamed reach (the Conditions map opens) */
  readonly water: { readonly item: string | null; readonly name: string } | null;
  readonly createdAt: number;
}

/** What the phone remembers of a rule between checks. `past` null: no usable reading yet. */
export interface AlertRuleState {
  readonly past: boolean | null;
  /** the reading last evaluated (its time, ISO as the feed wrote it) */
  readonly at?: string | null;
  readonly value?: number | null;
  /** when it last fired (epoch ms) */
  readonly firedAt?: number | null;
}

/** A gauge reading older than this is not evaluated (ECCC publishes hourly, about an hour behind). */
export const ALERT_STALE_MS = 6 * 3_600_000;
/** Back inside the line by this fraction of the line's value before a rule can fire again. */
export const REARM_FRACTION = 0.02;
/** ...and never by less than this, in the quantity's units (a line at 0 still needs a step back). */
export const REARM_MIN: Readonly<Record<AlertQuantity, number>> = { discharge: 0.05, level: 0.01 };

export interface AlertReading { readonly at: string; readonly value: number }

export interface AlertEvaluation {
  readonly state: AlertRuleState;
  /** a crossing: raise the notification */
  readonly fire: boolean;
  /** why nothing was evaluated (for the run log); null when it was */
  readonly skipped: "missing" | "stale" | null;
}

const isPast = (r: GaugeAlertRule, v: number) => (r.direction === "above" ? v > r.value : v < r.value);
/** Inside the line by the re-arm margin. */
const isBack = (r: GaugeAlertRule, v: number) => {
  const m = Math.max(Math.abs(r.value) * REARM_FRACTION, REARM_MIN[r.quantity]);
  return r.direction === "above" ? v <= r.value - m : v >= r.value + m;
};

/**
 * ONE RULE AGAINST ONE READING. The first usable reading only sets where the water is (a rule set while the river is
 * already past its line does not ring at once: that is not a crossing). Then: inside → past fires; past stays past
 * (quiet) until the reading is back inside by the margin; a reading between the line and the margin keeps the state.
 */
export function evaluateAlert(rule: GaugeAlertRule, state: AlertRuleState | null, reading: AlertReading | null,
                              nowMs: number): AlertEvaluation {
  const prev: AlertRuleState = state ?? { past: null };
  if (!reading || !Number.isFinite(reading.value)) return { state: prev, fire: false, skipped: "missing" };
  const t = Date.parse(reading.at);
  if (!Number.isFinite(t) || nowMs - t > ALERT_STALE_MS) return { state: prev, fire: false, skipped: "stale" };
  // the same reading again (the feed has not moved): nothing new to say
  if (prev.at === reading.at && prev.past !== null) return { state: prev, fire: false, skipped: null };
  const v = reading.value;
  const seen = { at: reading.at, value: v };
  if (prev.past === null) return { state: { ...prev, ...seen, past: isPast(rule, v) }, fire: false, skipped: null };
  if (!prev.past) {
    if (isPast(rule, v)) return { state: { ...seen, past: true, firedAt: nowMs }, fire: true, skipped: null };
    return { state: { ...prev, ...seen }, fire: false, skipped: null };
  }
  return { state: { ...prev, ...seen, past: !isBack(rule, v) }, fire: false, skipped: null };
}

/** The newest reading of a quantity in the feed's rows (`[time, level, discharge]`), or null. */
export function newestReading(rows: readonly (readonly [string, number | null, number | null])[] | undefined,
                              quantity: AlertQuantity): AlertReading | null {
  if (!rows) return null;
  let best: AlertReading | null = null;
  let bestT = -Infinity;
  for (const r of rows) {
    const v = quantity === "level" ? r[1] : r[2];
    const t = Date.parse(r[0]);
    if (v === null || v === undefined || !Number.isFinite(v) || !Number.isFinite(t) || t <= bestT) continue;
    best = { at: r[0], value: v };
    bestT = t;
  }
  return best;
}

/** Read a number the reader typed ("12.5", "12,5", " 3 "); null when it is not one. */
export function parseAlertValue(text: string): number | null {
  const t = text.trim().replace(",", ".");
  if (!/^-?\d+(\.\d+)?$|^-?\.\d+$/.test(t)) return null;
  const v = Number(t);
  return Number.isFinite(v) ? v : null;
}

const unitOf = (q: AlertQuantity) => CONDITIONS_WORDS.unit[q];
const quantityWord = (q: AlertQuantity) => CONDITIONS_WORDS.quantity[q].toLowerCase();

export const NOTIFY_WORDS = {
  intro: "Notifications are made on this phone. Nothing is sent to a server: the app checks the gauges and the data "
    + "itself, about every 15 minutes at best. Android may check less often while the phone is idle.",
  master: { label: "Notifications", on: "On", off: "Off", hint: "Turn every notification from this app on or off." },
  updateReady: { label: "New regulations data", hint: "Tell me when a newer regulations release is ready to download." },
  permission: {
    granted: "This phone lets the app notify you.",
    denied: "Notifications are blocked for this app in the phone's settings, so none can be shown.",
    undetermined: "The phone will ask you to allow notifications when you turn one on.",
    open: "Open the phone's settings",
    asking: "Asking the phone…",
  },
  alerts: {
    title: "Gauge alerts",
    none: "No gauge alerts. Add one from a gauge's chart on a water's Conditions: \"Alert me\".",
    remove: "Delete",
    removeAloud: (line: string) => `Delete the alert: ${line}`,
    edit: "Change",
  },
  runs: {
    title: "Last background checks",
    none: "No background check has run yet.",
    note: "When the phone ran the app's check, to measure how late alerts can be.",
  },
  /** (T234) the gauge card's control */
  alertMe: {
    button: "Alert me",
    hint: "Get a notification when this gauge passes a value you set",
    above: "Rises above",
    below: "Falls below",
    value: (q: AlertQuantity) => `Value in ${unitOf(q)}`,
    save: "Save alert",
    cancel: "Cancel",
    bad: "Type a number.",
    now: (q: AlertQuantity, v: number) => `Now ${chartFigure(v)} ${unitOf(q)}.`,
    saved: "Saved. Manage alerts in More → Notifications.",
    on: (n: number) => (n === 1 ? "1 alert on this gauge" : `${n} alerts on this gauge`),
    off: "Notifications are off. Turn them on in More → Notifications.",
  },
  /** the Android notification channels (the phone's own settings list them by these names) */
  channels: {
    gauges: { name: "Gauge alerts", description: "When a gauge passes a value you set" },
    updates: { name: "Regulations data", description: "When newer regulations data is ready" },
  },
  run: {
    offline: "offline",
    failed: (why: string) => `failed: ${why}`,
    fired: (n: number) => (n === 1 ? "1 alert" : `${n} alerts`),
    quiet: "nothing to tell",
    updateReady: "new data ready",
    noRules: "no alerts set",
  },
} as const;

/** A rule in words: "Flow rises above 50 m³/s". */
export function alertLine(r: Pick<GaugeAlertRule, "quantity" | "direction" | "value">): string {
  const q = CONDITIONS_WORDS.quantity[r.quantity];
  return `${q} ${r.direction === "above" ? "rises above" : "falls below"} ${chartFigure(r.value)} ${unitOf(r.quantity)}`;
}

/** The notification a crossing raises. */
export function alertNotification(r: GaugeAlertRule, reading: AlertReading): { title: string; body: string } {
  const where = r.water?.name ?? r.stationName;
  const verb = r.direction === "above" ? "above" : "below";
  const t = Date.parse(reading.at);
  return {
    title: `${where}: ${quantityWord(r.quantity)} ${verb} ${chartFigure(r.value)} ${unitOf(r.quantity)}`,
    body: `${r.stationName} read ${chartFigure(reading.value)} ${unitOf(r.quantity)} at ${bcClockTime(t)}.`,
  };
}

/** The update-ready notification. */
export const UPDATE_READY_NOTE = {
  title: "New regulations data is ready",
  body: "Open the app to update.",
} as const;

/** A background run's line in the log: "2026-11-05 at 14:05 PT · 1.2 s · 1 alert". */
export function runLine(r: { at: number; ms: number; result: string }): string {
  return `${localStamp(r.at)} · ${(r.ms / 1000).toFixed(1)} s · ${r.result}`;
}
