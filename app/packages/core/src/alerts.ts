/**
 * (T234) NOTIFICATIONS, PHONE ONLY (owner 2026-11-05): no server, no push token, no account. Raised as LOCAL
 * notifications by the phone's own background check (`@app/data` `notify/`, every ~15 min at best):
 *
 *   gauge alerts   the reader's own alerts, per gauge, on the gauge's flow or level (the units the app shows). Four kinds
 *                  (v1.1, owner 2026-11-05: "on x gauge if it rises to y point or z times"; z = a % change within a day):
 *                    above   the reading rises above Y          below   the reading falls below Y
 *                    rise    up by Z % or more within 24 h      fall    down by Z % or more within 24 h
 *                  The % kinds compare the newest reading with the one about 24 h before it from the gauge's own recent
 *                  series (`dayAgoReading`: the row nearest 24 h earlier, within `DAY_AGO_SLACK_MS`); none = no fire.
 *                  ONCE PER CROSSING (hysteresis): an alert fires when its condition goes from false to true and is not
 *                  told again while it stays true; it re-arms only once back inside by a margin (`REARM_FRACTION` of the
 *                  line for a value, `REARM_PCT_POINTS` for a %), so a reading wobbling on the line does not ring.
 *                  CADENCE (owner: "only should notify once on a river per day, or user can set max cadence"): each
 *                  alert has a minimum gap before it can fire again. The DEFAULT (`gapH: null`) is once per WATER per
 *                  day: no alert of that water fires while any of its alerts fired in the last 24 h. A gap the reader
 *                  picks (`ALERT_GAPS_H`) is the alert's own. A crossing the cadence blocks is DROPPED, never queued
 *                  (owner 2026-11-05: a rise and fall inside the window must not ring later); the alert stays past and
 *                  rings again only on a new crossing (back inside by the margin, then past) once the gap allows.
 *                  A reading missing, older than `ALERT_STALE_MS`, or the phone offline: nothing fires.
 *   update ready   a newer data release than the one in use: once per release.
 *   new app        (v1.1) a newer app version than the one installed: once per version (`appUpdate.ts`).
 *
 * Every word the notifications and their settings draw is here (AGENTS 33: tests pin the finished strings).
 */
import { CONDITIONS_WORDS, chartFigure } from "./conditions";
import { bcClockTime } from "./clock";
import { localStamp } from "./day";
import type { Band } from "./flow";

export type AlertQuantity = "discharge" | "level";
export type AlertKind = "above" | "below" | "rise" | "fall";
export const ALERT_KINDS: readonly AlertKind[] = ["above", "below", "rise", "fall"];
/** A % kind: compares with the reading a day earlier. */
export const isChangeKind = (k: AlertKind): boolean => k === "rise" || k === "fall";

/** The gaps a reader may pick, in hours; `null` (the default) is once per water per day across its alerts. */
export const ALERT_GAPS_H: readonly number[] = [1, 3, 6, 12, 24, 48];
const HOUR = 3_600_000;
/** The default cadence's window: one notification per water per this. */
export const WATER_DAY_MS = 24 * HOUR;

/** One gauge alert, as the reader set it. Binds to the STATION (and the water's durable item id), never a section. */
export interface GaugeAlertRule {
  readonly id: string;
  readonly station: string;
  /** the gauge's name as the chart titled it */
  readonly stationName: string;
  readonly quantity: AlertQuantity;
  readonly kind: AlertKind;
  /** above/below: in the app's units (m³/s for flow, m for level); rise/fall: a percent (50 = 50 %) */
  readonly value: number;
  /** the minimum gap before it can fire again, in hours; null: once per water per day (the default) */
  readonly gapH: number | null;
  /** the alert's own switch */
  readonly on: boolean;
  /** the water it was set from (a tap opens its Conditions); item null: an unnamed reach (the Conditions map opens) */
  readonly water: { readonly item: string | null; readonly name: string } | null;
  readonly createdAt: number;
}

/** What the phone remembers of an alert between checks. `past` null: no usable reading yet. */
export interface AlertRuleState {
  readonly past: boolean | null;
  /** the reading last evaluated (its time, ISO as the feed wrote it) */
  readonly at?: string | null;
  readonly value?: number | null;
  /** a % kind: the change last evaluated, in percent */
  readonly change?: number | null;
  /** when it last fired (epoch ms) */
  readonly firedAt?: number | null;
}

/** A gauge reading older than this is not evaluated (ECCC publishes hourly, about an hour behind). */
export const ALERT_STALE_MS = 6 * HOUR;
/** Back inside the line by this fraction of the line's value before a value alert can fire again. */
export const REARM_FRACTION = 0.02;
/** ...and never by less than this, in the quantity's units (a line at 0 still needs a step back). */
export const REARM_MIN: Readonly<Record<AlertQuantity, number>> = { discharge: 0.05, level: 0.01 };
/** A % alert re-arms once the day's change is this many percentage points back under its line. */
export const REARM_PCT_POINTS = 2;
/** The "24 h earlier" reading: the row nearest 24 h before the newest, no further off than this. */
export const DAY_AGO_SLACK_MS = 2 * HOUR;

export interface AlertReading { readonly at: string; readonly value: number }
/** What one alert is checked against: the newest reading and (the % kinds) the one about a day before it. */
export interface AlertInput { readonly now: AlertReading | null; readonly dayAgo?: AlertReading | null }

export type AlertSkip = "missing" | "stale" | "no-day-ago";

export interface AlertEvaluation {
  readonly state: AlertRuleState;
  /** the condition crossed now: tell it if the cadence allows (else it is dropped) */
  readonly due: boolean;
  /** why nothing was evaluated (for the run log); null when it was */
  readonly skipped: AlertSkip | null;
}

type Rule = Pick<GaugeAlertRule, "kind" | "value" | "quantity">;

/** The % change from `then` to `now`, signed for the kind (a rise positive for "rise", a fall positive for "fall"). */
function changeOf(kind: AlertKind, now: number, then: number): number {
  const pct = ((now - then) / then) * 100;
  return kind === "fall" ? -pct : pct;
}

const isPast = (r: Rule, m: number) =>
  (r.kind === "above" ? m > r.value : r.kind === "below" ? m < r.value : m >= r.value);
/** Inside the line by the re-arm margin. */
const isBack = (r: Rule, m: number) => {
  if (isChangeKind(r.kind)) return m <= r.value - REARM_PCT_POINTS;
  const g = Math.max(Math.abs(r.value) * REARM_FRACTION, REARM_MIN[r.quantity]);
  return r.kind === "above" ? m <= r.value - g : m >= r.value + g;
};

/**
 * ONE ALERT AGAINST ITS INPUT (no cadence: `evaluateAlerts` applies it). The first usable reading only sets where the
 * water is (an alert set while the river is already past its line does not ring at once: that is not a crossing).
 * Then: inside → past is due; past stays past (quiet) until back inside by the margin; between the line and the margin
 * keeps the state.
 */
export function evaluateAlert(rule: Rule, state: AlertRuleState | null, input: AlertInput | null,
                              nowMs: number): AlertEvaluation {
  const prev: AlertRuleState = state ?? { past: null };
  const reading = input?.now ?? null;
  if (!reading || !Number.isFinite(reading.value)) return { state: prev, due: false, skipped: "missing" };
  const t = Date.parse(reading.at);
  if (!Number.isFinite(t) || nowMs - t > ALERT_STALE_MS) return { state: prev, due: false, skipped: "stale" };
  let m = reading.value;
  let change: number | null = null;
  if (isChangeKind(rule.kind)) {
    const then = input?.dayAgo ?? null;
    if (!then || !Number.isFinite(then.value) || then.value <= 0) return { state: prev, due: false, skipped: "no-day-ago" };
    m = change = changeOf(rule.kind, reading.value, then.value);
  }
  // the same reading again (the feed has not moved): nothing new
  if (prev.at === reading.at && prev.past !== null) return { state: prev, due: false, skipped: null };
  const seen = { at: reading.at, value: reading.value, ...(change !== null ? { change } : {}) };
  if (prev.past === null) return { state: { ...prev, ...seen, past: isPast(rule, m) }, due: false, skipped: null };
  if (!prev.past) {
    if (isPast(rule, m)) return { state: { ...prev, ...seen, past: true }, due: true, skipped: null };
    return { state: { ...prev, ...seen }, due: false, skipped: null };
  }
  const back = isBack(rule, m);
  if (back) return { state: { ...prev, ...seen, past: false }, due: false, skipped: null };
  return { state: { ...prev, ...seen }, due: false, skipped: null };
}

/** The water an alert counts against for the default cadence: its durable item, else (an unnamed reach) its gauge. */
export const alertWaterKey = (r: Pick<GaugeAlertRule, "water" | "station">): string =>
  r.water?.item ?? `station:${r.station}`;

/** May this alert fire now? Default: no notification for its water in the last day; else its own gap since it last fired. */
export function cadenceAllows(r: Pick<GaugeAlertRule, "gapH">, ruleFiredAt: number | null | undefined,
                              waterFiredAt: number | null | undefined, nowMs: number): boolean {
  if (r.gapH === null) return waterFiredAt == null || nowMs - waterFiredAt >= WATER_DAY_MS;
  return ruleFiredAt == null || nowMs - ruleFiredAt >= r.gapH * HOUR;
}

export interface AlertsResult {
  readonly states: Readonly<Record<string, AlertRuleState>>;
  /** when each water was last told (the default cadence) */
  readonly waterFired: Readonly<Record<string, number>>;
  /** the alerts to tell now, in the alerts' order, with the reading each crossed on */
  readonly fire: readonly { readonly rule: GaugeAlertRule; readonly input: { now: AlertReading; dayAgo: AlertReading | null } }[];
  /** crossings the cadence dropped this check (for the run log) */
  readonly dropped: number;
  readonly skipped: readonly AlertSkip[];
}

/**
 * EVERY ALERT, ONE CHECK: each switched-on alert against its input, the cadence applied in the alerts' order (two of a
 * water's alerts crossing in one check under the default: the first is told, the second dropped). An alert switched off
 * is not evaluated and keeps no state.
 */
export function evaluateAlerts(rules: readonly GaugeAlertRule[], states: Readonly<Record<string, AlertRuleState>>,
                               inputOf: (r: GaugeAlertRule) => AlertInput | null,
                               waterFired: Readonly<Record<string, number>>, nowMs: number): AlertsResult {
  const out: Record<string, AlertRuleState> = {};
  const water: Record<string, number> = { ...waterFired };
  const fire: { rule: GaugeAlertRule; input: { now: AlertReading; dayAgo: AlertReading | null } }[] = [];
  const skipped: AlertSkip[] = [];
  let dropped = 0;
  for (const r of rules) {
    if (!r.on) continue;
    const input = inputOf(r);
    const prev = states[r.id] ?? null;
    const e = evaluateAlert(r, prev, input, nowMs);
    if (e.skipped) skipped.push(e.skipped);
    let s = e.state;
    if (e.due) {
      const key = alertWaterKey(r);
      if (cadenceAllows(r, prev?.firedAt, water[key], nowMs)) {
        s = { ...s, firedAt: nowMs };
        water[key] = nowMs;
        fire.push({ rule: r, input: { now: input!.now!, dayAgo: input?.dayAgo ?? null } });
      } else dropped++;
    }
    out[r.id] = s;
  }
  return { states: out, waterFired: water, fire, dropped, skipped };
}

type Row = readonly [string, number | null, number | null];
const valueOf = (r: Row, q: AlertQuantity) => (q === "level" ? r[1] : r[2]);

/** The newest reading of a quantity in the feed's rows (`[time, level, discharge]`), or null. */
export function newestReading(rows: readonly Row[] | undefined, quantity: AlertQuantity): AlertReading | null {
  if (!rows) return null;
  let best: AlertReading | null = null;
  let bestT = -Infinity;
  for (const r of rows) {
    const v = valueOf(r, quantity);
    const t = Date.parse(r[0]);
    if (v === null || v === undefined || !Number.isFinite(v) || !Number.isFinite(t) || t <= bestT) continue;
    best = { at: r[0], value: v };
    bestT = t;
  }
  return best;
}

/**
 * The reading about 24 h before `now` in the gauge's own rows: the row nearest `now − 24 h`, within
 * `DAY_AGO_SLACK_MS` either side; null when the series has none there (a gap, a short record).
 */
export function dayAgoReading(rows: readonly Row[] | undefined, quantity: AlertQuantity,
                              now: AlertReading | null): AlertReading | null {
  if (!rows || !now) return null;
  const target = Date.parse(now.at) - 24 * HOUR;
  if (!Number.isFinite(target)) return null;
  let best: AlertReading | null = null;
  let bestD = Infinity;
  for (const r of rows) {
    const v = valueOf(r, quantity);
    const t = Date.parse(r[0]);
    if (v === null || v === undefined || !Number.isFinite(v) || !Number.isFinite(t)) continue;
    const d = Math.abs(t - target);
    if (d <= DAY_AGO_SLACK_MS && d < bestD) { best = { at: r[0], value: v }; bestD = d; }
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

/** Is `v` a value this kind can take? A % must be above 0 (a fall of 100 % or more never happens). */
export function alertValueOk(kind: AlertKind, v: number | null): v is number {
  if (v === null || !Number.isFinite(v)) return false;
  if (kind === "fall") return v > 0 && v < 100;
  if (kind === "rise") return v > 0;
  return true;
}

/**
 * (T234 v1 → v1.1) AN ALERT SAVED BY v1 (`direction` "above"/"below", no kind, gap or switch) in the new shape: its
 * direction is its kind, the default cadence, switched on. Anything else unreadable: null (the caller reports it).
 */
export function migrateAlertRule(r: unknown): GaugeAlertRule | null {
  if (!r || typeof r !== "object") return null;
  const o = r as Record<string, unknown>;
  if (typeof o.id !== "string" || typeof o.station !== "string" || typeof o.value !== "number" || !Number.isFinite(o.value))
    return null;
  const quantity = o.quantity === "level" ? "level" : o.quantity === "discharge" ? "discharge" : null;
  const kind = (ALERT_KINDS as readonly unknown[]).includes(o.kind) ? o.kind as AlertKind
    : o.direction === "above" || o.direction === "below" ? o.direction : null;
  if (!quantity || !kind) return null;
  const w = o.water as { item?: unknown; name?: unknown } | null | undefined;
  return {
    id: o.id, station: o.station, stationName: typeof o.stationName === "string" ? o.stationName : o.station,
    quantity, kind, value: o.value,
    gapH: typeof o.gapH === "number" && ALERT_GAPS_H.includes(o.gapH) ? o.gapH : null,
    on: o.on !== false,
    water: w && typeof w.name === "string" ? { item: typeof w.item === "string" ? w.item : null, name: w.name } : null,
    createdAt: typeof o.createdAt === "number" ? o.createdAt : 0,
  };
}

// ── words ───────────────────────────────────────────────────────────────────────────────────────────────────────────

const unitOf = (q: AlertQuantity) => CONDITIONS_WORDS.unit[q];
const quantityWord = (q: AlertQuantity) => CONDITIONS_WORDS.quantity[q].toLowerCase();
const pct = (v: number) => `${chartFigure(v)} %`;
const gapWords = (h: number) => (h === 24 ? "once a day" : h === 48 ? "once every 2 days" : `once every ${h} h`);

export const NOTIFY_WORDS = {
  intro: "Notifications are made on this phone. Nothing is sent to a server: the app checks the gauges and the data "
    + "itself, about every 15 minutes at best. Android may check less often while the phone is idle.",
  master: { label: "Notifications", on: "On", off: "Off", hint: "Turn every notification from this app on or off." },
  updateReady: { label: "New regulations data", hint: "Tell me when a newer regulations release is ready to download." },
  appVersion: { label: "New app version", hint: "Tell me when a newer version of the app is out." },
  permission: {
    granted: "This phone lets the app notify you.",
    denied: "Notifications are blocked for this app in the phone's settings, so none can be shown.",
    undetermined: "The phone will ask you to allow notifications when you turn one on.",
    open: "Open the phone's settings",
    asking: "Asking the phone…",
  },
  sections: { alerts: "Gauge alerts", other: "Other notifications", phone: "This phone" },
  alerts: {
    none: "No gauge alerts yet. Add one here, or from a gauge's chart on a water's Conditions: \"Alert me\".",
    add: "Add an alert",
    addHint: "Find a water, then set the alert on its gauge",
    remove: "Delete",
    removeAloud: (line: string) => `Delete the alert: ${line}`,
    edit: "Edit",
    editAloud: (line: string) => `Edit the alert: ${line}`,
    switchAloud: (line: string) => `Alert on: ${line}`,
    lastSent: (ms: number) => `last sent ${localStamp(ms)}`,
    notSent: "not sent yet",
    off: "off",
    /** the card's heading: the water, then the gauge */
    heading: (water: string | null, gauge: string) => (water && water !== gauge ? `${water} · ${gauge}` : gauge),
  },
  /** (v1.1) "Add an alert": find the water whose gauge it is */
  add: {
    title: "Add an alert",
    search: "Search for a river or lake",
    searchAloud: "Search for a water to set an alert on",
    none: "No water by that name.",
    pick: "Opens the water's Conditions: set the alert with \"Alert me\" on its gauge.",
    cancel: "Cancel",
  },
  /** (v1.1) the editor: the gauge's card "Alert me", and Edit in More → Notifications */
  editor: {
    title: (gauge: string) => `Alert on ${gauge}`,
    quantity: "Quantity",
    condition: "When",
    kinds: { above: "Rises above", below: "Falls below", rise: "Rises by % in 24 h", fall: "Falls by % in 24 h" } as Record<AlertKind, string>,
    value: (kind: AlertKind, q: AlertQuantity) => (isChangeKind(kind) ? "Percent change" : `Value in ${unitOf(q)}`),
    cadence: "How often at most",
    /** beside the value: the unit, or a percent for the change kinds */
    unit: (kind: AlertKind, q: AlertQuantity) => (isChangeKind(kind) ? "%" : unitOf(q)),
    cadenceDefault: "Once a day for this water",
    cadenceGap: (h: number) => (h === 24 ? "Once a day (this alert)" : h === 48 ? "Every 2 days" : `Every ${h} h`),
    save: "Save alert",
    cancel: "Cancel",
    bad: "Type a number.",
    badPct: "Type a percent above 0 (a fall below 100).",
    now: (q: AlertQuantity, v: number) => `Now ${chartFigure(v)} ${unitOf(q)}`,
    nowUnknown: "No reading now",
    normal: (q: AlertQuantity, lo: number, hi: number) => `normal today ${chartFigure(lo)}–${chartFigure(hi)} ${unitOf(q)}`,
    normalUnknown: "no normal range for today",
    loading: "Reading the gauge…",
  },
  /** (T234) the gauge card's control */
  alertMe: {
    button: "Alert me",
    hint: "Get a notification when this gauge passes a value you set",
    saved: "Saved. Manage alerts in More → Notifications.",
    on: (n: number) => (n === 1 ? "1 alert on this gauge" : `${n} alerts on this gauge`),
    off: "Notifications are off. Turn them on in More → Notifications.",
  },
  runs: {
    title: "Last background checks",
    none: "No background check has run yet.",
    note: "When the phone ran the app's check, to measure how late alerts can be.",
  },
  /** the Android notification channels (the phone's own settings list them by these names) */
  channels: {
    gauges: { name: "Gauge alerts", description: "When a gauge passes a value you set" },
    updates: { name: "Regulations data", description: "When newer regulations data is ready" },
    app: { name: "App updates", description: "When a newer version of the app is out" },
  },
  run: {
    offline: "offline",
    failed: (why: string) => `failed: ${why}`,
    fired: (n: number) => (n === 1 ? "1 alert" : `${n} alerts`),
    dropped: (n: number) => `${n} not sent (cadence)`,
    quiet: "nothing to tell",
    updateReady: "new data ready",
    appVersion: "new app version",
    noRules: "no alerts set",
  },
} as const;

/** An alert in words: "Flow above 30 m³/s" / "Flow rises 50 % or more within 24 h". */
export function alertLine(r: Pick<GaugeAlertRule, "quantity" | "kind" | "value">): string {
  const q = CONDITIONS_WORDS.quantity[r.quantity];
  const u = unitOf(r.quantity);
  switch (r.kind) {
    case "above": return `${q} above ${chartFigure(r.value)} ${u}`;
    case "below": return `${q} below ${chartFigure(r.value)} ${u}`;
    case "rise": return `${q} rises ${pct(r.value)} or more within 24 h`;
    case "fall": return `${q} falls ${pct(r.value)} or more within 24 h`;
  }
}

/** The cadence in words: "at most once a day for this water" / "at most once every 3 h". */
export function cadenceLine(gapH: number | null): string {
  return gapH === null ? "at most once a day for this water" : `at most ${gapWords(gapH)}`;
}

/** The card's line: "Flow above 30 m³/s · at most once a day for this water · last sent 2026-11-05 at 14:05 PT". */
export function alertCardLine(r: GaugeAlertRule, s: AlertRuleState | null | undefined): string {
  const W = NOTIFY_WORDS.alerts;
  const sent = s?.firedAt ? W.lastSent(s.firedAt) : W.notSent;
  return [alertLine(r), cadenceLine(r.gapH), r.on ? sent : W.off].join(" · ");
}

/**
 * THE EDITOR'S ONE PLAIN LINE — what will happen: "You get a notification when the flow at Port Coquitlam rises above
 * 30 m³/s, at most once a day for Coquitlam River. Not again until it has dropped back below the line."
 */
export function alertSummary(r: Pick<GaugeAlertRule, "quantity" | "kind" | "value" | "gapH" | "stationName" | "water">): string {
  const q = quantityWord(r.quantity);
  const u = unitOf(r.quantity);
  const what = r.kind === "above" ? `rises above ${chartFigure(r.value)} ${u}`
    : r.kind === "below" ? `falls below ${chartFigure(r.value)} ${u}`
    : r.kind === "rise" ? `rises by ${pct(r.value)} or more within 24 hours`
    : `falls by ${pct(r.value)} or more within 24 hours`;
  const often = r.gapH === null ? `at most once a day for ${r.water?.name ?? "this water"}` : `at most ${gapWords(r.gapH)}`;
  const again = r.kind === "above" ? "Not again until it has dropped back under the line."
    : r.kind === "below" ? "Not again until it has risen back over the line."
    : "Not again until the day's change has eased off.";
  return `You get a notification when the ${q} at ${r.stationName} ${what}, ${often}. ${again}`;
}

/** The normal range for a day from its envelope: p25–p75, as the chart's band draws it; null without one. */
export function normalRangeOf(band: Band | null | undefined): { lo: number; hi: number } | null {
  return band ? { lo: band[1], hi: band[3] } : null;
}

/** Beside the editor's value: "Now 31.2 m³/s · normal today 12–25 m³/s". */
export function alertContextLine(q: AlertQuantity, current: number | null, normal: { lo: number; hi: number } | null): string {
  const W = NOTIFY_WORDS.editor;
  return [current !== null ? W.now(q, current) : W.nowUnknown,
          normal ? W.normal(q, normal.lo, normal.hi) : W.normalUnknown].join(" · ");
}

/** The notification an alert raises. */
export function alertNotification(r: GaugeAlertRule, input: { now: AlertReading; dayAgo?: AlertReading | null }):
  { title: string; body: string } {
  const where = r.water?.name ?? r.stationName;
  const q = quantityWord(r.quantity);
  const u = unitOf(r.quantity);
  const reading = input.now;
  const t = Date.parse(reading.at);
  const read = `${r.stationName} read ${chartFigure(reading.value)} ${u} at ${bcClockTime(t)}`;
  if (isChangeKind(r.kind) && input.dayAgo) {
    const ch = Math.abs(((reading.value - input.dayAgo.value) / input.dayAgo.value) * 100);
    return {
      title: `${where}: ${q} ${r.kind === "rise" ? "up" : "down"} ${pct(Math.round(ch))} in a day`,
      body: `${read}, from ${chartFigure(input.dayAgo.value)} ${u} a day earlier.`,
    };
  }
  return { title: `${where}: ${q} ${r.kind === "below" ? "below" : "above"} ${chartFigure(r.value)} ${u}`, body: `${read}.` };
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
