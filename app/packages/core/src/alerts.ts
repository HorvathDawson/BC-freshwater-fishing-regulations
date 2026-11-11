/**
 * (T234) NOTIFICATIONS, PHONE ONLY (owner 2026-11-05): no server, no push token, no account. Raised as LOCAL
 * notifications by the phone's own background check (`@app/data` `notify/`, every ~15 min at best):
 *
 *   gauge alerts   (T527, owner 2026-11-09: "a clean alerting system like you would have for stocks") ONE MODEL, like a
 *                  brokerage price alert: per gauge, on its flow or its level, "crosses above" or "crosses below" a line.
 *                  The line is either
 *                    a value        in the app's units (m³/s for flow, m for level), or
 *                    a percentile   of the gauge's own record FOR THE DAY ("passes the median / the 75th percentile"):
 *                                   the same per-day-of-year envelope the gauge chart draws (the release's climatology,
 *                                   `bandAt` per day, `valueAtPercentile` on its knots), so the line moves with the
 *                                   date. Kept on the alert as one value per day of the year (`percentileByDay`, worked out
 *                                   from the chart's year series and WORKED OUT AGAIN whenever the app has the record:
 *                                   `refreshAlertLines`, T563): the background check runs headless, with no release open. Only p10..p90: the envelope holds nothing past its outer knots.
 *                  EDGE-TRIGGERED: it fires ONCE when the reading goes from its side of the line to past it, is quiet
 *                  while it stays past, and RE-ARMS only once the reading is back across by a small margin
 *                  (`REARM_FRACTION` of the line, at least `REARM_MIN`), so a reading wobbling on the line does not ring.
 *                  The first usable reading only sets the side: an alert saved while already past waits for a crossing.
 *                  A reading missing, older than `ALERT_STALE_MS`, a day with no line (no record), or the phone offline:
 *                  nothing fires.
 *   update ready   a newer data release than the one in use: once per release.
 *   new app        (v1.1) a newer app version than the one installed: once per version (`appUpdate.ts`).
 *
 * T527 REPLACED the v1.1 model (four kinds incl. ±% in 24 h, a per-alert cadence, once per water per day). Saved alerts
 * are migrated explicitly (`migrateAlertRule`): above/below a value carry over; a ±% alert has no crossing to become, so
 * it is REFUSED and said on More → Notifications (`RefusedAlert`), never dropped quietly.
 *
 * Every word the notifications and their settings draw is here (AGENTS 33: tests pin the finished strings).
 */
import { CONDITIONS_WORDS } from "./conditions";
import { figure, levelChip, levelName, levelSpan } from "./standing";
import { bcClockTime } from "./clock";
import { calendarDayOfYear, localStamp } from "./day";
import { monthAbbr, valueAtPercentile, type Band } from "./flow";

export type AlertQuantity = "discharge" | "level";
export type AlertDirection = "above" | "below";
export const ALERT_DIRECTIONS: readonly AlertDirection[] = ["above", "below"];

/** The line an alert crosses: a fixed value, or a percentile of the gauge's record for the reading's day. */
export type AlertThreshold =
  | { readonly kind: "value"; readonly value: number }
  | {
    readonly kind: "percentile";
    /** 10..90 (`PERCENTILE_MIN`..`PERCENTILE_MAX`): 50 is the median */
    readonly pct: number;
    /** the line for each day of the year (index = day − 1, 366 long), in the quantity's units; null: no record that day */
    readonly byDay: readonly (number | null)[];
  };

/** One gauge alert, as the reader set it. Binds to the STATION (and the water's durable item id), never a section. */
export interface GaugeAlertRule {
  readonly id: string;
  readonly station: string;
  /** the gauge's name as the chart titled it */
  readonly stationName: string;
  readonly quantity: AlertQuantity;
  readonly direction: AlertDirection;
  readonly threshold: AlertThreshold;
  /** the alert's own switch */
  readonly on: boolean;
  /** the water it was set from (a tap opens its Conditions); null: set on the gauge alone (the Conditions map opens) */
  readonly water: { readonly item: string | null; readonly name: string } | null;
  readonly createdAt: number;
}

/** What the phone remembers of an alert between checks. `past` null: no usable reading yet. */
export interface AlertRuleState {
  readonly past: boolean | null;
  /** the reading last evaluated (its time, ISO as the feed wrote it) */
  readonly at?: string | null;
  readonly value?: number | null;
  /** when it last fired (epoch ms) */
  readonly firedAt?: number | null;
  /**
   * (T554 item 1) why the last check could not evaluate it (missing / stale / no-record), and since when it has not been
   * evaluated (epoch ms, the first check of the run of skips); cleared by the next evaluation. Shown on the alert's row
   * (`alertListLine`) and in the run log; a gauge silent for ALERT_SILENT_TELL_MS is told once (`silentTold`).
   */
  readonly skipped?: AlertSkip | null;
  readonly skippedSince?: number | null;
  /** (T554 item 1) when the reader was told this alert's gauge is silent (epoch ms); cleared when it reports again */
  readonly silentTold?: number | null;
}

const HOUR = 3_600_000;
/** A gauge reading older than this is not evaluated (ECCC publishes hourly, about an hour behind). */
export const ALERT_STALE_MS = 6 * HOUR;
/** (T554 item 1) An alert whose gauge has given no usable reading for this long is told once: it cannot be checked. */
export const ALERT_SILENT_TELL_MS = 24 * HOUR;
/** Back across the line by this fraction of the line before it can fire again (the hysteresis). */
export const REARM_FRACTION = 0.02;
/** ...and never by less than this, in the quantity's units (a line at 0 still needs a step back). */
export const REARM_MIN: Readonly<Record<AlertQuantity, number>> = { discharge: 0.05, level: 0.01 };

/** The percentiles a reader may pick: the envelope's outer knots bound them (p10, p90). */
export const PERCENTILE_MIN = 10;
export const PERCENTILE_MAX = 90;
/** The quick picks; anything else in range is "Custom". */
export const PERCENTILE_PRESETS: readonly number[] = [10, 25, 50, 75, 90];
/** Days in the per-day line (a leap year's). */
export const DAYS_IN_LINE = 366;

export interface AlertReading { readonly at: string; readonly value: number }

export type AlertSkip = "missing" | "stale" | "no-record";

export interface AlertEvaluation {
  readonly state: AlertRuleState;
  /** the reading crossed the line now: tell it */
  readonly due: boolean;
  /** the line the reading was checked against (null when nothing was evaluated) */
  readonly line: number | null;
  /** why nothing was evaluated (for the run log); null when it was */
  readonly skipped: AlertSkip | null;
}

/**
 * The line for one percentile on every day of the year, from the chart's year envelope (`Series.yearBand`: one `Band`
 * per day, index = day − 1). A day with no envelope is null (no line that day: the alert is not evaluated then).
 */
export function percentileByDay(yearBand: readonly (Band | null)[], pct: number): (number | null)[] {
  return Array.from({ length: DAYS_IN_LINE }, (_, i) => valueAtPercentile(yearBand[i] ?? null, pct / 100));
}

/**
 * (T563) THE LINE FOLLOWS THE RECORD THE PHONE HOLDS NOW. A percentile alert is its percentile; `byDay` is only the
 * line worked out from the gauge's record (headless, the check has no release open), so it is worked out AGAIN whenever
 * the app has the record in hand: a new HYDAT vintage applies by itself. `bandOf` gives the station's year envelope in
 * the alert's quantity, or null when it cannot be read (the old line then stays and the alert is counted in `unread`:
 * a line from the last record is better than none, and the caller says so). A value alert is never touched.
 */
export function refreshAlertLines(rules: readonly GaugeAlertRule[],
                                  bandOf: (station: string, quantity: AlertQuantity) => readonly (Band | null)[] | null):
  { readonly rules: readonly GaugeAlertRule[]; readonly changed: boolean; readonly unread: readonly string[] } {
  let changed = false;
  const unread: string[] = [];
  const next = rules.map((r) => {
    const t = r.threshold;
    if (t.kind !== "percentile") return r;
    const band = bandOf(r.station, r.quantity);
    if (!hasRecord(band)) { unread.push(r.id); return r; }
    const byDay = percentileByDay(band!, t.pct);
    if (byDay.every((v, i) => v === t.byDay[i])) return r;
    changed = true;
    return { ...r, threshold: { ...t, byDay } };
  });
  return { rules: changed ? next : rules, changed, unread };
}

/** Does this year envelope hold any day at all (else no percentile alert can be set on it)? */
export const hasRecord = (yearBand: readonly (Band | null)[] | null | undefined): boolean =>
  !!yearBand && yearBand.some((b) => b !== null);

/** The line for a reading taken at `at`: the value, or the percentile's line on the reading's own day (as the chart). */
export function thresholdAt(t: AlertThreshold, at: string): number | null {
  if (t.kind === "value") return t.value;
  const v = t.byDay[calendarDayOfYear(at) - 1];
  return v === null || v === undefined || !Number.isFinite(v) ? null : v;
}

/** The re-arm margin around a line, in the quantity's units. */
export const rearmMargin = (q: AlertQuantity, line: number): number => Math.max(Math.abs(line) * REARM_FRACTION, REARM_MIN[q]);

const isPast = (d: AlertDirection, m: number, line: number) => (d === "above" ? m > line : m < line);
const isBack = (d: AlertDirection, q: AlertQuantity, m: number, line: number) => {
  const g = rearmMargin(q, line);
  return d === "above" ? m <= line - g : m >= line + g;
};

type Rule = Pick<GaugeAlertRule, "direction" | "threshold" | "quantity">;

/**
 * ONE ALERT AGAINST ITS NEWEST READING. The first usable reading only sets the side (an alert set while the river is
 * already past its line does not ring at once: that is not a crossing). Then: on its side → past is due; past stays past
 * (quiet) until back across by the margin; between the line and the margin keeps the state. The same reading read again
 * (the feed has not moved) changes nothing.
 */
export function evaluateAlert(rule: Rule, state: AlertRuleState | null, reading: AlertReading | null,
                              nowMs: number): AlertEvaluation {
  const held: AlertRuleState = state ?? { past: null };
  // (T554 item 1) a skip is kept with since-when (the first of a run of skips); an evaluation clears it
  const skip = (why: AlertSkip): AlertEvaluation => ({
    state: { ...held, skipped: why, skippedSince: held.skipped ? (held.skippedSince ?? nowMs) : nowMs },
    due: false, line: null, skipped: why });
  const prev: AlertRuleState = held.skipped || held.skippedSince || held.silentTold
    ? { ...held, skipped: null, skippedSince: null, silentTold: null } : held;
  if (!reading || !Number.isFinite(reading.value)) return skip("missing");
  const t = Date.parse(reading.at);
  if (!Number.isFinite(t) || nowMs - t > ALERT_STALE_MS) return skip("stale");
  const line = thresholdAt(rule.threshold, reading.at);
  if (line === null) return skip("no-record");
  if (prev.at === reading.at && prev.past !== null) return { state: prev, due: false, line, skipped: null };
  const m = reading.value;
  const seen = { at: reading.at, value: m };
  if (prev.past === null) return { state: { ...prev, ...seen, past: isPast(rule.direction, m, line) }, due: false, line, skipped: null };
  if (!prev.past) {
    const past = isPast(rule.direction, m, line);
    return { state: { ...prev, ...seen, past }, due: past, line, skipped: null };
  }
  const back = isBack(rule.direction, rule.quantity, m, line);
  return { state: { ...prev, ...seen, past: !back }, due: false, line, skipped: null };
}

export interface AlertFire { readonly rule: GaugeAlertRule; readonly reading: AlertReading; readonly line: number }

export interface AlertsResult {
  readonly states: Readonly<Record<string, AlertRuleState>>;
  /** the alerts to tell now, in the alerts' order, with the reading and the line it crossed */
  readonly fire: readonly AlertFire[];
  readonly skipped: readonly AlertSkip[];
  /** (T554 item 1) the alerts whose gauge has given no usable reading for ALERT_SILENT_TELL_MS, to tell once now */
  readonly silent: readonly GaugeAlertRule[];
}

/** EVERY ALERT, ONE CHECK: each switched-on alert against its gauge's newest reading. An alert switched off is skipped. */
export function evaluateAlerts(rules: readonly GaugeAlertRule[], states: Readonly<Record<string, AlertRuleState>>,
                               readingOf: (r: GaugeAlertRule) => AlertReading | null, nowMs: number): AlertsResult {
  const out: Record<string, AlertRuleState> = {};
  const fire: AlertFire[] = [];
  const skipped: AlertSkip[] = [];
  const silent: GaugeAlertRule[] = [];
  for (const r of rules) {
    if (!r.on) continue;
    const reading = readingOf(r);
    const e = evaluateAlert(r, states[r.id] ?? null, reading, nowMs);
    if (e.skipped) skipped.push(e.skipped);
    out[r.id] = e.due ? { ...e.state, firedAt: nowMs } : e.state;
    if (e.due) fire.push({ rule: r, reading: reading!, line: e.line! });
    const s = e.state;
    if (silentGauge(s) && !s.silentTold && nowMs - s.skippedSince! >= ALERT_SILENT_TELL_MS) {
      out[r.id] = { ...s, silentTold: nowMs };
      silent.push(r);
    }
  }
  return { states: out, fire, skipped, silent };
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

/** Read a number the reader typed ("12.5", "12,5", " 3 "); null when it is not one. */
export function parseAlertValue(text: string): number | null {
  const t = text.trim().replace(",", ".");
  if (!/^-?\d+(\.\d+)?$|^-?\.\d+$/.test(t)) return null;
  const v = Number(t);
  return Number.isFinite(v) ? v : null;
}

/** A percentile the reader may pick: a whole number from 10 to 90 (the envelope's outer knots). */
export const percentileOk = (p: number | null): p is number =>
  p !== null && Number.isInteger(p) && p >= PERCENTILE_MIN && p <= PERCENTILE_MAX;

// ── saved alerts from before T527 ───────────────────────────────────────────────────────────────────────────────────

/** An alert a saved file held that this version cannot keep, in words: shown on More → Notifications until dismissed. */
export interface RefusedAlert { readonly line: string; readonly why: string }

export type Migrated = { readonly rule: GaugeAlertRule } | { readonly refused: RefusedAlert };

const isQuantity = (q: unknown): q is AlertQuantity => q === "discharge" || q === "level";
const isDirection = (d: unknown): d is AlertDirection => d === "above" || d === "below";

function thresholdOf(t: unknown): AlertThreshold | null {
  if (!t || typeof t !== "object") return null;
  const o = t as Record<string, unknown>;
  if (o.kind === "value" && typeof o.value === "number" && Number.isFinite(o.value)) return { kind: "value", value: o.value };
  if (o.kind === "percentile" && typeof o.pct === "number" && percentileOk(o.pct) && Array.isArray(o.byDay)
      && o.byDay.length === DAYS_IN_LINE && o.byDay.every((v) => v === null || (typeof v === "number" && Number.isFinite(v))))
    return { kind: "percentile", pct: o.pct, byDay: o.byDay as (number | null)[] };
  return null;
}

/**
 * EVERY SAVED ALERT THROUGH ONE READER (T527). The current shape reads as is. Before T527 (v1: `direction`; v1.1: `kind`
 * above/below/rise/fall, a cadence `gapH`): above / below a value IS a crossing alert — kept, with its switch and its
 * state (the cadence is gone: once per crossing replaces it). A ±% in 24 h alert has no line to cross, so it is REFUSED
 * with its words, and so is anything unreadable: the caller keeps the refusals for the page and reports them.
 */
export function migrateAlertRule(r: unknown): Migrated {
  const unreadable: Migrated = { refused: { line: NOTIFY_WORDS.refused.unreadableLine, why: NOTIFY_WORDS.refused.unreadable } };
  if (!r || typeof r !== "object") return unreadable;
  const o = r as Record<string, unknown>;
  if (typeof o.id !== "string" || typeof o.station !== "string" || !isQuantity(o.quantity)) return unreadable;
  const stationName = typeof o.stationName === "string" ? o.stationName : o.station;
  const w = o.water as { item?: unknown; name?: unknown } | null | undefined;
  const base = {
    id: o.id, station: o.station, stationName, quantity: o.quantity, on: o.on !== false,
    water: w && typeof w.name === "string" ? { item: typeof w.item === "string" ? w.item : null, name: w.name } : null,
    createdAt: typeof o.createdAt === "number" ? o.createdAt : 0,
  };
  const t = thresholdOf(o.threshold);
  if (t && isDirection(o.direction)) return { rule: { ...base, direction: o.direction, threshold: t } };
  const old = o.kind ?? o.direction;
  if (isDirection(old) && typeof o.value === "number" && Number.isFinite(o.value))
    return { rule: { ...base, direction: old, threshold: { kind: "value", value: o.value } } };
  if ((old === "rise" || old === "fall") && typeof o.value === "number" && Number.isFinite(o.value)) {
    const q = CONDITIONS_WORDS.quantity[o.quantity];
    return { refused: { line: `${stationName} · ${q} ${old === "rise" ? "rises" : "falls"} ${figure(o.value)} % or more within 24 h`,
                        why: NOTIFY_WORDS.refused.change } };
  }
  return unreadable;
}

// ── words ───────────────────────────────────────────────────────────────────────────────────────────────────────────

const unitOf = (q: AlertQuantity) => CONDITIONS_WORDS.unit[q];
const quantityWord = (q: AlertQuantity) => CONDITIONS_WORDS.quantity[q].toLowerCase();


/** "Nov 9", from the reading's own date (the day whose line it was checked against). */
export function readingDay(at: string): string {
  const m = Number(at.slice(5, 7)), d = Number(at.slice(8, 10));
  return `${monthAbbr(m - 1)} ${d}`;
}

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
  sections: { alerts: "Gauge alerts", other: "Other notifications" },
  alerts: {
    none: "No gauge alerts yet. Add one here, or from a gauge's chart on a water's Conditions: \"Alert me\".",
    how: "Each alert tells you once when the gauge crosses its line, and is ready again once the reading is back across.",
    add: "Add an alert",
    remove: "Delete",
    removeAloud: (line: string) => `Delete the alert: ${line}`,
    switchAloud: (line: string) => `Alert on: ${line}`,
    lastFired: (ms: number) => `last fired ${localStamp(ms)}`,
    notFired: "not fired yet",
    /** (T554 item 1) the alert's gauge gave no usable reading at the last check */
    silent: (ms: number) => `gauge not reporting since ${localStamp(ms)}`,
    off: "off",
  },
  /** "Add an alert" on More → Notifications: step 1, the gauge */
  add: {
    title: "Add an alert",
    step: "1. Pick a gauge",
    search: "Search gauges by name or number",
    searchAloud: "Search for a gauge to set an alert on",
    none: "No gauge by that name or number.",
    loading: "Reading the gauges…",
    noQuantity: "This gauge has no flow or level record in this release, so no alert can be set on it.",
    cancel: "Cancel",
  },
  /** the editor: steps 2 and 3, the condition and the value (the gauge card's "Alert me", and after step 1 above) */
  editor: {
    title: (gauge: string) => `Alert on ${gauge}`,
    quantity: "Quantity",
    condition: "When it",
    directions: { above: "Crosses above", below: "Crosses below" } as Record<AlertDirection, string>,
    line: "Line",
    lineKinds: { value: "A value", percentile: "A level for the day, against other years" },
    value: (q: AlertQuantity) => `Value in ${unitOf(q)}`,
    percentile: "Level for the day",
    presets: (p: number) => levelChip(p),
    custom: "Custom",
    customLabel: `Custom level, ${levelSpan(PERCENTILE_MIN, PERCENTILE_MAX)} (50 is the median)`,
    save: "Save alert",
    cancel: "Cancel",
    bad: "Type a number.",
    badPct: `Type a whole number from ${PERCENTILE_MIN} to ${PERCENTILE_MAX}: 10 is very low for the day, 90 very high.`,
    now: (q: AlertQuantity, v: number) => `Now ${figure(v)} ${unitOf(q)}`,
    nowUnknown: "No reading now",
    normal: (q: AlertQuantity, lo: number, hi: number) => `normal today ${figure(lo)}–${figure(hi)} ${unitOf(q)}`,
    normalUnknown: "no normal range for today",
    loading: "Reading the gauge…",
    /** why the percentile line cannot be picked (the chip is shown disabled, with this under it) */
    noRecord: (q: AlertQuantity) => `This gauge has no historical record of its ${quantityWord(q)}, so it has no usual levels to alert on.`,
    recordLoading: "Reading the gauge's record…",
    recordFailed: (why: string) => `The gauge's record could not be read, so no level for the day can be set: ${why}`,
  },
  /** (T234) the gauge card's control */
  alertMe: {
    button: "Alert me",
    hint: "Get a notification when this gauge crosses a line you set",
    saved: "Saved. Manage alerts in More → Notifications.",
    on: (n: number) => (n === 1 ? "1 alert on this gauge" : `${n} alerts on this gauge`),
  },
  /** (T527) saved alerts this version could not keep */
  refused: {
    title: "Alerts not kept",
    note: "This version alerts when a gauge crosses a line. These saved alerts could not become one; set them again if you still want them.",
    change: "a change within a day is no longer an alert kind",
    unreadable: "it could not be read",
    unreadableLine: "A saved alert",
    /** (T554 item 2) the whole alerts file (notify.json) could not be read: kept aside, never overwritten unkept */
    fileLine: "Your saved alerts",
    file: "the alerts file could not be read; a copy was kept aside on this phone",
    item: (r: RefusedAlert) => `${r.line}: ${r.why}`,
    ok: "OK, remove this note",
  },
  runs: {
    title: "Last background checks",
    none: "No background check has run yet.",
    note: "When the phone ran the app's check, to measure how late alerts can be.",
  },
  /** the Android notification channels (the phone's own settings list them by these names) */
  channels: {
    gauges: { name: "Gauge alerts", description: "When a gauge crosses a line you set" },
    updates: { name: "Regulations data", description: "When newer regulations data is ready" },
    app: { name: "App updates", description: "When a newer version of the app is out" },
  },
  run: {
    offline: "offline",
    failed: (why: string) => `failed: ${why}`,
    fired: (n: number) => (n === 1 ? "1 alert" : `${n} alerts`),
    /** (T554 item 1) alerts not checked this run because their gauge gave no usable reading */
    silent: (n: number) => (n === 1 ? "1 alert's gauge not reporting" : `${n} alerts' gauges not reporting`),
    quiet: "nothing to tell",
    updateReady: "new data ready",
    appVersion: "new app version",
    noRules: "no alerts set",
  },
} as const;

/** The line in words: "30 m³/s" / "the level higher than 8 days in 10 for the day". */
function thresholdWords(r: Pick<GaugeAlertRule, "quantity" | "threshold">): string {
  return r.threshold.kind === "value" ? `${figure(r.threshold.value)} ${unitOf(r.quantity)}`
    : `${levelName(r.threshold.pct)} for the day`;
}

/** An alert in words: "Flow crosses above 30 m³/s" / "Level crosses below the median for the day". */
export function alertLine(r: Pick<GaugeAlertRule, "quantity" | "direction" | "threshold">): string {
  return `${CONDITIONS_WORDS.quantity[r.quantity]} crosses ${r.direction} ${thresholdWords(r)}`;
}

/** (T554 item 1) The gauge gave no usable reading (none, or too old) at the last check: the alert could not be checked.
 *  A day with no line (`no-record`) is not silence: the gauge is reporting, the record has no line that day. */
export const silentGauge = (s: AlertRuleState | null | undefined): boolean =>
  !!s && (s.skipped === "missing" || s.skipped === "stale") && typeof s.skippedSince === "number";

/** Since when a silent alert's gauge has not reported: its last evaluated reading's time, else the first silent check. */
function silentFrom(s: AlertRuleState): number {
  const at = s.at ? Date.parse(s.at) : NaN;
  return Number.isFinite(at) ? at : s.skippedSince!;
}

/** The list's one line: "Vedder Crossing · Flow crosses above 30 m³/s · last fired 2026-11-05 at 14:05 PT", and
 *  (T554 item 1) "· gauge not reporting since …" while its gauge is silent. */
export function alertListLine(r: GaugeAlertRule, s: AlertRuleState | null | undefined): string {
  const W = NOTIFY_WORDS.alerts;
  const parts = [r.stationName, alertLine(r), !r.on ? W.off : s?.firedAt ? W.lastFired(s.firedAt) : W.notFired];
  if (r.on && silentGauge(s)) parts.push(W.silent(silentFrom(s!)));
  return parts.join(" · ");
}

/** (T554 item 1) The one notification when an alert's gauge has been silent for ALERT_SILENT_TELL_MS. */
export function alertSilentNotification(r: GaugeAlertRule, s: AlertRuleState): { title: string; body: string } {
  const where = r.water?.name ?? r.stationName;
  return {
    title: `${where}: ${quantityWord(r.quantity)} alert`,
    body: `${r.stationName} has not reported since ${localStamp(silentFrom(s))}, so this alert cannot be checked. ` +
      "It is checked again as soon as the gauge reports.",
  };
}

/** What the editor knows of the gauge now: its reading (and when), to say where it stands against the line. */
export interface AlertNow { readonly value: number; readonly at: string }

/**
 * THE EDITOR'S ONE PLAIN LINE — what will happen: "You get one notification each time the flow at Vedder Crossing
 * crosses above 30 m³/s. It is ready again once the flow is back below the line." A percentile says the line moves
 * and where it is today; a reading already past the line says the first notification waits for a crossing.
 */
export function alertSummary(r: Pick<GaugeAlertRule, "quantity" | "direction" | "threshold" | "stationName">,
                             now: AlertNow | null = null): string {
  const q = quantityWord(r.quantity);
  const u = unitOf(r.quantity);
  const parts = [`You get one notification each time the ${q} at ${r.stationName} crosses ${r.direction} ${thresholdWords(r)}.`];
  const line = now ? thresholdAt(r.threshold, now.at) : null;
  if (r.threshold.kind === "percentile")
    parts.push(line !== null && now
      ? `The line is worked out for each day from the gauge's record, so it moves with the date: ${figure(line)} ${u} for ${readingDay(now.at)}.`
      : "The line is worked out for each day from the gauge's record, so it moves with the date.");
  parts.push(`It is ready again once the ${q} is back ${r.direction === "above" ? "below" : "above"} the line.`);
  if (now && line !== null && isPast(r.direction, now.value, line))
    parts.push(`It is ${r.direction} that now (${figure(now.value)} ${u}), so the first notification comes after it crosses back and then ${r.direction} again.`);
  return parts.join(" ");
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

/**
 * The notification an alert raises — only what the gauge page shows: the gauge, the line it crossed, the reading and
 * when. "Chilliwack River: flow alert" / "Cowichan R. at Lake Cowichan crossed above the level higher than 8 days in 10 for Nov 9 —
 * 41.2 m³/s at 14:05 PT."
 */
export function alertNotification(r: GaugeAlertRule, reading: AlertReading): { title: string; body: string } {
  const where = r.water?.name ?? r.stationName;
  const u = unitOf(r.quantity);
  const line = r.threshold.kind === "value" ? `${figure(r.threshold.value)} ${u}`
    : `${levelName(r.threshold.pct)} for ${readingDay(reading.at)}`;
  return {
    title: `${where}: ${quantityWord(r.quantity)} alert`,
    body: `${r.stationName} crossed ${r.direction} ${line} — ${figure(reading.value)} ${u} at ${bcClockTime(Date.parse(reading.at))}.`,
  };
}

/** "Add an alert": the gauges whose name or number holds every word typed, best first (name starts, then number). */
export function findGauges<T extends { readonly station: string; readonly name: string }>(
  points: readonly T[], q: string, limit: number): T[] {
  const words = q.trim().toLowerCase().split(/\s+/).filter(Boolean);
  if (!words.length) return [];
  const hay = (p: T) => `${p.name} ${p.station}`.toLowerCase();
  const first = words[0]!;
  const rank = (p: T) => (p.name.toLowerCase().startsWith(first) ? 0 : p.station.toLowerCase().startsWith(first) ? 1 : 2);
  return points.filter((p) => words.every((w) => hay(p).includes(w)))
    .sort((a, b) => rank(a) - rank(b) || a.name.localeCompare(b.name) || a.station.localeCompare(b.station))
    .slice(0, limit);
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
