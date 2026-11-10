// T487: EACH GAUGE'S SHARE CARD, DRAWN IN THE GAUGE RUN (owner 2026-11-08: a preview card costs almost no worker compute —
// never drawn per request). The site's gauge pages (app/tools/seo/gauges.ts, /gauge/<station>/<slug>) name their card
// /gauge/<station>/card.png; the site's worker maps that URL to `feeds/cards/<station>.png` in this bucket and nothing
// else. A card the run has not drawn is answered with the site's fixed card AND marked under `private/cards-missing/`,
// which this run reads back into the owner's digest (no silent fallback).
//
//   which gauges   the site's own list, <SITE_ORIGIN>/gauge/stations.json (the stations that have a page, named as the
//                  page names them); unreachable → no card is drawn this run, and the run says so (an event)
//   when           a card is redrawn only when what it shows can have changed: its reading's time, standing and
//                  parameter in index.json, the forecast issue, the reading going stale, or a new day (the 7-day window)
//   what           the gauge's name, the reading (flow or level) with its standing for the time of year and its time,
//                  the forecast in words ("Rising to 31 m³/s by Wed", from the series the chart draws), and a chart of
//                  the last 7 days (with the forecast run's dashes when one is current) over the NORMAL RANGE for each
//                  day: the climatology's median and 25th–75th (darker) / 10th–90th percentile band, the very envelope
//                  that words the standing (T511). No open / closed status: the card is conditions only.
//   climatology    feeds/live/clim.json, read ONCE per run (and only when a card is to be drawn), kept for the listed
//                  stations; a run without it draws cards without the band and says so (an event).
import { bcDayWords, bcStampWords, bcWeekdayAbbr, localDate } from "../../../../../packages/core/src/day";
import { standing } from "../../../../../packages/core/src/flow";
import { figure, standingHeadline } from "../../../../../packages/core/src/standing";
import { byWeekdays, forecastDueBy, STALE_AFTER } from "../../../../../packages/core/src/staleness";
import { ATTRIBUTION } from "../forecast";
import { bandFor, PCTILES } from "../publish";
import type { FeedEvent } from "../../events";
import type { Fetch, Store } from "../../io";
import { pool } from "../py";
import { Canvas, fillBand, fit, hex, measure, png, stroke, text, wrap, type Pt, type Rgb } from "./raster";

export const CARD_W = 1200, CARD_H = 630;
/** bumped when the drawing changes without its inputs changing (every card is redrawn once) */
export const CARD_REV = 2;
export const CARDS_PREFIX = "feeds/cards/";
export const MISSING_PREFIX = "private/cards-missing/";
export const CARD_STATE = "private/feeds/cards/state.json";
export const CARD_CACHE = "public, max-age=900";
export const STATIONS_PATH = "/gauge/stations.json";
/** the site card's colours: the app's accent and the ink on it (tools/seo html.ts SHARE_GROUND / SHARE_INK; a test holds them equal) */
export const CARD_GROUND = "#5F26E0", CARD_INK = "#FFFFFF";
const DAY = 86_400_000, WEEK = 7 * DAY;
const MODELS = ["CLEVER", "COFFEE", "ELF"];

type Row = [string, number | null, number | null];
interface Series { at?: string[]; mid?: (number | null)[] }
/** the slice of a published station file (feeds/live/<station>.json, app/packages/data feed/http.ts StationFile) a card reads */
export interface CardStation {
  station: string;
  now?: { discharge: number | null; level: number | null; at: string | null; percentile: number | null; parameter?: "discharge" | "level" } | null;
  recent?: Row[];
  daily?: Row[];
  forecasts?: Record<string, { model?: string; issuedAt?: string; series?: Series | null }> | null;
  change?: { models?: string[] } | null;
}

export interface CardModel {
  station: string;
  name: string;
  /** when the card was drawn (the chart's right edge, unless a forecast runs on) */
  now: number;
  parameter: "discharge" | "level" | null;
  unit: "m³/s" | "m" | null;
  /** the reading, or null: no reading at all */
  reading: number | null;
  /** "22.2 m³/s" */
  value: string | null;
  standing: string | null;
  /** the reading's instant (epoch ms) and its time, Pacific ("Nov 7, 2:20 p.m. PST") */
  atMs: number | null;
  at: string | null;
  stale: boolean;
  /** [epoch ms, value], the last 7 days */
  week: [number, number][];
  /** the run drawn: the station's own first model with a current run (T347 working-day line, `dueBy`) */
  forecast: { model: string; issuedAt: string; issued: string; dueBy: number; points: [number, number][] } | null;
  /** a run exists but is past its working-day line (T347): said, not drawn */
  forecastOld: { model: string; issued: string } | null;
  /** the drawn forecast in words (T511), from the series the chart draws; null = no current forecast */
  outlook: Outlook | null;
  /** the climatology's normal range over the chart's window, a point a day: [epoch ms, p10, p25, p50, p75, p90]; null = none for this gauge */
  normal: [number, number, number, number, number, number][] | null;
}

/** a station's climatology envelope (clim.json `stations[<station>]`): 73 pentads x PCTILES a parameter */
export type Bands = Record<string, Record<string, Array<number | null>> | undefined>;

export interface Outlook {
  /** steady | rising / falling (a sustained move) | spike / dip (a brief one that returns) | receding (falling after a recent peak) */
  kind: "steady" | "rising" | "falling" | "spike" | "dip" | "receding";
  /** the figure said: the extreme the forecast reaches (the mean for steady) */
  value: number;
  /** when the extreme is (epoch ms); null for steady */
  at: number | null;
  /** where the run ends, said for a spike / dip ("then back near 12") */
  end: number | null;
  /** "Brief rise to 49 m³/s on Nov 3, then back to 12" */
  text: string;
}

/** the forecast moves the reading by at least this much (of the reading) before the card says anything but steady */
export const STEADY_WITHIN = 0.1;
/** a rise or dip that has come back to within this share of its own height by the run's end is "brief" (it did not last) */
export const RETURNS_WITHIN = 0.25;
/** … and it is brief only if it stood above (below) its own half-height for no more than this long */
export const BRIEF_MS = 2 * DAY;
/** a reading this far under the past week's peak is on the way down from it (receding) */
export const PEAK_BEFORE = 1.3;
/** "Wed" within six days, "Nov 14" beyond (a weekday alone would be ambiguous in a 10-day run); "today" the same Pacific day */
export function byWhen(t: number, now: number): string {
  if (dayOf(t) === dayOf(now)) return "today";
  return t - now < 6 * DAY ? bcWeekdayAbbr(t) : bcDayWords(t);
}
/** two significant figures: "49.3" -> "49", "172" -> "170", "2.43" -> "2.4" (a card does not claim more) */
export const about = (v: number): string => String(Number(v.toPrecision(2)));

/**
 * THE FORECAST IN WORDS, from the points the chart draws (the same series: nothing re-derived from another source), by its SHAPE:
 *   steady     every point within STEADY_WITHIN of the reading: "Steady around 100 m³/s"
 *   spike/dip  the larger move came back (the run ends within RETURNS_WITHIN of the reading) and stood past its half-height
 *              for BRIEF_MS or less: "Brief rise to 49 m³/s on Nov 3, then back to 12"
 *   rising     a rise that lasts: "Rising to 30 m³/s by Wed" (", then easing to 22" when it comes part-way back)
 *   falling    a fall that lasts: "Falling to 41 m³/s by Fri"; with `weekMax` (the past week's peak) well above the reading,
 *              "Dropping after a recent peak, to 41 m³/s by Fri"
 * A longer excursion that returns says "Rise to 49 m³/s around Nov 3, then back to 12".
 */
export function outlookOf(v0: number, points: readonly (readonly [number, number])[], unit: string, now: number, weekMax?: number | null): Outlook | null {
  if (!points.length || !Number.isFinite(v0)) return null;
  let hi = points[0]!, lo = points[0]!, sum = 0;
  for (const p of points) { if (p[1] > hi[1]) hi = p; if (p[1] < lo[1]) lo = p; sum += p[1]; }
  const end = points[points.length - 1]![1];
  const base = Math.max(Math.abs(v0), 1e-9);
  const up = (hi[1] - v0) / base, down = (v0 - lo[1]) / base;
  const u = (v: number) => `${about(v)} ${unit}`;
  if (up < STEADY_WITHIN && down < STEADY_WITHIN) {
    const value = sum / points.length;
    return { kind: "steady", value, at: null, end: null, text: `Steady around ${u(value)}` };
  }
  const rise = up >= down, ext = rise ? hi : lo, h = Math.abs(ext[1] - v0);
  // how long the run stood past its own half-height
  const half = v0 + (ext[1] - v0) / 2, past = points.filter((p) => (rise ? p[1] >= half : p[1] <= half));
  const width = past[past.length - 1]![0] - past[0]![0];
  const returned = Math.abs(end - v0) <= RETURNS_WITHIN * h || (rise ? end < v0 : end > v0);
  const when = byWhen(ext[0], now);
  if (returned) {
    const brief = width <= BRIEF_MS;
    return rise
      ? { kind: "spike", value: ext[1], at: ext[0], end, text: `${brief ? "Brief rise to" : "Rise to"} ${u(ext[1])} ${brief ? "on" : "around"} ${when}, then back to ${about(end)}` }
      : { kind: "dip", value: ext[1], at: ext[0], end, text: `${brief ? "Brief dip to" : "Dip to"} ${u(ext[1])} ${brief ? "on" : "around"} ${when}, then back to ${about(end)}` };
  }
  if (rise) {
    const easing = (hi[1] - end) > 0.4 * (hi[1] - v0);
    return { kind: "rising", value: hi[1], at: hi[0], end: easing ? end : null, text: `Rising to ${u(hi[1])} by ${when}${easing ? `, then easing to ${about(end)}` : ""}` };
  }
  if (weekMax != null && weekMax >= PEAK_BEFORE * v0)
    return { kind: "receding", value: lo[1], at: lo[0], end: null, text: `Dropping after a recent peak, to ${u(lo[1])} by ${when}` };
  return { kind: "falling", value: lo[1], at: lo[0], end: null, text: `Falling to ${u(lo[1])} by ${when}` };
}

/** the normal range a day from `t0` to `t1` from the climatology (the same `bandFor` that words the standing); null when none */
export function normalRange(bands: Record<string, Array<number | null>> | undefined | null, t0: number, t1: number): CardModel["normal"] {
  if (!bands) return null;
  const out: NonNullable<CardModel["normal"]> = [];
  for (let t = t0; ; t += DAY) {
    const at = Math.min(t, t1), b = bandFor(bands, at);
    if (b && b.length >= PCTILES.length && b.every((x) => typeof x === "number")) out.push([at, b[0]!, b[1]!, b[2]!, b[3]!, b[4]!]);
    if (at >= t1) break;
  }
  return out.length >= 2 ? out : null;
}

export { figure };

/** (T551) B.C.'s clock from core -- the app's own, not the runtime's tz data */
export const pacific = bcStampWords;
const dayOf = localDate;

/** what a card shows, from the station's file (null: the feed has no file for it) */
export function cardModel(station: string, name: string, f: CardStation | null, now: number, clim?: Bands | null): CardModel {
  const nowRow = f?.now ?? null;
  const parameter = nowRow?.parameter ?? (nowRow?.discharge != null ? "discharge" : nowRow?.level != null ? "level" : null);
  const col = parameter === "level" ? 1 : 2;
  const v = parameter ? (parameter === "level" ? nowRow?.level : nowRow?.discharge) ?? null : null;
  const t = nowRow?.at ? Date.parse(nowRow.at) : NaN;
  const week: [number, number][] = [];
  if (parameter) {
    const recent = (f?.recent ?? []).map((r) => [Date.parse(r[0]), r[col]] as const).filter((r): r is readonly [number, number] => Number.isFinite(r[0]) && typeof r[1] === "number");
    const first = recent.length ? recent[0]![0] : Infinity;
    for (const r of f?.daily ?? []) {
      const d = Date.parse(`${r[0]}T12:00:00-08:00`), x = r[col];
      if (Number.isFinite(d) && typeof x === "number" && d < first && d >= now - WEEK) week.push([d, x]);
    }
    for (const r of recent) if (r[0] >= now - WEEK && r[0] <= now) week.push([r[0], r[1]]);
  }
  let forecast: CardModel["forecast"] = null, forecastOld: CardModel["forecastOld"] = null;
  if (parameter === "discharge" && f?.forecasts) {
    const order = [...(f.change?.models ?? []), ...MODELS].filter((m, i, a) => a.indexOf(m) === i);
    for (const m of order) {
      const run = f.forecasts[m];
      const s = run?.series;
      if (!run || !s?.at || !s.mid) continue;
      const from = week.length ? week[week.length - 1]![0] : now;
      const points = s.at.map((a, i) => [Date.parse(a.length === 10 ? `${a}T12:00:00-08:00` : a), s.mid![i]] as const)
        .filter((p): p is readonly [number, number] => Number.isFinite(p[0]) && typeof p[1] === "number" && p[0] >= from && p[0] <= now + 10 * DAY)
        .map((p) => [p[0], p[1]] as [number, number]);
      if (!points.length) continue;
      // the Centre's stamps are its own clock, a fixed UTC−8 (core staleness.ts CENTRE_OFFSET_MS)
      const issued = run.issuedAt ? Date.parse(/[zZ]|[+-]\d\d:\d\d$/.test(run.issuedAt) ? run.issuedAt : `${run.issuedAt}-08:00`) : NaN;
      if (!Number.isFinite(issued)) continue;
      if (byWeekdays(issued, now) === "stale") { forecastOld ??= { model: m, issued: pacific(issued) }; continue; }
      forecast = { model: m, issuedAt: new Date(issued).toISOString(), issued: pacific(issued), dueBy: forecastDueBy(issued), points };
      forecastOld = null;
      break;
    }
  }
  const unit = parameter === null ? null : parameter === "level" ? "m" : "m³/s";
  const last = forecast ? forecast.points[forecast.points.length - 1]![0] : now;
  return {
    station, name, now, parameter,
    unit,
    reading: v,
    value: v === null ? null : `${figure(v)} ${parameter === "level" ? "m" : "m³/s"}`,
    standing: v === null ? null : standingHeadline(standing(nowRow?.percentile ?? null)),
    atMs: Number.isFinite(t) ? t : null,
    at: Number.isFinite(t) ? pacific(t) : null,
    stale: !Number.isFinite(t) || now - t > STALE_AFTER.feed.ms,
    week, forecast, forecastOld,
    outlook: forecast && v !== null && unit ? outlookOf(v, forecast.points, unit, now, week.length ? Math.max(...week.map((w) => w[1])) : null) : null,
    normal: parameter && clim ? normalRange(clim[parameter], now - WEEK, Math.max(now, last)) : null,
  };
}

/** the finest run of round figures (1, 2 or 5 x 10^k) inside [lo, hi] that is at most `n` of them (at least two when the range allows) */
export function niceTicks(lo: number, hi: number, n: number): number[] {
  const mag = 10 ** Math.floor(Math.log10((hi - lo) / n)) / 10;
  let out: number[] = [];
  for (const k of [1, 2, 2.5, 5, 10, 20, 25, 50, 100, 200, 250, 500]) {
    const step = k * mag;
    out = [];
    for (let v = Math.ceil(lo / step - 1e-9) * step; v <= hi + 1e-9; v += step) out.push(Math.round(v / step) * step || 0);
    if (out.length <= n) break;
  }
  return out;
}

const mix = (a: Rgb, b: Rgb, t: number): Rgb => [0, 1, 2].map((i) => Math.round(a[i]! + (b[i]! - a[i]!) * t)) as unknown as Rgb;

/** the card as a PNG (1200 x 630, indexed colour); `missing` = characters no face holds (drawn as "?") */
export async function drawCard(m: CardModel): Promise<{ png: Uint8Array; missing: string[] }> {
  const ground = hex(CARD_GROUND), ink = hex(CARD_INK), soft = mix(ground, ink, 0.78), faint = mix(ground, ink, 0.35);
  const cv = new Canvas(CARD_W, CARD_H, ground);
  const L = 64, R = CARD_W - 64;
  const brand = "canifishthis.ca", bw = measure("small", brand);
  await text(cv, "small", fit("small", `RIVER CONDITIONS · WATER SURVEY OF CANADA GAUGE ${m.station}`, R - L - bw - 24), L, 72, soft);
  await text(cv, "small", brand, R - bw, 72, soft);
  const title = wrap("title", m.name, R - L, 2);
  for (const [i, l] of title.entries()) await text(cv, "title", l, L, 138 + i * 64, ink);
  const top = 138 + (title.length - 1) * 64 + 40;
  // left: the reading
  const colW = 490;
  if (m.value) {
    await text(cv, "body", m.parameter === "level" ? "Water level" : "Flow", L, top + 40, soft);
    await text(cv, "big", fit("big", m.value, colW), L, top + 140, ink);
    let y = top + 196;
    const st = wrap("body", m.standing ?? "", colW, 2);
    for (const [i, l] of st.entries()) await text(cv, "body", l, L, y + i * 42, ink);
    y += st.length * 42 + 8;
    const when = wrap("small", m.stale ? `No new reading since ${m.at ?? "an unknown time"}` : `Measured ${m.at}`, colW, 2);
    for (const [i, l] of when.entries()) await text(cv, "small", l, L, y + i * 34, soft);
  } else {
    await text(cv, "title", "No current reading", L, top + 110, ink);
    await text(cv, "small", fit("small", m.at ? `Last reading ${m.at}` : "The live feed has no reading for this gauge", colW), L, top + 160, soft);
  }
  // right: the last 7 days, the forecast's dashes, and the normal range for each day behind them. Text here is the body face
  // in full ink: a card is shown a third of this size in Messages / Facebook (T511).
  const GUT = 612, cx0 = GUT + 14, cx1 = R, cy0 = top + 62, cy1 = CARD_H - 112;
  const now = m.now;
  const t0 = now - WEEK, t1 = m.forecast ? Math.max(now, m.forecast.points[m.forecast.points.length - 1]![0]) : now;
  const vals = [...m.week.map((p) => p[1]), ...(m.forecast?.points.map((p) => p[1]) ?? [])];
  const nr = m.normal;
  if (vals.length >= 2) {
    // the scale is the readings and the forecast alone, so the line uses the plot's height; the normal range is cut at its edges
    let lo = Math.min(...vals), hi = Math.max(...vals);
    if (hi - lo < 1e-9) { lo -= 1; hi += 1; }
    const pad = (hi - lo) * 0.08; lo -= pad; hi += pad;
    const x = (t: number) => cx0 + ((t - t0) / (t1 - t0 || 1)) * (cx1 - cx0);
    const y = (v: number) => cy1 - ((v - lo) / (hi - lo)) * (cy1 - cy0);
    // a few ticks only: two or three round figures, each on a faint line, labelled in full ink
    const ticks = niceTicks(lo, hi, cy1 - cy0 >= 200 ? 3 : 2);
    for (const v of ticks) {
      const ty = y(v);
      cv.fill(cx0, ty, cx1 - cx0, 2, faint);
      const label = figure(v);
      await text(cv, "body", label, GUT - measure("body", label), ty + 12, ink);
    }
    if (nr) {
      const col = (k: number) => nr.map((n) => [x(n[0]), y(n[k]!)] as Pt);
      fillBand(cv, col(5), col(1), cy0, cy1, ink, 0.1);
      fillBand(cv, col(4), col(2), cy0, cy1, ink, 0.16);
      const mid = col(3);
      fillBand(cv, mid.map((p) => [p[0], p[1] - 1.5] as Pt), mid.map((p) => [p[0], p[1] + 1.5] as Pt), cy0, cy1, ink, 0.7);
    }
    const obs: Pt[] = m.week.map((p) => [x(p[0]), y(p[1])]);
    if (m.forecast) {
      const fp: Pt[] = [...(obs.length ? [obs[obs.length - 1]!] : []), ...m.forecast.points.map((p) => [x(p[0]), y(p[1])] as Pt)];
      cv.composite(stroke(cv, fp, 4, undefined, [14, 10]), soft);
    }
    const mask = stroke(cv, obs, 5);
    if (obs.length) stroke(cv, [obs[obs.length - 1]!, obs[obs.length - 1]!], 16, mask);
    cv.composite(mask, ink);
    if (m.forecast) cv.fill(x(now), cy0, 2, cy1 - cy0, faint);
  } else {
    await text(cv, "body", "No readings in the last 7 days", cx0, (cy0 + cy1) / 2, soft);
  }
  cv.fill(cx0, cy1, cx1 - cx0, 2, faint);
  // the unit (once) above the figures; the key to the band at the right
  if (m.unit) await text(cv, "body", m.unit, GUT - measure("body", m.unit), top + 40, ink);
  if (m.normal) {
    const key = "normal range", kw = measure("body", key);
    await text(cv, "body", key, cx1 - kw, top + 40, ink);
    cv.fill(cx1 - kw - 40, top + 14, 28, 26, mix(ground, ink, 0.2));
    cv.fill(cx1 - kw - 40, top + 22, 28, 10, mix(ground, ink, 0.38));
  }
  const below = cy1 + 40;
  await text(cv, "body", "7 days ago", cx0, below, ink);
  const right = m.forecast ? "Forecast" : "Now";
  await text(cv, "body", right, cx1 - measure("body", right), below, ink);
  // the forecast in one line across the card's foot (its words are outlookOf's); none / out of date said plainly
  const fw = m.outlook ? `Forecast: ${m.outlook.text.charAt(0).toLowerCase()}${m.outlook.text.slice(1)}` : m.forecastOld ? "Forecast out of date" : "No forecast";
  if (m.value) await text(cv, "body", fit("body", fw, R - L), L, CARD_H - 24, m.outlook ? ink : soft);
  return { png: await png(cv), missing: [...cv.missing] };
}

/**
 * WHAT THE GAUGE PAGE SHOWS (`feeds/cards/<station>.json`, written with the PNG): the card's facts in full, so the page
 * reads one small file and says exactly what the card says — every word chosen here, from core (flow.ts standing words,
 * staleness.ts lines), never again in the page. The page judges staleness at its own time from the instants given.
 */
export const FACTS_FORMAT = "gauge-card/1";
export function cardFacts(m: CardModel, drawnAt: Date): Record<string, unknown> {
  return {
    format: FACTS_FORMAT, station: m.station, name: m.name, drawnAt: drawnAt.toISOString(),
    parameter: m.parameter, unit: m.unit, reading: m.reading, value: m.value, standing: m.standing,
    at: m.atMs === null ? null : new Date(m.atMs).toISOString(), atText: m.at, staleAfterMs: STALE_AFTER.feed.ms,
    week: m.week.map(([t, v]) => [t, v]),
    forecast: m.forecast && { ...m.forecast, attribution: ATTRIBUTION },
    forecastOld: m.forecastOld,
    outlook: m.outlook,
    normal: m.normal,
  };
}

// ── the run ──────────────────────────────────────────────────────────────────────────────────────────────────────────
/** the slice of the R2 binding the card run uses (the real binding takes bytes as well as text) */
export interface CardBucket {
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  put(key: string, body: string | Uint8Array, opts?: { httpMetadata?: { contentType?: string; cacheControl?: string } }): Promise<unknown>;
  delete(key: string): Promise<void>;
  list?(o: { prefix: string; cursor?: string; limit?: number }): Promise<{ objects: { key: string }[]; truncated: boolean; cursor?: string }>;
}
interface IndexEntry { observedAt?: string | null; percentile?: number | null; parameter?: string | null }
interface StationsFile { format: string; release: string; stations: { station: string; name: string; path: string }[] }
type State = Record<string, { s: string; t: string }>;

export interface CardRun {
  ok: boolean;
  listed: number;
  drawn: number;
  unchanged: number;
  failed: { station: string; error: string }[];
  /** stations whose name holds a letter no face has (drawn as "?") */
  missingGlyphs: Record<string, string[]>;
  /** why the climatology (normal range) could not be read this run: cards were drawn without the band */
  climate?: { error: string };
  error?: string;
  ms: number;
}

/** the pre-draw key: what the card shows can only have changed when this has */
export function cardSig(name: string, e: IndexEntry | undefined, forecastIssuedAt: string | null, now: number): string {
  const t = e?.observedAt ? Date.parse(e.observedAt) : NaN;
  const stale = !Number.isFinite(t) || now - t > STALE_AFTER.feed.ms;
  return JSON.stringify([CARD_REV, name, e?.observedAt ?? null, e?.percentile ?? null, e?.parameter ?? null, forecastIssuedAt, stale, dayOf(now)]);
}

export async function runCards(o: { fetch: Fetch; feed: Store; bucket: CardBucket; siteOrigin: string | null | undefined; now: Date; limit?: number;
                                    log?: (s: string) => void }): Promise<CardRun> {
  const started = Date.now(), now = o.now.getTime();
  const res: CardRun = { ok: false, listed: 0, drawn: 0, unchanged: 0, failed: [], missingGlyphs: {}, ms: 0 };
  const done = () => ({ ...res, ms: Date.now() - started });
  if (!o.siteOrigin) { res.error = "SITE_ORIGIN is not set: the worker does not know the site's gauge list"; return done(); }
  let list: StationsFile;
  try {
    const r = await o.fetch(`${o.siteOrigin}${STATIONS_PATH}`, { headers: { "User-Agent": "BC-FishRegs-Cards/1.0" } });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    list = await r.json();
    if (list.format !== "gauge-stations/1" || !Array.isArray(list.stations)) throw new Error(`not a gauge-stations/1 file`);
  } catch (e) {
    res.error = `${o.siteOrigin}${STATIONS_PATH}: ${e instanceof Error ? e.message : String(e)}`;
    return done();
  }
  const indexText = await o.feed.get("index.json");
  if (indexText === null) { res.error = "feeds/live/index.json is missing"; return done(); }
  const index = JSON.parse(indexText) as { forecastIssuedAt?: string | null; stations?: Record<string, IndexEntry> };
  let state: State = {};
  const st = await o.bucket.get(CARD_STATE);
  if (st) state = JSON.parse(await st.text()) as State;
  const stations = list.stations.slice(0, o.limit ?? Infinity);
  res.listed = stations.length;
  const at = o.now.toISOString();
  // the climatology, read once and only when a card is to be drawn; kept for the listed stations (the rest is dropped)
  let climP: Promise<Map<string, Bands> | null> | undefined;
  const climate = () => climP ??= (async () => {
    try {
      const t = await o.feed.get("clim.json");
      if (t === null) throw new Error("feeds/live/clim.json is missing");
      const all = (JSON.parse(t) as { stations?: Record<string, Bands> }).stations;
      if (!all || typeof all !== "object") throw new Error("clim.json has no stations");
      return new Map(stations.flatMap(({ station }) => (all[station] ? [[station, all[station]!] as const] : [])));
    } catch (e) {
      res.climate = { error: e instanceof Error ? e.message : String(e) };
      return null;
    }
  })();
  await pool(stations, 6, async ({ station, name }) => {
    const sig = cardSig(name, index.stations?.[station], index.forecastIssuedAt ?? null, now);
    if (state[station]?.s === sig) { res.unchanged++; return; }
    try {
      const bands = (await climate())?.get(station) ?? null;
      const text = await o.feed.get(`${station}.json`);
      const m = cardModel(station, name, text === null ? null : (JSON.parse(text) as CardStation), now, bands);
      const card = await drawCard(m);
      if (card.missing.length) res.missingGlyphs[station] = card.missing;
      await o.bucket.put(`${CARDS_PREFIX}${station}.png`, card.png, { httpMetadata: { contentType: "image/png", cacheControl: CARD_CACHE } });
      await o.bucket.put(`${CARDS_PREFIX}${station}.json`, JSON.stringify(cardFacts(m, o.now)), { httpMetadata: { contentType: "application/json", cacheControl: CARD_CACHE } });
      state[station] = { s: sig, t: at };
      res.drawn++;
    } catch (e) {
      res.failed.push({ station, error: e instanceof Error ? e.message : String(e) });
    }
  });
  await o.bucket.put(CARD_STATE, JSON.stringify(state), { httpMetadata: { contentType: "application/json" } });
  res.ok = true;
  o.log?.(`  cards: ${res.drawn} drawn, ${res.unchanged} unchanged, ${res.failed.length} failed of ${res.listed} (${Date.now() - started} ms)`);
  return done();
}

/** a missing card the site's worker answered with the site card (written by the site worker, tools/seo legacy.ts) */
interface Missing { station: string; first: string; last: string; count: number }

/**
 * The digest lines for the card run and the cards the site found missing: one per station per first-seen (a marker
 * whose card has since been drawn is cleared, so a card that goes missing again is told again).
 */
export async function cardEvents(bucket: CardBucket, run: CardRun | null, known: (key: string) => boolean, at: string): Promise<FeedEvent[]> {
  const out: FeedEvent[] = [];
  const day = at.slice(0, 10);
  if (run && !run.ok) out.push({ key: `card-run:${day}:${run.error}`, kind: "card", title: `Gauge share cards not drawn: ${run.error}`,
    text: `The gauge run could not draw the share cards: ${run.error}. The cards already on R2 stay, older each run; a gauge without one shows the site card.`, at });
  if (run?.failed.length) out.push({ key: `card-failed:${day}:${run.failed.map((f) => f.station).join(",")}`, kind: "card",
    title: `${run.failed.length} gauge share card(s) failed to draw`, text: run.failed.map((f) => `${f.station}: ${f.error}`).join("\n"), at });
  if (run?.climate?.error) out.push({ key: `card-clim:${day}:${run.climate.error}`, kind: "card", title: `Gauge share cards drawn without the normal range: ${run.climate.error}`,
    text: `The gauge run could not read the climatology (${run.climate.error}), so this run's cards show the readings and forecast but no normal-range band.`, at });
  const glyphs = Object.entries(run?.missingGlyphs ?? {});
  if (glyphs.length) out.push({ key: `card-glyphs:${glyphs.map(([s, c]) => s + c.join("")).join(",")}`, kind: "card",
    title: `${glyphs.length} gauge card(s) hold a letter the card face lacks (drawn as "?")`,
    text: `${glyphs.map(([s, c]) => `${s}: ${c.join(" ")}`).join("\n")}\nAdd the letters to TEXT in app/workers/feeds/tools/card_glyphs.py and rerun it.`, at });
  if (!bucket.list) return out;
  let state: State = {};
  const st = await bucket.get(CARD_STATE);
  if (st) state = JSON.parse(await st.text()) as State;
  let cursor: string | undefined;
  const keys: string[] = [];
  do {
    const r = await bucket.list({ prefix: MISSING_PREFIX, cursor, limit: 1000 });
    keys.push(...r.objects.map((x) => x.key));
    cursor = r.truncated ? r.cursor : undefined;
  } while (cursor && keys.length < 5000);
  for (const k of keys) {
    const o = await bucket.get(k);
    if (!o) continue;
    const m = JSON.parse(await o.text()) as Missing;
    if (state[m.station] && state[m.station]!.t > m.last) { await bucket.delete(k); continue; }   // drawn since: cleared
    const key = `card-missing:${m.station}:${m.first}`;
    if (known(key)) continue;
    out.push({ key, kind: "card", title: `Gauge card missing: ${m.station} (the site card was shown)`,
      text: `/gauge/${m.station}/card.png had no card on R2 (${CARDS_PREFIX}${m.station}.png); the site's fixed card was served. ` +
            `Seen ${m.count} time(s), first ${m.first}, last ${m.last}. The gauge run draws a card for every station in the site's ${STATIONS_PATH}.`, at });
  }
  return out;
}
