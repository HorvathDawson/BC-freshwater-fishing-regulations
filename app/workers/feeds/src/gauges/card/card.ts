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
//   what           the gauge's name, the reading (flow or level) with its standing for the date and its time (every word
//                  is core's: standing.ts, freshness.ts, change.ts, T622), the forecast in core's words for the
//                  station's expected change ("rises to 4× (Wed)"; the publisher's `change`), and a chart of
//                  the last 7 days (with the forecast run's dashes when one is current) over the NORMAL RANGE for each
//                  day: the climatology's median and 25th–75th (darker) / 10th–90th percentile band, the very envelope
//                  that words the standing (T511). No open / closed status: the card is conditions only.
//   climatology    feeds/live/clim.json, read ONCE per run (and only when a card is to be drawn), kept for the listed
//                  stations; a run without it draws cards without the band and says so (an event).
import { changeShort, type SourceChange } from "../../../../../packages/core/src/change";
import { CONDITIONS_WORDS } from "../../../../../packages/core/src/conditions";
import { localDate, localStamp } from "../../../../../packages/core/src/day";
import { standing } from "../../../../../packages/core/src/flow";
import { freshness, MAY_BE_OLD } from "../../../../../packages/core/src/freshness";
import { figure, standingLong } from "../../../../../packages/core/src/standing";
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
  /** when the feed wrote this file: the age of OUR copy, which is what the app judges a reading's staleness by (T622 G7) */
  fetchedAt?: string | null;
  now?: { discharge: number | null; level: number | null; at: string | null; percentile: number | null; parameter?: "discharge" | "level" } | null;
  recent?: Row[];
  daily?: Row[];
  forecasts?: Record<string, { model?: string; issuedAt?: string; series?: Series | null }> | null;
  /** the station's expected change (the publisher's `change_source`): the forecast is worded from it, by core (T622 G9) */
  change?: SourceChange | null;
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
  /** the reading's instant (epoch ms) and its time as the app says it (core `localStamp`: "2026-11-07 at 14:20 PT") */
  atMs: number | null;
  at: string | null;
  /** (T622 G7) when the feed last wrote the station's file (ISO), or null: the age of our copy */
  fetchedAt: string | null;
  /** past core's `freshness` of OUR copy (the app's rule), or its age unknown, or no reading at all */
  stale: boolean;
  /** [epoch ms, value], the last 7 days */
  week: [number, number][];
  /** the run drawn: the station's own first model with a current run (T347 working-day line, `dueBy`) */
  forecast: { model: string; issuedAt: string; issued: string; dueBy: number; points: [number, number][] } | null;
  /** a run exists but is past its working-day line (T347): said, not drawn */
  forecastOld: { model: string; issued: string } | null;
  /** the drawn forecast in core's words for the station's expected change ("rises to 4× (Wed)", "little change"); null = no change published */
  change: string | null;
  /** the climatology's normal range over the chart's window, a point a day: [epoch ms, p10, p25, p50, p75, p90]; null = none for this gauge */
  normal: [number, number, number, number, number, number][] | null;
}

/** a station's climatology envelope (clim.json `stations[<station>]`): 73 pentads x PCTILES a parameter */
export type Bands = Record<string, Record<string, Array<number | null>> | undefined>;

/** "Wed" within six days, "Nov 14" beyond is gone with the series-reading words: the forecast is worded by core's `changeShort` (T622 G9) */

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

/** (T551, T622 G24) B.C.'s clock from core -- the app's own, not the runtime's tz data -- said as the app says it */
export const pacific = localStamp;
const dayOf = localDate;

/** what a card shows, from the station's file (null: the feed has no file for it) */
export function cardModel(station: string, name: string, f: CardStation | null, now: number, clim?: Bands | null): CardModel {
  const nowRow = f?.now ?? null;
  const parameter = nowRow?.parameter ?? (nowRow?.discharge != null ? "discharge" : nowRow?.level != null ? "level" : null);
  const col = parameter === "level" ? 1 : 2;
  const v = parameter ? (parameter === "level" ? nowRow?.level : nowRow?.discharge) ?? null : null;
  const t = nowRow?.at ? Date.parse(nowRow.at) : NaN;
  const copyAt = f?.fetchedAt ? Date.parse(f.fetchedAt) : null;
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
    // the app's own line for a reading's standing (G28: the same words for "no record" too)
    standing: v === null ? null : standingLong(nowRow?.percentile ?? null, nowRow?.percentile == null ? null : standing(nowRow.percentile)),
    atMs: Number.isFinite(t) ? t : null,
    at: Number.isFinite(t) ? pacific(t) : null,
    fetchedAt: copyAt === null ? null : new Date(copyAt).toISOString(),
    stale: !Number.isFinite(t) || freshness(copyAt, now).state !== "live",
    week, forecast, forecastOld,
    change: forecast && f?.change ? changeShort(f.change, localDate(now)) : null,
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
    // the reading's own time, and (core's words) that it may be out of date when OUR copy is past its time; no age that would be wrong by the time the picture is seen
    const when = wrap("small", `Observed ${m.at}${m.stale ? ` · ${MAY_BE_OLD}` : ""}`, colW, 2);
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
  // the forecast in one line across the card's foot (its words are core's `changeShort` of the publisher's change); none / out of date said plainly
  const fw = m.change ? `Forecast: ${m.change}` : m.forecast ? "Forecast: the dashed line" : m.forecastOld ? `Forecast ${CONDITIONS_WORDS.runOld}` : "No forecast";
  if (m.value) await text(cv, "body", fit("body", fw, R - L), L, CARD_H - 24, m.forecast ? ink : soft);
  return { png: await png(cv), missing: [...cv.missing] };
}

/**
 * WHAT THE GAUGE PAGE SHOWS (`feeds/cards/<station>.json`, written with the PNG): the card's facts in full, so the page
 * reads one small file and says exactly what the card says — the words chosen here, from core (standing.ts, change.ts), and
 * the page's age line written in the browser from core's own functions (freshness.ts, inlined by tools/seo `clientScript`)
 * out of the instants given: when the reading was taken and when the feed wrote our copy.
 */
export const FACTS_FORMAT = "gauge-card/1";
export function cardFacts(m: CardModel, drawnAt: Date): Record<string, unknown> {
  return {
    format: FACTS_FORMAT, station: m.station, name: m.name, drawnAt: drawnAt.toISOString(),
    parameter: m.parameter, unit: m.unit, reading: m.reading, value: m.value, standing: m.standing,
    at: m.atMs === null ? null : new Date(m.atMs).toISOString(), atText: m.at, fetchedAt: m.fetchedAt, staleAfterMs: STALE_AFTER.feed.ms,
    runOld: CONDITIONS_WORDS.runOld, mayBeOld: MAY_BE_OLD,
    week: m.week.map(([t, v]) => [t, v]),
    forecast: m.forecast && { ...m.forecast, attribution: ATTRIBUTION },
    forecastOld: m.forecastOld,
    change: m.change,
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
export function cardSig(name: string, e: IndexEntry | undefined, forecastIssuedAt: string | null, now: number, indexAt: string | null = null): string {
  // (T622 G7) stale by the app's rule: OUR copy's age (the index's `fetchedAt`, written with the station files), not the reading's
  const t = e?.observedAt ? Date.parse(e.observedAt) : NaN;
  const stale = !Number.isFinite(t) || freshness(indexAt ? Date.parse(indexAt) : null, now).state !== "live";
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
  const index = JSON.parse(indexText) as { fetchedAt?: string | null; forecastIssuedAt?: string | null; stations?: Record<string, IndexEntry> };
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
    const sig = cardSig(name, index.stations?.[station], index.forecastIssuedAt ?? null, now, index.fetchedAt ?? null);
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
