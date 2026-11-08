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
//                  and a mini chart of the last 7 days (with the forecast run's dashes when one is current). No open /
//                  closed status: the card is conditions only.
import { plainStanding, standing } from "../../../../../packages/core/src/flow";
import { byWeekdays, forecastDueBy, STALE_AFTER } from "../../../../../packages/core/src/staleness";
import { ATTRIBUTION } from "../forecast";
import type { FeedEvent } from "../../events";
import type { Fetch, Store } from "../../io";
import { pool } from "../py";
import { Canvas, fillUnder, fit, hex, measure, png, stroke, text, wrap, type Pt, type Rgb } from "./raster";

export const CARD_W = 1200, CARD_H = 630;
/** bumped when the drawing changes without its inputs changing (every card is redrawn once) */
export const CARD_REV = 1;
export const CARDS_PREFIX = "feeds/cards/";
export const MISSING_PREFIX = "private/cards-missing/";
export const CARD_STATE = "private/feeds/cards/state.json";
export const CARD_CACHE = "public, max-age=900";
export const STATIONS_PATH = "/gauge/stations.json";
/** the site card's colours: the app's accent and the ink on it (tools/seo html.ts SHARE_GROUND / SHARE_INK; a test holds them equal) */
export const CARD_GROUND = "#5F26E0", CARD_INK = "#FFFFFF";
const ZONE = "America/Vancouver";
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
}

/** three figures, no false precision — core conditions.ts `chartFigure` (a test holds the two equal) */
export function figure(v: number): string {
  const a = Math.abs(v);
  if (a >= 100) return String(Math.round(v));
  if (a >= 10) return String(Math.round(v * 10) / 10);
  return String(Math.round(v * 100) / 100);
}

const stamp = new Intl.DateTimeFormat("en-CA", { timeZone: ZONE, month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" });
export const pacific = (t: number): string => stamp.format(new Date(t));
const dayFmt = new Intl.DateTimeFormat("en-CA", { timeZone: ZONE, year: "numeric", month: "2-digit", day: "2-digit" });
const dayOf = (t: number) => dayFmt.format(new Date(t));

/** what a card shows, from the station's file (null: the feed has no file for it) */
export function cardModel(station: string, name: string, f: CardStation | null, now: number): CardModel {
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
  return {
    station, name, now, parameter,
    unit: parameter === null ? null : parameter === "level" ? "m" : "m³/s",
    reading: v,
    value: v === null ? null : `${figure(v)} ${parameter === "level" ? "m" : "m³/s"}`,
    standing: v === null ? null : plainStanding(standing(nowRow?.percentile ?? null)),
    atMs: Number.isFinite(t) ? t : null,
    at: Number.isFinite(t) ? pacific(t) : null,
    stale: !Number.isFinite(t) || now - t > STALE_AFTER.feed.ms,
    week, forecast, forecastOld,
  };
}

const mix = (a: Rgb, b: Rgb, t: number): Rgb => [0, 1, 2].map((i) => Math.round(a[i]! + (b[i]! - a[i]!) * t)) as unknown as Rgb;

/** the card as a PNG (1200 x 630, indexed colour); `missing` = characters no face holds (drawn as "?") */
export async function drawCard(m: CardModel): Promise<{ png: Uint8Array; missing: string[] }> {
  const ground = hex(CARD_GROUND), ink = hex(CARD_INK), soft = mix(ground, ink, 0.78), faint = mix(ground, ink, 0.35);
  const cv = new Canvas(CARD_W, CARD_H, ground);
  const L = 64, R = CARD_W - 64;
  await text(cv, "small", fit("small", `RIVER CONDITIONS · WATER SURVEY OF CANADA GAUGE ${m.station}`, R - L), L, 72, soft);
  const title = wrap("title", m.name, R - L, 2);
  for (const [i, l] of title.entries()) await text(cv, "title", l, L, 138 + i * 64, ink);
  const top = 138 + (title.length - 1) * 64 + 40;
  // left: the reading
  const colW = 540;
  if (m.value) {
    await text(cv, "body", m.parameter === "level" ? "Water level" : "Flow", L, top + 40, soft);
    await text(cv, "big", fit("big", m.value, colW), L, top + 140, ink);
    const st = wrap("body", m.standing ?? "", colW, 2);
    for (const [i, l] of st.entries()) await text(cv, "body", l, L, top + 196 + i * 42, ink);
    const when = wrap("small", m.stale ? `No new reading since ${m.at ?? "an unknown time"}` : `Measured ${m.at}`, colW, 2);
    for (const [i, l] of when.entries()) await text(cv, "small", l, L, top + 196 + st.length * 42 + 8 + i * 34, soft);
  } else {
    await text(cv, "title", "No current reading", L, top + 110, ink);
    await text(cv, "small", fit("small", m.at ? `Last reading ${m.at}` : "The live feed has no reading for this gauge", colW), L, top + 160, soft);
  }
  // right: the last 7 days (and the forecast's dashes)
  const cx0 = 660, cx1 = R, cy0 = top + 20, cy1 = CARD_H - 96;
  const now = m.now;
  const t0 = now - WEEK, t1 = m.forecast ? Math.max(now, m.forecast.points[m.forecast.points.length - 1]![0]) : now;
  const vals = [...m.week.map((p) => p[1]), ...(m.forecast?.points.map((p) => p[1]) ?? [])];
  cv.fill(cx0, cy1, cx1 - cx0, 2, faint);
  if (vals.length >= 2) {
    let lo = Math.min(...vals), hi = Math.max(...vals);
    if (hi - lo < 1e-9) { lo -= 1; hi += 1; }
    const pad = (hi - lo) * 0.1; lo -= pad; hi += pad;
    const x = (t: number) => cx0 + ((t - t0) / (t1 - t0 || 1)) * (cx1 - cx0);
    const y = (v: number) => cy1 - ((v - lo) / (hi - lo)) * (cy1 - cy0);
    const obs: Pt[] = m.week.map((p) => [x(p[0]), y(p[1])]);
    if (obs.length >= 2) fillUnder(cv, obs, cy1, ink, 0.16);
    if (m.forecast) {
      const fp: Pt[] = [...(obs.length ? [obs[obs.length - 1]!] : []), ...m.forecast.points.map((p) => [x(p[0]), y(p[1])] as Pt)];
      cv.composite(stroke(cv, fp, 3, undefined, [14, 10]), soft);
    }
    const mask = stroke(cv, obs, 4);
    if (obs.length) stroke(cv, [obs[obs.length - 1]!, obs[obs.length - 1]!], 14, mask);
    cv.composite(mask, ink);
    if (m.forecast) cv.fill(x(now), cy0, 2, cy1 - cy0, faint);
  } else {
    await text(cv, "body", "No readings in the last 7 days", cx0, (cy0 + cy1) / 2, soft);
  }
  const below = cy1 + 34;
  await text(cv, "small", "7 days ago", cx0, below, soft);
  const right = m.forecast ? `forecast (${m.forecast.model}) →` : m.forecastOld ? `${m.forecastOld.model} forecast out of date` : "no flow forecast";
  await text(cv, "small", right, cx1 - measure("small", right), below, soft);
  await text(cv, "small", "canifishthis.ca", L, CARD_H - 40, soft);
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
  await pool(stations, 6, async ({ station, name }) => {
    const sig = cardSig(name, index.stations?.[station], index.forecastIssuedAt ?? null, now);
    if (state[station]?.s === sig) { res.unchanged++; return; }
    try {
      const text = await o.feed.get(`${station}.json`);
      const m = cardModel(station, name, text === null ? null : (JSON.parse(text) as CardStation), now);
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
