/**
 * T487 — A PAGE PER GAUGE STATION: `/gauge/<station>/<slug>`, public and indexable like the water pages, in the sitemap
 * beside them, with an index at `/gauges`.
 *
 * The page is a static file (no worker compute); its live half is gaugeClient.ts in the reader's browser, reading the
 * facts the feeds worker writes beside each gauge's share card (`feeds/cards/<station>.json`) and the weather at the
 * gauge (Open-Meteo). What the build writes here is what does not move between feed runs: the gauge's name and number,
 * the waters it serves (one line each to that water's regulations page — never an open/closed word on this page), and
 * "Open in the app" (the web app on that water's Conditions view; the phone app takes no links, D1).
 *
 * WHICH GAUGES: every station in the release that serves at least one water with a page — its own water (`gauge.item_id`),
 * a lake it gauges (`lake_gauge`), or a section it is the panel gauge of (`section_gauge`, good / fair; weak ones are
 * listed apart as a rough guide). The feeds worker draws a card for each, from `gauge/stations.json`.
 */
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import type { ReleaseLoader } from "@app/data/release";
import { skyWord } from "../../packages/data/src/spots/words";
import { OPEN_METEO_ATTRIBUTION, weatherPlace } from "../../packages/data/src/spots/weather";
import { crashUrlFor, ORIGINS, REPORT_URLS } from "../../apps/mobile/src/env";
import { clip, esc, SHARE_FONT, SHARE_GROUND, SHARE_H, SHARE_INK, SHARE_W, type Site } from "./html";
import type { GaugeConfig } from "./gaugeClient";
import { slugOf } from "./slug";

export type Trust = "own" | "lake" | "good" | "fair" | "weak";
export interface GaugeRow { readonly station: string; readonly name: string; readonly item: string | null; readonly lat: number | null; readonly lon: number | null }
export interface ServeRow { readonly station: string; readonly item: string; readonly trust: Trust }
export interface GaugeRows { readonly gauges: readonly GaugeRow[]; readonly serves: readonly ServeRow[] }

/** the release's gauges and the waters each serves; null when the release carries no conditions file (the regs fixture) */
export async function readGaugeRows(loader: ReleaseLoader): Promise<GaugeRows | null> {
  if (!loader.manifest.files.conditions) return null;
  const cond = await loader.db("conditions"), core = await loader.db("core");
  const num = (v: unknown) => (v === null || v === undefined ? null : Number(v));
  const gauges = (await cond.all("SELECT station, name, item_id, lat, lon FROM gauge ORDER BY station")).map((r) => ({
    station: String(r.station), name: String(r.name), item: r.item_id == null || r.item_id === "" ? null : String(r.item_id), lat: num(r.lat), lon: num(r.lon),
  }));
  const itemOfSid = new Map<number, string[]>();
  for (const r of await core.all("SELECT s.sid AS sid, i.item_id AS item FROM item_section s JOIN item i ON i.ord = s.ord")) {
    const l = itemOfSid.get(Number(r.sid));
    if (l) l.push(String(r.item)); else itemOfSid.set(Number(r.sid), [String(r.item)]);
  }
  const serves: ServeRow[] = [];
  for (const g of gauges) if (g.item) serves.push({ station: g.station, item: g.item, trust: "own" });
  for (const r of await cond.all("SELECT item_id, station FROM lake_gauge")) serves.push({ station: String(r.station), item: String(r.item_id), trust: "lake" });
  for (const r of await cond.all("SELECT sid, station, trust FROM section_gauge")) {
    const t = String(r.trust) as Trust;
    if (t !== "good" && t !== "fair" && t !== "weak") throw new Error(`seo: section_gauge trust "${String(r.trust)}" is not good/fair/weak`);
    for (const item of itemOfSid.get(Number(r.sid)) ?? []) serves.push({ station: String(r.station), item, trust: t });
  }
  return { gauges, serves };
}

export interface WaterLink { readonly id: string; readonly name: string; readonly path: string }
export interface GaugeModel {
  readonly station: string; readonly name: string; readonly path: string; readonly lat: number | null; readonly lon: number | null;
  /** its own water, lakes it gauges, sections it is a good / fair gauge for — by best trust, then name */
  readonly close: readonly WaterLink[];
  /** waters it is only a weak (rough) guide for */
  readonly rough: readonly WaterLink[];
}

const RANK: Record<Trust, number> = { own: 0, lake: 1, good: 2, fair: 3, weak: 4 };
export const gaugePath = (station: string, name: string): string => `/gauge/${station}/${slugOf(name) || "gauge"}`;

export function gaugeModels(rows: GaugeRows, water: (item: string) => WaterLink | null): GaugeModel[] {
  const best = new Map<string, Map<string, Trust>>();
  for (const s of rows.serves) {
    const m = best.get(s.station) ?? new Map<string, Trust>();
    const was = m.get(s.item);
    if (was === undefined || RANK[s.trust] < RANK[was]) m.set(s.item, s.trust);
    best.set(s.station, m);
  }
  const out: GaugeModel[] = [];
  for (const g of rows.gauges) {
    const links = [...(best.get(g.station) ?? [])].map(([item, t]) => ({ w: water(item), t })).filter((x): x is { w: WaterLink; t: Trust } => !!x.w);
    if (!links.length) continue;
    links.sort((a, b) => RANK[a.t] - RANK[b.t] || a.w.name.localeCompare(b.w.name) || a.w.id.localeCompare(b.w.id));
    out.push({ station: g.station, name: g.name, path: gaugePath(g.station, g.name), lat: g.lat, lon: g.lon,
               close: links.filter((l) => l.t !== "weak").map((l) => l.w), rough: links.filter((l) => l.t === "weak").map((l) => l.w) });
  }
  return out.sort((a, b) => a.name.localeCompare(b.name) || a.station.localeCompare(b.station));
}

// ── the live half ────────────────────────────────────────────────────────────────────────────────────────────────────
const HERE = dirname(fileURLToPath(import.meta.url));
/** gaugeClient.ts as one plain script (TypeScript's own transpile: the file has no imports), calling itself on load */
export function clientScript(): string {
  const src = readFileSync(join(HERE, "gaugeClient.ts"), "utf8");
  const js = ts.transpileModule(src, { compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.ESNext, removeComments: true } }).outputText
    .replace(/^export /gm, "");
  if (/^\s*import\b/m.test(js)) throw new Error("seo: gaugeClient.ts must not import anything (it ships as one plain script)");
  return `(function(){"use strict";\n${js}\ngaugePage(document,Date.now,window.fetch.bind(window)).catch(function(e){console.error(e)});\n})();\n`;
}

/** the production hosts the page names; deploy/staging/site.sh points a staging copy at staging's */
export const LIVE = {
  data: ORIGINS.production,
  crash: crashUrlFor(REPORT_URLS.production),
  env: "production",
} as const;
const OPEN_METEO = "https://api.open-meteo.com/v1/forecast";
/** WMO weather codes 0-99 in the app's sky words (data/spots/words.ts `skyWord`) */
const SKY: Readonly<Record<string, string>> = Object.fromEntries(Array.from({ length: 100 }, (_, c) => [String(c), skyWord(c)]));

export function gaugeConfig(g: GaugeModel, release: string): GaugeConfig {
  return {
    station: g.station,
    facts: `${LIVE.data}/feeds/cards/${g.station}.json`,
    weather: g.lat === null || g.lon === null ? null
      : `${OPEN_METEO}?${weatherPlace(g.lat, g.lon)}&current=temperature_2m,precipitation,weather_code,wind_speed_10m` +
        "&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max&timezone=America%2FVancouver&forecast_days=5",
    weatherAttribution: OPEN_METEO_ATTRIBUTION, sky: SKY,
    crash: LIVE.crash, env: LIVE.env, release,
  };
}

// ── the pages ────────────────────────────────────────────────────────────────────────────────────────────────────────
/** a unique title under 60 characters: the name is cut, the station never */
export function gaugeTitle(g: Pick<GaugeModel, "name" | "station">): string {
  const tail = ` conditions (${g.station})`;
  return `${clip(g.name, 60 - tail.length)}${tail}`;
}

export function gaugeDescription(g: GaugeModel): string {
  const w = g.close[0]?.name ?? g.rough[0]?.name;
  return clip(`Live flow, the last 7 days, the flow forecast and the weather at the ${g.name} gauge (${g.station}), BC` +
    `${w ? `, for ${w}` : ""}. Free, from Water Survey of Canada data.`);
}

/** the page body: everything that does not move between feed runs; the live sections are filled by gaugeClient.ts */
export function gaugeBody(g: GaugeModel, release: string, clientHref: string, asOf: string): string {
  const first = g.close[0] ?? g.rough[0]!;
  const line = (w: WaterLink) => `<li><a href="${esc(w.path)}">${esc(w.name)} fishing regulations</a></li>`;
  const cfg = JSON.stringify(gaugeConfig(g, release)).replace(/</g, "\\u003c");
  return `<div class="gv"><nav class="crumbs"><a href="/">Home</a> › <a href="/gauges">River gauges</a> › ${esc(g.name)}</nav>` +
    `<h1>${esc(g.name)}</h1>` +
    `<p class="small">Water Survey of Canada gauge ${esc(g.station)} · live river conditions, read each time this page opens.</p>` +
    `<h2>Now</h2><div id="gauge-now"><p class="small">Reading the latest from the gauge…</p></div>` +
    `<noscript><p class="warn">The live reading, chart and weather need JavaScript.</p></noscript>` +
    `<h2>Last 7 days and the forecast</h2><div id="gauge-chart"></div>` +
    `<h2>Weather at the gauge</h2><div id="gauge-weather"><p class="small">Reading the forecast…</p></div>` +
    `<h2>Regulations</h2>` +
    (g.close.length ? `<p>The waters this gauge serves:</p><ul>${g.close.map(line).join("")}</ul>` : "") +
    (g.rough.length ? `<details><summary>${g.close.length ? "Also a rough guide for" : "A rough guide for"} ${g.rough.length} more ${g.rough.length === 1 ? "water" : "waters"}</summary>` +
      `<ul class="cols">${g.rough.map(line).join("")}</ul></details>` : "") +
    `<p class="small">Waters as of <time datetime="${esc(asOf)}">${esc(asOf)}</time>, from release ${esc(release)}.</p>` +
    `<div class="card"><p>See this gauge with the map, alerts and every other gauge in the app.</p>` +
    `<a class="app" href="${esc(`${first.path}?view=conditions`)}">Open in the app</a></div></div>` +
    `<script type="application/json" id="gauge-config">${cfg}</script><script src="${esc(clientHref)}" defer></script>`;
}

export function gaugesIndexBody(gs: readonly GaugeModel[], asOf: string): string {
  return `<nav class="crumbs"><a href="/">Home</a> › River gauges</nav><h1>River gauges in BC</h1>` +
    `<p>${gs.length} Water Survey of Canada gauges on waters with fishing regulations: each page shows the live flow, the last ` +
    `7 days, the flow forecast where the BC River Forecast Centre runs one, and the weather at the gauge. ` +
    `List as of <time datetime="${esc(asOf)}">${esc(asOf)}</time>.</p>` +
    `<ul class="cols">${gs.map((g) => `<li><a href="${esc(g.path)}">${esc(g.name)}</a> <span class="tag">${esc(g.station)}</span></li>`).join("")}</ul>`;
}

/** what the feeds worker reads to know which cards to draw (src/gauges/card/card.ts `runCards`) */
export function stationsFile(gs: readonly GaugeModel[], release: string): string {
  return `${JSON.stringify({ format: "gauge-stations/1", release, stations: gs.map((g) => ({ station: g.station, name: g.name, path: g.path })) })}\n`;
}

/** the fixed site card a gauge without its own card is answered with (the site's worker, legacy.ts) */
export function siteGaugeCardSvg(site: Site): string {
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${SHARE_W}" height="${SHARE_H}" viewBox="0 0 ${SHARE_W} ${SHARE_H}" font-family="${SHARE_FONT}">` +
    `<rect width="${SHARE_W}" height="${SHARE_H}" fill="${SHARE_GROUND}"/><g fill="${SHARE_INK}">` +
    `<text x="64" y="250" font-size="84" font-weight="700">River conditions</text>` +
    `<text x="64" y="340" font-size="44">Live flow, forecast and weather at BC's river gauges</text>` +
    `<text x="64" y="560" font-size="34" opacity=".85">${esc(site.name)} · ${esc(site.origin.replace("https://", ""))}</text></g></svg>`;
}
