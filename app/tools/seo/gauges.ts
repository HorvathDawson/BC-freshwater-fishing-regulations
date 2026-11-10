/**
 * T487 — A PAGE PER GAUGE STATION: `/gauge/<station>/<slug>`, public and indexable like the water pages, in the sitemap
 * beside them, with an index at `/gauges`.
 *
 * The page is a static file (no worker compute); its live half is gaugeClient.ts in the reader's browser, reading the
 * facts the feeds worker writes beside each gauge's share card (`feeds/cards/<station>.json`) and the weather at the
 * gauge (Open-Meteo). What the build writes here is what does not move between feed runs: the gauge's name and number,
 * the waters it serves (one line each to that water's regulations page — never an open/closed word on this page), and
 * "Open on the website" (the web app on that water's Conditions view) beside a greyed "Open in the app · coming soon"
 * (the phone app takes no links yet, D1; T514).
 *
 * WHICH GAUGES: every station in the release that serves at least one water with a page — its own water (`gauge.item_id`),
 * a lake it gauges (`lake_gauge`), or a section it is the panel gauge of (`section_gauge`, good / fair; weak ones are
 * listed apart as a rough guide). The feeds worker draws a card for each, from `gauge/stations.json`.
 */
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { skyWord } from "../../packages/data/src/spots/words";
import { OPEN_METEO_ATTRIBUTION, weatherPlace } from "../../packages/data/src/spots/weather";
import { crashUrlFor, ORIGINS, REPORT_URLS } from "../../apps/mobile/src/env";
import { clip, esc, SHARE_FONT, SHARE_GROUND, SHARE_H, SHARE_INK, SHARE_W, type Site } from "./html";
import type { GaugeConfig } from "./gaugeClient";
import { bestServes, figure, gaugePath, TRUST_RANK, type GaugeRows, type ServesTrust } from "@app/core";
import { readGaugeRows } from "@app/data/release";

export { readGaugeRows, type GaugeRows };

export interface WaterLink { readonly id: string; readonly name: string; readonly path: string }
export interface GaugeModel {
  readonly station: string; readonly name: string; readonly path: string; readonly lat: number | null; readonly lon: number | null;
  /** its own water, lakes it gauges, sections it is a good / fair gauge for — by best trust, then name */
  readonly close: readonly WaterLink[];
  /** waters it is only a weak (rough) guide for */
  readonly rough: readonly WaterLink[];
}

export { gaugePath };

export function gaugeModels(rows: GaugeRows, water: (item: string) => WaterLink | null): GaugeModel[] {
  const best = bestServes(rows);
  const out: GaugeModel[] = [];
  for (const g of rows.gauges) {
    const links = [...(best.get(g.station) ?? [])].map(([item, t]) => ({ w: water(item), t })).filter((x): x is { w: WaterLink; t: ServesTrust } => !!x.w);
    if (!links.length) continue;
    links.sort((a, b) => TRUST_RANK[a.t] - TRUST_RANK[b.t] || a.w.name.localeCompare(b.w.name) || a.w.id.localeCompare(b.w.id));
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
    .replace(/^export /gm, "").replace(/^import \{ figure \} from "@app\/core";\n/m, "");
  if (/^\s*import\b/m.test(js)) throw new Error("seo: gaugeClient.ts must not import anything but core's figure (it ships as one plain script)");
  // (T583) THE FIGURE IS CORE'S, written into the script from the function itself: the page cannot round differently
  return `(function(){"use strict";\nconst figure=${figure.toString()};\n${js}\ngaugePage(document,Date.now,window.fetch.bind(window)).catch(function(e){console.error(e)});\n})();\n`;
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
        "&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max&timezone=Etc%2FGMT%2B7&forecast_days=5",
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
    // (owner 2026-11-08) the website now; the phone app greyed until its store listing is public (T514)
    `<div class="opens"><a class="app" href="${esc(`${first.path}?view=conditions`)}">Open on the website</a>` +
    `<span class="app soon" aria-disabled="true">Open in the app · coming soon</span></div></div></div>` +
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
