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
import { bestServes, gaugePath, OPEN_IN_APP_URL, STALE_AFTER_MS, TRUST_RANK, type GaugeRows, type ServesTrust } from "@app/core";
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
/**
 * (T622, T605 item 1) THE CORE FUNCTIONS THE PAGE SCRIPT USES, written into it from their own source. The page ships as one
 * plain script that cannot import, so this takes each named top-level declaration of a core file (a function or a const),
 * transpiles it and puts it in the script: the page cannot round a figure, say a B.C. day or time, or age a reading
 * differently from the app, and none is typed twice. Every name a function here calls must be listed (a missing one is a
 * ReferenceError in the browser: `gauges.test.ts` runs the script and compares its output to core's).
 */
const CORE = join(HERE, "../../packages/core/src");
export const CORE_INLINE: readonly (readonly [file: string, names: readonly string[]])[] = [
  ["clock.ts", ["BC_PERMANENT_FROM", "BC_TIME_LABEL", "bcOffsetHours"]],
  ["flow.ts", ["MONTHS", "monthAbbr", "WEEKDAY_NAMES", "weekdayAbbr"]],
  ["day.ts", ["pad2", "HOUR_MS", "bcWall", "fromBcWall", "bcMidnight", "bcFields", "bcDayWords", "bcWeekdayAbbr", "bcStampWords", "bcIsoDayWords"]],
  ["chartTicks.ts", ["niceTicks"]],
  ["chartMarks.ts", ["RULE_GAP", "LABEL_GAP", "IMPORTANCE", "MARK_ROWS", "markTextWidth", "meets", "layoutDayMarks"]],
  ["alerts.ts", ["normalRangeOf"]],
  ["freshness.ts", ["AGO_WORDS", "ageWords", "freshness", "MAY_BE_OLD", "READING_LEAD", "readingAgeParts", "readingAgeWords"]],
  ["standing.ts", ["figure"]],
];
const plain = (ts_: string): string =>
  ts.transpileModule(ts_, { compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.ESNext, removeComments: true } }).outputText
    .replace(/^export /gm, "");
function inlineCore(file: string, names: readonly string[]): string {
  const path = join(CORE, file), src = readFileSync(path, "utf8");
  const sf = ts.createSourceFile(path, src, ts.ScriptTarget.ES2020, true);
  const found = new Set<string>(), out: string[] = [];
  for (const st of sf.statements) {
    const declared = ts.isFunctionDeclaration(st) && st.name ? [st.name.text]
      : ts.isVariableStatement(st) ? st.declarationList.declarations.flatMap((d) => (ts.isIdentifier(d.name) ? [d.name.text] : [])) : [];
    if (!declared.some((n) => names.includes(n))) continue;
    for (const n of declared) found.add(n);
    out.push(src.slice(st.getStart(sf), st.end));
  }
  const missing = names.filter((n) => !found.has(n));
  if (missing.length) throw new Error(`seo: core/${file} has no top-level ${missing.join(", ")} to write into the gauge page's script`);
  return plain(out.join("\n"));
}
/** gaugeClient.ts as one plain script (TypeScript's own transpile), core's functions written in from their source, calling itself on load */
export function clientScript(): string {
  const src = readFileSync(join(HERE, "gaugeClient.ts"), "utf8");
  const js = plain(src).replace(/^import \{[^}]*\} from "@app\/core";\n/m, "");
  if (/^\s*import\b/m.test(js)) throw new Error("seo: gaugeClient.ts must not import anything but core (it ships as one plain script)");
  const core = CORE_INLINE.map(([f, names]) => inlineCore(f, names)).join("\n");
  // the feed's age line is judged by the stale time the facts file names; `freshness`' default is that same table's value
  return `(function(){"use strict";\nconst STALE_AFTER_MS=${STALE_AFTER_MS};\n${core}\n${js}\ngaugePage(document,Date.now,window.fetch.bind(window)).catch(function(e){console.error(e)});\n})();\n`;
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

/**
 * (T514) the page's "Open in the app": greyed "coming soon" while the Play listing is not public (`OPEN_IN_APP_URL` null), the
 * listing's link once it is. The one config value is `@app/core` share.ts; a phone with the app opens the app (T625).
 */
export function openInAppControl(listing: string | null = OPEN_IN_APP_URL): string {
  return listing === null
    ? `<span class="app soon" aria-disabled="true">Open in the app · coming soon</span>`
    : `<a class="app" href="${esc(listing)}">Open in the app</a>`;
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
    `${openInAppControl()}</div></div></div>` +
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
