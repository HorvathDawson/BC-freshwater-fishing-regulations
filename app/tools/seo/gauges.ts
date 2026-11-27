/**
 * T487 — A PAGE PER GAUGE STATION: `/gauge/<station>/<slug>`, public and indexable like the water pages, in the sitemap
 * beside them, with an index at `/gauges`.
 *
 * The page is a static file (no worker compute); its live half is gaugeClient.ts in the reader's browser, reading the
 * facts the feeds worker writes beside each gauge's share card (`feeds/cards/<station>.json`) and the weather at the
 * gauge (Open-Meteo). What the build writes here is what does not move between feed runs: the gauge's name and number,
 * the waters it serves (one line each to that water's regulations page — never an open/closed word on this page), and
 * "Open on the website" (the web app on that water's Conditions view) above the web app's own "Get the app" block (ui-native
 * `GetAppBlock`, mounted by the chart's script; T1039).
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
import { FIND_BODY } from "./regionMap";
import { gaugeFindForm } from "./gaugeFind";
import type { SearchGauge } from "./gaugeSearch";
import { bestServes, gaugePath, gaugeSubject, STORE_CONTACT, TRUST_RANK, type GaugeParameter, type GaugeRows, type ServesTrust } from "@app/core";
import type { StationClim } from "@app/data";
import { readGaugeClim, readGaugeRows } from "@app/data/release";

export { readGaugeClim, readGaugeRows, type GaugeRows };

export interface WaterLink { readonly id: string; readonly name: string; readonly path: string }
export interface GaugeModel {
  readonly station: string; readonly name: string; readonly path: string; readonly lat: number | null; readonly lon: number | null;
  /** its own water, lakes it gauges, sections it is a good / fair gauge for — by best trust, then name */
  readonly close: readonly WaterLink[];
  /** (T908) the main waters, the only ones listed open: its own water, lakes it gauges, sections it is a good gauge for */
  readonly main: readonly WaterLink[];
  /** (T908) the rest, folded: fair guides, then the weak (rough) ones */
  readonly more: readonly WaterLink[];
  /** waters it is only a weak (rough) guide for */
  readonly rough: readonly WaterLink[];
  /** (T1045) what the release holds for the station (`gauge_clim`): the words of its title, heading and links */
  readonly parameters: readonly GaugeParameter[];
}

export { gaugePath };

export function gaugeModels(rows: GaugeRows, water: (item: string) => WaterLink | null,
                            clim?: ReadonlyMap<string, readonly Pick<StationClim, "parameter">[]> | null): GaugeModel[] {
  const best = bestServes(rows);
  const out: GaugeModel[] = [];
  for (const g of rows.gauges) {
    const links = [...(best.get(g.station) ?? [])].map(([item, t]) => ({ w: water(item), t })).filter((x): x is { w: WaterLink; t: ServesTrust } => !!x.w);
    if (!links.length) continue;
    links.sort((a, b) => TRUST_RANK[a.t] - TRUST_RANK[b.t] || a.w.name.localeCompare(b.w.name) || a.w.id.localeCompare(b.w.id));
    out.push({ station: g.station, name: g.name, path: gaugePath(g.station, g.name), lat: g.lat, lon: g.lon,
               close: links.filter((l) => l.t !== "weak").map((l) => l.w),
               main: links.filter((l) => TRUST_RANK[l.t] <= TRUST_RANK.good).map((l) => l.w), more: links.filter((l) => TRUST_RANK[l.t] > TRUST_RANK.good).map((l) => l.w), rough: links.filter((l) => l.t === "weak").map((l) => l.w),
               parameters: (clim?.get(g.station) ?? []).map((c) => c.parameter) });
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
  ["clock.ts", ["BC_PERMANENT_FROM", "BC_TIME_LABEL", "bcOffsetHours", "bcClockTime"]],
  // (T1039, owner 2026-11-23: "could add other almanac info too?") the app's sunrise and sunset (the water page's sun line)
  // and its weather figures and words (a spot's record and weather window)
  ["sun.ts", ["RAD", "J2000", "UNIX_JD", "DAY_MS", "sunTimes", "SUN_WORDS", "bare", "short", "sunLine"]],
  ["weatherFigure.ts", ["round1", "weatherFigure"]],
  ["spotWords.ts", ["SPOT_WINDOW"]],
  ["flow.ts", ["MONTHS", "monthAbbr", "WEEKDAY_NAMES", "weekdayAbbr"]],
  // (T1194) the reading, standing and colour bar are the shared piece's now (gaugeChart.tsx): only the day, time and weather words stay
  ["standing.ts", ["figure"]],
  ["day.ts", ["pad2", "HOUR_MS", "bcWall", "fromBcWall", "bcMidnight", "bcFields", "bcDayWords", "bcWeekdayAbbr", "bcStampWords", "bcClockWords", "bcIsoDayWords"]],
];
const plain = (ts_: string): string =>
  ts.transpileModule(ts_, { compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.ESNext, removeComments: true } }).outputText
    .replace(/^export /gm, "");
function inlineCore(file: string, names: readonly string[], dir: string = CORE): string {
  const path = join(dir, file), src = readFileSync(path, "utf8");
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
  if (missing.length) throw new Error(`seo: ${dir === CORE ? "core/" : ""}${file} has no top-level ${missing.join(", ")} to write into the gauge page's script`);
  return plain(out.join("\n"));
}
/** gaugeClient.ts as one plain script (TypeScript's own transpile), core's functions written in from their source, calling itself on load */
export function clientScript(): string {
  const src = readFileSync(join(HERE, "gaugeClient.ts"), "utf8");
  const js = plain(src).replace(/^import \{[^}]*\} from "@app\/core";\n/m, "");
  if (/^\s*import\b/m.test(js)) throw new Error("seo: gaugeClient.ts must not import anything but core (it ships as one plain script)");
  const core = CORE_INLINE.map(([f, names]) => inlineCore(f, names)).join("\n");
  // the feed's age line is judged by the stale time the facts file names; `freshness`' default is that same table's value
  return `(function(){"use strict";\n${core}\n${js}\ngaugePage(document,Date.now,window.fetch.bind(window)).catch(function(e){console.error(e)});\n})();\n`;
}

/** the page reporter (gaugeClient.ts `pageReporter` and what it calls), written into a region hub's find script */
const JOIN_REPORTER: readonly string[] = ["isEnvironment", "crashReport", "pageReporter"];

/** (T1010) a region hub's water search: its script (regionMap.ts) with the page reporter written in, as the Join card's */
export function findScript(): string {
  return `(function(){"use strict";\n${inlineCore("gaugeClient.ts", JOIN_REPORTER, HERE)}\n${FIND_BODY}\n})();\n`;
}

/**
 * (T1107) THE /gauges SEARCH'S SCRIPT: gaugeFindEntry.ts bundled (core's search index and the page's drawing, one plain script), with the page
 * reporter written in before it (the entry names it as a global, as the region find's script is wrapped).
 */
export async function gaugeFindScript(): Promise<string> {
  const { build: esbuild } = await import("esbuild");
  const r = await esbuild({
    entryPoints: [join(HERE, "gaugeFindEntry.ts")], absWorkingDir: join(HERE, "../.."),
    bundle: true, minify: true, format: "iife", platform: "browser", target: "es2020", write: false, logLevel: "silent",
    // the reporter is the page's own (written in below), never bundled from gaugeClient.ts
    plugins: [], define: { "process.env.NODE_ENV": '"production"' },
  });
  if (r.errors.length) throw new Error(`seo: the gauges search script did not build: ${r.errors.map((e) => e.text).join("; ")}`);
  const js = r.outputFiles[0]!.text;
  browserSafe(js);
  return `(function(){"use strict";\n${inlineCore("gaugeClient.ts", JOIN_REPORTER, HERE)}\n${js}\n})();\n`;
}

/**
 * (T1039, owner Q183 2026-11-23: "App's chart, ~260 KB") THE APP'S CHART AS THE PAGE'S SECOND SCRIPT: gaugeChart.tsx and the
 * app code it draws with (@app/ui-native `StationChart`, @app/ui `useStationChart`, @app/data's feed and series code, React and
 * react-native-web), bundled into one minified file the page loads deferred, after its words. Resolved as the app's web build
 * and the tests resolve them (vitest.config.ts): react-native is react-native-web, each `.web` sibling first, react-native-svg's
 * web entry, the gesture handler's ES-module build. A bundle that does not build stops the site build (thrown).
 */
/**
 * A bundle that reads an environment variable (an EXPO_PUBLIC_ one, inlined by Metro in the app) would throw in the
 * browser: the site build stops instead. React's own `typeof process` checks are guarded and stay; NODE_ENV is defined.
 */
export function browserSafe(js: string): void {
  const env = /\bprocess\.env\b(?!\.NODE_ENV\b)[.\w]*/.exec(js);
  if (env) throw new Error(`seo: the gauge chart's script reads ${env[0]}, which no browser has`);
}

export interface ChartBundle {
  readonly js: string;
  /** the pictures the bundle names (the coho loader's sheets), at their served paths under /assets/chart/ */
  readonly assets: ReadonlyMap<string, Uint8Array>;
}
export async function chartScript(): Promise<ChartBundle> {
  // loaded here, not at the top: esbuild refuses to start in a DOM-like test environment (jsdom), and the tests that run
  // this file's other scripts there never build the chart
  const { build: esbuild } = await import("esbuild");
  const nm = (p: string) => join(HERE, "../../node_modules", p);
  const out = join(HERE, ".chart-bundle");                  // nothing is written there (write: false): the paths name the files
  const r = await esbuild({
    stdin: { contents: `import { mountGaugeChart } from "./gaugeChart";\nmountGaugeChart(document, window.fetch.bind(window), Date.now);\n`,
             resolveDir: HERE, loader: "ts", sourcefile: "gauge-chart-entry.ts" },
    // (0.9.0 release) an alias resolves in the WORKING directory, not the importer's: site-all.sh runs from the repo root,
    // where no node_modules holds react-native-web, so the build is pinned to app/ (gaugeChart.bundle.test.ts)
    absWorkingDir: join(HERE, "../.."),
    bundle: true, minify: true, format: "iife", platform: "browser", target: "es2020", write: false, logLevel: "silent",
    outdir: out, entryNames: "gauge-chart", assetNames: "chart/[name].[hash]", publicPath: "/assets",
    alias: { "react-native": "react-native-web", "react-native-svg": nm("react-native-svg/lib/module/ReactNativeSVG.web.js"),
             "@react-native/assets-registry/registry": "react-native-web/asset-registry",
             "react-native-gesture-handler": nm("react-native-gesture-handler/lib/module/index.js") },
    resolveExtensions: [".web.tsx", ".web.ts", ".web.js", ".tsx", ".ts", ".js", ".mjs", ".json"],
    // react-native-svg's touch sound: a `require` made on Android only (SvgTouchableMixin, `Platform.OS === "android"`), of a
    // TurboModule react-native-web has no registry for; left out of the bundle, as Metro's web build never runs it either
    external: ["../fabric/NativeSvgSoundModule"],
    define: { "process.env.NODE_ENV": '"production"', __DEV__: "false", global: "globalThis" },
    // a picture the app's components import is served as its own file, as the app's web export serves it
    loader: { ".js": "jsx", ".webp": "file", ".png": "file" },
  });
  if (r.errors.length) throw new Error(`seo: the gauge chart's script did not build: ${r.errors.map((e) => e.text).join("; ")}`);
  const main = r.outputFiles.find((f) => f.path === join(out, "gauge-chart.js"));
  if (!main) throw new Error("seo: the gauge chart's build wrote no gauge-chart.js");
  const js = main.text;
  browserSafe(js);
  const assets = new Map(r.outputFiles.filter((f) => f !== main).map((f) => [`assets/${f.path.slice(out.length + 1)}`, f.contents] as const));
  return { js, assets };
}

/** the production hosts the page names; deploy/staging/site.sh points a staging copy at staging's */
export const LIVE = {
  data: ORIGINS.production,
  crash: crashUrlFor(REPORT_URLS.production),
  env: "production",
} as const;
/** the days the page asks the weather for (the one number; the page's words say it from here) */
export const WEATHER_DAYS = 5;
const OPEN_METEO = "https://api.open-meteo.com/v1/forecast";
/** WMO weather codes 0-99 in the app's sky words (data/spots/words.ts `skyWord`) */
const SKY: Readonly<Record<string, string>> = Object.fromEntries(Array.from({ length: 100 }, (_, c) => [String(c), skyWord(c)]));

/** (T1039) the chart's inputs the build writes into each page: the station's climatology and the app's faces */
export interface ChartInputs { readonly clim: readonly StationClim[]; readonly fonts: string | null }

/**
 * A station's chart inputs from the release's climatology map (`readGaugeClim`): its rows (none: the release has no envelope
 * for it, which the app charts the same way) and the app's faces. No map at all is a build that skipped the read: refused,
 * since every page would chart no normal range.
 */
export function chartInputsOf(clim: ReadonlyMap<string, readonly StationClim[]> | null | undefined, station: string, fonts: string | null): ChartInputs {
  if (!clim) throw new Error("seo: gauge pages without the release's climatology (gaugeClim) would draw no normal range");
  return { clim: clim.get(station) ?? [], fonts };
}

export function gaugeConfig(g: GaugeModel, release: string, chart: ChartInputs): GaugeConfig {
  return {
    station: g.station,
    facts: `${LIVE.data}/feeds/cards/${g.station}.json`,
    weather: g.lat === null || g.lon === null ? null
      // (T1039) and the measures a spot's weather window shows: cloud cover, pressure, humidity (data/spots/weather.ts FIELDS)
      : `${OPEN_METEO}?${weatherPlace(g.lat, g.lon)}&current=temperature_2m,precipitation,weather_code,wind_speed_10m,cloud_cover,pressure_msl,relative_humidity_2m` +
        `&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max&timezone=Etc%2FGMT%2B7&forecast_days=${WEATHER_DAYS}`,
    weatherAttribution: OPEN_METEO_ATTRIBUTION, sky: SKY,
    crash: LIVE.crash, env: LIVE.env, release,
    // the app's own feed base (apps/mobile env.ts `feed`): the chart reads the station's file the app reads
    live: `${LIVE.data}/feeds/live`,
    clim: chart.clim.map((c) => ({ parameter: c.parameter, bands: Buffer.from(c.bands).toString("base64") })),
    fonts: chart.fonts || null,
    contact: STORE_CONTACT,
    // (T1039) where the day's sunrise and sunset are worked out (the gauge's own point; core `sunLine`, read time)
    place: g.lat === null || g.lon === null ? null : { lat: g.lat, lon: g.lon },
  };
}

// ── the pages ────────────────────────────────────────────────────────────────────────────────────────────────────────
/**
 * (T1045) a unique title under 60 characters: the name is cut, the station never. The words a searcher types follow the gauge's
 * own name, which already holds the water's word ("Alouette River Near Haney"), so the title adds none of its own and never says
 * "river river". WHAT it measures is the release's (`gaugeSubject` of its `gauge_clim`): a level-only gauge says "level", a
 * flow-only one "flow", one the release knows nothing of "conditions". "Forecast" is not in it: whether the River Forecast
 * Centre runs one is live data, said in the description as "where ... runs one".
 */
export function gaugeTitle(g: Pick<GaugeModel, "name" | "station" | "parameters">): string {
  const tail = ` ${gaugeSubject(g.parameters)} (${g.station})`;
  return `${clip(g.name, 60 - tail.length)}${tail}`;
}
/** (T1045) the page's one heading */
export const gaugeHeading = (g: Pick<GaugeModel, "name" | "parameters">): string => `${g.name} ${gaugeSubject(g.parameters)}`;
/** (T1045) the forecast, said only as what it is: the River Forecast Centre's run, where it runs one, for as many days as that run covers */
export const FORECAST_WORDS = "where the BC River Forecast Centre runs one";
export function gaugeDescription(g: GaugeModel): string {
  const w = g.close[0]?.name ?? g.rough[0]?.name;
  const what = gaugeSubject(g.parameters) === "conditions" ? "conditions" : gaugeSubject(g.parameters).replace(" and ", " or ");
  return clip(`Live ${what}, the flow forecast ${FORECAST_WORDS}, and the weather at the ${g.name} gauge (${g.station}), BC` +
    `${w ? `, for ${w}` : ""}. Free, from Water Survey of Canada data.`);
}

/**
 * (T908) the fold's summary: "Also" after the main waters; with none, "rough" only when every folded water is a weak
 * guide (the fold holds fair guides too, and a fair guide is never called rough).
 */
export function moreSummary(g: Pick<GaugeModel, "main" | "more" | "rough">): string {
  const n = `${g.more.length} more ${g.more.length === 1 ? "water" : "waters"}`;
  if (g.main.length) return `Also a guide for ${n}`;
  const rough = new Set(g.rough.map((w) => w.id));
  return g.more.every((w) => rough.has(w.id)) ? `A rough guide for ${n}` : `A guide for ${n}`;
}

/** the page body: everything that does not move between feed runs; the live sections are filled by gaugeClient.ts */
export function gaugeBody(g: GaugeModel, release: string, clientHref: string, asOf: string, chartHref: string, chart: ChartInputs): string {
  const first = g.close[0] ?? g.rough[0]!;
  const line = (w: WaterLink) => `<li><a href="${esc(w.path)}">${esc(w.name)} fishing regulations</a></li>`;
  const cfg = JSON.stringify(gaugeConfig(g, release, chart)).replace(/</g, "\\u003c");
  return `<div class="gv"><nav class="crumbs"><a href="/">Home</a> › <a href="/gauges">River gauges</a> › ${esc(g.name)}</nav>` +
    `<h1>${esc(gaugeHeading(g))}</h1>` +
    // (T1194, owner 2026-11-26: "this is bloated … or actually just removed") NO INTRO PARAGRAPH: the page opens as the app's does (the name, then
    // the reading); what it is and where it is from live in the description, the JSON-LD place and the credit at the foot. This line stays
    // because the JSON-LD's station and coordinates are held to the page (`checkJsonLd`)
    `<p class="small">Station ${esc(g.station)}${g.lat !== null && g.lon !== null ? ` · ${g.lat.toFixed(4)}, ${g.lon.toFixed(4)}` : ""}</p>` +
    `<noscript><p class="warn">The live reading, chart and weather need JavaScript.</p><style>#gauge-chart{display:none}</style></noscript>` +
    // (T1039) the app's own chart, drawn by its deferred script (gaugeChart.tsx); the box keeps its place meanwhile
    `<div id="gauge-chart"><p class="small">Drawing the chart…</p></div>` +
    `<div id="gauge-weather"><p class="small">Reading the forecast…</p></div><div id="gauge-sun"></div>` +
    `<h2>Waters this gauge serves</h2>` +
    (g.main.length ? `<ul>${g.main.map(line).join("")}</ul>` : "") +
    // (T908) the fair and weak guides are one fold, never the page's body
    (g.more.length ? `<details><summary>${esc(moreSummary(g))}</summary>` +
      `<ul class="cols">${g.more.map(line).join("")}</ul></details>` : "") +
    `<p class="small">Waters as of <time datetime="${esc(asOf)}">${esc(asOf)}</time>, from release ${esc(release)}.</p>` +
    // (T1194) the sources at the foot, as every page has them: the readings' and the forecast's credit (gaugeClient.ts `creditHtml`)
    `<div id="gauge-credit"></div>` +
    `<div class="card"><p>See this gauge with the map, alerts and every other gauge in the app.</p>` +
    // (owner 2026-11-08) the website now; the phone app greyed until its store listing is public (T514)
    `<div class="opens"><a class="app" href="${esc(`${first.path}?view=conditions`)}">Open on the website</a></div>` +
    // (T1039, owner 2026-11-23: "chart should use same buttons as spots too for now") the web app's own "Get the app"
    // block (ui-native `GetAppBlock`), drawn by the chart's script
    `<div id="gauge-get-app"></div></div></div>` +
    `<script type="application/json" id="gauge-config">${cfg}</script><script src="${esc(clientHref)}" defer></script>` +
    `<script src="${esc(chartHref)}" defer></script>`;
}

/**
 * (T1045) what is wrong with a gauge page's search words, against the gauge's data: the title and heading say what the release
 * holds for the station, once, no word twice in a row; "forecast" is only ever hedged ("where the BC River
 * Forecast Centre runs one"), never in the title or heading.
 */
export function gaugeWordsProblems(g: GaugeModel, title: string, description: string, body: string): string[] {
  const bad: string[] = [];
  const h1 = (/<h1>([^<]*)<\/h1>/.exec(body)?.[1] ?? "").replace(/&amp;/g, "&");
  const subject = gaugeSubject(g.parameters);
  if (!title.includes(` ${subject} (${g.station})`)) bad.push(`title "${title}" lacks "${subject}" (the release holds ${g.parameters.join(" + ") || "no parameter"} for the station)`);
  if (!h1.endsWith(` ${subject}`)) bad.push(`heading "${h1}" lacks "${subject}"`);
  for (const [what, t] of [["title", title], ["heading", h1]] as const) {
    if (/forecast/i.test(t)) bad.push(`${what} "${t}" says forecast: whether a forecast runs is live data`);
    const twice = /\b([A-Za-z]{3,})\s+\1\b/i.exec(t);
    if (twice) bad.push(`${what} "${t}" says "${twice[0]}" twice in a row`);
  }
  if (/forecast/i.test(description) && !description.includes(FORECAST_WORDS) && !description.endsWith("…")) bad.push(`description says forecast without "${FORECAST_WORDS}"`);
  if (!/forecast/i.test(description) && !description.endsWith("…")) bad.push(`description lacks "forecast"`);
  return bad;
}

/** (T1107) the search's gauges: each station's name, page, position, main water and the waters it is a gauge ON (own, lake, good) */
export function searchGauges(gs: readonly GaugeModel[]): SearchGauge[] {
  return gs.map((g) => ({ station: g.station, name: g.name, path: g.path, lat: g.lat, lon: g.lon,
    water: (g.main[0] ?? g.close[0] ?? g.rough[0])?.name ?? null, on: g.main.map((w) => w.id) }));
}

/** (T1107) the one line a visitor from a search engine reads first: what the page is, and where the rules are */
export const GAUGES_LEDE = "Find the river gauge nearest a town, or on a river or lake: live flow and level, the last 7 days and the forecast where one runs. " +
  "Each gauge's page links to the fishing regulations of the waters it serves.";

/** (owner 2026-11-25: "to view the conditions visually on a map go here") the line and its link text: a proposal, the owner's words are an example */
export const CONDITIONS_MAP_WORDS = "To see the conditions of every water on a map, open the Conditions map.";
export const CONDITIONS_MAP_LINK = "Open the Conditions map";

export function gaugesIndexBody(gs: readonly GaugeModel[], asOf: string, find: { index: string; script: string; cfg: { crash: string | null; env: string; release: string; live: string }; conditions: string } | null = null): string {
  return `<nav class="crumbs"><a href="/">Home</a> › River gauges</nav><h1>River gauges in BC</h1>` +
    `<p>${esc(GAUGES_LEDE)}</p>` + (find ? `<div class="card mapline"><p>${esc(CONDITIONS_MAP_WORDS)}</p><div class="opens"><a class="app" href="${esc(find.conditions)}">${esc(CONDITIONS_MAP_LINK)}</a></div></div>` : "") + (find ? `${gaugeFindForm(find.index, find.cfg)}<script src="${esc(find.script)}" defer></script>` : "") +
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
