/**
 * T487 — the gauge pages on the regs fixture (which carries no conditions file: the gauges are given) and the site
 * worker's /gauge routes (the card mapped from R2, never drawn; a missing one answered with the site card and marked).
 * The page's live half: gaugeClient.test.ts.
 */
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { BUDGET, chartJsProblems } from "./budget";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll, type Built } from "./build";
import { TODAY } from "./fixture";
import { checkJsonLd } from "./jsonld";
import { pngSize } from "./share";
import { browserSafe, chartInputsOf, FORECAST_WORDS, gaugeBody, gaugeDescription, gaugeHeading, gaugeLede, gaugeModels, gaugePath, gaugeTitle, gaugeWordsProblems, LIVE, moreSummary, WEATHER_DAYS, type GaugeModel, type GaugeRows } from "./gauges";
import { readConfig } from "./build";
import { STORE_CONTACT } from "@app/core";

const site = readConfig();
const CLIM = Uint8Array.from([1, 2, 3, 250, 251]);
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
let built: Built, rows: GaugeRows, ids: string[];

beforeAll(async () => {
  const probe = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: null, bare: true });
  ids = probe.build.models.slice(0, 4).map((m) => m.id);
  rows = {
    gauges: [
      { station: "08MH001", name: "Chilliwack River at Vedder Crossing", item: ids[0]!, lat: 49.0973, lon: -121.9638 },
      { station: "08MH016", name: "Chilliwack River at Chilliwack Lake", item: null, lat: 49.0886, lon: -121.4565 },
      { station: "08ZZ001", name: "Nowhere Creek near Nothing", item: "gnis:999999999", lat: null, lon: null },   // serves no water with a page
    ],
    serves: [
      { station: "08MH001", item: ids[0]!, trust: "own" }, { station: "08MH001", item: ids[1]!, trust: "fair" }, { station: "08MH001", item: ids[0]!, trust: "weak" },
      { station: "08MH016", item: ids[2]!, trust: "weak" }, { station: "08MH016", item: ids[3]!, trust: "weak" },
    ],
  };
  // (T1039) the climatology the release would hold: one station's flow bands (bytes the page carries whole, never read here)
  built = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: rows, crosswalk: { "w-one": [ids[0]!] },
                           gaugeClim: new Map([["08MH001", [{ parameter: "discharge" as const, bands: CLIM }]]]) });
}, 300_000);

const gaugePages = () => built.build.pages.filter((p) => p.kind === "gauge");

describe("T487 gauge pages", () => {
  it("one page per gauge serving a water with a page, at /gauge/<station>/<slug>, in the sitemap with the water pages", () => {
    expect(gaugePages().map((p) => p.path).sort()).toEqual(["/gauge/08MH001/chilliwack-river-at-vedder-crossing", "/gauge/08MH016/chilliwack-river-at-chilliwack-lake"]);
    const sitemap = built.files.get("sitemap-1.xml")!;
    for (const p of [...gaugePages().map((x) => x.path), "/gauges", "/water/"]) expect(sitemap, p).toContain(`<loc>${site.origin}${p}`);
    expect(built.problems).toEqual([]);
    const stations = JSON.parse(built.files.get("gauge/stations.json")!);
    expect(stations).toMatchObject({ format: "gauge-stations/1", stations: [
      { station: "08MH016", path: "/gauge/08MH016/chilliwack-river-at-chilliwack-lake" }, { station: "08MH001", path: "/gauge/08MH001/chilliwack-river-at-vedder-crossing" }] });
    expect(pngSize(built.binary.get("share/gauge.png")!)).toEqual({ width: 1200, height: 630 }); // the Open Graph card size, fixed by design
    expect(built.files.has("share/gauge.svg")).toBe(false);
  });

  it("a page names its waters (one regulations link each, best first; weak ones apart), opens the app on the water's conditions, and never says open or closed", () => {
    const p = gaugePages().find((x) => x.path.startsWith("/gauge/08MH001"))!;
    const m = built.build.models, path = (id: string) => `/water/${built.build.slugMap.waters[id]!.slug}`;
    const links = [...p.html.matchAll(/<a href="(\/water\/[^"?]+)">([^<]*) fishing regulations<\/a>/g)].map((x) => x[1]);
    expect(links).toEqual([path(ids[0]!), path(ids[1]!)]);                 // own water first, then the fair one; the weak duplicate folded
    expect(p.html).toContain(`<a class="app" href="${path(ids[0]!)}?view=conditions">Open on the website</a>`);
    // (T1039) the "Get the app" block is the web app's own, drawn by the chart's script (gaugePage.parity.test.ts)
    expect(p.html).toContain(`<div id="gauge-get-app"></div>`);
    const q = gaugePages().find((x) => x.path.startsWith("/gauge/08MH016"))!;
    expect(q.html).toMatch(/<details><summary>A rough guide for 2 more waters<\/summary>/);
    for (const g of gaugePages()) {
      expect(text(g.html), g.path).not.toMatch(/\b(open|closed) (today|now)\b|\bclosed\b/i);
      expect(g.html).toContain(`<meta property="og:image" content="${site.origin}/gauge/${g.path.split("/")[2]}/card.png">`);
      expect(checkJsonLd(g.html, site.origin), g.path).toEqual([]);
      expect(g.title.length).toBeLessThanOrEqual(60);
    }
    // (T908, owner 2026-11-19) no "Browse by region" on a gauge page (it leads to the hidden site map); the fair guides are in the fold, the main waters above it
    for (const g of gaugePages()) expect(g.html, g.path).not.toContain("Browse by region");
    expect(p.html).toMatch(/<h2>Waters this gauge serves<\/h2><ul><li><a href="[^"]*">[^<]*<\/a><\/li><\/ul><details><summary>Also a guide for 1 more water<\/summary>/);
    expect(built.build.pages.find((x) => x.kind === "region")!.html).toContain("Browse by region");
    expect(m.length).toBeGreaterThan(4);
  });

  it("the live half ships as one deferred script with the production hosts in its config (staging rewrites them, site.sh)", () => {
    const p = gaugePages()[0]!;
    const src = /<script src="(\/assets\/gauge\.[0-9a-f]{10}\.js)" defer><\/script>/.exec(p.html)![1]!;
    const js = built.files.get(src.slice(1))!;
    expect(js).toContain("function gaugePage(");
    expect(js).not.toMatch(/^\s*(import|export)\b/m);
    new Function(js.replace("gaugePage(document", "(void 0) && gaugePage(document"));   // it parses as a plain script
    const cfg = JSON.parse(/<script type="application\/json" id="gauge-config">([^<]*)<\/script>/.exec(p.html)![1]!);
    expect(cfg.facts).toBe(`${LIVE.data}/feeds/cards/${cfg.station}.json`);
    expect([LIVE.data, LIVE.crash, cfg.env]).toEqual(["https://data.canifishthis.ca", "https://feeds.canifishthis.ca/crash", "production"]);
    expect(cfg.weather).toMatch(/^https:\/\/api\.open-meteo\.com\/v1\/forecast\?latitude=49\.\d\d&longitude=-121\.\d\d&/);
    // (T1039) the spot window's measures, and the gauge's point for the day's sun
    expect(cfg.weather).toContain("&current=temperature_2m,precipitation,weather_code,wind_speed_10m,cloud_cover,pressure_msl,relative_humidity_2m&");
    const g = rows.gauges.find((x) => x.station === cfg.station)!;
    expect(cfg.place).toEqual({ lat: g.lat, lon: g.lon });
    expect(cfg.sky["61"]).toBe("Rain");
  });

  it("(T1039) the chart is the app's: a second deferred script, the app's feed base and the station's climatology in the config", () => {
    const p = gaugePages().find((g) => g.path.startsWith("/gauge/08MH001/"))!;
    const src = /<script src="(\/assets\/gauge-chart\.[0-9a-f]{10}\.js)" defer><\/script>/.exec(p.html)![1]!;
    const js = built.files.get(src.slice(1))!;
    // the app's own components and data code are in it, by their own words (not a page's re-drawing)
    for (const w of ["Real-time only", "Scroll to zoom", "gauge-page-chart", "/obs.json"]) expect(js, w).toContain(w);
    expect(js).not.toMatch(/feeds\/cards\//);                                    // the chart never reads the share card's facts
    expect(p.html).toMatch(/<div id="gauge-chart"><p class="small">Drawing the chart…<\/p><\/div><div id="gauge-credit"><\/div>/);
    const cfg = JSON.parse(/<script type="application\/json" id="gauge-config">([^<]*)<\/script>/.exec(p.html)![1]!);
    expect(cfg.live).toBe(`${LIVE.data}/feeds/live`);
    expect(cfg.clim).toEqual([{ parameter: "discharge", bands: Buffer.from(CLIM).toString("base64") }]);
    // a station the release holds no climatology for: none, as the app has none for it
    const other = gaugePages().find((g) => g.path.startsWith("/gauge/08MH016/"))!;
    expect(JSON.parse(/id="gauge-config">([^<]*)<\/script>/.exec(other.html)![1]!).clim).toEqual([]);
    // (T1039, owner: "same buttons as spots") the web app's Get the app block, mounted by the chart's script: no static copy,
    // no Join script of the page's own; its contact is the one the web app is exported with
    expect(p.html).toContain('<div id="gauge-get-app"></div>');
    // (T1039 review) with no JavaScript the chart's box (550 px, "Drawing the chart…") is not shown at all
    expect(p.html).toContain('<noscript><p class="warn">The live reading, chart and weather need JavaScript.</p><style>#gauge-chart{display:none}</style></noscript>');
    // the chart's script is within its budget, and a gauge page is in the Lighthouse sample
    expect(chartJsProblems(built.files)).toEqual([]);
    expect(JSON.parse(built.files.get("lighthouserc.json")!).ci.collect.url.some((u: string) => u.startsWith("gauge/"))).toBe(true);
    expect(p.html).not.toMatch(/join\.[0-9a-f]{10}\.js|data-join=|<dialog/);
    expect(cfg.contact).toBe(STORE_CONTACT);
    const pkg = JSON.parse(readFileSync(new URL("../../apps/mobile/package.json", import.meta.url), "utf8")).scripts;
    for (const k of ["export:web", "export:web:staging"]) expect(pkg[k], k).toContain(`EXPO_PUBLIC_CONTACT=${STORE_CONTACT} `);
    // the coho loader's sheets the app's LoadingState names, served beside the script
    expect([...built.binary.keys()].filter((k) => k.startsWith("assets/chart/")).length).toBeGreaterThanOrEqual(2);
  });

  it("(T1039) the Conditions card charts through the component the page mounts (one chart, two callers; gaugePage.parity.test.ts)", () => {
    const panel = readFileSync(new URL("../../packages/ui-native/src/ConditionsPanel.tsx", import.meta.url), "utf8");
    const page = readFileSync(new URL("./gaugeChart.tsx", import.meta.url), "utf8");
    expect(panel).toMatch(/<StationChart vm=\{sc\}/);
    expect(panel).toMatch(/useStationChart\(source, station,/);
    expect(panel).not.toMatch(/<Hydrograph\b/);
    // (T1194) the page mounts the shared reading + chart piece, which draws the chart through the same component and hook
    const shared = readFileSync(new URL("../../packages/ui-native/src/GaugeReading.tsx", import.meta.url), "utf8");
    expect(page).toMatch(/h\(GaugeReading, \{ source, station,/);
    expect(shared).toMatch(/<StationChart vm=\{vm\}/);
    expect(shared).toMatch(/useStationChart\(source, station,/);
  });

  it("(T1039) the chart's script may not read an environment variable a browser lacks; NODE_ENV and React's guarded checks pass", () => {
    expect(() => browserSafe('x=process.env.EXPO_PUBLIC_DATA_BASE')).toThrow(/reads process\.env\.EXPO_PUBLIC_DATA_BASE/);
    expect(() => browserSafe("x=process.env")).toThrow(/reads process\.env/);
    expect(() => browserSafe('if("production"!==process.env.NODE_ENV)a();typeof process=="object"&&process.emit("x")')).not.toThrow();
  });

  it("(T1039 review) the chart's script over its gzip budget is a build problem, named", () => {
    const big = randomBytes(BUDGET.chartJsGzipBytes + 20_000).toString("latin1");
    expect(chartJsProblems(new Map([["assets/gauge-chart.0123456789.js", big]]))[0])
      .toMatch(new RegExp(`^assets/gauge-chart\\.0123456789\\.js is \\d+ B gzip, over ${BUDGET.chartJsGzipBytes}$`));
    expect(chartJsProblems(new Map([["assets/gauge-chart.0123456789.js", "x".repeat(BUDGET.chartJsGzipBytes)]]))).toEqual([]);   // compresses to nothing
  });

  it("(T1039) a station's chart inputs: its rows, none when the release has none for it; no climatology map at all is refused", () => {
    const rowsOf = new Map([["08MH001", [{ parameter: "discharge" as const, bands: CLIM }]]]);
    expect(chartInputsOf(rowsOf, "08MH001", "F")).toEqual({ clim: [{ parameter: "discharge", bands: CLIM }], fonts: "F" });
    expect(chartInputsOf(rowsOf, "08XX001", null)).toEqual({ clim: [], fonts: null });
    for (const none of [null, undefined]) expect(() => chartInputsOf(none, "08MH001", null)).toThrow(/without the release's climatology/);
  });

  it("a station's title keeps its number however long the name; the path is the name's slug", () => {
    const long = { station: "08AB123", name: "A Very Long River Name Indeed That Goes On And On Below The Long Falls" };
    expect(gaugeTitle({ ...long, parameters: ["discharge", "level"] }).length).toBeLessThanOrEqual(60);
    expect(gaugeTitle({ ...long, parameters: [] })).toMatch(/\(08AB123\)$/);
    expect(gaugePath("08MH001", "Chilliwack River at Vedder Crossing")).toBe("/gauge/08MH001/chilliwack-river-at-vedder-crossing");
    expect(gaugeModels({ gauges: [{ station: "X1", name: "N", item: null, lat: null, lon: null }], serves: [] }, () => null)).toEqual([]);
  });
});

describe("(T1045) gauge pages for the searcher's words", () => {
  const g = (name: string, parameters: GaugeModel["parameters"], station = "08MH005") => ({ station, name, parameters });
  const model = (name: string, parameters: GaugeModel["parameters"]): GaugeModel => ({ station: "08MH005", name, path: "/gauge/08MH005/x", lat: 49.2, lon: -122.4,
    close: [{ id: "w", name: "Alouette River", path: "/water/alouette-river" }], main: [], more: [], rough: [], parameters });
  const body = (m: GaugeModel) => gaugeBody(m, "r-x", "/c.js", "2026-11-23", "/ch.js", { clim: [], fonts: null });
  it("the words come from what the release holds for the station: level, flow, both, or none known; never twice, never 'forecast'", () => {
    expect(gaugeTitle(g("Alouette River Near Haney", ["discharge", "level"]))).toBe("Alouette River Near Haney level and flow (08MH005)");
    expect(gaugeTitle(g("Alouette River Near Haney", ["level"]))).toBe("Alouette River Near Haney level (08MH005)");
    expect(gaugeTitle(g("Alouette River Near Haney", ["discharge"]))).toBe("Alouette River Near Haney flow (08MH005)");
    expect(gaugeTitle(g("Alouette River Near Haney", []))).toBe("Alouette River Near Haney conditions (08MH005)");
    expect(gaugeHeading(g("Alouette River Near Haney", ["level"]))).toBe("Alouette River Near Haney level");
    expect(gaugeTitle(g("A Very Long River Name Indeed That Goes On And On Below The Long Falls", ["discharge", "level"])).length).toBeLessThanOrEqual(60);
    // the fixture's gauges: 08MH001 has discharge climatology (flow), 08MH016 none (conditions)
    const byStation = (st: string) => gaugePages().find((x) => x.path.startsWith(`/gauge/${st}`))!;
    expect(byStation("08MH001").title).toMatch(/ flow \(08MH001\)$/);
    expect(byStation("08MH016").title).toMatch(/ conditions \(08MH016\)$/);
    for (const gp of gaugePages()) {
      expect(gp.html, gp.path).toMatch(/<h1>[^<]*<\/h1><p class="lede">/);
      expect(gp.title + text(gp.html.match(/<h1>[^<]*<\/h1>/)![0]), gp.path).not.toMatch(/forecast|\b(\w{3,}) \1\b/i);
    }
  });
  it("the flow forecast is only ever 'where the BC River Forecast Centre runs one', with no day count; the weather has its own horizon (the one constant)", () => {
    const m = model("Alouette River Near Haney", ["discharge"]);
    const l = gaugeLede(m);
    expect(l).toContain(FORECAST_WORDS);
    expect(l).toContain(`The weather is for the next ${WEATHER_DAYS} days.`);
    expect(l.replace(`${WEATHER_DAYS} days`, "")).not.toMatch(/\d+[- ]days?\b/i);
    expect(gaugeDescription(m)).toContain(FORECAST_WORDS);
    const p = gaugePages()[0]!;
    expect(p.html).toContain(`forecast_days=${WEATHER_DAYS}`);
  });
  it("the gate checks the words against the data: a level-only gauge titled 'level and flow', a doubled word, a forecast claimed outright, no lede", () => {
    const lvl = model("Alouette River Near Haney", ["level"]);
    expect(gaugeWordsProblems(lvl, gaugeTitle(lvl), gaugeDescription(lvl), body(lvl))).toEqual([]);
    expect(gaugeWordsProblems(lvl, "Alouette River Near Haney level and flow (08MH005)", gaugeDescription(lvl), body(lvl))).toEqual([expect.stringMatching(/lacks "level" \(the release holds level for the station\)/)]);
    expect(gaugeWordsProblems(lvl, "Alouette River Near Haney conditions (08MH005)", gaugeDescription(lvl), body(lvl))).toEqual([expect.stringMatching(/lacks "level"/)]);
    expect(gaugeWordsProblems(lvl, "Alouette River River level (08MH005)", gaugeDescription(lvl), body(lvl))).toEqual([expect.stringMatching(/"River River" twice/)]);
    expect(gaugeWordsProblems(lvl, "Alouette River Near Haney level forecast (08MH005)", gaugeDescription(lvl), body(lvl)).join()).toMatch(/says forecast/);
    expect(gaugeWordsProblems(lvl, gaugeTitle(lvl), "Live level and the flow forecast.", body(lvl)).join()).toMatch(/description says forecast without/);
    expect(gaugeWordsProblems(lvl, gaugeTitle(lvl), gaugeDescription(lvl), body(lvl).replace('<p class="lede">', "<p>"))).toEqual(["no lede"]);
    expect(gaugeWordsProblems(lvl, gaugeTitle(lvl), gaugeDescription(lvl), body(lvl).replace(/<h1>[^<]*<\/h1>/, "<h1>Alouette</h1>"))).toEqual([expect.stringMatching(/heading "Alouette" lacks "level"/)]);
  });
  it("a gauge page's JSON-LD names its place by name, station and coordinates, and the gate holds each to the page", () => {
    for (const gp of gaugePages()) {
      const ld = JSON.parse(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/.exec(gp.html)![1]!.replace(/\\u003c/g, "<"))["@graph"];
      const place = ld.find((n: { "@type": string }) => n["@type"] === "Place");
      expect(place).toMatchObject({ identifier: gp.path.split("/")[2], name: expect.any(String) });
      expect(ld.find((n: { "@type": string }) => n["@type"] === "WebPage").about).toEqual({ "@id": place["@id"] });
      expect(checkJsonLd(gp.html, site.origin), gp.path).toEqual([]);
    }
    const vedder = gaugePages().find((x) => x.path.startsWith("/gauge/08MH001"))!;
    expect(/"geo":\{"@type":"GeoCoordinates","latitude":49\.0973,"longitude":-121\.9638\}/.test(vedder.html)).toBe(true);
    expect(text(vedder.html)).toContain("at 49.0973, -121.9638");
  });
  it("(owner 2026-11-23) a water page carries no link to a gauge page: gauge pages are reached by share links, /gauges and the sitemap", () => {
    for (const w of built.build.pages.filter((x) => x.kind === "water")) expect(w.html, w.path).not.toMatch(/href="\/gauge\//);
  });
});

describe("T487 the site worker's /gauge routes", () => {
  const load = async () => (await import(/* @vite-ignore */ `data:text/javascript;base64,${Buffer.from(built.files.get("_worker.js")!).toString("base64")}`)).default;
  const PNG = new Uint8Array([0x89, 0x50, 0x4e, 0x47]);
  const env = (cards: Record<string, Uint8Array | string>) => {
    const put = vi.fn(async (k: string, v: string) => { cards[k] = v; });
    return {
      put,
      ASSETS: { fetch: async (r: Request) => new URL(r.url).pathname === "/share/gauge.png"
        ? new Response(new Uint8Array([1, 2, 3]), { status: 200, headers: { "content-type": "image/png" } }) : new Response("asset", { status: 200 }) },
      CARDS: {
        get: async (k: string) => (k in cards ? { body: cards[k], httpEtag: '"e"', json: async () => JSON.parse(String(cards[k])) } : null),
        put,
      },
    };
  };
  const ctx = () => { const w: Promise<unknown>[] = []; return { waitUntil: (p: Promise<unknown>) => w.push(p), all: () => Promise.all(w) }; };

  it("a card on R2 is served as is; the worker draws nothing", async () => {
    const w = await load(), e = env({ "feeds/cards/08MH001.png": PNG }), c = ctx();
    const r = await w.fetch(new Request("https://canifishthis.ca/gauge/08MH001/card.png"), e, c);
    expect([r.status, r.headers.get("content-type"), r.headers.get("cache-control"), r.headers.get("x-card")]).toEqual([200, "image/png", "public, max-age=900", null]);
    expect(new Uint8Array(await r.arrayBuffer())).toEqual(PNG);
    await c.all();
    expect(e.put).not.toHaveBeenCalled();
  });

  it("a missing card: the site card, said in a header, AND a marker the feeds worker reports (once an hour at most)", async () => {
    const w = await load(), cards: Record<string, Uint8Array | string> = {}, e = env(cards), c = ctx();
    const r = await w.fetch(new Request("https://canifishthis.ca/gauge/08MH016/card.png"), e, c);
    expect([r.status, r.headers.get("x-card")]).toEqual([200, "site"]);
    expect(new Uint8Array(await r.arrayBuffer())).toEqual(new Uint8Array([1, 2, 3]));
    await c.all();
    const marker = JSON.parse(String(cards["private/cards-missing/08MH016.json"]));
    expect(marker).toMatchObject({ station: "08MH016", count: 1 });
    const c2 = ctx();
    await w.fetch(new Request("https://canifishthis.ca/gauge/08MH016/card.png"), e, c2);
    await c2.all();
    expect(e.put).toHaveBeenCalledTimes(1);
  });

  it("the station alone, or an old slug, is one 301 to the page; the page and unknown stations go to the assets", async () => {
    const w = await load(), e = env({}), c = ctx();
    const bare = await w.fetch(new Request("https://canifishthis.ca/gauge/08MH001?x=1"), e, c);
    expect([bare.status, bare.headers.get("location")]).toEqual([301, "https://canifishthis.ca/gauge/08MH001/chilliwack-river-at-vedder-crossing?x=1"]);
    const old = await w.fetch(new Request("https://canifishthis.ca/gauge/08MH001/old-name"), e, c);
    expect(old.status).toBe(301);
    expect(await (await w.fetch(new Request("https://canifishthis.ca/gauge/08MH001/chilliwack-river-at-vedder-crossing"), e, c)).text()).toBe("asset");
    expect(await (await w.fetch(new Request("https://canifishthis.ca/gauge/99XX999/card.png"), e, c)).text()).toBe("asset");
  });
});

describe("(T908) the fold's summary names the guides it holds", () => {
  const w = (id: string) => ({ id, name: id, path: `/water/${id}` });
  const a = w("a"), b = w("b"), c = w("c");
  it("rough only when every folded water is a weak guide; a fair one is never called rough", () => {
    expect(moreSummary({ main: [a], more: [b], rough: [] })).toBe("Also a guide for 1 more water");
    expect(moreSummary({ main: [], more: [b, c], rough: [b, c] })).toBe("A rough guide for 2 more waters");
    expect(moreSummary({ main: [], more: [b, c], rough: [c] })).toBe("A guide for 2 more waters");
    expect(moreSummary({ main: [], more: [b], rough: [] })).toBe("A guide for 1 more water");
  });
});
