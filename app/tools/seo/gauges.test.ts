/**
 * T487 — the gauge pages on the regs fixture (which carries no conditions file: the gauges are given) and the site
 * worker's /gauge routes (the card mapped from R2, never drawn; a missing one answered with the site card and marked).
 * The page's live half: gaugeClient.test.ts.
 */
import { readFileSync } from "node:fs";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll, type Built } from "./build";
import { TODAY } from "./fixture";
import { checkJsonLd } from "./jsonld";
import { pngSize } from "./share";
import { browserSafe, gaugeModels, gaugePath, gaugeTitle, LIVE, moreSummary, type GaugeRows } from "./gauges";
import { readConfig } from "./build";

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
    expect(p.html).toContain(`<p class="small coming">Coming soon to Google Play and the App Store.</p>`);
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
    // the coho loader's sheets the app's LoadingState names, served beside the script
    expect([...built.binary.keys()].filter((k) => k.startsWith("assets/chart/")).length).toBeGreaterThanOrEqual(2);
  });

  it("(T1039) the Conditions card charts through the component the page mounts (one chart, two callers; gaugePage.parity.test.ts)", () => {
    const panel = readFileSync(new URL("../../packages/ui-native/src/ConditionsPanel.tsx", import.meta.url), "utf8");
    const page = readFileSync(new URL("./gaugeChart.tsx", import.meta.url), "utf8");
    expect(panel).toMatch(/<StationChart vm=\{sc\}/);
    expect(panel).toMatch(/useStationChart\(source, station,/);
    expect(panel).not.toMatch(/<Hydrograph\b/);
    expect(page).toMatch(/h\(StationChart, \{ vm, palette/);
    expect(page).toMatch(/useStationChart\(source, station,/);
  });

  it("(T1039) the chart's script may not read an environment variable a browser lacks; NODE_ENV and React's guarded checks pass", () => {
    expect(() => browserSafe('x=process.env.EXPO_PUBLIC_DATA_BASE')).toThrow(/reads process\.env\.EXPO_PUBLIC_DATA_BASE/);
    expect(() => browserSafe("x=process.env")).toThrow(/reads process\.env/);
    expect(() => browserSafe('if("production"!==process.env.NODE_ENV)a();typeof process=="object"&&process.emit("x")')).not.toThrow();
  });

  it("(T1039) gauge pages with no climatology at all are refused: they would chart no normal range", async () => {
    await expect(buildAll({ loader: openRegsFixture(), on: TODAY, gauges: rows, bare: true, gaugeClim: null }))
      .rejects.toThrow(/without the release's climatology/);
  }, 120_000);

  it("a station's title keeps its number however long the name; the path is the name's slug", () => {
    const long = { station: "08AB123", name: "A Very Long River Name Indeed That Goes On And On Below The Long Falls" };
    expect(gaugeTitle(long).length).toBeLessThanOrEqual(60);
    expect(gaugeTitle(long)).toMatch(/\(08AB123\)$/);
    expect(gaugePath("08MH001", "Chilliwack River at Vedder Crossing")).toBe("/gauge/08MH001/chilliwack-river-at-vedder-crossing");
    expect(gaugeModels({ gauges: [{ station: "X1", name: "N", item: null, lat: null, lon: null }], serves: [] }, () => null)).toEqual([]);
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
