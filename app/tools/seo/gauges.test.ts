/**
 * T487 — the gauge pages on the regs fixture (which carries no conditions file: the gauges are given) and the site
 * worker's /gauge routes (the card mapped from R2, never drawn; a missing one answered with the site card and marked).
 * The page's live half: gaugeClient.test.ts.
 */
import { beforeAll, describe, expect, it, vi } from "vitest";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll, type Built } from "./build";
import { TODAY } from "./fixture";
import { checkJsonLd } from "./jsonld";
import { pngSize } from "./share";
import { gaugeModels, gaugePath, gaugeTitle, LIVE, type GaugeRows } from "./gauges";
import { readConfig } from "./build";

const site = readConfig();
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
let built: Built, rows: GaugeRows, ids: string[];

beforeAll(async () => {
  const probe = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: null });
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
  built = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: rows, crosswalk: { "w-one": [ids[0]!] } });
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
    expect(pngSize(built.binary.get("share/gauge.png")!)).toEqual({ width: 1200, height: 630 });
    expect(built.files.has("share/gauge.svg")).toBe(false);
  });

  it("a page names its waters (one regulations link each, best first; weak ones apart), opens the app on the water's conditions, and never says open or closed", () => {
    const p = gaugePages().find((x) => x.path.startsWith("/gauge/08MH001"))!;
    const m = built.build.models, path = (id: string) => `/water/${built.build.slugMap.waters[id]!.slug}`;
    const links = [...p.html.matchAll(/<a href="(\/water\/[^"?]+)">([^<]*) fishing regulations<\/a>/g)].map((x) => x[1]);
    expect(links).toEqual([path(ids[0]!), path(ids[1]!)]);                 // own water first, then the fair one; the weak duplicate folded
    expect(p.html).toContain(`<a class="app" href="${path(ids[0]!)}?view=conditions">Open in the app</a>`);
    const q = gaugePages().find((x) => x.path.startsWith("/gauge/08MH016"))!;
    expect(q.html).toMatch(/<details><summary>A rough guide for 2 more waters<\/summary>/);
    for (const g of gaugePages()) {
      expect(text(g.html), g.path).not.toMatch(/\b(open|closed) (today|now)\b|\bclosed\b/i);
      expect(g.html).toContain(`<meta property="og:image" content="${site.origin}/gauge/${g.path.split("/")[2]}/card.png">`);
      expect(checkJsonLd(g.html, site.origin), g.path).toEqual([]);
      expect(g.title.length).toBeLessThanOrEqual(60);
    }
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
