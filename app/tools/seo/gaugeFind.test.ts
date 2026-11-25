// @vitest-environment jsdom
/**
 * (T1107) THE /gauges SEARCH IN A DOCUMENT: results with distances and the app's current-data words, no silent empty, a failed
 * file or live index said AND reported, a 404 said as out of date, and the link to the Conditions map being the address the app
 * itself writes for it.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { HOME, parseStartLink, reduce, routeSearch } from "@app/ui";
import { gaugesIndexBody, CONDITIONS_MAP_LINK } from "./gauges";
import { LIVE_FAILED, LOAD_FAILED, NO_MATCH, STALE_FILE, gaugeFindForm, mountGaugeFind } from "./gaugeFind";
import { gaugeSearchFile, type GaugeSearchFile, type LiveIndex, type SearchGauge } from "./gaugeSearch";

const G = (station: string, name: string, lat: number, lon: number, water: string, on: string[]): SearchGauge => ({ station, name, path: `/gauge/${station}/x`, lat, lon, water, on });
const GAUGES = [G("08AA001", "Alpha River at Town", 50, -120, "Alpha River", ["w:alpha"]), G("08AA002", "Alpha River at Mouth", 50.2, -120, "Alpha River", ["w:alpha"]),
                G("08BB001", "Beta Creek near Far", 51, -121, "Beta Creek", ["w:beta"])];
const FILE: GaugeSearchFile = gaugeSearchFile({
  waters: [{ item: "w:alpha", name: "Alpha River", kind: "river", size: null, areaHa: null, pieces: 3, regions: [], mus: [], near: null },
           { item: "w:beta", name: "Beta Creek", kind: "stream", size: null, areaHa: null, pieces: 1, regions: [], mus: [], near: null }],
  aliases: [], places: [{ place: "p1", name: "Townsville", kind: "town", lat: 50.01, lon: -120, pop: 500 }] }, GAUGES, "r-test");
const LIVE: LiveIndex = { fetchedAt: "2026-11-25T12:00:00Z", listed: ["08AA001", "08AA002"], stations: {
  "08AA001": { percentile: 50, observedAt: "2026-11-25T11:00:00Z" }, "08AA002": { percentile: null, observedAt: "2026-11-20T11:00:00Z" } } };
const cfg = { crash: "https://crash.test", env: "test", release: "r-test", live: "https://feed.test/index.json" };
const tick = () => new Promise((r) => setTimeout(r, 0));
const res = (body: unknown, status = 200) => Promise.resolve({ ok: status < 400, status, json: () => Promise.resolve(body) } as Response);

let reports: unknown[], hrefs: string[];
function mount(over: { file?: () => Promise<Response>; live?: () => Promise<Response> } = {}) {
  document.body.innerHTML = gaugeFindForm("/assets/gs.json", cfg);
  reports = [];
  const fetchImpl = ((u: string) => (u === cfg.live ? (over.live ?? (() => res(LIVE)))() : (over.file ?? (() => res(FILE)))())) as unknown as typeof fetch;
  mountGaugeFind(document, fetchImpl, () => (e) => { reports.push(e); }, { href: "https://x/gauges", reload: () => { hrefs.push("reload"); } });
}
const type = async (v: string) => { const q = document.querySelector<HTMLInputElement>("#gf-q")!; q.focus(); q.value = v; q.dispatchEvent(new Event("input")); await tick(); await tick(); await tick(); };
const said = () => document.querySelector(".said")!.textContent;
const rows = () => [...document.querySelectorAll(".gres li:not(.gsep)")].map((l) => l.textContent!.replace(/\s+/g, " ").trim());
beforeEach(() => { hrefs = []; });

describe("T1107 the /gauges search page", () => {
  it("a place: nearest gauges with the distance, and each says whether it has current data in the app's words", async () => {
    mount(); await type("Townsville");
    expect(document.querySelector(".gres .gh")!.textContent).toBe("Nearest gauges to Townsville");
    expect(rows()).toEqual(["Alpha River at Town 08AA001 · Alpha River · 1.1 km away · Reporting now",
                            "Alpha River at Mouth 08AA002 · Alpha River · 21 km away · Not reporting today",
                            "Beta Creek near Far 08BB001 · Beta Creek · 131 km away · Not a real-time gauge"]);
    expect(document.querySelector<HTMLAnchorElement>(".gres a")!.getAttribute("href")).toBe("/gauge/08AA001/x");
  });
  it("a river: its gauges, then the nearest others under a label", async () => {
    mount(); await type("Alpha");
    expect(document.querySelector(".gres .gh")!.textContent).toBe("Gauges on Alpha River");
    expect(rows()[0]).toContain("Alpha River at Mouth"); expect(rows().length).toBe(3);
    expect(document.querySelector(".gsep")!.textContent).toBe("Nearest others");
  });
  it("a station number: that gauge", async () => { mount(); await type("08BB001"); expect(rows().length).toBe(1); expect(rows()[0]).toContain("Beta Creek near Far"); });
  it("no match is said, never a blank", async () => { mount(); await type("Atlantis"); expect(said()).toBe(NO_MATCH("Atlantis")); expect(rows()).toEqual([]); });
  it("a gauge file that fails is said and reported; a 404 says reload and is reported as out of date", async () => {
    mount({ file: () => res({}, 500) }); await type("Alpha");
    expect(said()).toBe(LOAD_FAILED("gauge search file 500")); expect(reports.length).toBe(1);
    mount({ file: () => res({}, 404) }); await type("Alpha");
    expect(said()).toContain("out of date"); expect((reports[0] as Error).message).toBe(STALE_FILE);
  });
  it("a live index that cannot be read is said and reported, and the results still show (without the words)", async () => {
    mount({ live: () => res({}, 503) }); await type("Alpha");
    expect(said()).toBe(LIVE_FAILED); expect(reports.length).toBe(1); expect(rows().length).toBe(3);
    expect(rows().join()).not.toContain("Reporting");
  });
  it("the Conditions map link is the address the app itself writes for the Conditions tab, and it opens that tab", () => {
    const own = routeSearch(reduce(HOME, { type: "tab", tab: "conditions" }));
    expect(own).toBe("?browse=conditions");
    const body = gaugesIndexBody([], "2026-11-25", { index: "/i.json", script: "/s.js", cfg, conditions: `https://canifishthis.ca/${own}` });
    const href = /<a href="([^"]*)">open the Conditions map<\/a>/.exec(body)![1]!;
    expect(CONDITIONS_MAP_LINK).toBe("open the Conditions map");
    expect(href).toBe(`https://canifishthis.ca/${own}`);
    const start = parseStartLink(new URL(href).search)!;
    expect(start.route).toMatchObject({ mode: "browse", tab: "conditions" });
  });
});
