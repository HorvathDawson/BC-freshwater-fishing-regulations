/**
 * (T530) The app's Share on a gauge and the SEO build name AND LIST a gauge page with ONE rule (core `gaugePath`/`gaugeUrl`,
 * `gaugePageStations` over data's `waterHasPage`): over the fixture release, the set of stations the app offers a link for is
 * the build's page list, and every link is the build's page path.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { gaugePageStations, gaugePath, gaugeUrl, servedItems, SITE_ORIGIN, type GaugeRows } from "@app/core";
import { openRegsStore, waterHasPage } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll, type Built } from "./build";
import { TODAY } from "./fixture";

let built: Built, rows: GaugeRows, ids: string[], pageIds: Set<string>;
beforeAll(async () => {
  const loader = openRegsFixture();
  const probe = await buildAll({ loader, on: TODAY, gauges: null });
  ids = probe.build.models.slice(0, 4).map((m) => m.id);
  rows = {
    gauges: [
      { station: "08MH001", name: "Chilliwack River at Vedder Crossing", item: ids[0]!, lat: null, lon: null },
      { station: "08GA022", name: "Écho Creek / O'Brien's Fork", item: null, lat: null, lon: null },
      { station: "08ZZ001", name: "Nowhere Creek near Nothing", item: "gnis:999999999", lat: null, lon: null },   // a water not in the book
      { station: "08XX002", name: "!!!", item: null, lat: null, lon: null },
      { station: "08NO000", name: "Serves nothing", item: null, lat: null, lon: null },
    ],
    serves: [
      { station: "08MH001", item: ids[0]!, trust: "own" }, { station: "08MH001", item: ids[1]!, trust: "fair" },
      { station: "08GA022", item: "gnis:999999999", trust: "weak" }, { station: "08GA022", item: ids[2]!, trust: "weak" },
      { station: "08ZZ001", item: "gnis:999999999", trust: "own" }, { station: "08XX002", item: ids[3]!, trust: "lake" },
    ],
  };
  built = await buildAll({ loader, on: TODAY, gauges: rows });
  // the app's side: ask the one rule of the same release's store, for the waters the stations serve
  const store = await openRegsStore(loader);
  pageIds = new Set();
  for (const item of new Set(rows.serves.map((s) => s.item))) if (await waterHasPage(store, item, TODAY)) pageIds.add(item);
}, 300_000);

describe("(T530) the app's gauge link is the SEO build's page", () => {
  it("the stations the app offers a link for are exactly the pages the build lists", () => {
    const pages = built.build.pages.filter((p) => p.kind === "gauge").map((p) => p.path).sort();
    const app = [...gaugePageStations(rows, (i) => pageIds.has(i))].sort();
    expect(app).toEqual(["08GA022", "08MH001", "08XX002"]);
    // the app's one-station check (servedItems, best first) agrees station by station
    for (const g of rows.gauges) {
      const one = servedItems(rows, g.station).some((i) => pageIds.has(i));
      expect(one).toBe(app.includes(g.station));
    }
    expect(app.map((st) => gaugePath(st, rows.gauges.find((g) => g.station === st)!.name)).sort()).toEqual(pages);
    for (const st of app) expect(gaugeUrl(st, rows.gauges.find((g) => g.station === st)!.name)).toMatch(new RegExp(`^${SITE_ORIGIN}/gauge/${st}/`));
  });

  it("a station that serves no water with a page is neither listed nor offered", () => {
    expect(built.build.pages.some((p) => p.path.includes("08ZZ001") || p.path.includes("08NO000"))).toBe(false);
    expect(gaugePageStations(rows, (i) => pageIds.has(i)).has("08ZZ001")).toBe(false);
  });
});
