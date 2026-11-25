/** (T1107) the site build writes the /gauges search: the form, the one-line lede, the Conditions-map link, the gauge file and the script. */
import { beforeAll, describe, expect, it } from "vitest";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll, type Built } from "./build";
import { TODAY } from "./fixture";
import { GAUGES_LEDE, type GaugeRows } from "./gauges";
import type { GaugeSearchFile } from "./gaugeSearch";

let built: Built, page: string;
beforeAll(async () => {
  const probe = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: null, bare: true });
  const ids = probe.build.models.slice(0, 2).map((m) => m.id);
  const rows: GaugeRows = { gauges: [{ station: "08MH001", name: "Chilliwack River at Vedder Crossing", item: ids[0]!, lat: 49.0973, lon: -121.9638 }],
                            serves: [{ station: "08MH001", item: ids[0]!, trust: "own" }] };
  const w = (item: string, name: string) => ({ item, name, kind: "stream", size: null, areaHa: null, pieces: 1, regions: [], mus: [], near: null });
  built = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: rows, gaugeClim: new Map(),
    gaugeSearch: { waters: [w(ids[0]!, "Chilliwack River"), w("x:unserved", "Unserved Creek")], aliases: [],
                   places: [{ place: "p", name: "Chilliwack", kind: "town", lat: 49.16, lon: -121.95, pop: 90000 }] } });
  page = built.files.get("gauges/index.html")!;
}, 300_000);

describe("T1107 the built /gauges page", () => {
  it("has the one-line lede, the search form, the Conditions-map link and the list", () => {
    expect(page).toContain(GAUGES_LEDE);
    expect(page).toContain('role="search" hidden');   // JavaScript off: the form stays hidden, the list alone
    expect(page).toContain('href="https://canifishthis.ca/?browse=conditions"');
    expect(page).toContain("/gauge/08MH001/");
    expect(page).toContain("River levels and flow in BC");
  });
  it("writes the gauge file (served waters + gauges + towns only) and the script the form names", () => {
    const idx = /data-index="([^"]+)"/.exec(page)![1]!, js = /<script src="(\/assets\/gauges-find[^"]+)" defer>/.exec(page)![1]!;
    const file = JSON.parse(built.files.get(idx.slice(1))!) as GaugeSearchFile;
    expect(file.index.waters.map((x) => x.name).sort()).toEqual(["Chilliwack River", "Chilliwack River at Vedder Crossing"]);
    expect(file.gauges.map((g) => g.station)).toEqual(["08MH001"]);
    expect(file.index.places.map((p) => p.name)).toEqual(["Chilliwack"]);
    expect(built.files.get(js.slice(1))).toContain("gauge-search/1");
    expect(built.problems).toEqual([]);
  });
});
