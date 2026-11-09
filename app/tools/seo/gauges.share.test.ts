/**
 * (T530) The app's Share on a gauge and the SEO build name a gauge page with ONE function (`@app/core` `gaugePath` /
 * `gaugeUrl`): every page the build lists is the address the app sends, and a station the build lists no page for gets none.
 */
import { describe, expect, it } from "vitest";
import { gaugeHasPage, gaugePath, gaugeUrl, SITE_ORIGIN } from "@app/core";
import { gaugeModels, stationsFile, type GaugeRows } from "./gauges";

const rows: GaugeRows = {
  gauges: [
    { station: "08MH001", name: "Chilliwack River at Vedder Crossing", item: "gnis:1", lat: null, lon: null },
    { station: "08GA022", name: "Écho Creek / O'Brien's Fork", item: null, lat: null, lon: null },
    { station: "08ZZ001", name: "Nowhere Creek near Nothing", item: "gnis:999", lat: null, lon: null },
    { station: "08XX002", name: "!!!", item: "gnis:1", lat: null, lon: null },
  ],
  serves: [
    { station: "08MH001", item: "gnis:1", trust: "own" }, { station: "08GA022", item: "gnis:1", trust: "weak" },
    { station: "08ZZ001", item: "gnis:999", trust: "own" }, { station: "08XX002", item: "gnis:1", trust: "lake" },
  ],
};
const WITH_PAGE = new Set(["gnis:1"]);
const models = gaugeModels(rows, (item) => (WITH_PAGE.has(item) ? { id: item, name: item, path: `/water/${item}` } : null));

describe("(T530) the app's gauge link is the SEO build's page", () => {
  it("every page the build lists (path, /gauge/stations.json) is the path and URL the app makes", () => {
    const listed = (JSON.parse(stationsFile(models, "r1")) as { stations: { station: string; name: string; path: string }[] }).stations;
    expect(listed.map((s) => s.station).sort()).toEqual(["08GA022", "08MH001", "08XX002"]);
    for (const s of listed) {
      expect(gaugePath(s.station, s.name)).toBe(s.path);
      expect(gaugeUrl(s.station, s.name)).toBe(`${SITE_ORIGIN}${s.path}`);
    }
    expect(listed.map((s) => s.path).sort()).toEqual(["/gauge/08GA022/echo-creek-obriens-fork", "/gauge/08MH001/chilliwack-river-at-vedder-crossing", "/gauge/08XX002/gauge"]);
  });

  it("a station that serves no water with a page is listed nowhere, and the app's rule gives it no link", () => {
    expect(models.some((m) => m.station === "08ZZ001")).toBe(false);
    expect(gaugeHasPage(0)).toBe(false);
    expect(gaugeHasPage(1)).toBe(true);
  });
});
