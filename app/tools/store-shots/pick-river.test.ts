// (T319) the day's river: the pick is the clearest forecast swing, weighed by models, band, distance and name; same input, same pick
import { describe, expect, it } from "vitest";
import { rankRivers, scoreGauge } from "./pick-river.mjs";

const feed = (now: number, models: Record<string, [number, number]>, band = true) => ({
  now: { discharge: now, parameter: "discharge", at: "2026-11-19T10:00:00Z", ...(band ? { percentile: 0.4 } : {}) },
  // the drawn line: today's flow, then down to min, then up to max (hours after the reading at 10:00)
  forecasts: Object.fromEntries(Object.entries(models).map(([m, [min, max]]) => [m, { model: m,
    series: { at: ["2026-11-19T09:00:00Z", "2026-11-19T11:00:00Z", "2026-11-19T12:00:00Z", "2026-11-19T13:00:00Z"], mid: [999, now, min, max] } }])),
});
const g = (station: string, item_id: string | null, lat: number, lon: number, extra = {}) =>
  ({ station, name: `${station} River`, item_id, lat, lon, tidal: 0, backwater: 0, diversion: 0, ...extra });
// near Vancouver, the Okanagan (~300 km), the north (~1000 km)
const COQ = g("08MH002", "gnis:19986", 49.265, -122.78), VED = g("08MH001", "gnis:8634", 49.097, -121.967);
const OK = g("08NM050", "gnis:1", 49.88, -119.49), NORTH = g("08EB005", "gnis:2", 55.0, -127.3);

describe("pick-river (T319)", () => {
  it("picks the clearest forecast swing near the Lower Mainland, with every model and the band", () => {
    const feeds = {
      "08MH002": feed(5.5, { CLEVER: [5.4, 50], COFFEE: [5.4, 30], ELF: [5, 12] }),   // rises 8x (the 999 before the reading is not counted)
      "08MH001": feed(24.9, { CLEVER: [24, 26], COFFEE: [24, 27], ELF: [23, 26] }),    // flat
      "08NM050": feed(10, { CLEVER: [9, 90], COFFEE: [9, 40], ELF: [9, 30] }),        // rises as much (8x), but 300 km off
      "08EB005": feed(100, { CLEVER: [90, 900] }),                                    // north, one model
    };
    const { ranked } = rankRivers({ gauges: [VED, OK, NORTH, COQ], feeds });
    expect(ranked[0]).toMatchObject({ station: "08MH002", item_id: "gnis:19986", move: 8.090909, models: 1, band: 1, near: 1, named: 1, score: 8.090909 });
    expect(ranked.map((r) => r.station)).toEqual(["08MH002", "08NM050", "08EB005", "08MH001"]);
    // a second gauge on the Coquitlam, flat: the water is only as good as its weakest gauge (the card may lead with either)
    const COQ2 = g("08MH141", "gnis:19986", 49.35, -122.77);
    const two = rankRivers({ gauges: [VED, OK, NORTH, COQ, COQ2], feeds: { ...feeds, "08MH141": feed(3.5, { CLEVER: [3.4, 3.6], COFFEE: [3.4, 3.6], ELF: [3, 4] }) } });
    expect(two.ranked[0]).toMatchObject({ item_id: "gnis:1" });
    expect(two.ranked.find((r) => r.item_id === "gnis:19986")).toMatchObject({ station: "08MH141", stations: ["08MH002", "08MH141"] });
    // a water with a gauge the feed has no forecast for is out, said
    expect(rankRivers({ gauges: [COQ, COQ2], feeds: { "08MH002": feeds["08MH002"] } }).out[0]).toMatchObject({ item_id: "gnis:19986", out: "gauge 08MH141: no forecast in the feed" });
    // deterministic: the same inputs in another order give the same ranking
    expect(rankRivers({ gauges: [COQ, NORTH, OK, VED], feeds }).ranked.map((r) => r.station)).toEqual(ranked.map((r) => r.station));
  });
  it("breaks a tie by the lower item id", () => {
    const a = g("08MH005", "gnis:5", 49.3, -122.9), b = g("08MH004", "gnis:4", 49.3, -122.9);
    const f = feed(10, { CLEVER: [10, 20], COFFEE: [10, 20], ELF: [10, 20] });
    expect(rankRivers({ gauges: [a, b], feeds: { "08MH005": f, "08MH004": f } }).ranked.map((r) => r.item_id)).toEqual(["gnis:4", "gnis:5"]);
  });
  it("leaves out what a chart cannot show well, and says why", () => {
    const f = feed(10, { CLEVER: [10, 40] });
    expect(scoreGauge(g("X1", null, 49.3, -123), f).out).toMatch(/no water/);
    expect(scoreGauge(g("X2", "gnis:9", 49.3, -123, { tidal: 1 }), f).out).toMatch(/tidal/);
    expect(scoreGauge(g("X3", "gnis:9", 49.3, -123), feed(0.4, { CLEVER: [0.3, 4] })).out).toMatch(/under 1/);
    expect(scoreGauge(g("X4", "gnis:9", 49.3, -123), feed(10, { CLEVER: [10, 40] }, false)).out).toMatch(/no past-years band/);
    expect(scoreGauge(g("X5", "gnis:9", 49.3, -123), { now: { discharge: 10, percentile: 0.5 }, forecasts: {} }).out).toMatch(/no short-range/);
    expect(scoreGauge(g("X7", "gnis:9", 49.3, -123), feed(7.3, { CLEVER: [7, 11], ELF: [40, 6125] })).out).toMatch(/ELF line off/);
    expect(scoreGauge(g("X6", "wbk:9", 49.3, -123), f)).toMatchObject({ named: 0.5, band: 1, models: 0.333333 });
  });
});
