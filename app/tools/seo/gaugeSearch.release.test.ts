/**
 * (T1107) THE /gauges SEARCH ON THE REAL RELEASE: the three kinds of query the owner named, proved on the live release's own gauges,
 * waters and towns (read as the site build reads them), and the current-data words on the feed's own rule.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { openReleaseDir } from "@app/data/release/node";
import { makeReleaseSource } from "@app/data/release";
import { liveRelease } from "../../conformance/src/fixtureRelease";
import { gaugeModels, readGaugeRows, searchGauges } from "./gauges";
import { answerGauges, dataStateOf, gaugeFinder, gaugeSearchFile, type GaugeFinder, type LiveIndex } from "./gaugeSearch";

const LIVE = liveRelease();
describe.skipIf(!LIVE)(`T1107: the /gauges search on the release (${LIVE ?? "none"})`, () => {
  let f: GaugeFinder;
  beforeAll(async () => {
    const loader = openReleaseDir(LIVE!);
    const rows = (await readGaugeRows(loader))!;
    const full = await makeReleaseSource(loader).searchIndex();
    const byId = new Map(full.waters.map((w) => [w.item, w]));
    const models = gaugeModels(rows, (i) => { const w = byId.get(i); return w ? { id: i, name: w.name, path: `/water/${i}` } : null; });
    f = gaugeFinder(gaugeSearchFile(full, searchGauges(models), "test"));
  }, 120_000);

  it("a river: its own gauges first (no distance), then the nearest others, nearest first", () => {
    const g = answerGauges(f, "Chilliwack River").groups.find((x) => x.kind === "water" && x.title === "Chilliwack River")!;
    expect(g).toBeTruthy();
    const on = g.hits.filter((h) => h.km === null).map((h) => h.station).sort();
    expect(on).toEqual(["08MH001", "08MH016", "08MH103"]);
    const firstOther = g.hits.findIndex((h) => h.km !== null);
    expect(g.hits.slice(0, firstOther).every((h) => h.km === null)).toBe(true);
    const others = g.hits.slice(firstOther);
    expect(others.length).toBeGreaterThan(0);
    expect(others.map((h) => h.km!)).toEqual([...others.map((h) => h.km!)].sort((a, b) => a - b));
    expect(others.some((h) => on.includes(h.station))).toBe(false);
  });

  it("a place: the gauges nearest it, with the distance, nearest first (Smithers: the Bulkley's gauge in town)", () => {
    const g = answerGauges(f, "Smithers").groups.find((x) => x.kind === "place" && x.title === "Smithers")!;
    expect(g.hits[0]!.station).toBe("08EE005");
    expect(g.hits[0]!.km!).toBeGreaterThan(1); expect(g.hits[0]!.km!).toBeLessThan(6);
    const km = g.hits.map((h) => h.km!);
    expect(km).toEqual([...km].sort((a, b) => a - b));
    expect(g.hits.length).toBeLessThanOrEqual(6);
  });

  it("a station number: that gauge alone; a station name finds it too", () => {
    const a = answerGauges(f, "08MH001");
    expect(a.groups.map((g) => [g.kind, g.hits.map((h) => h.station)])).toEqual([["station", ["08MH001"]]]);
    const b = answerGauges(f, "Vedder Crossing");
    expect(b.groups.flatMap((g) => g.hits.map((h) => h.station))).toContain("08MH001");
  });

  it("nothing found is no group (the page says so); and only gauges ever come back, each a real gauge of the file", () => {
    expect(answerGauges(f, "Atlantis").groups).toEqual([]);
    const known = new Set(f.file.gauges.map((g) => g.station));
    for (const q of ["Chilliwack River", "Smithers", "08MH001", "Fraser", "Kamloops", "Vedder"])
      for (const g of answerGauges(f, q).groups) for (const h of g.hits) {
        expect(known.has(h.station), `${q}: ${h.station}`).toBe(true);
        expect(h.path.startsWith("/gauge/"), `${q}: ${h.path}`).toBe(true);
      }
  });

  it("current data, on the app's own rule: a recent reading reports, an old one is quiet, an unlisted station is not real-time", () => {
    const fetchedAt = "2026-11-25T12:00:00Z", at = (h: number) => new Date(Date.parse(fetchedAt) - h * 3600_000).toISOString();
    const live: LiveIndex = { fetchedAt, listed: ["08EE005", "08EE012", "08EE004"], stations: {
      "08EE005": { percentile: 40, observedAt: at(1) },     // ranked, recent
      "08EE012": { percentile: null, observedAt: at(80) },  // listed, nothing recent
      "08EF006": { percentile: null, observedAt: at(1) },   // not on the real-time list
    } };
    expect(dataStateOf("08EE005", live)).toBe("reporting");
    expect(dataStateOf("08EE012", live)).toBe("quiet");
    expect(dataStateOf("08EE004", live)).toBe("quiet");        // listed, absent from the index entries
    expect(dataStateOf("08EF006", live)).toBe("not-realtime");
  });
});
