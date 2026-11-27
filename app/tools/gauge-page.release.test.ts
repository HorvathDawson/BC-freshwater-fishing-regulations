/**
 * (T1194, acceptance 1, 2, 9) THE GAUGE PAGE ON THE LIVE RELEASE: every drawn gauge (a `gauge` row with a point) has a page of
 * its own station, the 7 tidal ones say so and the water-less ones serve nothing, and the counts are PINNED, so a re-cut that
 * loses a gauge, a page or a tidal flag is a red test (re-pin them, reading the list, when a release moves them on purpose).
 */
import { describe, expect, it } from "vitest";
import { readGaugePage, readGaugeRows } from "@app/data/release";
import { gaugeNotes, servedItems } from "@app/core";
import { gaugeGeoJSON } from "../packages/ui/src/gaugePoints";
import { queryGauge, type GaugeFeature } from "../packages/map/src/gauge-hit";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { liveReleaseAt, openReleaseDir } from "../conformance/src/fixtureRelease";

const at = liveReleaseAt();

it(`T1194 gauge pages on the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T1194 gauge pages skipped: ${at.reason}`);
});

/**
 * THE ROSTERS AT THE 0.10.0 CUT (release r-71d09ad82e81), re-measured: 430 gauges are drawn (a release row with a point AND in the
 * feed's index of 434: the index is live data, so the 430 itself is measured against it by hand, not pinned here), of which 8
 * serve no water and 7 are tidal. The 8 and the 7 are named and pinned below.
 */
const WATERLESS = ["07EF002", "07EF003", "08EC003", "08EC014", "08HB087", "08JB002", "08NH064", "08NH067"];
/** measured over the frozen index (see the block below); re-pin, reading the list, when the fixture or the release moves them on purpose */
const DRAWN_FLOW_ON_FROZEN_INDEX = 422;
const FIFTEEN_IN_INDEX: string[] = ["07EF002", "07EF003", "08EC003", "08EC014", "08HB087", "08JB002", "08MH024", "08MH028", "08MH032", "08MH035", "08MH044", "08MH053", "08MH126", "08NH064", "08NH067"];
const TIDAL = ["08MH024", "08MH028", "08MH032", "08MH035", "08MH044", "08MH053", "08MH126"];

describe.skipIf(!at.dir)("(T1194) a page for every drawn gauge", () => {
  it("every drawn gauge opens its own station; the counts hold; the tidal say the tide on Level; the water-less serve nothing", async () => {
    const loader = openReleaseDir(at.dir!);
    const rows = (await readGaugeRows(loader))!;
    const pointed = rows.gauges.filter((g) => g.lat !== null && g.lon !== null);
    const served = new Set(rows.serves.map((x) => x.station));
    // the 8 water-less drawn gauges still have a release row with a point and serve nothing; every other pointed gauge named here serves a water
    expect(pointed.filter((g) => WATERLESS.includes(g.station)).map((g) => g.station).sort()).toEqual(WATERLESS);
    expect(WATERLESS.filter((st) => served.has(st))).toEqual([]);
    const tidalRows = await (await loader.db("conditions")).all("SELECT station FROM gauge WHERE tidal = 1 AND lon IS NOT NULL AND lat IS NOT NULL ORDER BY station");
    expect(tidalRows.map((r) => String(r.station))).toEqual(TIDAL);
    expect(TIDAL.filter((st) => !served.has(st))).toEqual([]);        // all 7 tidal gauges serve waters
    // a page's waters are the waters the ALL-STATION reader says that station serves (two independent paths over the same release): a
    // spread of every pointed gauge. A page that named another station's waters, or lost one, fails here
    const bad: string[] = [];
    for (const g of pointed.filter((_, i) => i % 20 === 0)) {
      const p = await readGaugePage(loader, g.station);
      const want = [...servedItems(rows, g.station)].sort();
      if (!p || p.name !== g.name || JSON.stringify(p.serves.map((w) => w.item).sort()) !== JSON.stringify(want)) bad.push(g.station);
    }
    expect(bad).toEqual([]);
    for (const s of TIDAL) {
      const p = (await readGaugePage(loader, s))!;
      expect(p.tidal, s).toBe(true);
      expect(gaugeNotes(p, "level"), s).toHaveLength(1);
      expect(gaugeNotes(p, "discharge"), s).toEqual([]);          // the tide is a Level note only
    }
    for (const s of WATERLESS) expect((await readGaugePage(loader, s))!.serves, s).toEqual([]);
    expect(await readGaugePage(loader, "ZZZZZZZ")).toBeNull();
  }, 300_000);
});

/**
 * THE DOTS, AND THE TAP PATH (review 2026-11-27): the map's own dots (`gaugeGeoJSON`, the release's points over the feed's index) and the
 * tap's own question (`queryGauge`, the rule both engines run) — each of the 15 gauges the ticket calls unreachable by the water (the 7
 * tidal, the 8 water-less) is DRAWN, and a tap on its dot opens THAT station. The feed's index is the publisher's own, frozen
 * (conformance/src/fixtures/live: 2026-10-23, 432 stations): the live index of the day is not a test input, so the ticket's 430 (release
 * rows with a point that the 2026-10-28 index holds) is not re-measured here; the count over THIS frozen index is pinned instead.
 */
const INDEX = JSON.parse(readFileSync(join(__dirname, "../conformance/src/fixtures/live/feed-index-2026-10-23T0350Z.json"), "utf8"));
describe.skipIf(!at.dir)("(T1194) every dot, through the tap path", () => {
  it("the drawn dots are pinned; each tidal and water-less gauge in the index is one of them, and a tap on its dot opens that station", async () => {
    const loader = openReleaseDir(at.dir!);
    const rows = (await readGaugeRows(loader))!;
    const points = rows.gauges.filter((g) => g.lat !== null && g.lon !== null).map((g) => ({ station: g.station, name: g.name, lon: g.lon!, lat: g.lat!, mag: null }));
    const fc = JSON.parse(gaugeGeoJSON(points, INDEX, "flow", 0)!) as { features: (GaugeFeature & { geometry: { coordinates: [number, number] } })[] };
    const drawn = fc.features.map((f) => String(f.properties!.station));
    expect(new Set(drawn).size).toBe(drawn.length);
    expect(drawn.length).toBe(DRAWN_FLOW_ON_FROZEN_INDEX);
    const fifteen = [...TIDAL, ...WATERLESS].filter((st) => INDEX.stations[st]);
    expect(fifteen.sort()).toEqual(FIFTEEN_IN_INDEX);
    for (const st of fifteen) {
      const f = fc.features.find((x) => x.properties!.station === st);
      if (!f) continue;                                       // quiet in Flow (a level-only gauge): drawn under Level, checked below
      // the tap path: a fake screen with this dot at its place, the real hit rule
      const [lon, lat] = f.geometry.coordinates;
      const at2: [number, number] = [lon, lat];
      expect(await queryGauge((box, layer) => (layer === "gauge-dot" && box[0][0] <= 100 && box[1][0] >= 100 && box[0][1] <= 100 && box[1][1] >= 100 ? [f] : []), 100, 100, at2), st).toBe(st);
    }
    const level = JSON.parse(gaugeGeoJSON(points, INDEX, "level", 0)!) as { features: GaugeFeature[] };
    const drawnAny = new Set([...drawn, ...level.features.map((x) => String(x.properties!.station))]);
    expect(fifteen.filter((st) => !drawnAny.has(st))).toEqual([]);          // every one of the 15 has a dot under Flow or Level
  }, 300_000);
});
