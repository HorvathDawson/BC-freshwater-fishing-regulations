/**
 * T294: a spot holds several visits. THE MIGRATION IS LOSSLESS — a spot saved before visits existed loads as a spot of
 * one visit with every byte as it was, and saves back unchanged; each visit reads as a spot and is written back alone;
 * a visit removed takes its photos; a refresh fills each visit as of its own day; a new visit is read the way a spot is.
 */
import { describe, expect, it, vi } from "vitest";
import { percentileInBand, type Band } from "@app/core";
import { openSpotsWith, type SpotsFs } from "./core";
import { fromDisk } from "./migrate";
import { captureSpot } from "./capture";
import { refreshSpot, needsRefresh, refreshReason } from "./refresh";
import { addVisit, captureVisit, firstVisitId, latestVisitId, onVisit, rateVisit, removeVisit, spotVisits,
         visitIdOf, visitSpot, withVisit } from "./visits";
import { allPhotos } from "./photos";
import { answerFrom } from "../panel";
import type { Aged, Panel, WaterSource, SectionId, Series, StationId } from "../index";
import type { Spot, Visit } from "./model";

function memFs(init: Record<string, string> = {}) {
  const files = new Map(Object.entries(init));
  const fs: SpotsFs & { files: Map<string, string> } = {
    files,
    exists: (n) => files.has(n),
    read: (n) => { const t = files.get(n); if (t === undefined) throw new Error(`no ${n}`); return t; },
    write: (n, t) => { files.set(n, t); },
    rename: (a, b) => { const t = files.get(a)!; files.delete(a); files.set(b, t); },
    remove: (n) => { files.delete(n); },
  };
  return fs;
}

const T = (d: number, h = 9) => new Date(2026, 9, d, h).getTime();
/** A spot exactly as an app before T294 wrote it: every field a full record carries, no rating, no visits. */
const OLD: Spot = {
  id: "s1", createdAt: T(12, 18), updatedAt: T(13), visitedAt: T(12, 8), lat: 49.0985, lon: -121.9585,
  item: "gnis:3062" as Spot["item"], waterName: "Vedder River", title: "Bridge pool", notes: "Two on a bead.",
  photos: ["photos/s1-1.jpg", "photos/s1-2-abc.jpg"],
  reading: { station: "08MH001", discharge: 41.2, level: null, parameter: "discharge", percentile: 0.42, at: new Date(T(12, 8)).toISOString() },
  weather: { tempC: 14, windKph: 9, windDir: 250, rain3h: 0, pressureHpa: 1012, code: 2, at: new Date(T(12, 8)).toISOString(), window: null },
  trace: { station: "08MH001", stationName: "Vedder", trust: "good", reaches: 3, metres: 1200, reachMagnitude: 4,
           gaugeMagnitude: 4, lon: -121.9, lat: 49.1 } as unknown as Spot["trace"],
  panel: null,
  leadUp: { weather: null, station: "08MH001", gauge: null },
};
const later = (id: string, visitedAt: number, over: Partial<Visit> = {}): Visit => ({
  id, createdAt: visitedAt, visitedAt, rating: null, notes: "", photos: [], reading: null, weather: null, trace: null,
  panel: null, leadUp: { weather: [], station: null, gauge: null }, ...over });

describe("the migration (on-device spots → one visit, losslessly)", () => {
  it("an old spot is ONE visit holding every field it had; saved back, its record is byte-for-byte what it was", async () => {
    const before = JSON.stringify([OLD]);
    const fs = memFs({ "s.json": before });
    const store = openSpotsWith(fs, "s.json");
    const [loaded] = await store.list();
    expect(loaded).toEqual(OLD);
    const visits = spotVisits(loaded!);
    expect(visits).toHaveLength(1);
    const { id, createdAt, visitedAt, notes, photos, reading, weather, trace, panel, leadUp } = OLD;
    expect(visits[0]).toEqual({ id, rating: null, createdAt, visitedAt, notes, photos, reading, weather, trace, panel, leadUp });
    // a write of the list (another spot saved) leaves the old one exactly as it was stored
    await store.put({ ...OLD, id: "s2" });
    const after = JSON.parse(fs.files.get("s.json")!) as Spot[];
    expect(JSON.stringify(after.find((s) => s.id === "s1"))).toBe(JSON.stringify(OLD));
  });

  it("the first visit read as a spot and written back unchanged is the same spot", () => {
    const v = visitSpot(OLD, firstVisitId(OLD));
    expect(v.visitId).toBe("s1");
    expect(JSON.stringify(withVisit(OLD, v.visitId, v))).toBe(JSON.stringify(OLD));
  });

  it("later visits are cleaned like the first (section handles dropped); a malformed list is not visits", () => {
    const raw = { ...OLD, visits: [{ ...later("v1", T(20)), section: 7, trace: { station: "x", path: [1, 2] } }, 3, { id: "no-date" }] };
    const s = fromDisk(JSON.parse(JSON.stringify(raw)));
    expect(s.visits).toHaveLength(1);
    expect(s.visits![0]).not.toHaveProperty("section");
    expect(s.visits![0]!.trace).toEqual({ station: "x", reaches: 2 });
    expect(fromDisk({ ...OLD, visits: "nope" })).not.toHaveProperty("visits");
  });
});

describe("visits on a spot", () => {
  const two = addVisit(OLD, later("v1", T(20), { notes: "Slow.", rating: 2, photos: ["photos/v1-1.jpg"] }), T(21));

  it("lists oldest first and opens on the latest; each reads as a spot with its own record and the spot's place", () => {
    expect(spotVisits(two).map((v) => v.id)).toEqual(["s1", "v1"]);
    expect(latestVisitId(two)).toBe("v1");
    const v = visitSpot(two, "v1");
    expect([v.visitId, v.id, v.title, v.lat, v.notes, v.rating, v.visitedAt, v.reading]).toEqual(
      ["v1", "s1", "Bridge pool", 49.0985, "Slow.", 2, T(20), null]);
    expect(v).not.toHaveProperty("visits");
    expect(v).not.toHaveProperty("dayFlow");                // the first visit's optional fields never leak through
    expect(visitIdOf(v)).toBe("v1");
    expect(visitSpot(two, "gone").visitId).toBe("v1");      // an unknown id reads as the latest
  });

  it("rates one visit, clears it with null, refuses a non-rating; the others are untouched", () => {
    const r = rateVisit(two, "s1", 5, T(22));
    expect([r.rating, r.visits![0]!.rating, r.updatedAt]).toEqual([5, 2, T(22)]);
    expect(rateVisit(r, "s1", null).rating).toBeNull();
    expect(rateVisit(two, "v1", 9).visits![0]!.rating).toBeNull();
  });

  it("writes an edit back to that visit alone", () => {
    const e = withVisit(two, "v1", { ...visitSpot(two, "v1"), notes: "Slow, then two late.", title: "ignored" });
    expect([e.notes, e.visits![0]!.notes, e.title]).toEqual(["Two on a bead.", "Slow, then two late.", "Bridge pool"]);
  });

  it("removes a later visit; removing the first promotes the next into the spot's fields; the last one stays", () => {
    expect(removeVisit(two, "v1", T(22))).not.toHaveProperty("visits.0");
    const p = removeVisit(two, "s1", T(22));
    expect([p.id, p.visitId, p.notes, p.rating, p.visitedAt, p.photos]).toEqual(["s1", "v1", "Slow.", 2, T(20), ["photos/v1-1.jpg"]]);
    expect(p).not.toHaveProperty("visits");
    expect(spotVisits(p).map((v) => v.id)).toEqual(["v1"]);
    expect(removeVisit(OLD, "s1")).toBe(OLD);
  });

  it("the store deletes the photos of a removed visit, and every visit's photos with the spot", async () => {
    const removed: string[] = [];
    const store = openSpotsWith(memFs(), "s.json", Date.now, { remove: async (r) => { removed.push(r); } });
    await store.put(two);
    expect(allPhotos(two)).toEqual(["photos/s1-1.jpg", "photos/s1-2-abc.jpg", "photos/v1-1.jpg"]);
    await store.put(removeVisit(two, "v1"));
    expect(removed).toEqual(["photos/v1-1.jpg"]);
    await store.remove("s1");
    expect(removed).toEqual(["photos/v1-1.jpg", "photos/s1-1.jpg", "photos/s1-2-abc.jpg"]);
  });

  it("onVisit changes one visit through a function written for a spot", async () => {
    const s = await onVisit(two, "v1", async (v) => ({ ...v, notes: "changed" }));
    expect([s.notes, s.visits![0]!.notes]).toEqual(["Two on a bead.", "changed"]);
    expect(await onVisit(two, "v1", async (v) => v)).toBe(two);
  });
});

describe("a refresh fills each visit as of its own day", () => {
  it("asks the weather for the later visit's own instant and writes it to that visit only", async () => {
    const s = addVisit({ ...OLD, leadUp: { weather: [], station: null, gauge: null } },
                       later("v1", T(20, 7), { weather: null }), T(21));
    expect(needsRefresh(s)).toBe(true);
    expect(needsRefresh(visitSpot(s, "s1"))).toBe(false);   // read as one visit: that visit's own state
    expect(refreshReason(visitSpot(s, "v1"))).toMatch(/No weather/);
    const asked: number[] = [];
    const got = await refreshSpot(s, { at: async (_a, _b, when) => { asked.push(when.getTime()); return { tempC: 3 } as never; } });
    expect(asked).toEqual([T(20, 7)]);
    expect([got.weather, got.visits![0]!.weather]).toEqual([OLD.weather, { tempC: 3 }]);
    expect(needsRefresh(got)).toBe(false);
  });
});

// ——— a new visit, read the way a spot is captured (fixtures as capture.tripday.test.ts) ———
const BAND: Band = [10, 20, 40, 60, 100];
const NOW = new Date(2026, 9, 23, 12).getTime();
function yearOf(values: Record<string, number>): Aged<Series> {
  const doy = (d: string) => Math.round((Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10))
    - Date.UTC(+d.slice(0, 4), 0, 1)) / 86_400_000);
  const v: (number | null)[] = Array.from({ length: 366 }, () => null);
  for (const [d, x] of Object.entries(values)) v[doy(d)] = x;
  return { fetchedAt: NOW, value: { step: "1d", from: "2026-01-01", parameter: "discharge", at: [], values: v,
                                    band: v.map(() => BAND), now: null, forecast: null, forecasts: [],
                                    priorYears: [] } as unknown as Series };
}
const PANEL: Panel = { areaKm2: 40, members: ["08MH001", "08MH999"].map((station) => (
  { station: station as StationId, role: "up" as const, areaKm2: 60, years: { discharge: 40, level: 40 },
    regulated: false, sameRiver: true })) };
const LIVE = { stations: { "08MH001": { percentile: 0.05, discharge: 0.05, parameter: "discharge" as const },
                           "08MH999": { percentile: 0.05, discharge: 0.05, parameter: "discharge" as const } } };
const DAYS: Record<string, Record<string, number>> = { "08MH001": { "2026-10-20": 30 }, "08MH999": { "2026-10-20": 60 } };
const gaugeNow = vi.fn(async () => ({ fetchedAt: NOW, value: { discharge: 11.2, level: 1.38, parameter: "discharge",
  percentile: 0.05, at: new Date(2026, 9, 23, 11, 20).toISOString() } }) as never);
const SRC = {
  panelsFor: async () => new Map([[1 as SectionId, PANEL]]), lakeStationsFor: async () => new Map(),
  gaugeForSection: async () => null, traceToGauge: async () => [],
  gaugeParameters: async () => ["discharge", "level"], gaugeSeries: async (st: string) => yearOf(DAYS[st] ?? {}), gaugeNow,
} as unknown as WaterSource;
const sky = { at: async () => ({ tempC: 9 }) as never, days: async () => null };

describe("captureVisit — a new visit to a saved spot", () => {
  const saved = () => captureSpot({ source: SRC, weather: sky, at: { lat: 49.1, lon: -121.96 }, item: null,
    section: 1 as SectionId, waterName: "Chilliwack River", title: "", now: NOW, visitedAt: NOW,
    feed: { index: async () => LIVE }, id: () => "s1" });

  it("today: the live reading and the panel of the spot's own gauges, through answerFrom", async () => {
    const spot = await saved();
    const v = await captureVisit(spot, { source: SRC, weather: sky, feed: { index: async () => LIVE }, now: NOW, id: () => "v1" });
    expect([v.id, v.rating, v.notes, v.photos, v.visitedAt, v.weather]).toEqual(["v1", null, "", [], NOW, { tempC: 9 }]);
    expect(v.reading).toMatchObject({ station: "08MH001", discharge: 11.2, percentile: 0.05 });
    expect(v.panel).toEqual(spot.panel);
    expect(v.panel).toEqual(answerFrom(PANEL, LIVE, "discharge"));
  });

  it("an earlier day: that day's record (Q161), never the live feed", async () => {
    const spot = await saved();
    const v = await captureVisit(spot, { source: SRC, weather: sky, feed: { index: async () => LIVE }, now: NOW,
                                         visitedAt: new Date(2026, 9, 20, 9, 30).getTime(), id: () => "v2" });
    expect(v.reading).toEqual({ station: "08MH001", discharge: null, level: null, percentile: null, at: null });
    expect(v.dayFlow).toMatchObject({ state: "filled", date: "2026-10-20", discharge: 30, percentile: percentileInBand(30, BAND) });
    expect(v.panelDay).toEqual({ state: "day", date: "2026-10-20" });
    expect(v.panel?.rows.map((r) => r.percentile)).toEqual([0.375, 0.75]);
  });
});
