/**
 * Capturing a spot must never invent what it does not know.
 *
 * A spot is a RECORD, so a wrong value written here is wrong forever — and worse, it looks
 * authoritative because it is dated. Every one of these asserts a null where a plausible
 * fake would be easy.
 */
import { describe, expect, it, vi } from "vitest";
import { captureSpot, noWeather } from "./capture";
import { isUntitled, spotLabel, type Spot } from "./model";
import type { RegsSource, SectionId, StationId } from "../index";

// One instant, and every date on the record is derived from it.
const VISITED = Date.parse("2026-08-30T09:00:00Z");

const source = (over: Partial<RegsSource> = {}): RegsSource => ({
  info: async () => ({ version: "t", validUntil: null }),
  itemForSection: async () => null,
  water: async () => null,
  searchNames: async () => [],
  searchPlaces: async () => [],
  watersNear: async () => [],
  locate: async () => ({ on: [], near: [] }),
  gaugeForSection: async () => null,
  panelsFor: async () => new Map(),
  lakeStationsFor: async () => new Map(),
  gaugeNow: async () => null,
  gaugeSeries: async () => null,
  traceToGauge: async () => [],
  lakeInfo: async () => null,
  stockingHistory: async () => [],
  ...over,
} as RegsSource);

const base = {
  at: { lat: 49.0974, lon: -121.9675 },
  item: null, section: 1 as SectionId, waterName: "Chilliwack River",
  title: "", now: VISITED, visitedAt: VISITED,
};

const PANEL_OF = (...stations: string[]) => ({ areaKm2: 40, members: stations.map((station) => (
  { station: station as never, role: "up" as const, areaKm2: 60, years: 40, regulated: false, sameRiver: true })) });
const MATCHED = async () => ({ station: "08MH001", name: "Vedder", trust: "good", reachMagnitude: 40,
                               gaugeMagnitude: 80, lon: -121.96, lat: 49.09 }) as never;
const NOW_READS = async (station: string) => ({ fetchedAt: 1, value: { discharge: station === "LAKE" ? null : 30,
  level: 2.5, parameter: station === "LAKE" ? "level" : "discharge", percentile: 0.4, at: "2026-08-30T08:00:00Z" } }) as never;

/*
 * ONE SPEAKER (T105 review): a spot freezes what the sheet showed — the station `sheetLead` picks from `speakerFor`
 * (the panel, else the station in the lake) — never the station `section_gauge` matched to the reach on its own.
 * Mutation: `readingFor(source, section)` reading `gaugeForSection(section).station` again fails "matched only";
 * dropping the `speaks` gate in `traceFor` fails "matched outside the panel".
 */
describe("captureSpot freezes the speaker's station (T105)", () => {
  const whole = (s: Awaited<ReturnType<typeof captureSpot>>) =>
    [s.reading?.station ?? null, s.reading?.discharge ?? null, s.trace?.station ?? null, s.leadUp?.station ?? null];

  it("matched only (no panel, no lake): no reading, no trace, no lead-up station — as the sheet refuses", async () => {
    const gaugeNow = vi.fn(NOW_READS);
    const s = await captureSpot({ ...base, source: source({ gaugeForSection: MATCHED, gaugeNow }) });
    expect([whole(s), gaugeNow.mock.calls.length]).toEqual([[null, null, null, null], 0]);
  });

  it("a panel: its first member offline, and the match traced only when it is a member", async () => {
    const inPanel = await captureSpot({ ...base, source: source({ gaugeForSection: MATCHED, gaugeNow: NOW_READS,
      panelsFor: async () => new Map([[1 as SectionId, PANEL_OF("08MH001", "08MH999")]]) }) });
    const outside = await captureSpot({ ...base, source: source({ gaugeForSection: MATCHED, gaugeNow: NOW_READS,
      panelsFor: async () => new Map([[1 as SectionId, PANEL_OF("08MF005")]]) }) });
    expect([whole(inPanel), whole(outside)]).toEqual([
      ["08MH001", 30, "08MH001", "08MH001"],
      ["08MF005", 30, null, "08MF005"],
    ]);
  });

  it("a gauged lake: its own station's reading", async () => {
    const s = await captureSpot({ ...base, source: source({ gaugeNow: NOW_READS,
      lakeStationsFor: async () => new Map([[1 as SectionId, "LAKE" as StationId]]) }) });
    expect(whole(s)).toEqual(["LAKE", null, null, "LAKE"]);
  });
});

describe("captureSpot", () => {
  it("records no reading when no gauge may speak for the water", async () => {
    const s = await captureSpot({ source: source(), ...base });
    expect(s.reading).toBeNull();      // not a zero, not an empty object
  });

  it("refuses a reading when no gauge is entitled to speak for the reach", async () => {
    // Nothing speaks for the reach — no panel, no station in its lake — so there is no station to read. Freezing a
    // number into a permanent record on that basis is worse than showing it once on a screen.
    const s = await captureSpot({
      ...base,
      source: source({
        gaugeForSection: async () => null,
        gaugeNow: async () => ({ value: { discharge: 2400, level: 8.1, at: "x" },
                                 fetchedAt: 1 } as never),
      }),
    });
    expect(s.reading).toBeNull();
    expect(s.trace!.station).toBeNull();
  });

  it("records no weather when nothing is wired, rather than a temperature of zero", async () => {
    const s = await captureSpot({ source: source(), weather: noWeather, ...base });
    expect(s.weather).toBeNull();
  });

  it("always stores where it is, even with no water attached", async () => {
    // 97.6% of BC's water carries no registry item, so a spot with no item is ORDINARY.
    const s = await captureSpot({ ...base, source: source(), item: null, section: null });
    expect(s.lat).toBe(49.0974);
    expect(s.lon).toBe(-121.9675);
    expect(s.item).toBeNull();
    expect(s.reading).toBeNull();
  });

  it("keeps the name the water had when it was pinned", async () => {
    const s = await captureSpot({ source: source(), ...base });
    expect(s.waterName).toBe("Chilliwack River");
    expect(s.title).toBe("Chilliwack River");
  });

  it("stores no section id anywhere — a coordinate and the item only (AGENTS 5)", async () => {
    // section_id survives a rebuild only 94% of the time and must never leave the bundle;
    // a saved spot is a file a person keeps. The trace keeps its facts and its reach COUNT.
    const s = await captureSpot({
      ...base, item: "it_42" as never,
      source: source({
        panelsFor: async () => new Map([[1 as SectionId, PANEL_OF("08MH001")]]),
        gaugeForSection: async () => ({ station: "08MH001", name: "Vedder", trust: "good",
          reachMagnitude: 40, gaugeMagnitude: 80, lon: -121.96, lat: 49.09 }) as never,
        traceToGauge: async () => [101, 102, 103] as never,
      }),
    });
    const stored = JSON.parse(JSON.stringify(s));
    expect("section" in stored).toBe(false);
    expect(stored.item).toBe("it_42");
    expect(stored.trace).toEqual({
      station: "08MH001", stationName: "Vedder", trust: "good", reaches: 3, metres: null,
      reachMagnitude: 40, gaugeMagnitude: 80, lon: -121.96, lat: 49.09,
    });
    expect(JSON.stringify(s)).not.toMatch(/\b10[123]\b/);
  });
});

describe("the visit date", () => {
  it("fetches weather for the DAY AT THE WATER, not the day it was typed up", async () => {
    // The whole reason the capture flow asks. A spot entered on the Tuesday drive home for
    // a Sunday must not carry Tuesday's sky.
    const seen: Date[] = [];
    await captureSpot({
      ...base, source: source(), now: Date.parse("2026-09-01T20:00:00Z"),
      visitedAt: Date.parse("2026-08-30T09:00:00Z"),
      weather: { at: async (_la, _lo, when) => { seen.push(when); return null; } },
    });
    expect(seen[0]!.toISOString()).toBe("2026-08-30T09:00:00.000Z");
  });

  it("keeps the visit and the record as separate instants", async () => {
    const s = await captureSpot({
      ...base, source: source(), now: Date.parse("2026-09-01T20:00:00Z"),
      visitedAt: Date.parse("2026-08-30T09:00:00Z"),
    });
    expect(s.createdAt).toBe(Date.parse("2026-09-01T20:00:00Z"));
    expect(s.visitedAt).toBe(Date.parse("2026-08-30T09:00:00Z"));
  });

  it("defaults the visit to now, because that is the ordinary case", async () => {
    const s = await captureSpot({ ...base, source: source(), visitedAt: undefined });
    expect(s.visitedAt).toBe(s.createdAt);
  });
});

describe("spotLabel", () => {
  const s = (over: Partial<Spot>) =>
    ({ title: "", waterName: null, lat: 49.0974, lon: -121.9675, ...over }) as Spot;

  it("prefers the name a person gave it", () => {
    expect(spotLabel(s({ title: "Tamihi run", waterName: "Chilliwack River" })))
      .toBe("Tamihi run");
  });

  it("uses the water before the coordinates, because it means more", () => {
    expect(spotLabel(s({ waterName: "Chilliwack River" }))).toBe("Chilliwack River");
  });

  it("uses coordinates on unnamed water, which is 97.6% of BC", () => {
    expect(spotLabel(s({}))).toBe("49.0974, -121.9675");
  });

  it("treats whitespace as no title, so a stray space is not a name", () => {
    expect(spotLabel(s({ title: "   ", waterName: "Chilliwack River" })))
      .toBe("Chilliwack River");
    expect(isUntitled(s({ title: "   " }))).toBe(true);
  });

  it("captures an untitled spot as untitled, not as 'Untitled spot'", async () => {
    const c = await captureSpot({ ...base, source: source(), title: "", waterName: null });
    expect(c.title).toBe("");
  });
});
