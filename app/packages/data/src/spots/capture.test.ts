/**
 * Capturing a spot must never invent what it does not know.
 *
 * A spot is a RECORD, so a wrong value written here is wrong forever — and worse, it looks
 * authoritative because it is dated. Every one of these asserts a null where a plausible
 * fake would be easy.
 */
import { describe, expect, it } from "vitest";
import { captureSpot, noWeather } from "./capture";
import { isUntitled, spotLabel, type Spot } from "./model";
import type { RegsSource, SectionId } from "../index";

// One instant, and every date on the record is derived from it.
const VISITED = Date.parse("2026-08-30T09:00:00Z");

const source = (over: Partial<RegsSource> = {}): RegsSource => ({
  info: async () => ({ version: "t", validUntil: null }),
  itemExists: async () => true,
  itemForSection: async () => null,
  regsForItem: async () => null,
  statusFor: async () => new Map(),
  searchNames: async () => [],
  searchPlaces: async () => [],
  watersNear: async () => [],
  gaugeForSection: async () => null,
  gaugeNow: async () => null,
  gaugeSeries: async () => null,
  traceToGauge: async () => [],
  lakeInfo: async () => null,
  stockingHistory: async () => [],
  ...over,
} as RegsSource);

const base = {
  at: { lat: 49.0974, lon: -121.9675 },
  item: null, section: "380887781:0" as SectionId, waterName: "Chilliwack River",
  group: "provincial" as const, title: "", now: VISITED, visitedAt: VISITED,
};

describe("captureSpot", () => {
  it("records no reading when no gauge may speak for the water", async () => {
    const s = await captureSpot({ source: source(), ...base });
    expect(s.reading).toBeNull();      // not a zero, not an empty object
  });

  it("refuses a reading from a gauge whose trust is none", async () => {
    // `none` means the station drains far too much to describe this water. Freezing its
    // number into a permanent record is worse than showing it once on a screen.
    const s = await captureSpot({
      ...base,
      source: source({
        gaugeForSection: async () => ({ station: "08MF005", name: "Fraser at Hope",
                                        trust: "none", reachMagnitude: 12,
                                        gaugeMagnitude: 273576 } as never),
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

  it("stores the DATE with the regulation, because half of them are seasonal", async () => {
    const s = await captureSpot({
      ...base,
      source: source({
        statusFor: async (ids) => new Map(ids.map((i) => [i, {
          outcome: "closed" as const, provenance: "specific" as const, from: [],
        }])),
      }),
    });
    expect(s.regulation).toEqual({ outcome: "closed", provenance: "specific",
                                   on: "2026-08-30" });
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

  it("does not store a section id as an identity", async () => {
    // section_id survives a rebuild only 94% of the time. It is kept to replay the trace
    // and nothing may look a spot up by it — `item` is the durable id.
    const s = await captureSpot({ source: source(), ...base });
    expect(s.section).toBe("380887781:0");
    expect(s.item).toBeNull();
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

  it("dates the regulation by the visit too, since half of them are seasonal", async () => {
    const s = await captureSpot({
      ...base, now: Date.parse("2026-09-01T20:00:00Z"),
      visitedAt: Date.parse("2026-06-15T07:00:00Z"),
      source: source({
        statusFor: async (ids) => new Map(ids.map((i) => [i, {
          outcome: "closed" as const, provenance: "specific" as const, from: [],
        }])),
      }),
    });
    // June, not September — a closure that lifted in July must still show for a June visit.
    expect(s.regulation!.on).toBe("2026-06-15");
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
