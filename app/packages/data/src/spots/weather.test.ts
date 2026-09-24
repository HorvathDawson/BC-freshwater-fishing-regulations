/**
 * Weather must be right about WHEN, and honest when it does not know.
 *
 * The failure this guards: fetching today's weather for a spot pinned last October. It
 * would look completely plausible on the record and be entirely wrong.
 */
import { describe, expect, it, vi } from "vitest";
import { openMeteo } from "./weather";
import { needsRefresh, refreshSpot } from "./refresh";
import type { Spot } from "./model";

const spot = (over: Partial<Spot> = {}): Spot => ({
  id: "s1", createdAt: Date.parse("2026-08-30T09:00:00Z"), visitedAt: Date.parse("2026-08-30T09:00:00Z"), updatedAt: 0,
  lat: 49.09, lon: -121.96, item: null, section: null, waterName: null,
  title: "t", notes: "", photos: [],
  reading: null, weather: null, trace: null, panel: null, ...over,
});

const ok = (body: unknown) =>
  vi.fn(async (_url: string) => ({ ok: true, json: async () => body }) as unknown as Response);

describe("openMeteo", () => {
  it("asks the ARCHIVE for a date in the past, not the forecast", async () => {
    const fetchImpl = ok({ hourly: { time: ["2026-08-30T09:00"], temperature_2m: [14.2] } });
    await openMeteo(fetchImpl as never).at(49, -122, new Date("2026-08-30T09:00:00Z"));
    const url = String(fetchImpl.mock.calls[0]![0]);
    expect(url).toContain("archive-api");
    expect(url).toContain("start_date=2026-08-30");
  });

  it("marks an archived reading as backfilled", async () => {
    const fetchImpl = ok({ hourly: { time: ["2026-08-30T09:00"], temperature_2m: [14.2] } });
    const w = await openMeteo(fetchImpl as never).at(49, -122, new Date("2026-08-30T09:00:00Z"));
    expect(w!.tempC).toBe(14.2);
    expect(w!.backfilled).toBe(true);   // honest about being fetched after the fact
  });

  it("picks the HOUR the spot was pinned out of the day", async () => {
    const fetchImpl = ok({ hourly: {
      time: ["2026-08-30T07:00", "2026-08-30T08:00", "2026-08-30T09:00"],
      temperature_2m: [9.1, 11.4, 14.2],
    } });
    const w = await openMeteo(fetchImpl as never).at(49, -122, new Date("2026-08-30T09:00:00Z"));
    expect(w!.tempC).toBe(14.2);
  });

  it("returns null when offline rather than a plausible temperature", async () => {
    const fetchImpl = vi.fn(async () => { throw new Error("offline"); });
    expect(await openMeteo(fetchImpl as never).at(49, -122, new Date())).toBeNull();
  });

  it("returns null on a bad response rather than a partial record", async () => {
    const fetchImpl = vi.fn(async () => ({ ok: false }) as unknown as Response);
    expect(await openMeteo(fetchImpl as never).at(49, -122, new Date())).toBeNull();
  });
});

describe("refresh", () => {
  it("flags a spot that never got its weather", () => {
    expect(needsRefresh(spot())).toBe(true);
    expect(needsRefresh(spot({ weather: { window: null, tempC: 12, windKph: null, windDir: null,
                                          rain3h: null, pressureHpa: null, code: null,
                                          at: null } }))).toBe(false);
  });

  it("fetches for the day the spot was PINNED, not today", async () => {
    const at = vi.fn(async (_lat: number, _lon: number, _when: Date) => null);
    await refreshSpot(spot(), { at });
    expect(at.mock.calls[0]![2]).toEqual(new Date(Date.parse("2026-08-30T09:00:00Z")));
  });

  it("leaves the record alone when the retry also fails", async () => {
    const s = spot();
    expect(await refreshSpot(s, { at: async () => null })).toBe(s);
  });

  it("does not re-ask for a spot that already has weather", async () => {
    const at = vi.fn(async (_lat: number, _lon: number, _when: Date) => null);
    const s = spot({ weather: { window: null, tempC: 1, windKph: null, windDir: null, rain3h: null,
                                pressureHpa: null, code: null, at: null } });
    expect(await refreshSpot(s, { at })).toBe(s);
    expect(at).not.toHaveBeenCalled();
  });
});

describe("the cloud window", () => {
  const HOURLY = {
    time: ["2026-08-30T07:00", "2026-08-30T08:00", "2026-08-30T09:00",
           "2026-08-30T10:00", "2026-08-30T11:00"],
    temperature_2m: [11, 12, 14, 16, 17],
    cloud_cover: [20, 35, 60, 85, 90],
    wind_speed_10m: [4, 5, 6, 7, 8], wind_direction_10m: [200, 205, 210, 215, 220],
    precipitation: [0, 0, 0, 0, 0], pressure_msl: [1014, 1014, 1013, 1013, 1012],
    relative_humidity_2m: [78, 74, 68, 61, 58],
    weather_code: [1, 2, 3, 3, 3],
  };
  const archive = (body: unknown) =>
    openMeteo((async () => ({ ok: true, json: async () => body })) as unknown as typeof fetch);

  // 2026-08-30 is far enough in the past for `isHistoric`, so this exercises the archive.
  const at9 = new Date("2026-08-30T09:00:00Z");

  it("records the hour either side, not just the hour", async () => {
    const w = await archive({ hourly: HOURLY }).at(49, -121, at9);
    expect(w!.window!.before!.cloudPct).toBe(35);
    expect(w!.window!.at!.cloudPct).toBe(60);
    expect(w!.window!.after!.cloudPct).toBe(85);
  });

  it("carries pressure, humidity and temperature at every hour, not just cloud", async () => {
    // The level of any one of these is far less use than its direction, and a barometer
    // that was only sampled once cannot have a direction.
    const w = await archive({ hourly: HOURLY }).at(49, -121, at9);
    for (const s of [w!.window!.before!, w!.window!.at!, w!.window!.after!]) {
      expect(s.pressureHpa).not.toBeNull();
      expect(s.humidityPct).not.toBeNull();
      expect(s.tempC).not.toBeNull();
      expect(s.at).not.toBeNull();
    }
  });

  it("takes the hour that was asked for, not the first hour of the day", async () => {
    // The bug this guards: `findIndex` returning -1 and being clamped to 0, which reports
    // midnight's sky for an afternoon visit.
    const w = await archive({ hourly: HOURLY }).at(49, -121, at9);
    expect(w!.tempC).toBe(14);
    expect(w!.at).toBe("2026-08-30T09:00");
  });

  it("refuses to answer at all when the day came back without that hour", async () => {
    const w = await archive({ hourly: { ...HOURLY, time: ["2026-08-30T02:00"] } })
      .at(49, -121, at9);
    expect(w).toBeNull();
  });

  it("leaves an edge of the window null rather than repeating its neighbour", async () => {
    const w = await archive({ hourly: HOURLY }).at(49, -121,
      new Date("2026-08-30T07:00:00Z"));
    expect(w!.window!.before).toBeNull();   // nothing before the first hour returned
    expect(w!.window!.at!.cloudPct).toBe(20);
    expect(w!.window!.after!.cloudPct).toBe(35);
  });

  it("marks an archive answer as backfilled", async () => {
    const w = await archive({ hourly: HOURLY }).at(49, -121, at9);
    expect(w!.backfilled).toBe(true);
  });
});
