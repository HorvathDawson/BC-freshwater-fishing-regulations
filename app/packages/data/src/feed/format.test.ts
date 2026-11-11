/**
 * (T557 items 3-4) The gauge feed's format: the app refuses another one loudly (reported) and keeps what it holds.
 */
import { afterEach, describe, expect, it } from "vitest";
import { GAUGE_FEED_FORMAT, gaugeFeedRefusal, setFailureSink } from "@app/core";
import { fetchObs, httpFeed } from "./http";
import type { StationId } from "../index";

const S = "08MH001" as StationId;
const STATION = { format: GAUGE_FEED_FORMAT, fetchedAt: "2026-09-01T12:00:00Z", station: "08MH001",
                  now: { discharge: 15.7, level: 1.487, at: "2026-09-01T11:30:00Z", percentile: 0.038 } };
const INDEX = { format: GAUGE_FEED_FORMAT, fetchedAt: "2026-09-01T12:00:00Z",
                stations: { "08MH001": { percentile: 0.038, observedAt: "2026-09-01T11:30:00Z" } } };

let undo: (() => void) | null = null;
afterEach(() => { undo?.(); undo = null; });
const reports = () => {
  const got: string[] = [];
  undo = setFailureSink((e) => got.push(String(e)));
  return got;
};
const server = (files: Record<string, unknown>) => (async (url: string) => {
  const key = url.split("/").pop()!;
  return key in files ? { ok: true, json: async () => files[key] } : { ok: false, status: 404, json: async () => ({}) };
}) as unknown as typeof fetch;

describe("gaugeFeedRefusal", () => {
  it("reads this format", () => {
    expect(gaugeFeedRefusal(INDEX, "index")).toBeNull();
    expect(gaugeFeedRefusal(STATION, "station")).toBeNull();
    expect(gaugeFeedRefusal({ format: GAUGE_FEED_FORMAT, stations: {} }, "obs")).toBeNull();
  });
  it("refuses another format, NO format (T633 item 2: the transition is over), a file that is not the shape, an index with no time", () => {
    for (const what of ["index", "station", "obs"] as const)
      expect(gaugeFeedRefusal({ ...INDEX, ...STATION, format: undefined }, what))
        .toBe(`the gauge feed's ${what} file names no format; this app reads gauge-feed/1`);
    expect(gaugeFeedRefusal({ ...INDEX, format: "gauge-feed/2" }, "index"))
      .toBe('the gauge feed\'s index file is "gauge-feed/2"; this app reads gauge-feed/1');
    expect(gaugeFeedRefusal([], "index")).toBe("the gauge feed's index file is not a JSON object");
    expect(gaugeFeedRefusal({ format: GAUGE_FEED_FORMAT }, "obs")).toBe("the gauge feed's obs file has no stations");
    expect(gaugeFeedRefusal({ format: GAUGE_FEED_FORMAT, stations: {} }, "index")).toBe("the gauge feed's index has no fetchedAt");
    expect(gaugeFeedRefusal({ format: GAUGE_FEED_FORMAT, now: {} }, "station")).toBe("the gauge feed's station file names no station");
  });
});

describe("the http feed", () => {
  it("refuses an index in another format: reported, nothing held is replaced", async () => {
    const got = reports();
    const f = httpFeed("http://x/g", server({ "index.json": { ...INDEX, format: "gauge-feed/2" } }));
    expect(await f.index()).toBeNull();
    expect(got.some((s) => s.includes("gauge-feed/2"))).toBe(true);
  });

  it("refuses a station file in another format and reads one in this format", async () => {
    const got = reports();
    const bad = httpFeed("http://x/g", server({ "index.json": INDEX, "08MH001.json": { ...STATION, format: "x/9" } }));
    expect(await bad.now(S)).toBeNull();
    expect(got.some((s) => s.includes('"x/9"'))).toBe(true);
    const good = httpFeed("http://x/g", server({ "index.json": INDEX, "08MH001.json": STATION }));
    expect((await good.now(S))!.value.discharge).toBe(15.7);
  });

  it("offer() and fetchObs refuse another format the same way", async () => {
    const f = httpFeed("http://x/g", server({}));
    expect(() => f.offer!({ ...INDEX, format: "gauge-feed/2" })).toThrow(/gauge-feed\/2/);
    await expect(fetchObs("http://x/g", server({ "obs.json": { format: "gauge-feed/2", stations: {} } })))
      .rejects.toThrow(/gauge-feed\/2/);
  });
});

// (T585 F8) the card's rank is the index's, for the reading the index ranked
describe("now(): one rank for one reading", () => {
  const at = "2026-09-01T11:30:00Z";
  it("takes the index row's rank when the station file is the same reading", async () => {
    const idx = { ...INDEX, stations: { "08MH001": { percentile: 0.5, observedAt: at } } };
    const r = (await httpFeed("http://x/g", server({ "index.json": idx, "08MH001.json": STATION })).now(S))!;
    expect([r.value.percentile, r.value.rankedAt]).toEqual([0.5, undefined]);
  });
  it("gives a station file a run ahead of the index its figure and no rank, naming the reading the index ranked", async () => {
    const idx = { ...INDEX, stations: { "08MH001": { percentile: 0.5, observedAt: "2026-09-01T10:30:00Z" } } };
    const r = (await httpFeed("http://x/g", server({ "index.json": idx, "08MH001.json": STATION })).now(S))!;
    expect(r.value.discharge).toBe(15.7);
    expect([r.value.percentile, r.value.rankedAt]).toEqual([null, "2026-09-01T10:30:00Z"]);
  });
});
