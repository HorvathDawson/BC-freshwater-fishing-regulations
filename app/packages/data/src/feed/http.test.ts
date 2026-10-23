/**
 * The HTTP gauge feed.
 *
 * Every test here is really "what happens when the network is not there", because for this
 * app that is the ordinary case rather than the exception. A rejected promise on a river
 * bank turns into an error screen where "we could not check" is the truth.
 */
import { describe, expect, it, vi } from "vitest";
import { httpFeed, lastReportAt } from "./http";
import { seriesOf } from "../bundle/shared";
import type { StationId } from "../index";

const S = "08MH001" as StationId;

const server = (files: Record<string, unknown>, spy?: (u: string) => void) =>
  (async (url: string) => {
    spy?.(url);
    const key = url.split("/").pop()!;
    return key in files
      ? { ok: true, json: async () => files[key] }
      : { ok: false, status: 404, json: async () => ({}) };
  }) as unknown as typeof fetch;

const STATION = {
  fetchedAt: "2026-09-01T12:00:00Z", station: "08MH001",
  now: { discharge: 15.7, level: 1.487, at: "2026-09-01T11:30:00Z", percentile: 0.038 },
  recent: [["2026-09-01T10:00:00Z", 1.48, 15.9],
           ["2026-09-01T11:00:00Z", 1.49, 15.7]],
};
const INDEX = {
  fetchedAt: "2026-09-01T12:00:00Z",
  stations: { "08MH001": { percentile: 0.038, observedAt: "2026-09-01T11:30:00Z",
                           forecast: [0.04, 0.05, 0.07] } },
};

describe("httpFeed", () => {
  it("reads a reading addressed by station id alone", async () => {
    const f = httpFeed("http://x/feeds/gauge", server({ "08MH001.json": STATION }));
    const got = await f.now(S);
    expect(got!.value.discharge).toBe(15.7);
    expect(got!.value.percentile).toBe(0.038);
  });

  it("carries the declared quantity on the reading, and the series follows it — never the value present (T106)",
     async () => {
    // 08ME030 as the feed published it 2026-10-22: both quantities, the percentile about the level.
    const both = { ...STATION, station: "08ME030",
                   now: { discharge: 5.46, level: 4.162, at: "2026-10-22T18:40:00-08:00", percentile: 0.432,
                          parameter: "level" } };
    const f = httpFeed("http://x/feeds/gauge", server({ "08MH001.json": both }));
    expect((await f.now(S))!.value).toEqual({ discharge: 5.46, level: 4.162, at: "2026-10-22T18:40:00-08:00",
                                              percentile: 0.432, standing: "normal", parameter: "level" });
    expect((await f.observations(S))!.parameter).toBe("level");
    // A file from before the publisher declared one: null on the reading; the series reads core's default
    // (discharge) even where only a level was present — not a guess from which value is there.
    const old = { ...STATION, now: { discharge: null, level: 1.487, at: "2026-09-01T11:30:00Z", percentile: null } };
    const g = httpFeed("http://x/feeds/gauge", server({ "08MH001.json": old }));
    expect((await g.now(S))!.value.parameter).toBeNull();
    expect((await g.observations(S))!.parameter).toBe("discharge");
  });

  it("asks for exactly {base}/{station}.json — no query string, so an edge can cache it",
     async () => {
    const seen: string[] = [];
    const f = httpFeed("http://x/feeds/gauge", server({ "08MH001.json": STATION },
                                                      (u) => seen.push(u)));
    await f.now(S);
    // The index comes first — it says whether there is a file to ask for. Both URLs are
    // plain paths: no query string, so an edge can cache either.
    expect(seen).toEqual(["http://x/feeds/gauge/index.json",
                          "http://x/feeds/gauge/08MH001.json"]);
  });

  it("(the sync icon) refresh forgets the cache: the next index ask goes to the network again", async () => {
    const seen: string[] = [];
    const f = httpFeed("http://x/feeds/gauge", server({ "index.json": INDEX }, (u) => seen.push(u)));
    await f.index();
    await f.index();                 // cached: no second request
    f.refresh!();
    await f.index();
    expect(seen).toEqual(["http://x/feeds/gauge/index.json", "http://x/feeds/gauge/index.json"]);
  });

  it("does not ask for a station the index does not list", async () => {
    /*
     * The bundle and the feed are built separately, so the bundle can name a station the
     * publisher never wrote — retired between builds, or short of the record threshold.
     * Every view of such a water fired a request that could only 404.
     */
    const seen: string[] = [];
    const f = httpFeed("http://x", server({ "index.json": INDEX }, (u) => seen.push(u)));
    await expect(f.now("08PA001" as StationId)).resolves.toBeNull();
    expect(seen).toEqual(["http://x/index.json"]);
  });

  it("still asks when the index itself could not be loaded", async () => {
    // A failed index must not read as "no station has data" — that turns one bad request
    // into a total outage.
    const seen: string[] = [];
    const f = httpFeed("http://x", server({ "08MH001.json": STATION }, (u) => seen.push(u)));
    expect((await f.now(S))!.value.discharge).toBe(15.7);
    expect(seen).toContain("http://x/08MH001.json");
  });

  it("returns null offline rather than throwing", async () => {
    const dead = (async () => { throw new Error("offline"); }) as unknown as typeof fetch;
    const f = httpFeed("http://x", dead);
    await expect(f.now(S)).resolves.toBeNull();
    await expect(f.live()).resolves.toBeNull();
    await expect(f.observations(S)).resolves.toBeNull();
  });

  it("treats a 404 as 'not transmitting', not as an error", async () => {
    const f = httpFeed("http://x", server({}));
    expect(await f.now(S)).toBeNull();
  });

  it("distinguishes 'could not check' from 'nobody is reporting'", async () => {
    // The whole reason `live` is nullable. An offline reader must not be told the
    // province's gauges have all shut down.
    const dead = (async () => { throw new Error("offline"); }) as unknown as typeof fetch;
    expect(await httpFeed("http://x", dead).live()).toBeNull();

    const empty = { fetchedAt: "t", stations: {} };
    expect(await httpFeed("http://x", server({ "index.json": empty })).live())
      .toEqual(new Set());
  });

  it("says which stations are transmitting by their presence in the index", async () => {
    const f = httpFeed("http://x", server({ "index.json": INDEX }));
    const live = await f.live();
    expect(live!.has("08MH001")).toBe(true);
    expect(live!.has("08MH999")).toBe(false);
  });

  it("does not open three connections when three components ask at once", async () => {
    // One request PER URL, not per caller: three askers produce the index and the station
    // file once each, and the third asker adds nothing.
    let calls = 0;
    const f = httpFeed("http://x", server({ "08MH001.json": STATION }, () => { calls++; }));
    await Promise.all([f.now(S), f.now(S), f.now(S)]);
    expect(calls).toBe(2);
  });

  it("hands back both quantities and lets the caller pick", async () => {
    // The feed does NOT choose. 237 BC stations measure stage and never discharge, and a
    // feed that picked one would decide for them — so it publishes what it has, says which
    // one the station leads with, and the source joins it to the matching envelope.
    const f = httpFeed("http://x", server({ "08MH001.json": STATION }));
    const obs = (await f.observations(S))!;
    expect(obs.discharge.length).toBe(obs.level.length);
    expect(obs.at.length).toBe(obs.discharge.length);
    expect(["discharge", "level"]).toContain(obs.parameter);
  });

  it("carries the station's change (T144) from the file to the series, as the publisher wrote it", async () => {
    const change = { models: ["CLEVER", "COFFEE"], lowWater: false,
                     reach: { CLEVER: "2026-09-11", COFFEE: "2026-09-06" },
                     dir: "rise", base: 1, peak: { day: "2026-09-03", ratio: 6, value: 6, back: "2026-09-04" },
                     trough: { day: "2026-09-01", ratio: 1, value: 1, back: null },
                     days: [["2026-09-01", 1, 1], ["2026-09-03", 6, 6]] };
    const f = httpFeed("http://x", server({ "08MH001.json": { ...STATION, change } }));
    const obs = (await f.observations(S))!;
    expect(obs.change).toEqual(change);
    expect(seriesOf([], obs, "72h", "discharge")!.value.change).toEqual(change);
    // an older file carries none: null, never a guess
    const old = (await httpFeed("http://x", server({ "08MH001.json": STATION })).observations(S))!;
    expect([old.change, seriesOf([], old, "72h", "discharge")!.value.change]).toEqual([null, null]);
  });

  it("reports no reading when the station answered with no numbers", async () => {
    const blank = { ...STATION, now: { discharge: null, level: null, at: null,
                                       percentile: null } };
    const f = httpFeed("http://x", server({ "08MH001.json": blank }));
    expect(await f.now(S)).toBeNull();
  });

  it("takes the standing word from core rather than restating the thresholds", async () => {
    const f = httpFeed("http://x", server({ "08MH001.json": STATION }));
    // 0.038 is below the 0.10 floor -> "much-below" by @app/core's own definition.
    expect((await f.now(S))!.value.standing).toBe("much-below");
  });
});

describe("record length and HYDAT provenance", () => {
  it("carries which HYDAT release the percentiles were measured against", async () => {
    // A percentile with no stated source claims more authority than it has.
    const idx = { ...INDEX, hydat: { release: "20260717", latest: "20260717",
                                     stale: false } };
    const f = httpFeed("http://x", server({ "index.json": idx }));
    expect((await f.index())!.hydat!.release).toBe("20260717");
  });

  it("carries the publisher's maintenance notices whole (owner 2026-10-22 late)", async () => {
    const notice = { kind: "hydat", since: "2026-10-22T21:00:00Z", current: "20260717", latest: "20261020",
                     action: "scripts/update-hydat.sh" };
    const idx = { ...INDEX, hydat: { release: "20260717", latest: "20261020", stale: true }, maintenance: [notice] };
    const f = httpFeed("http://x", server({ "index.json": idx }));
    expect((await f.index())!.maintenance).toEqual([notice]);
    // an index from before notices: absent, never an empty list (that would claim "checked, nothing due")
    const old = httpFeed("http://x", server({ "index.json": INDEX }));
    expect((await old.index())!.maintenance).toBeUndefined();
  });

  it("surfaces a stale envelope rather than hiding it", async () => {
    const idx = { ...INDEX, hydat: { release: "20250101", latest: "20260717",
                                     stale: true } };
    const f = httpFeed("http://x", server({ "index.json": idx }));
    expect((await f.index())!.hydat!.stale).toBe(true);
  });
});

describe("feed freshness (P02): no fabricated fetchedAt", () => {
  const withStamp = (fetchedAt: unknown) => {
    const file: Record<string, unknown> = { ...STATION };
    if (fetchedAt === undefined) delete file.fetchedAt; else file.fetchedAt = fetchedAt;
    return httpFeed("http://x/feeds/gauge", server({ "08MH001.json": file }));
  };
  it("a station file without fetchedAt has an UNKNOWN age, never now", async () => {
    const f = withStamp(undefined);
    expect((await f.now(S))!.fetchedAt).toBeNull();
    expect((await f.observations(S))!.fetchedAt).toBeNull();
  });
  it("a malformed fetchedAt is unknown too", async () => {
    expect((await withStamp("garbage").now(S))!.fetchedAt).toBeNull();
  });
  it("a well-formed fetchedAt is that instant", async () => {
    expect((await withStamp("2026-09-01T12:00:00Z").now(S))!.fetchedAt).toBe(Date.parse("2026-09-01T12:00:00Z"));
  });
});

describe("when a quiet gauge last reported (owner 2026-10-21)", () => {
  it("is the file's own reading time, then its last recent row with a value, then its last daily mean's day", () => {
    expect(lastReportAt({ now: { at: "2026-09-25T21:35:00-08:00" },
                          recent: [["2026-09-25T21:30:00-08:00", 3.9, null]] }))
      .toBe("2026-09-25T21:35:00-08:00");
    expect(lastReportAt({ now: { at: null },
                          recent: [["2026-09-25T21:00:00-08:00", 3.9, null],
                                   ["2026-09-25T21:30:00-08:00", null, null]] }))
      .toBe("2026-09-25T21:00:00-08:00");
    expect(lastReportAt({ now: null, recent: [], daily: [["2026-09-24", 1, null], ["2026-09-25", null, null]] }))
      .toBe("2026-09-24");
    expect(lastReportAt({})).toBeNull();
    expect(lastReportAt(null)).toBeNull();
  });

  it("reads the station's file even when the index has dropped it — a dropped station is the quiet one", async () => {
    const f = httpFeed("http://x/feeds/gauge", server({
      "index.json": { fetchedAt: "2026-10-21T05:00:00Z", stations: {} },
      "08MH001.json": { ...STATION, now: { ...STATION.now, at: "2026-09-25T21:35:00-08:00" } } }));
    expect(await f.now(S)).toBeNull();
    expect(await f.lastReport(S)).toBe("2026-09-25T21:35:00-08:00");
  });

  it("is undefined — not known, never \"nothing\" — when there is no file to ask or the network is not there", async () => {
    // A 404 or an offline blip says nothing about the station's history: the silent line must not hear
    // "has never reported" from it (review 2026-10-22).
    const missing = httpFeed("http://x/feeds/gauge", server({ "index.json": { fetchedAt: "x", stations: {} } }));
    expect(await missing.lastReport(S)).toBeUndefined();
    const offline = httpFeed("http://x/feeds/gauge", (async () => { throw new TypeError("offline"); }) as never);
    expect(await offline.lastReport(S)).toBeUndefined();
  });

  it("is null only when the station's own file was read and holds nothing", async () => {
    const f = httpFeed("http://x/feeds/gauge", server({
      "08MH001.json": { fetchedAt: "x", station: "08MH001",
                        now: { discharge: null, level: null, at: null, percentile: null }, recent: [] } }));
    expect(await f.lastReport(S)).toBeNull();
  });
});
