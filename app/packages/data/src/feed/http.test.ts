/**
 * The HTTP gauge feed.
 *
 * Every test here is really "what happens when the network is not there", because for this
 * app that is the ordinary case rather than the exception. A rejected promise on a river
 * bank turns into an error screen where "we could not check" is the truth.
 */
import { describe, expect, it, vi } from "vitest";
import { httpFeed } from "./http";
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

  it("does not ask for a station the index does not list", async () => {
    /*
     * The bundle and the feed are built separately, so the bundle can name a station the
     * publisher never wrote — retired between builds, or short of the record threshold.
     * Every view of such a water fired a request that could only 404.
     */
    const seen: string[] = [];
    const f = httpFeed("http://x", server({ "index.json": INDEX }, (u) => seen.push(u)));
    await expect(f.now("08PA001")).resolves.toBeNull();
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
  const STATS = { release: "20260717",
                  stations: { "08MH001": { from_year: 1913, to_year: 2026, years: 97 } } };

  it("reports how long a station's record is", async () => {
    const f = httpFeed("http://x", server({ "stats.json": STATS }));
    expect(await f.record(S)).toEqual({ fromYear: 1913, toYear: 2026, years: 97 });
  });

  it("returns null for a station with no published record", async () => {
    const f = httpFeed("http://x", server({ "stats.json": STATS }));
    expect(await f.record("08XX999" as StationId)).toBeNull();
  });

  it("carries which HYDAT release the percentiles were measured against", async () => {
    // A percentile with no stated source claims more authority than it has.
    const idx = { ...INDEX, hydat: { release: "20260717", latest: "20260717",
                                     stale: false } };
    const f = httpFeed("http://x", server({ "index.json": idx }));
    expect((await f.index())!.hydat!.release).toBe("20260717");
  });

  it("surfaces a stale envelope rather than hiding it", async () => {
    const idx = { ...INDEX, hydat: { release: "20250101", latest: "20260717",
                                     stale: true } };
    const f = httpFeed("http://x", server({ "index.json": idx }));
    expect((await f.index())!.hydat!.stale).toBe(true);
  });
});
