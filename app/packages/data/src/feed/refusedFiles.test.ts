/**
 * (T676 leftovers L2–L4, 2026-11-20) A GAUGE FILE REFUSED AS NEWER THAN THE APP:
 *   L2 it is remembered by its validator, so an unchanged refused file costs a 304 (or, inside the feed TTL, nothing),
 *      in the background run (`fetchObs` with its kept copy, `createCheckSources`) and in the foreground (`httpFeed`);
 *   L3 the gauges row on More (the update service's snapshot) shows a file's hold, steadily: a 304 of the index does not
 *      clear it, only that file reading again does;
 *   L4 a foreground obs.json failure is reported (once per kind), never swallowed; offline and a stamp are not reported.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { createFeedGate, FeedNeedsNewerApp, GAUGE_FEED_FORMAT, resolveAppVersion, setFailureSink } from "@app/core";
import { sha256Hex } from "../sha256";
import type { StationId } from "../index";
import { createUpdateService } from "../update/service";
import { createCheckSources } from "../notify/sources";
import { gaugeHistory } from "./history";
import { FEED_TTL_MS, fetchObs, httpFeed } from "./http";

const APP = resolveAppVersion({ version: "0.8.4" });
const A = "A" as StationId;
const BASE = "http://x/feeds/live";
const enc = new TextEncoder();
const IDX = { format: GAUGE_FEED_FORMAT, minAppVersion: "0.0.0", fetchedAt: "2026-11-20T12:00:00Z", stations: { A: {} } };
const ST = { format: GAUGE_FEED_FORMAT, minAppVersion: "0.0.0", fetchedAt: "2026-11-20T12:00:00Z", station: "A",
             now: { at: "2026-11-20T11:00:00-08:00", discharge: 12, level: 1.1 } };
const OBS = { format: GAUGE_FEED_FORMAT, minAppVersion: "0.0.0", fetchedAt: "2026-11-20T12:00:00Z", windowHours: 24,
              stations: { A: [["2026-11-20T11:00:00-08:00", 1.1, 12]] } };
const bump = <T extends object>(d: T) => ({ ...d, format: "gauge-feed/2", minAppVersion: "0.9.0" });

/** An origin: path → body (a 200 with an ETag of the body; the STRONG ETag in If-None-Match: 304). `log`: path + status. */
function origin(files: Record<string, unknown>) {
  const log: string[] = [];
  const f = (async (u: RequestInfo | URL, i?: RequestInit) => {
    const path = String(u).slice(BASE.length + 1);
    const body = files[path];
    if (body === undefined) { log.push(`${path} 404`); return new Response("", { status: 404 }); }
    if (body instanceof Error) { log.push(`${path} throw`); throw body; }
    if (typeof body === "number") { log.push(`${path} ${body}`); return new Response("", { status: body }); }
    const text = JSON.stringify(body);
    const etag = `"${sha256Hex(enc.encode(text)).slice(0, 12)}"`;
    if (new Headers(i?.headers).get("If-None-Match") === etag) { log.push(`${path} 304`); return new Response(null, { status: 304, headers: { ETag: etag } }); }
    log.push(`${path} 200`);
    // the edge weakens the ETag, as staging's does: the client sends it back strong
    return new Response(text, { status: 200, headers: { ETag: `W/${etag}` } });
  }) as typeof fetch;
  return { files, fetch: f, log };
}
const memHeld = () => { const h = { text: null as string | null, read: () => h.text, write: (t: string) => { h.text = t; } }; return h; };

let reported: string[] = [];
let undo: (() => void) | null = null;
const sink = () => { reported = []; undo = setFailureSink((e) => reported.push(String((e as Error)?.message ?? e))); };
afterEach(() => { undo?.(); undo = null; vi.restoreAllMocks(); });

describe("(T676 L2) the background run: a refused obs.json is remembered by its validator", () => {
  it("the next run asks with it: a 304, nothing downloaded, the same refusal (alerts wait); a readable file reads whole again", async () => {
    sink();
    const o = origin({ "obs.json": OBS });
    const held = memHeld();
    const run = () => fetchObs(BASE, o.fetch, undefined, held, createFeedGate(APP));   // a new gate per run, as the check
    expect((await run()).stations).toEqual(OBS.stations);
    o.files["obs.json"] = bump(OBS);
    await expect(run()).rejects.toThrow(FeedNeedsNewerApp);
    // THE LEFTOVER: before, the kept copy's validator went out and the refused file came back whole on every run
    await expect(run()).rejects.toThrow(FeedNeedsNewerApp);
    await expect(run()).rejects.toThrow(FeedNeedsNewerApp);
    expect(o.log).toEqual(["obs.json 200", "obs.json 200", "obs.json 304", "obs.json 304"]);
    // the good copy is still kept beside the refusal
    expect(JSON.parse(held.text!)).toMatchObject({ value: OBS, refused: { need: "0.9.0" } });
    // the origin takes the bump back: the refused validator no longer matches, the file is read whole and kept
    o.files["obs.json"] = { ...OBS, fetchedAt: "2026-11-20T13:00:00Z" };
    expect((await run()).fetchedAt).toBe("2026-11-20T13:00:00Z");
    expect(JSON.parse(held.text!).refused).toBeUndefined();
    expect(o.log.at(-1)).toBe("obs.json 200");
    expect(reported).toEqual([]);
  });

  it("an app updated since (new enough for the remembered need) drops the refusal and reads the file whole", async () => {
    // stamped for 0.9.0 in this build's own format (a build that reads it), so the newer app can read it here
    const o = origin({ "obs.json": { ...OBS, minAppVersion: "0.9.0" } });
    const held = memHeld();
    await expect(fetchObs(BASE, o.fetch, undefined, held, createFeedGate(APP))).rejects.toThrow(FeedNeedsNewerApp);
    const v = await fetchObs(BASE, o.fetch, undefined, held, createFeedGate(resolveAppVersion({ version: "0.9.0" })));
    expect([v.stations, o.log]).toEqual([OBS.stations, ["obs.json 200", "obs.json 200"]]);
    expect(JSON.parse(held.text!).refused).toBeUndefined();
  });

  it("through the check's sources factory (HELD.obs): the same 304", async () => {
    vi.spyOn(console, "info").mockImplementation(() => {});
    const o = origin({ "obs.json": bump(OBS) });
    const held = { obs: memHeld(), manifest: memHeld(), version: memHeld() };
    const s = createCheckSources({ fetch: o.fetch, alertFeed: BASE, manifestUrl: `${BASE}/m.json`, versionFile: null, appVersion: APP, held });
    await expect(s.obs()).rejects.toThrow(FeedNeedsNewerApp);
    await expect(s.obs()).rejects.toThrow(FeedNeedsNewerApp);
    expect(o.log).toEqual(["obs.json 200", "obs.json 304"]);
  });

  it("a kept refusal not in its shape is dropped (a whole read), never trusted", async () => {
    const o = origin({ "obs.json": OBS });
    for (const refused of [{ etag: 3, need: "0.9.0" }, { etag: '"x"', need: "soon" }, "0.9.0"]) {
      const held = memHeld();
      held.text = JSON.stringify({ url: `${BASE}/obs.json`, refused });
      expect((await fetchObs(BASE, o.fetch, undefined, held, createFeedGate(APP))).stations).toEqual(OBS.stations);
    }
    expect(o.log).toEqual(["obs.json 200", "obs.json 200", "obs.json 200"]);
  });
});

describe("(T676 L2) the foreground: a refused station file is not downloaded on every read", () => {
  it("inside the TTL nothing is asked; after it, a 304 by its validator; the session's copy answers throughout", async () => {
    sink();
    let t = Date.parse("2026-11-20T12:00:00Z");
    vi.spyOn(Date, "now").mockImplementation(() => t);
    const o = origin({ "index.json": IDX, "A.json": ST });
    const gate = createFeedGate(APP);
    const f = httpFeed(BASE, o.fetch, { gate });
    expect(await f.lastReport(A)).toBe(ST.now.at);
    o.files["A.json"] = bump({ ...ST, now: { ...ST.now, at: "2026-11-20T12:00:00-08:00" } });
    t += FEED_TTL_MS + 1;
    expect(await f.lastReport(A)).toBe(ST.now.at);                    // refused: the session's copy
    const asked = () => o.log.filter((l) => l.startsWith("A.json"));
    expect(asked()).toEqual(["A.json 200", "A.json 200"]);
    // THE LEFTOVER: every read went out and came back whole; now none inside the TTL …
    for (let i = 0; i < 5; i++) expect(await f.lastReport(A)).toBe(ST.now.at);
    expect(asked()).toHaveLength(2);
    // … and a 304 after it, by the refused file's validator
    t += FEED_TTL_MS + 1;
    expect(await f.lastReport(A)).toBe(ST.now.at);
    expect(asked()).toEqual(["A.json 200", "A.json 200", "A.json 304"]);
    expect(gate.refused()).toEqual({ gauges: "0.9.0" });
    // the sync icon asks again at once (still by the validator)
    f.refresh!();
    await f.lastReport(A);
    expect(asked().at(-1)).toBe("A.json 304");
    // the origin takes it back: read whole, the hold cleared
    o.files["A.json"] = { ...ST, now: { ...ST.now, at: "2026-11-20T12:30:00-08:00" } };
    f.refresh!();
    expect(await f.lastReport(A)).toBe("2026-11-20T12:30:00-08:00");
    expect([asked().at(-1), gate.needs()]).toEqual(["A.json 200", null]);
    expect(reported).toEqual([]);
  });

  it("a refused file the origin then drops (404): its hold goes with it", async () => {
    const o = origin({ "index.json": IDX, "A.json": bump(ST) });
    const gate = createFeedGate(APP);
    const f = httpFeed(BASE, o.fetch, { gate });
    await f.lastReport(A);
    expect(gate.needs()).toBe("0.9.0");
    delete o.files["A.json"];
    f.refresh!();
    expect(await f.lastReport(A)).toBeUndefined();
    expect(gate.needs()).toBeNull();
  });
});

describe("(T676 L3) the gauges row on More shows a file's hold, steadily", () => {
  it("a station file refused: the row says it; a 304 of the index leaves it; the file reading again clears it", async () => {
    const o = origin({ "index.json": IDX, "A.json": bump(ST) });
    const gate = createFeedGate(APP);
    const f = httpFeed(BASE, o.fetch, { gate, indexHeld: true });
    const svc = createUpdateService({ fetch: o.fetch, appVersion: APP, feedGate: gate,
                                      feeds: [{ id: "gauges", url: `${BASE}/index.json`, apply: (text) => f.offer!(JSON.parse(text)) }] });
    const row = () => svc.snapshot().feeds[0]!.needsApp ?? null;
    const seen: (string | null)[] = [];
    svc.subscribe(() => seen.push(row()));
    await svc.check();                                   // the index read whole: nothing held
    expect([row(), svc.snapshot().needsApp]).toEqual([null, null]);
    await f.lastReport(A);                               // a station read meets the bumped file
    expect([row(), svc.snapshot().needsApp]).toEqual(["0.9.0", "0.9.0"]);
    // THE LEFTOVER: the index answered 304 and the row (and the prompt) cleared until the next station read set it again
    await svc.check();
    await svc.check();
    expect(o.log.filter((l) => l.startsWith("index.json"))).toEqual(["index.json 200", "index.json 304", "index.json 304"]);
    expect([row(), svc.snapshot().needsApp]).toEqual(["0.9.0", "0.9.0"]);
    expect(seen.includes(null) && seen.lastIndexOf(null) > seen.indexOf("0.9.0")).toBe(false);   // never cleared between
    // the file readable again: the row clears
    o.files["A.json"] = ST;
    f.refresh!();
    await f.lastReport(A);
    expect([row(), svc.snapshot().needsApp]).toEqual([null, null]);
  });

  it("obs.json refused in the foreground sync: the row says it too", async () => {
    const o = origin({ "index.json": IDX, "obs.json": bump(OBS) });
    const gate = createFeedGate(APP);
    const f = httpFeed(BASE, o.fetch, { gate, history: gaugeHistory(null) });
    const svc = createUpdateService({ fetch: o.fetch, appVersion: APP, feedGate: gate,
                                      feeds: [{ id: "gauges", url: `${BASE}/index.json`, apply: (text) => f.offer!(JSON.parse(text)) }] });
    await svc.check();
    await f.syncObservations!();
    await svc.check();
    expect(svc.snapshot().feeds[0]!.needsApp).toBe("0.9.0");
    // and the next sync costs a 304 (L2, the foreground's own kept copy)
    await f.syncObservations!();
    expect(o.log.filter((l) => l.startsWith("obs.json"))).toEqual(["obs.json 200", "obs.json 304"]);
  });
});

describe("(T676 L4) a foreground obs.json failure is reported, never swallowed", () => {
  it("a 500 and a file that is not the feed's: each reported once (not on every sync); offline and a stamp: not reported", async () => {
    sink();
    const o = origin({ "obs.json": 500 });
    const gate = createFeedGate(APP);
    const f = httpFeed(BASE, o.fetch, { gate, history: gaugeHistory(null) });
    await f.syncObservations!();
    await f.syncObservations!();
    expect(reported).toEqual([`${BASE}/obs.json: HTTP 500`]);
    o.files["obs.json"] = { format: GAUGE_FEED_FORMAT, minAppVersion: "0.0.0", fetchedAt: "2026-11-20T12:00:00Z" };   // no stations
    await f.syncObservations!();
    await f.syncObservations!();
    expect(reported).toHaveLength(2);
    expect(reported[1]).toMatch(/^http:\/\/x\/feeds\/live\/obs\.json: /);
    reported.length = 0;
    o.files["obs.json"] = new TypeError("Network request failed");                 // offline: the environment's
    await f.syncObservations!();
    o.files["obs.json"] = bump(OBS);                                                // a stamp: a known state
    await f.syncObservations!();
    expect([reported, gate.needs()]).toEqual([[], "0.9.0"]);
  });
});
