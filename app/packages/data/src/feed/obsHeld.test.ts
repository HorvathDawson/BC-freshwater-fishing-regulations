/**
 * (T741) THE BACKGROUND CHECK'S obs.json BY CONDITIONAL GET (`fetchObs` with `held`): exactly as fresh as a whole read.
 * A 304 answers with the kept copy only when the origin's validator proves the object has not moved; a changed object is
 * read whole; an answer with no validator is read whole every time, and that is said; a kept copy that is unreadable or
 * not the feed's is reported and read whole.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { GAUGE_FEED_FORMAT, setFailureSink } from "@app/core";
import { fetchObs, type ObsHeld } from "./http";

const BASE = "http://x/feeds/live";
const URL_ = `${BASE}/obs.json`;
const OBS_A = { format: GAUGE_FEED_FORMAT, stations: { "08MH001": { t: "2026-11-14T12:00:00Z", d: 15.7 } } };
const OBS_B = { format: GAUGE_FEED_FORMAT, stations: { "08MH001": { t: "2026-11-14T13:00:00Z", d: 16.1 } } };

let undo: (() => void) | null = null;
afterEach(() => { undo?.(); undo = null; vi.restoreAllMocks(); });
const reports = () => { const got: string[] = []; undo = setFailureSink((e) => got.push(String(e))); return got; };

function memHeld(text: string | null = null): ObsHeld & { text: string | null } {
  const h = { text, read: () => h.text, write: (t: string) => { h.text = t; } };
  return h;
}

/** An origin holding one object and its ETag (the edge weakens it, as staging does); answers 304 to the STRONG form. */
function origin(o: { body: unknown; etag: string | null }) {
  const asked: (string | null)[] = [];
  const f = (async (_url: string, init?: { headers?: Record<string, string> }) => {
    const inm = init?.headers?.["If-None-Match"] ?? null;
    asked.push(inm);
    const strong = o.etag?.replace(/^W\//, "") ?? null;
    if (inm && strong && inm === strong) return { ok: false, status: 304, headers: new Headers(), json: async () => { throw new Error("no body"); } };
    return { ok: true, status: 200, headers: new Headers(o.etag ? { ETag: o.etag } : {}), json: async () => o.body };
  }) as unknown as typeof fetch;
  return { f, asked };
}

describe("(T741) fetchObs with a kept copy: a conditional GET", () => {
  it("unchanged: the first run reads whole and keeps it; the next sends the strong validator, gets 304, answers the copy", async () => {
    const o = { body: OBS_A, etag: 'W/"aaa"' }, srv = origin(o), held = memHeld();
    expect(await fetchObs(BASE, srv.f, undefined, held)).toEqual(OBS_A);
    expect(JSON.parse(held.text!)).toEqual({ url: URL_, etag: 'W/"aaa"', value: OBS_A });
    expect(await fetchObs(BASE, srv.f, undefined, held)).toEqual(OBS_A);
    expect(srv.asked).toEqual([null, '"aaa"']);
  });

  it("changed: the origin's object moved, so the validator does not match: read whole, the new copy kept and answered", async () => {
    const o = { body: OBS_A, etag: 'W/"aaa"' }, srv = origin(o), held = memHeld();
    await fetchObs(BASE, srv.f, undefined, held);
    o.body = OBS_B; o.etag = 'W/"bbb"';
    expect(await fetchObs(BASE, srv.f, undefined, held)).toEqual(OBS_B);
    expect(JSON.parse(held.text!).etag).toBe('W/"bbb"');
    expect(srv.asked).toEqual([null, '"aaa"']);
  });

  it("no validator: every run is a whole read (nothing kept to ask with), and it is said", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const srv = origin({ body: OBS_A, etag: null }), held = memHeld();
    expect(await fetchObs(BASE, srv.f, undefined, held)).toEqual(OBS_A);
    expect(await fetchObs(BASE, srv.f, undefined, held)).toEqual(OBS_A);
    expect(srv.asked).toEqual([null, null]);
    expect(held.text).toBe("");
    expect(info).toHaveBeenCalledWith(`[obs] ${URL_}: no ETag in the answer; the next check reads the whole file again`);
  });

  it("a kept copy for another url, unreadable, or not the feed's: no conditional header, a whole read; the bad ones reported", async () => {
    const got = reports();
    const srv = origin({ body: OBS_A, etag: '"aaa"' });
    for (const text of [JSON.stringify({ url: "http://other/obs.json", etag: '"aaa"', value: OBS_A }), "{not json",
                        JSON.stringify({ url: URL_, etag: '"aaa"', value: { format: "gauge-feed/9", stations: {} } })])
      expect(await fetchObs(BASE, srv.f, undefined, memHeld(text))).toEqual(OBS_A);
    expect(srv.asked).toEqual([null, null, null]);
    expect(got).toHaveLength(2);
    expect(got[0]).toMatch(/^Error: kept obs\.json is not JSON \(.+\); reading it whole$/);
    expect(got[1]).toBe('Error: kept obs.json is not the feed\'s (the gauge feed\'s obs file is "gauge-feed/9"; this app reads gauge-feed/1); reading it whole');
  });

  it("without a kept-copy store it is the plain whole read it always was (no header, nothing written)", async () => {
    const srv = origin({ body: OBS_A, etag: '"aaa"' });
    expect(await fetchObs(BASE, srv.f)).toEqual(OBS_A);
    expect(await fetchObs(BASE, srv.f)).toEqual(OBS_A);
    expect(srv.asked).toEqual([null, null]);
  });

  it("a 304 with no kept copy (an origin answering a header we never sent) is a failure, never an empty answer", async () => {
    const f = (async () => ({ ok: false, status: 304, headers: new Headers(), json: async () => ({}) })) as unknown as typeof fetch;
    await expect(fetchObs(BASE, f, undefined, memHeld())).rejects.toThrow(`${URL_}: HTTP 304`);
  });
});
