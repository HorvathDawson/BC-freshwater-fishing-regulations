/**
 * T1205 — the judgement re-runs as time passes (a stopped feed, a phone offline with the app open or back at the front), and
 * `useDonorView` judges a reading's age at the real now, not at the picked day.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { GAUGE_FEED_FORMAT } from "@app/core";
import { answerFrom, FEED_TTL_MS, httpFeed, type Panel, type StationId } from "@app/data";
import { useGaugeGeoJSON } from "./hooks";
import { appBecameActive, appBecameInactive } from "./today";
import { useDonorView } from "./panel/donor";

const H = 3_600_000;
const FED = Date.parse("2026-10-27T18:00:00Z");
const iso = (ms: number) => new Date(ms).toISOString();
const file = { format: GAUGE_FEED_FORMAT, fetchedAt: iso(FED),
  stations: { "08MH001": { percentile: 0.3, discharge: 0.3, parameter: "discharge", observedAt: iso(FED - 30 * 60_000) } } };
const source = { gaugePoints: async () => [{ station: "08MH001", name: "Chilliwack R", lon: -121.9, lat: 49.1 }] } as never;

describe("the map's dots are judged again with no new feed", () => {
  beforeEach(() => { vi.useFakeTimers({ shouldAdvanceTime: true }); vi.setSystemTime(FED + 20 * 60_000); });
  afterEach(() => { cleanup(); vi.useRealTimers(); });

  it("on the feed beat, and on coming back to the front, a held index's old readings leave the map", async () => {
    let asked = 0;
    const feed = httpFeed("http://x/g", (async () => { asked += 1; return { ok: true, json: async () => file }; }) as never, { indexHeld: true });
    const { result } = renderHook(() => useGaugeGeoJSON(source, feed, true));
    await waitFor(() => expect(result.current).not.toBeNull());
    vi.setSystemTime(FED + 9 * H);
    act(() => { vi.advanceTimersByTime(FEED_TTL_MS + 1000); });          // the beat; no fetch
    await waitFor(() => expect(result.current).toBeNull());
    expect(asked).toBe(1);
  });
  it("and at once when the app comes back to the front", async () => {
    const feed = httpFeed("http://x/g", (async () => ({ ok: true, json: async () => file })) as never, { indexHeld: true });
    const { result } = renderHook(() => useGaugeGeoJSON(source, feed, true));
    await waitFor(() => expect(result.current).not.toBeNull());
    vi.setSystemTime(FED + 9 * H);
    act(() => { appBecameActive(); });
    await waitFor(() => expect(result.current).toBeNull());
  });
});

describe("the beat sleeps in the background", () => {
  beforeEach(() => { vi.useFakeTimers({ shouldAdvanceTime: true }); vi.setSystemTime(FED + 20 * 60_000); });
  afterEach(() => { cleanup(); vi.useRealTimers(); });
  it("no re-judging while the app is away; coming back judges at once", async () => {
    const feed = httpFeed("http://x/g", (async () => ({ ok: true, json: async () => file })) as never, { indexHeld: true });
    const { result } = renderHook(() => useGaugeGeoJSON(source, feed, true));
    await waitFor(() => expect(result.current).not.toBeNull());
    act(() => { appBecameInactive(); });
    vi.setSystemTime(FED + 9 * H);
    act(() => { vi.advanceTimersByTime(3 * FEED_TTL_MS); });
    // nothing ran in the background: the dots stay for as long as a re-ask would have taken to land (it lands within a second at the front)
    await expect(waitFor(() => expect(result.current).toBeNull(), { timeout: 1500 })).rejects.toThrow();
    expect(result.current).not.toBeNull();
    act(() => { appBecameActive(); });
    await waitFor(() => expect(result.current).toBeNull());
  });
});

describe("useDonorView reads a reading's age at `now`, not at the picked day", () => {
  const PANEL: Panel = { areaKm2: 100, members: [{ station: "08MH001" as StationId, role: "up", areaKm2: 100,
    years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true }] };
  // a reading with no rank (no band) and a stamp: its state depends only on the age rule
  const answer = (observed: number) => answerFrom(PANEL, { stations: { "08MH001": { percentile: null, observedAt: iso(observed) } } } as never);
  const opts = (ref: number, now?: number) => ({ day: new Date(ref), ref, ...(now !== undefined ? { now } : {}), horizon: 0 as const,
    from: null, size: { width: 300, height: 300 }, selected: null, pickable: false });
  const silent = (observed: number, ref: number, now?: number) => {
    const { result } = renderHook(() => useDonorView(answer(observed), opts(ref, now) as never));
    return result.current.ok ? null : result.current.silent;
  };
  it("a month-old reading is not reporting on a day picked back to its own evening", () => {
    const picked = FED - 12 * H;                               // the picked day's instant, before the reading
    expect(silent(FED, picked, FED + 29 * 24 * H)).toMatch(/^Not reporting: /);
  });
  it("a fresh reading is reporting on a day picked ahead", () => {
    const now = FED + H;
    expect(silent(FED, now + 3 * 24 * H, now)).toBeNull();      // it speaks: reporting, not silent
  });
});
