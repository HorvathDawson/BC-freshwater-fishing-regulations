/** P01: useSpots awaits every write and hands a failure back, said whole; the list never empties on a bad file. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { leadUpDays } from "@app/core";
import type { WaterSource } from "@app/data";
import { needsRefresh, openSpotsWith, refreshSpot, type Spot, type SpotsFs } from "@app/data/spots";
import { spotSaveFailed, useRefreshOnReconnect, useSpots, type OnlineLike, type SpotsStoreLike } from "./spots";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });

function Probe({ store, out }: { store: SpotsStoreLike; out: { api?: ReturnType<typeof useSpots> } }) {
  out.api = useSpots(store);
  const s = out.api.spots;
  return <p>{s.state}:{s.state === "ready" ? s.value.length : ""}|{out.api.error ?? ""}</p>;
}

const store = (over: Partial<SpotsStoreLike> = {}): SpotsStoreLike => {
  let all: Spot[] = [];
  return { list: async () => all, put: async (s) => { all = [s, ...all]; }, remove: async () => {},
           patch: async () => {}, ...over };
};

describe("useSpots (P01)", () => {
  it("a rejected save rejects its caller and says why, whole", async () => {
    const out: { api?: ReturnType<typeof useSpots> } = {};
    const { container } = render(<Probe store={store({ put: async () => { throw new Error("disk full"); } })} out={out} />);
    await settle();
    let thrown: unknown = null;
    await act(async () => { await out.api!.save({ id: "a" } as Spot).catch((e) => { thrown = e; }); });
    await settle();
    expect((thrown as Error).message).toBe("disk full");
    expect(container.textContent).toBe("ready:0|This spot was not saved (disk full). It is still on screen; try again.");
  });

  it("a saved spot is in the list after the save resolves", async () => {
    const out: { api?: ReturnType<typeof useSpots> } = {};
    const { container } = render(<Probe store={store()} out={out} />);
    await settle();
    await act(async () => { await out.api!.save({ id: "a" } as Spot); });
    await settle();
    expect(container.textContent).toBe("ready:1|");
  });

  it("the refresh fills a spot's week-before days not out yet, writes them, and the list reads them back (owner bug)",
     async () => {
    // a real store over memory "files", the real refreshSpot, mocked sources: the week was saved with its last two
    // gauge days not published yet; the feed now has them
    const files = new Map<string, string>();
    const fs: SpotsFs = { exists: (n) => files.has(n), read: (n) => files.get(n)!, write: (n, t) => { files.set(n, t); },
                          rename: (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
                          remove: (n) => { files.delete(n); } };
    const visit = new Date(2026, 7, 30, 9).getTime();
    const days = leadUpDays(visit);
    const saved = {
      id: "s1", createdAt: visit, updatedAt: visit, visitedAt: visit, lat: 49.1, lon: -121.96, item: null,
      waterName: null, title: "", notes: "", photos: [], trace: null, panel: null,
      // a reading taken on the visit day (T170): what is outstanding here is the week alone
      reading: { station: "08MH001", discharge: 40, level: null, percentile: 0.5, at: new Date(visit).toISOString() },
      weather: { tempC: 14, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014, code: 3, window: null, at: "x" },
      leadUp: {
        weather: days.map((date) => ({ date, maxC: 20, minC: 9, precipMm: 0, windMaxKph: 10, code: 1 })),
        station: "08MH001",
        gauge: days.map((date, i) => ({ date, discharge: i < 5 ? 40 : null, level: null,
                                        dischargePlace: i < 5 ? 0.5 : null, levelPlace: null })),
      },
    } as Spot;
    files.set("s.json", JSON.stringify([saved]));
    const values = Array.from({ length: 300 }, () => null as number | null);
    for (const d of days) values[Math.round((Date.parse(d) - Date.parse("2026-01-01")) / 86_400_000)] = 33;
    const source = {
      gaugeParameters: async () => ["discharge"],
      gaugeSeries: async () => ({ fetchedAt: visit, value: { step: "1d", from: "2026-01-01", parameter: "discharge",
        at: [], values, band: values.map(() => [10, 20, 40, 60, 100]), now: null, forecast: null, forecasts: [],
        priorYears: [] } }),
    } as unknown as WaterSource;
    const weather = { at: async () => null, days: async () => { throw new Error("the weather was whole"); } };
    const out: { api?: ReturnType<typeof useSpots> } = {};
    render(<Probe store={openSpotsWith(fs, "s.json")} out={out} />);
    await settle();
    expect(needsRefresh(saved)).toBe(true);
    await act(async () => { await out.api!.refresh((s) => refreshSpot(s, weather, source)); });
    await settle();
    const back = JSON.parse(files.get("s.json")!)[0] as Spot;
    expect(back.leadUp!.gauge!.map((d) => d.discharge)).toEqual([40, 40, 40, 40, 40, 33, 33]);
    expect(back.leadUp!.backfilled).toBe(true);
    const listed = out.api!.spots;
    expect(listed.state === "ready" && needsRefresh(listed.value[0]!)).toBe(false);
  });

  it("a corrupt store is an error the reader sees, never an empty list", async () => {
    const corrupt = Object.assign(new Error("The saved spots could not be read. The file was kept as s.corrupt-1 and nothing was overwritten."), { name: "SpotsCorrupt" });
    const { container } = render(<Probe store={store({ list: async () => { throw corrupt; } })} out={{}} />);
    await settle();
    expect(container.textContent).toBe("failed:|" + corrupt.message);
    expect(spotSaveFailed(corrupt)).toBe(corrupt.message);
  });
});

describe("useRefreshOnReconnect (T255)", () => {
  function net(start: boolean) {
    let online = start;
    const subs = new Set<(o: boolean) => void>();
    const c: OnlineLike = { online: () => online, subscribe: (f) => { subs.add(f); return () => { subs.delete(f); }; } };
    return { c, set(o: boolean) { online = o; subs.forEach((f) => f(o)); } };
  }
  function R({ c, pending, refresh }: { c: OnlineLike; pending: boolean; refresh: () => Promise<void> }) {
    useRefreshOnReconnect(c, pending, refresh, [100, 1000]);
    return null;
  }
  const tick = (ms: number) => act(async () => { vi.advanceTimersByTime(ms); for (let i = 0; i < 4; i++) await Promise.resolve(); });
  afterEach(() => vi.useRealTimers());

  it("offline with a spot waiting: nothing; back online: a try after the line settles, a retry while it still waits", async () => {
    vi.useFakeTimers();
    const n = net(false);
    let calls = 0;
    const refresh = async () => { calls++; };
    render(<R c={n.c} pending refresh={refresh} />);
    await tick(5000);
    expect(calls).toBe(0);
    await act(async () => { n.set(true); });
    expect(calls).toBe(0);                                       // not at the instant the phone says "connected"
    await tick(100);
    expect(calls).toBe(1);
    await tick(1000);
    expect(calls).toBe(2);                                       // still waiting: tried again
    await tick(60_000);
    expect(calls).toBe(2);                                       // the tries are spent until the next reconnect
    await act(async () => { n.set(true); });                     // still online: no new round
    await tick(2000);
    expect(calls).toBe(2);
    await act(async () => { n.set(false); });
    await act(async () => { n.set(true); });
    await tick(100);
    expect(calls).toBe(3);
  });

  it("filled in: no retry; offline again: the pending try is dropped", async () => {
    vi.useFakeTimers();
    const n = net(false);
    let calls = 0;
    const refresh = async () => { calls++; };
    const { rerender } = render(<R c={n.c} pending refresh={refresh} />);
    await act(async () => { n.set(true); });
    await tick(100);
    expect(calls).toBe(1);
    rerender(<R c={n.c} pending={false} refresh={refresh} />);  // the refresh filled it
    await tick(5000);
    expect(calls).toBe(1);
    rerender(<R c={n.c} pending refresh={refresh} />);           // another spot made, while online: at once
    await tick(0);
    expect(calls).toBe(2);
    await act(async () => { n.set(false); });
    await act(async () => { n.set(true); });
    await act(async () => { n.set(false); });                   // dropped before its try
    await tick(5000);
    expect(calls).toBe(2);
  });

  it("nothing waits: a reconnect asks nothing", async () => {
    vi.useFakeTimers();
    const n = net(true);
    let calls = 0;
    render(<R c={n.c} pending={false} refresh={async () => { calls++; }} />);
    await act(async () => { n.set(false); });
    await act(async () => { n.set(true); });
    await tick(5000);
    expect(calls).toBe(0);
  });

  it("one at a time: a try while a refresh runs does not start another", async () => {
    vi.useFakeTimers();
    const n = net(false);
    let calls = 0;
    let finish: () => void = () => {};
    const refresh = () => { calls++; return new Promise<void>((ok) => { finish = ok; }); };
    render(<R c={n.c} pending refresh={refresh} />);
    await act(async () => { n.set(true); });
    await tick(100);
    expect(calls).toBe(1);
    await tick(1000);                                            // the retry lands while the first still runs
    expect(calls).toBe(1);
    await act(async () => { finish(); });
    await act(async () => { n.set(false); });
    await act(async () => { n.set(true); });
    await tick(100);
    expect(calls).toBe(2);
  });
});
