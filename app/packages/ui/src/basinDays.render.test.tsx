/**
 * (T377 item 1) The forecast-day list of one watershed group: browse asks the map's group (T417: the one most on
 * screen), a water its own — never
 * one for the other; a failure is a failed state (shown), a place in no group is said, a day no gauge reads is said.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import { buildStandingClasses, type Basin, type BasinMember, type ItemId, type StationId,
         type WaterSource } from "@app/data";
import { viewBasinAsk, useBasinDays, type BasinDaysState } from "./basinDays";
import { useWaterBasin } from "./basin";
import type { PanelFeed } from "./panel";

afterEach(cleanup);

const TUE = new Date(2026, 10, 3, 12);
const CHWK: Basin = { id: "CHWK", name: "Chilliwack River" };
const LFRA: Basin = { id: "LFRA", name: "Lower Fraser" };
const m = (station: string): BasinMember =>
  ({ station: station as StationId, areaKm2: 100, years: { discharge: 40, level: 40 } });
const CLASSES = buildStandingClasses("r-test", [], new Map(), new Map(),
  new Map([["CHWK", [m("A")]], ["LFRA", [m("B")]]]));
/** West of lon 0: Chilliwack; 0 to 10: Lower Fraser; past 10: no group. gnis:1's own point is in the Lower Fraser. */
const source = {
  standingClasses: async () => CLASSES,
  basinAt: async (p: { lon: number }) => (p.lon < 0 ? CHWK : p.lon < 10 ? LFRA : null),
  waterPoint: async (item: ItemId) => (item === ("gnis:1" as ItemId) ? { lon: 5, lat: 49 } : null),
} as unknown as WaterSource;
/** A (Chilliwack): much below now, much above tomorrow, nothing after. B (Lower Fraser): normal now and on +2. */
const IDX = { stations: {
  A: { percentile: 0.05, discharge: 0.05, parameter: "discharge", ahead: { 1: { discharge: 0.95 } } },
  B: { percentile: 0.5, discharge: 0.5, parameter: "discharge", ahead: { 2: { discharge: 0.5 } } },
} };
const feed = (index: () => Promise<unknown> = async () => IDX) => ({ index } as unknown as PanelFeed);

const words = (s: BasinDaysState) => (s.state === "ready" ? s.vm.days.slice(0, 3).map((d) => d.word) : s.state);

describe("useBasinDays", () => {
  it("browse: the map's group, each day in the standing words, a day no gauge reads said", async () => {
    const h = renderHook(() => useBasinDays({ source, feed: feed(), basin: viewBasinAsk(CHWK, null), for: "browse",
                                              quantity: "discharge", horizon: 1, enabled: true, now: TUE }));
    await waitFor(() => expect(h.result.current.state).toBe("ready"));
    const s = h.result.current as Extract<BasinDaysState, { state: "ready" }>;
    expect(s.vm.title).toBe("Chilliwack River watershed");
    expect(words(s)).toEqual(["Much below normal", "Much above normal", "No gauge reading"]);
    expect(s.vm.days[1]!.label).toBe("Forecast 1 day ahead: much above normal, showing");
    expect(s.vm.days[2]!.label).toBe("Forecast 2 days ahead: no gauge reading");
  });

  it("a water: its own group (its own point), never the map's centre", async () => {
    const h = renderHook(() => {
      const basin = useWaterBasin(source, { item: "gnis:1" as ItemId, tapSid: null, at: null });
      return useBasinDays({ source, feed: feed(), basin, for: "water", quantity: "discharge", horizon: 0, enabled: true,
                            now: TUE });
    });
    await waitFor(() => expect(h.result.current.state).toBe("ready"));
    const s = h.result.current as Extract<BasinDaysState, { state: "ready" }>;
    expect(s.vm.title).toBe("Lower Fraser watershed");
    expect(words(s)).toEqual(["Normal", "No gauge reading", "Normal"]);
  });

  it("failures are a failed state: the centre's group, the water's group, the feed", async () => {
    const centre = renderHook(() => useBasinDays({ source, feed: feed(), basin: viewBasinAsk(undefined, new Error("no groups")),
                                                   for: "browse", quantity: "discharge", horizon: 0, enabled: true }));
    expect(centre.result.current).toMatchObject({ state: "failed", error: { message: "no groups" } });
    const water = renderHook(() => {
      const basin = useWaterBasin(source, { item: "gnis:2" as ItemId, tapSid: null, at: null });
      return useBasinDays({ source, feed: feed(), basin, for: "water", quantity: "discharge", horizon: 0, enabled: true });
    });
    await waitFor(() => expect(water.result.current.state).toBe("failed"));
    const bad = renderHook(() => useBasinDays({ source, feed: feed(async () => { throw new Error("feed 500"); }),
                                                basin: viewBasinAsk(CHWK, null), for: "browse", quantity: "discharge",
                                                horizon: 0, enabled: true }));
    await waitFor(() => expect(bad.result.current).toMatchObject({ state: "failed", error: { message: "feed 500" } }));
  });

  it("a place in no group is said (browse and water words), not loading and not blank", () => {
    const h = renderHook(() => useBasinDays({ source, feed: feed(), basin: viewBasinAsk(null, null), for: "browse",
                                              quantity: "discharge", horizon: 0, enabled: true }));
    expect(h.result.current).toEqual({ state: "none", line: "No watershed group is on the map" });
    expect(viewBasinAsk(undefined, null).state).toBe("loading");
  });

  it("(T417) NO FLICKER: moving to another group keeps the last group's days up, under ITS name, then swaps in one render",
     async () => {
    let release: (() => void) | null = null;
    let slow = false;
    const f = feed(() => (slow ? new Promise((r) => { release = () => r(IDX); }) : Promise.resolve(IDX)));
    // EVERY RENDER is checked, not just the settled one: no render between the two groups may be empty or loading, and
    // none may show one group's days under the other's name
    const seen: { asked: Basin; got: BasinDaysState }[] = [];
    const h = renderHook(({ b }) => {
      const got = useBasinDays({ source, feed: f, basin: viewBasinAsk(b, null), for: "browse",
                                 quantity: "discharge", horizon: 0, enabled: true, now: TUE });
      seen.push({ asked: b, got });
      return got;
    }, { initialProps: { b: CHWK } });
    await waitFor(() => expect(h.result.current.state).toBe("ready"));
    expect((h.result.current as { basin: Basin }).basin).toEqual(CHWK);
    const firstReady = seen.findIndex((x) => x.got.state === "ready");
    slow = true;
    h.rerender({ b: LFRA });
    // the Lower Fraser's days are not in yet: the Chilliwack's stay up, as the Chilliwack's
    expect(h.result.current).toMatchObject({ state: "ready", basin: CHWK, vm: { title: "Chilliwack River watershed" } });
    expect(words(h.result.current)).toEqual(["Much below normal", "Much above normal", "No gauge reading"]);
    release!();
    await waitFor(() => expect((h.result.current as { basin?: Basin }).basin).toEqual(LFRA));
    const s = h.result.current as Extract<BasinDaysState, { state: "ready" }>;
    expect(s.vm.title).toBe("Lower Fraser watershed");
    expect(words(s)).toEqual(["Normal", "No gauge reading", "Normal"]);
    // from the first ready render on: never a loading/empty frame, and each render's days are its OWN group's
    for (const x of seen.slice(firstReady)) {
      expect(x.got.state).toBe("ready");
      const r = x.got as Extract<BasinDaysState, { state: "ready" }>;
      expect(r.vm.title).toBe(`${r.basin.name} watershed`);
      expect(words(r)).toEqual(r.basin.id === "CHWK" ? ["Much below normal", "Much above normal", "No gauge reading"]
                                                     : ["Normal", "No gauge reading", "Normal"]);
    }
  });

  it("(T417) no group on the map is said at once (nothing held over it); another quantity is a new question", async () => {
    let slow = false;
    const f = feed(() => (slow ? new Promise(() => {}) : Promise.resolve(IDX)));
    const h = renderHook(({ b, q }) => useBasinDays({ source, feed: f, basin: viewBasinAsk(b, null), for: "browse",
                                                       quantity: q, horizon: 0, enabled: true, now: TUE }),
                         { initialProps: { b: CHWK as Basin | null, q: "discharge" as "discharge" | "level" } });
    await waitFor(() => expect(h.result.current.state).toBe("ready"));
    h.rerender({ b: null, q: "discharge" });
    expect(h.result.current).toEqual({ state: "none", line: "No watershed group is on the map" });
    h.rerender({ b: CHWK, q: "discharge" });
    await waitFor(() => expect(h.result.current.state).toBe("ready"));
    // the discharge days never stand in for the level days
    slow = true;
    h.rerender({ b: CHWK, q: "level" });
    expect(h.result.current.state).toBe("loading");
  });
});
