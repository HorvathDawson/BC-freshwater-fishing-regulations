/**
 * (T377 item 1) The forecast-day list of one watershed group: browse asks the map centre's group, a water its own — never
 * one for the other; a failure is a failed state (shown), a place in no group is said, a day no gauge reads is said.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import { buildStandingClasses, type Basin, type BasinMember, type ItemId, type StationId,
         type WaterSource } from "@app/data";
import { centreBasinAsk, useBasinDays, type BasinDaysState } from "./basinDays";
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
  it("browse: the centre's group, each day in the standing words, a day no gauge reads said", async () => {
    const h = renderHook(() => useBasinDays({ source, feed: feed(), basin: centreBasinAsk(CHWK, null), for: "browse",
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
    const centre = renderHook(() => useBasinDays({ source, feed: feed(), basin: centreBasinAsk(undefined, new Error("no groups")),
                                                   for: "browse", quantity: "discharge", horizon: 0, enabled: true }));
    expect(centre.result.current).toMatchObject({ state: "failed", error: { message: "no groups" } });
    const water = renderHook(() => {
      const basin = useWaterBasin(source, { item: "gnis:2" as ItemId, tapSid: null, at: null });
      return useBasinDays({ source, feed: feed(), basin, for: "water", quantity: "discharge", horizon: 0, enabled: true });
    });
    await waitFor(() => expect(water.result.current.state).toBe("failed"));
    const bad = renderHook(() => useBasinDays({ source, feed: feed(async () => { throw new Error("feed 500"); }),
                                                basin: centreBasinAsk(CHWK, null), for: "browse", quantity: "discharge",
                                                horizon: 0, enabled: true }));
    await waitFor(() => expect(bad.result.current).toMatchObject({ state: "failed", error: { message: "feed 500" } }));
  });

  it("a place in no group is said (browse and water words), not loading and not blank", () => {
    const h = renderHook(() => useBasinDays({ source, feed: feed(), basin: centreBasinAsk(null, null), for: "browse",
                                              quantity: "discharge", horizon: 0, enabled: true }));
    expect(h.result.current).toEqual({ state: "none", line: "The map's centre is in no watershed group" });
    expect(centreBasinAsk(undefined, null).state).toBe("loading");
  });

  it("MUTATION PIN: the list follows the group asked — a new group never shows the last group's days", async () => {
    let release: (() => void) | null = null;
    let slow = false;
    const f = feed(() => (slow ? new Promise((r) => { release = () => r(IDX); }) : Promise.resolve(IDX)));
    // EVERY RENDER is checked, not just the settled one: the render before the new question's effect runs is the one
    // that could show the last group's days
    const seen: { asked: Basin; got: BasinDaysState }[] = [];
    const h = renderHook(({ b }) => {
      const got = useBasinDays({ source, feed: f, basin: centreBasinAsk(b, null), for: "browse",
                                 quantity: "discharge", horizon: 0, enabled: true, now: TUE });
      seen.push({ asked: b, got });
      return got;
    }, { initialProps: { b: CHWK } });
    await waitFor(() => expect(h.result.current.state).toBe("ready"));
    expect((h.result.current as { basin: Basin }).basin).toEqual(CHWK);
    slow = true;
    h.rerender({ b: LFRA });
    // the Lower Fraser's days are not in yet: loading — never the Chilliwack's days under the Lower Fraser's name
    expect(h.result.current.state).toBe("loading");
    release!();
    await waitFor(() => expect(h.result.current.state).toBe("ready"));
    const s = h.result.current as Extract<BasinDaysState, { state: "ready" }>;
    expect(s.basin).toEqual(LFRA);
    expect(s.vm.title).toBe("Lower Fraser watershed");
    expect(words(s)).toEqual(["Normal", "No gauge reading", "Normal"]);
    const lfra = seen.filter((x) => x.asked === LFRA && x.got.state === "ready");
    expect(lfra.length).toBeGreaterThan(0);
    for (const x of lfra) expect(words(x.got)).toEqual(["Normal", "No gauge reading", "Normal"]);
    for (const x of seen) if (x.got.state === "ready") expect(x.got.basin).toBe(x.asked);
  });
});
