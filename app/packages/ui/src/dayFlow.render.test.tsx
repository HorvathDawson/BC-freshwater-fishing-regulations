/** T170: what the spot sheet says about the visit day's flow (whole), and the action: asks, waits offline, asks again. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { DayFlowAnswer, Spot } from "@app/data/spots";
import { dayFlowView, useDayFlow, type DayFlowView } from "./dayFlow";
import type { Connectivity } from "./imagery";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });

const VISIT = new Date(2024, 7, 15, 9, 20).getTime();
const SAVED = new Date(2026, 9, 23, 11).getTime();
const backdated: Spot = {
  id: "s1", createdAt: SAVED, updatedAt: SAVED, visitedAt: VISIT, lat: 49.1, lon: -121.96, item: null,
  waterName: "Chilliwack River", title: "", notes: "", photos: [], trace: null, weather: null,
  reading: { station: "08MH001", discharge: 11.2, level: 1.38, percentile: 0.05, parameter: "discharge",
             at: new Date(2026, 9, 23, 10, 20).toISOString() },
  panel: { answer: { ok: false, why: "no-record" }, areaKm2: 100, routesReady: false, rows: [] } as never,
};

describe("dayFlowView", () => {
  it("a backdated spot never asked: the action (the save-day label is gone, Q161)", () => {
    expect(dayFlowView(backdated, "idle")).toEqual({
      heading: "Flow that day", line: null, status: null,
      action: { label: "Get the flow for that day", busy: false },
    });
  });

  it("filled: the day's flow said whole, and no action", () => {
    const s: Spot = { ...backdated, dayFlow: { state: "filled", station: "08MH001", date: "2024-08-15", discharge: 19.6,
                                               percentile: 0.244, place: 0.244, source: "hydat", filledAt: SAVED } };
    expect(dayFlowView(s, "idle")).toEqual({
      heading: "Flow that day", status: null, action: null,
      line: "19.6 m³/s, the 24th percentile for the date — filled in from the day's record (HYDAT daily mean, "
        + "15 Aug 2024).",
    });
  });

  it("not published yet: said, with the action to try again; offline and asking are said too", () => {
    const s: Spot = { ...backdated, dayFlow: { state: "not-published", station: "08MH001", date: "2024-08-15",
                                               askedAt: SAVED } };
    const v = dayFlowView(s, "offline")!;
    expect([v.line, v.action, v.status]).toEqual([
      "The flow for 15 Aug 2024 is not published yet: it is older than ECCC's recent record and not in HYDAT yet. "
        + "Try again later.",
      { label: "Get the flow for that day", busy: false },
      "Offline: the flow for that day will be asked for when you are back online."]);
    expect(dayFlowView(s, "asking")!.action).toEqual({ label: "Get the flow for that day", busy: true });
  });

  it("nothing to say on a spot whose reading is the day's, or with no station", () => {
    const same: Spot = { ...backdated, visitedAt: SAVED, createdAt: SAVED };
    const none: Spot = { ...backdated, reading: null, leadUp: null };
    expect([dayFlowView(same, "idle"), dayFlowView(none, "idle")]).toEqual([null, null]);
  });
});

function Probe({ spot, ask, conn, out }: { spot: Spot; ask: (s: Spot) => Promise<DayFlowAnswer | null>;
                                           conn: Connectivity; out: { v?: DayFlowView | null; onAsk?: (() => void) | null } }) {
  const r = useDayFlow(spot, ask, conn);
  out.v = r.view;
  out.onAsk = r.onAsk;
  return null;
}

describe("useDayFlow — offline waits, and asks again when back", () => {
  it("offline: nothing asked, said; back online: asked by itself", async () => {
    let up = false;
    const subs: ((u: boolean) => void)[] = [];
    const conn: Connectivity = { watches: true, probeWhileUp: false, online: () => up,
                                 subscribe: (on) => { subs.push(on); return () => { subs.splice(subs.indexOf(on), 1); }; } };
    const asked: string[] = [];
    const ask = async (s: Spot): Promise<DayFlowAnswer> => {
      asked.push(s.id);
      return { kind: "flow", discharge: 19.6, percentile: 0.244, place: 0.244, source: "hydat" };
    };
    const out: { v?: DayFlowView | null; onAsk?: (() => void) | null } = {};
    render(<Probe spot={backdated} ask={ask} conn={conn} out={out} />);
    await settle();
    act(() => out.onAsk!());
    await settle();
    expect([asked, out.v!.status]).toEqual([[], "Offline: the flow for that day will be asked for when you are back online."]);
    up = true;
    act(() => subs.forEach((f) => f(true)));
    await settle();
    expect([asked, out.v!.status]).toEqual([["s1"], null]);
  });

  it("an answer that could not reach HYDAT is offline too; a day not over says try tomorrow", async () => {
    const conn: Connectivity = { watches: true, probeWhileUp: false, online: () => true, subscribe: () => () => {} };
    const out: { v?: DayFlowView | null; onAsk?: (() => void) | null } = {};
    const { rerender } = render(<Probe spot={backdated} ask={async () => ({ kind: "offline" })} conn={conn} out={out} />);
    await settle();
    act(() => out.onAsk!());
    await settle();
    expect(out.v!.status).toBe("Offline: the flow for that day will be asked for when you are back online.");
    rerender(<Probe spot={backdated} ask={async () => ({ kind: "pending" })} conn={conn} out={out} />);
    act(() => out.onAsk!());
    await settle();
    expect(out.v!.status).toBe("That day is not over in the gauge's record yet. Try again tomorrow.");
  });
});
