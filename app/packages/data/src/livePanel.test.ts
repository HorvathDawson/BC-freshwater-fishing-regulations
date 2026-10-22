/**
 * ESTIMATES ARE CHOSEN LIVE (owner 2026-10-22, decisions.md "estimates use every gauge that could inform a section,
 * chosen live"): "make sure the section gauge percentage in prediction computes for all gauges we have possible so that
 * even if some drop/get added in that list it is still able to make a prediction using all available gauges".
 *
 * The panel the bundle ships is a RANKED RESERVE — every station with a record that could describe the section, live
 * or not. Which of them answers is decided here, at read time, from the feed: the members reporting the asked quantity,
 * their weights re-normalised among them; the own-gauge lead goes to the best-ranked own gauge reporting now. A
 * station joining or leaving ECCC's real-time list moves the answer on the next feed, with no rebuild. Nothing outside
 * the panel is ever borrowed, and a level never stands in for a flow.
 */
import { describe, expect, it } from "vitest";
import { CORE_MEMBERS, estimate, weightFor } from "@app/core";
import type { Panel, StationId } from "./index";
import { answerFrom, contributionsOf, panelStanding } from "./panel";

const st = (s: string) => s as StationId;
const m = (station: string, areaKm2: number, years: number, extra: { lead?: number; sameRiver?: boolean } = {}) =>
  ({ station: st(station), role: "up" as const, areaKm2, years, regulated: false, sameRiver: extra.sameRiver ?? true,
     ...(extra.lead ? { lead: extra.lead } : {}) });

/** A 100 km² reach with three donors: A same size (weight 1), B 20x bigger, C 5x smaller on a tributary. */
const PANEL: Panel = { areaKm2: 100, members: [m("A", 100, 40), m("B", 2000, 40), m("C", 20, 40, { sameRiver: false })] };
const W = { A: weightFor(1, "up", 40), B: weightFor(100 / 2000, "up", 40), C: weightFor(20 / 100, "up", 40, false) };

type Row = { percentile: number; parameter?: "discharge" | "level"; discharge?: number; level?: number };
const flow = (p: number): Row => ({ percentile: p, parameter: "discharge", discharge: p });
const feed = (stations: Record<string, Row>) => ({ stations });

const shares = (p: Panel, idx: ReturnType<typeof feed>, q: "discharge" | "level" = "discharge") =>
  answerFrom(p, idx, q).rows.map((r) => [r.station, Number(r.weight.toFixed(4))]);
const round4 = (x: number) => Number(x.toFixed(4));

describe("a member going silent", () => {
  it("re-weights the members still reporting among themselves", () => {
    const all = feed({ A: flow(0.3), B: flow(0.5), C: flow(0.7) });
    const tot = W.A + W.B + W.C;
    expect(shares(PANEL, all)).toEqual([["A", round4(W.A / tot)], ["B", round4(W.B / tot)], ["C", round4(W.C / tot)]]);
    const aQuiet = feed({ B: flow(0.5), C: flow(0.7) });
    expect(shares(PANEL, aQuiet)).toEqual([["B", round4(W.B / (W.B + W.C))], ["C", round4(W.C / (W.B + W.C))],
                                           ["A", 0]]);
  });

  it("answers exactly as a panel that never held it would", () => {
    const aQuiet = feed({ B: flow(0.5), C: flow(0.7) });
    const without: Panel = { ...PANEL, members: PANEL.members.filter((x) => x.station !== "A") };
    expect(answerFrom(PANEL, aQuiet).answer).toEqual(answerFrom(without, aQuiet).answer);
    expect(panelStanding(PANEL, aQuiet, "discharge", 0)).toBe(panelStanding(without, aQuiet, "discharge", 0));
  });
});

describe("a new transmitter in the panel", () => {
  it("takes its share on the next feed, from the same panel, with no rebuild", () => {
    const before = feed({ B: flow(0.5) });
    expect(shares(PANEL, before)).toEqual([["B", 1], ["A", 0], ["C", 0]]);
    const after = feed({ A: flow(0.3), B: flow(0.5) });
    expect(shares(PANEL, after)).toEqual([["A", round4(W.A / (W.A + W.B))], ["B", round4(W.B / (W.A + W.B))],
                                          ["C", 0]]);
    const a = answerFrom(PANEL, after).answer;
    expect(a).toEqual(estimate(100, contributionsOf(PANEL, after, "discharge", 0)));
    expect(a.ok && a.value.donors).toBe(2);
  });
});

describe("the own gauge's lead", () => {
  const OWN: Panel = { areaKm2: 100, members: [m("O1", 100, 40, { lead: 1 }), m("O2", 120, 40, { lead: 2 }),
                                               m("AUX", 2000, 40, { sameRiver: false })] };

  it("goes to the best-ranked own gauge reporting now", () => {
    expect(shares(OWN, feed({ O1: flow(0.2), O2: flow(0.4), AUX: flow(0.6) }))[0]).toEqual(["O1", 0.7]);
  });

  it("moves to the next own gauge when the first goes silent", () => {
    const wO2 = weightFor(100 / 120, "up", 40), wAux = weightFor(100 / 2000, "up", 40, false);
    expect(wO2 / (wO2 + wAux)).toBeGreaterThan(0.7);    // O2 already carries more than the floor on its own merits
    expect(shares(OWN, feed({ O2: flow(0.4), AUX: flow(0.6) })))
      .toEqual([["O2", round4(wO2 / (wO2 + wAux))], ["AUX", round4(wAux / (wO2 + wAux))], ["O1", 0]]);
    // a three-year O2 counts 3/20 on its own merits: leading raises it to the floor
    const lowered: Panel = { ...OWN, members: [OWN.members[0]!, m("O2", 120, 3, { lead: 2 }), OWN.members[2]!] };
    expect(shares(lowered, feed({ O2: flow(0.4), AUX: flow(0.6) }))).toEqual([["O2", 0.7], ["AUX", 0.3], ["O1", 0]]);
  });

  it("is nobody's when no own gauge reports: the usual weights", () => {
    expect(shares(OWN, feed({ AUX: flow(0.6) }))).toEqual([["AUX", 1], ["O1", 0], ["O2", 0]]);
  });
});

describe("a panel whose members are all silent", () => {
  it("says no-record — the gauges are there, none has a reading to rank — not no-station", () => {
    const got = answerFrom(PANEL, feed({}));
    expect(got.answer).toEqual({ ok: false, why: "no-record" });
    expect(got.rows.map((r) => [r.station, r.weight, r.percentile])).toEqual([["A", 0, null], ["B", 0, null],
                                                                              ["C", 0, null]]);
    expect(panelStanding(PANEL, feed({}), "discharge", 0)).toBe(-0.01);
  });

  it("never borrows a reporting station from outside its panel", () => {
    const outside = feed({ ELSEWHERE: flow(0.9) });
    expect(answerFrom(PANEL, outside).answer).toEqual({ ok: false, why: "no-record" });
    const b = feed({ B: flow(0.5), ELSEWHERE: flow(0.9) });
    expect(answerFrom(PANEL, b).answer).toEqual(answerFrom(PANEL, feed({ B: flow(0.5) })).answer);
  });

  it("never lets a level stand in for a flow: level-only reports leave Flow with no-record", () => {
    const levels = feed({ A: { percentile: 0.3, parameter: "level", level: 0.3 } });
    expect(answerFrom(PANEL, levels, "discharge").answer).toEqual({ ok: false, why: "no-record" });
    const lv = answerFrom(PANEL, levels, "level").answer;
    expect(lv.ok && Math.round(lv.value.percentile * 100)).toBe(30);
  });

  it("still says no-station where there is no panel at all", () => {
    expect(answerFrom(undefined, feed({ A: flow(0.3) })).answer).toEqual({ ok: false, why: "no-station" });
  });
});

/**
 * THE RESERVE NEVER SILENCES THE PANEL IT STANDS BEHIND (review 2026-10-22: the deeper reserve made 54 Level sections,
 * 384 km, lose an answer they had on the same feed — reserve members reporting, disagreeing, and the spread gate
 * refusing). The first `CORE_MEMBERS` of a panel are the panel as it stood before the reserve; its own disagreement is
 * still an honest refusal. A reserve member is there to answer, never to silence: when bringing the reserve in would
 * make the answer too uncertain while a core member reports, the lowest-weight reserve members stand back.
 */
describe("the reserve behind the core", () => {
  const trib = (station: string) => m(station, 100, 40, { sameRiver: false });
  // four core seats (A silent in every feed below), two reserve seats
  const CORE: Panel = { areaKm2: 100, members: [trib("A"), trib("B"), trib("C"), trib("D")] };
  const DEEP: Panel = { ...CORE, members: [...CORE.members, trib("R1"), trib("R2")] };
  const agreeing = { B: flow(0.45), C: flow(0.5), D: flow(0.55) };

  it("marks the members past the core as reserve", () => {
    expect(CORE_MEMBERS).toBe(4);
    const all = feed({ A: flow(0.5), ...agreeing, R1: flow(0.5), R2: flow(0.5) });
    expect(contributionsOf(DEEP, all, "discharge", 0).map((c) => c.reserve ?? false))
      .toEqual([false, false, false, false, true, true]);
  });

  it("stands back when it would silence the core: the answer is the core's", () => {
    const f = feed({ ...agreeing, R1: flow(0.02), R2: flow(0.98) });
    const core = answerFrom(CORE, f).answer;
    expect(core.ok).toBe(true);
    expect(answerFrom(DEEP, f).answer).toEqual(core);
    expect(panelStanding(DEEP, f, "discharge", 0)).toBe(panelStanding(CORE, f, "discharge", 0));
    // the table agrees with the number: the reserve that stood back carries nothing, and still shows its reading
    const rows = answerFrom(DEEP, f).rows;
    expect(rows.filter((r) => r.station === "R1" || r.station === "R2").map((r) => [r.station, r.weight, r.percentile]))
      .toEqual([["R1", 0, 0.02], ["R2", 0, 0.98]]);
    expect(round4(rows.reduce((a, r) => a + r.weight, 0))).toBe(1);
  });

  it("takes its share when it agrees", () => {
    const f = feed({ ...agreeing, R1: flow(0.5), R2: flow(0.52) });
    const got = answerFrom(DEEP, f).answer;
    expect(got.ok && got.value.donors).toBe(5);
  });

  it("is the panel where the core is silent: its own disagreement still refuses", () => {
    const f = feed({ R1: flow(0.02), R2: flow(0.98) });
    expect(answerFrom(DEEP, f).answer).toEqual({ ok: false, why: "too-uncertain" });
  });

  it("leaves the core's own disagreement a refusal, as before", () => {
    const f = feed({ B: flow(0.02), C: flow(0.98), D: flow(0.5) });
    expect(answerFrom(CORE, f).answer).toEqual({ ok: false, why: "too-uncertain" });
    expect(answerFrom(DEEP, f).answer).toEqual({ ok: false, why: "too-uncertain" });
  });

  it("is the same rule in estimate, given the reserve flag", () => {
    const cs = (ps: [number, boolean][]) => ps.map(([p, reserve]) => ({ percentile: p, role: "up" as const,
      areaKm2: 100, years: 40, sameRiver: false, ...(reserve ? { reserve } : {}) }));
    const core = estimate(100, cs([[0.45, false], [0.5, false], [0.55, false]]));
    expect(estimate(100, cs([[0.45, false], [0.5, false], [0.55, false], [0.02, true], [0.98, true]]))).toEqual(core);
    // unflagged, the same five refuse: the back-off is the reserve's alone
    expect(estimate(100, cs([[0.45, false], [0.5, false], [0.55, false], [0.02, false], [0.98, false]])))
      .toEqual({ ok: false, why: "too-uncertain" });
  });
});
