/**
 * ON THIS WATER, OR ESTIMATING FROM ANOTHER (T168). Whole values (AGENTS 33): every side, group and sentence is compared
 * whole. The fixture is Nicomen Slough's panel 4739 as the 2026-10-23 bundle holds it — its own 08MH085/086 (level
 * only, not on ECCC's real-time list) and eleven Fraser gauges — with the `lead` ranks the bundle gives the Fraser
 * gauges (3–8), which is why `lead` cannot be the test.
 *
 * Mutations (each run 2026-10-24, each failed the named test and was reverted):
 *   - `gaugeSide` returning "own" for any placed gauge (`if (g.item !== null) return "own"`) fails "the Fraser's
 *     gauges are donors on Nicomen Slough" and the count line.
 *   - `gaugeSide` dropping the section rule (`return "donor"` for an item-less gauge) fails "a gauge on no named water".
 *   - `groupGauges` folding on `state === "quiet"` only (unranked / not-real-time stay shown) fails "Nicomen's groups".
 *   - `quietWord` without the unranked clause fails the "or not ranked" lines.
 *   - `gaugeCountLine` counting quiet own gauges as "on this water" fails "the count line, every shape".
 */
import { describe, expect, it } from "vitest";
import { gaugeCountLine, gaugeSide, gaugeSides, groupGauges, quietWord, stateAt, type GaugePlace,
         type GaugeSide } from "./gaugeSide";
import type { DonorState } from "./conditions";

const NICOMEN = "gnis:21105", FRASER = "gnis:39325", VEDDER = "gnis:3062", CHILLIWACK = "gnis:8634";
const SEC = 59618;
/** `gauge.item_id` / `gauge.sid` for panel 4739's thirteen stations, as the bundle holds them. */
const PLACES = new Map<string, GaugePlace>([
  ["08MH085", { item: NICOMEN, section: 59618 }], ["08MH086", { item: NICOMEN, section: 59618 }],
  ["08KA004", { item: FRASER, section: 315450 }], ["08KB001", { item: FRASER, section: 315445 }],
  ["08ME019", { item: FRASER, section: 315415 }], ["08MF005", { item: FRASER, section: 315401 }],
  ["08MF035", { item: FRASER, section: 315396 }], ["08MF040", { item: FRASER, section: 315413 }],
  ["08MH024", { item: FRASER, section: 315391 }], ["08KA005", { item: FRASER, section: 315462 }],
  ["08MH126", { item: FRASER, section: 315388 }], ["08MH044", { item: FRASER, section: 315390 }],
  ["08MH071", { item: FRASER, section: 315392 }],
]);
const STATIONS = [...PLACES.keys()];

/** Each member's state on 2026-10-23 (the live feed then): five read, the rest as the panel showed them. */
const STATE: Record<string, DonorState["kind"]> = {
  "08MF005": "reads", "08KA004": "reads", "08KB001": "reads", "08MF040": "reads", "08KA005": "reads",
  "08MH085": "not-realtime", "08MH086": "not-realtime", "08ME019": "not-realtime", "08MH071": "not-realtime",
  "08MF035": "not-realtime", "08MH024": "unranked", "08MH126": "unranked", "08MH044": "unranked",
};
/** In the panel's weight order (the switch's order). */
const ORDER = ["08MF005", "08KA004", "08KB001", "08MF040", "08KA005", "08MH085", "08MH086", "08ME019", "08MH024",
               "08MH126", "08MH044", "08MH071", "08MF035"];

describe("gaugeSide", () => {
  it("the Fraser's gauges are donors on Nicomen Slough; its own two are on this water", () => {
    const sides = gaugeSides(STATIONS, { waters: [NICOMEN], section: SEC }, PLACES);
    expect([...sides].filter(([, s]) => s === "own").map(([st]) => st)).toEqual(["08MH085", "08MH086"]);
    expect([...sides].filter(([, s]) => s === "donor").length).toBe(11);
  });

  it("a continuation is one water: the Chilliwack's gauge is the Vedder's own (water_continue, both ways)", () => {
    const g = { item: CHILLIWACK, section: 1235779 };
    expect(gaugeSide(g, { waters: [CHILLIWACK, VEDDER].sort(), section: 1 })).toBe("own");
    expect(gaugeSide(g, { waters: [VEDDER], section: 1 })).toBe("donor");
  });

  it("a gauge on no named water is own only on the very section it stands on; an unknown place is a donor", () => {
    expect(gaugeSide({ item: null, section: 7 }, { waters: [], section: 7 })).toBe("own");
    expect(gaugeSide({ item: null, section: 7 }, { waters: [], section: 8 })).toBe("donor");
    expect(gaugeSide({ item: null, section: null }, { waters: [], section: null })).toBe("donor");
    expect(gaugeSide(undefined, { waters: [NICOMEN], section: SEC })).toBe("donor");
    // a named gauge on another water standing on this section's id is still another water's
    expect(gaugeSide({ item: FRASER, section: SEC }, { waters: [NICOMEN], section: SEC })).toBe("donor");
  });
});

describe("groupGauges", () => {
  const sides = gaugeSides(STATIONS, { waters: [NICOMEN], section: SEC }, PLACES);
  const side = (s: string): GaugeSide => sides.get(s)!;
  const state = (s: string) => STATE[s]!;

  it("Nicomen's groups: own first, every not-reporting member folded, the fold's words whole", () => {
    expect(groupGauges(ORDER, side, state)).toEqual([
      { side: "own", heading: "ON THIS WATER", shown: [], folded: ["08MH085", "08MH086"],
        fold: { label: "2 not reporting",
                closed: "2 gauges on this water not reporting. Show them",
                open: "2 gauges on this water not reporting. Hide them" } },
      { side: "donor", heading: "ESTIMATING FROM OTHER WATERS",
        shown: ["08MF005", "08KA004", "08KB001", "08MF040", "08KA005"],
        folded: ["08ME019", "08MH024", "08MH126", "08MH044", "08MH071", "08MF035"],
        fold: { label: "6 not reporting or not ranked",
                closed: "6 gauges from other waters not reporting or not ranked. Show them",
                open: "6 gauges from other waters not reporting or not ranked. Hide them" } },
    ]);
  });

  it("`keep` holds the chart's gauge in sight whatever its state; a group with nothing folded has no fold", () => {
    const g = groupGauges(ORDER, side, state, { keep: (s) => s === "08MH086" });
    expect(g[0]).toEqual({ side: "own", heading: "ON THIS WATER", shown: ["08MH086"], folded: ["08MH085"],
                           fold: { label: "1 not reporting", closed: "1 gauge on this water not reporting. Show them",
                                   open: "1 gauge on this water not reporting. Hide them" } });
    expect(groupGauges(["08MF005"], side, state)).toEqual([
      { side: "donor", heading: "ESTIMATING FROM OTHER WATERS", shown: ["08MF005"], folded: [], fold: null }]);
    expect(groupGauges([], side, state)).toEqual([]);
  });

  it("ahead, the folded are not counted this far ahead; under Change, they have no forecast running", () => {
    expect(groupGauges(["08MH085"], side, () => "no-forecast", { horizon: 3 })[0]!.fold!.label)
      .toBe("1 not counted this far ahead");
    expect(groupGauges(["08MH085"], side, () => "no-forecast", { horizon: "change" })[0]!.fold!.label)
      .toBe("1 with no forecast running");
  });
});

describe("the count line, every shape", () => {
  const all = (pairs: [GaugeSide, DonorState["kind"]][]) => pairs.map(([side, state]) => ({ side, state }));

  it("Nicomen Slough today", () => {
    const sides = gaugeSides(STATIONS, { waters: [NICOMEN], section: SEC }, PLACES);
    expect(gaugeCountLine(ORDER.map((s) => ({ side: sides.get(s)!, state: STATE[s]! }))))
      .toBe("No gauge on this water reporting · 5 estimating from other waters · 8 not reporting or not ranked");
  });

  it("own and donors reporting, some silent", () => {
    expect(gaugeCountLine(all([["own", "reads"], ["own", "reads"], ["donor", "reads"], ["donor", "quiet"]])))
      .toBe("2 gauges on this water · 1 estimating from another water · 1 not reporting");
  });

  it("a lake read at its own gauge", () => {
    expect(gaugeCountLine(all([["own", "reads"]]))).toBe("1 gauge on this water");
    expect(gaugeCountLine(all([["own", "quiet"]]))).toBe("No gauge on this water reporting · 1 not reporting");
  });

  it("no gauge on this water", () => {
    expect(gaugeCountLine(all([["donor", "reads"], ["donor", "reads"]])))
      .toBe("No gauge on this water · 2 estimating from other waters");
    expect(gaugeCountLine([])).toBeNull();
  });

  it("an own gauge reporting unranked is not called silent", () => {
    expect(gaugeCountLine(all([["own", "unranked"], ["donor", "reads"]])))
      .toBe("No gauge on this water ranked · 1 estimating from another water · 1 not reporting or not ranked");
  });

  it("ahead and under Change, in the horizon's words", () => {
    expect(gaugeCountLine(all([["own", "no-forecast"], ["donor", "reads"]]), 3))
      .toBe("No gauge on this water counted this far ahead · 1 estimating from another water · "
            + "1 not counted this far ahead");
    expect(gaugeCountLine(all([["own", "no-forecast"], ["donor", "reads"]]), "change"))
      .toBe("No gauge on this water with a forecast running · 1 estimating from another water · "
            + "1 with no forecast running");
  });
});

describe("quietWord and stateAt", () => {
  it("say the fold's word and read the state through donorState", () => {
    expect(quietWord(["quiet", "not-realtime"])).toBe("not reporting");
    expect(quietWord(["quiet", "unranked"])).toBe("not reporting or not ranked");
    expect(stateAt({ percentile: 0.4 })).toBe("reads");
    expect(stateAt({ percentile: null, realtime: false })).toBe("not-realtime");
    expect(stateAt({ percentile: null }, { horizon: 3 })).toBe("no-forecast");
  });
});
