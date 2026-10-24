/**
 * (T168) THE DONOR PANEL, SWITCH AND KEY GROUPED: on this water first, then the donors; each group's not-reporting
 * members (core `donorState` not `reads`) under a closed fold. Whole values (AGENTS 33): the groups' stations, tones,
 * headings and fold words are compared whole, in a fixed zone against a fixed day.
 *
 * Mutations (each run 2026-10-24, each failed the named test and was reverted):
 *   - `donorView` grouping `shown` (the speaking rows) instead of every member: the silent own gauge STALE vanishes
 *     from its fold — fails "the rows group".
 *   - `gaugeChoiceGroups` without `keep: (c) => c.on`: the chart's quiet gauge folds away — fails "the switch".
 *   - `keyGroups` without `keep: () => true`: RANKLESS's key folds while its pin is drawn — fails "the key".
 */
import { describe, expect, it } from "vitest";
import type { DonorRow, Panel, PanelAnswer, StationId } from "@app/data";
import { answerFrom, donorView, gaugeChoiceGroups, type GaugeSides } from "./panel";

const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true },
  { station: "STALE" as never, role: "up", areaKm2: 55, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true },
  { station: "SECOND" as never, role: "down", areaKm2: 52, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true },
  { station: "COAL" as never, role: "up", areaKm2: 58, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true },
  { station: "RANKLESS" as never, role: "down", areaKm2: 51, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true },
] };
// LEAD and SECOND read; RANKLESS reports unranked; STALE last spoke on the 14th; COAL is not real-time.
const INDEX = {
  listed: ["LEAD", "STALE", "SECOND", "RANKLESS"],
  stations: {
    LEAD: { percentile: 0.2, parameter: "discharge" as const, discharge: 0.2, observedAt: "2026-10-20T20:30:00-08:00" },
    SECOND: { percentile: 0.3, parameter: "discharge" as const, discharge: 0.3, observedAt: "2026-10-20T20:00:00-08:00" },
    STALE: { percentile: null, parameter: "discharge" as const, observedAt: "2026-10-14T08:00:00-08:00" },
    RANKLESS: { percentile: null, parameter: "discharge" as const, observedAt: "2026-10-20T20:35:00-08:00" },
  },
};
const DAY = new Date("2026-10-21T05:00:00Z");
const VAN = "America/Vancouver";
const LAT: Record<string, number> = { LEAD: 49.2, STALE: 49.3, SECOND: 49.0, COAL: 49.4, RANKLESS: 49.1 };

function routed(a: PanelAnswer): PanelAnswer {
  return { ...a, routesReady: true, rows: a.rows.map((r): DonorRow => ({ ...r, route: {
    station: r.station, name: `${r.station} Creek`, lat: LAT[r.station]!, lon: -121.5, role: r.role,
    path: [`${r.station}:1`] as never } })) };
}
// LEAD and STALE stand on this water; SECOND, COAL and RANKLESS are donors
const SIDES: GaugeSides = new Map([["LEAD", "own"], ["STALE", "own"], ["SECOND", "donor"], ["COAL", "donor"],
                                    ["RANKLESS", "donor"]]);
const opts = (sides: GaugeSides) => ({ day: DAY, horizon: 0 as const, from: null, size: { width: 360, height: 220 },
                                       selected: null, pickable: false, zone: VAN, sides });
const answer = () => routed(answerFrom(PANEL, INDEX, "discharge", 0));

describe("(T168) on this water first", () => {
  it("the rows group: own first, each group's not-reporting members folded, as rows with their own words", () => {
    const vm = donorView(answer(), opts(SIDES));
    if (!vm.ok || !vm.groups) throw new Error("expected groups");
    expect(vm.groups.map((g) => ({ side: g.side, heading: g.heading,
                                   shown: g.shown.map((r) => [r.station, r.row, r.quiet]),
                                   folded: g.folded.map((r) => [r.station, r.row, r.quiet, r.reads, r.lastReport]),
                                   fold: g.fold }))).toEqual([
      { side: "own", heading: "ON THIS WATER", shown: [["LEAD", 0, false]],
        folded: [["STALE", 2, true, "Not reporting today.", "Last reported 2026-10-14 at 09:00 PDT."]],
        fold: { label: "1 not reporting", closed: "1 gauge on this water not reporting. Show them",
                open: "1 gauge on this water not reporting. Hide them" } },
      { side: "donor", heading: "ESTIMATING FROM OTHER WATERS", shown: [["SECOND", 1, false]],
        folded: [["COAL", 3, true, "Not a real-time gauge.", "When it last reported is not known."],
                 ["RANKLESS", 4, true, "Reporting, but its reading cannot be ranked today.",
                  "Last reported 2026-10-20 at 21:35 PDT."]],
        fold: { label: "2 not reporting or not ranked",
                closed: "2 gauges from other waters not reporting or not ranked. Show them",
                open: "2 gauges from other waters not reporting or not ranked. Hide them" } },
    ]);
  });

  it("without sides (a saved spot, or still reading) nothing groups and the rows are as before", () => {
    const vm = donorView(answer(), opts(null));
    if (!vm.ok || !vm.map) throw new Error("expected an estimate");
    expect(vm.groups).toBeNull();
    expect(vm.map.keyGroups).toBeNull();
    expect(vm.rows.map((r) => r.station)).toEqual(["LEAD", "SECOND", "RANKLESS"]);
    expect(vm.map.pins.map((p) => [p.row, p.rank])).toEqual([[0, 0], [1, 1], [4, 4]]);
  });

  it("the key: own first, every drawn pin's key in sight (none folds), labels own first on a crowded map", () => {
    const vm = donorView(answer(), opts(new Map([...SIDES!, ["SECOND", "own"], ["LEAD", "donor"]])));
    if (!vm.ok || !vm.map?.keyGroups) throw new Error("expected key groups");
    expect(vm.map.keyGroups).toEqual([
      { side: "own", heading: "ON THIS WATER", shown: [{ row: 1, label: "SECOND · 46%" }], folded: [], fold: null },
      { side: "donor", heading: "ESTIMATING FROM OTHER WATERS",
        shown: [{ row: 0, label: "LEAD · 54%" }, { row: 4, label: "RANKLESS · reporting, not ranked" }],
        folded: [], fold: null },
    ]);
    expect(vm.map.keys.map((k) => k.label)).toEqual(["SECOND · 46%", "LEAD · 54%", "RANKLESS · reporting, not ranked"]);
    expect(vm.map.pins.map((p) => [p.row, p.rank])).toEqual([[0, 1], [1, 0], [4, 2]]);
  });

  it("the switch: grouped the same way, the chart's gauge kept in sight though it is quiet", () => {
    const rows = answer().rows;
    const g = gaugeChoiceGroups(rows, "STALE" as StationId, { ref: DAY.getTime(), horizon: 0 }, SIDES);
    expect(g?.map((x) => ({ side: x.side, shown: x.shown.map((c) => [c.station, c.on, c.share]),
                            folded: x.folded.map((c) => c.station), fold: x.fold?.label ?? null }))).toEqual([
      { side: "own", shown: [["LEAD", false, "54%"], ["STALE", true, "not reporting"]], folded: [], fold: null },
      { side: "donor", shown: [["SECOND", false, "46%"]], folded: ["COAL", "RANKLESS"],
        fold: "2 not reporting or not ranked" },
    ]);
    expect(gaugeChoiceGroups(rows, null, {}, null)).toBeNull();
  });
});
