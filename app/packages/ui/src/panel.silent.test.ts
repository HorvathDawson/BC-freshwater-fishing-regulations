/**
 * SILENT MEMBERS ARE HIDDEN, AND NAMED (owner 2026-10-22: "yes they should be hidden but mentioned with a last data
 * time"). A panel holds up to eight members per quantity; one with no reading at the horizon is not a row in the gauge
 * table and not a pin on its route map. One line under the table names them, each with its last data time.
 *
 * Whole values (AGENTS 33): the rows, pins, routes and keys are compared as whole lists, the line as its whole string,
 * in a fixed zone against a fixed day.
 */
import { describe, expect, it } from "vitest";
import type { DonorRow, Panel, PanelAnswer } from "@app/data";
import { answerFrom, donorView, withLastReports } from "./panel";

const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: 40, regulated: false, sameRiver: true },
  { station: "STALE" as never, role: "up", areaKm2: 55, years: 30, regulated: false, sameRiver: true },
  { station: "SECOND" as never, role: "down", areaKm2: 52, years: 30, regulated: false, sameRiver: true },
  { station: "COAL" as never, role: "up", areaKm2: 58, years: 30, regulated: false, sameRiver: true },
  { station: "RANKLESS" as never, role: "down", areaKm2: 51, years: 30, regulated: false, sameRiver: true },
  { station: "GONE" as never, role: "up", areaKm2: 57, years: 30, regulated: false, sameRiver: true },
] };

// LEAD and SECOND read; RANKLESS reports with nothing to rank; STALE last spoke on the 14th; COAL is not on ECCC's
// real-time list; GONE is not in the index at all.
const INDEX = {
  listed: ["LEAD", "STALE", "SECOND", "RANKLESS", "GONE"],
  stations: {
    LEAD: { percentile: 0.2, parameter: "discharge" as const, discharge: 0.2, observedAt: "2026-10-20T20:30:00-08:00" },
    SECOND: { percentile: 0.3, parameter: "discharge" as const, discharge: 0.3, observedAt: "2026-10-20T20:00:00-08:00" },
    STALE: { percentile: null, parameter: "discharge" as const, observedAt: "2026-10-14T08:00:00-08:00" },
    RANKLESS: { percentile: null, parameter: "discharge" as const, observedAt: "2026-10-20T20:35:00-08:00" },
  },
};

const DAY = new Date("2026-10-21T05:00:00Z");              // 2026-10-20, 22:00 in Vancouver
const VAN = "America/Vancouver";
const LAT: Record<string, number> = { LEAD: 49.2, STALE: 49.3, SECOND: 49.0, COAL: 49.4, RANKLESS: 49.1, GONE: 49.5 };
const NAME: Record<string, string> = { LEAD: "Lead Creek", STALE: "Liard River at Upper Crossing",
  SECOND: "Second Creek", COAL: "Coal River", RANKLESS: "Rankless Creek", GONE: "Gone Creek" };

/** Every member placed on the map with its own one-reach route, as `usePanelRoutes` hands them over. */
function routed(a: PanelAnswer): PanelAnswer {
  return { ...a, routesReady: true, rows: a.rows.map((r): DonorRow => ({ ...r, route: {
    station: r.station, name: NAME[r.station]!, lat: LAT[r.station]!, lon: -121.5, role: r.role,
    path: [`${r.station}:1`] as never } })) };
}

const opts = (horizon: 0 | 3 = 0) => ({ day: DAY, horizon, from: null, size: { width: 360, height: 220 },
                                        selected: null, pickable: false, zone: VAN });
const answer = (horizon: 0 | 3 = 0) => routed(answerFrom(PANEL, INDEX, "discharge", horizon));

describe("a silent panel member", () => {
  it("is not a row: the table holds only the members that speak at this horizon", () => {
    const vm = donorView(answer(), opts());
    if (!vm.ok) throw new Error("expected an estimate");
    // Weight order is LEAD, SECOND, then the weight-0 members in panel order: STALE, COAL, RANKLESS, GONE.
    expect(answer().rows.map((r) => r.station)).toEqual(["LEAD", "SECOND", "STALE", "COAL", "RANKLESS", "GONE"]);
    expect(vm.rows.map((r) => [r.station, r.row, r.quiet, r.reads])).toEqual([
      ["LEAD", 0, false, "Reads lower than 8 days in 10 at this time of year."],
      ["SECOND", 1, false, "Reads lower than 7 days in 10 at this time of year."],
      ["RANKLESS", 4, true, "Reporting, but its reading cannot be ranked today."],
    ]);
  });

  it("is not a pin, a route or a key on the route map — and the map's rows are the table's rows", () => {
    const vm = donorView(answer(), opts());
    if (!vm.ok || !vm.map) throw new Error("expected a map");
    expect(vm.map.pins.map((p) => [p.row, p.lat, p.label])).toEqual([
      [0, 49.2, "54%"], [1, 49.0, "46%"], [4, 49.1, "reporting, not ranked"]]);
    expect(vm.map.keys.map((k) => k.label)).toEqual(["LEAD · 54%", "SECOND · 46%", "RANKLESS · reporting, not ranked"]);
    expect(vm.map.routes.map((r) => [r.row, r.path])).toEqual([
      [0, ["LEAD:1"]], [1, ["SECOND:1"]], [4, ["RANKLESS:1"]]]);
    expect(vm.map.keys.map((k) => k.row)).toEqual([0, 1, 4]);
    expect(vm.map.chain).toEqual(["LEAD:1", "SECOND:1", "RANKLESS:1"]);
    expect(vm.map.bbox).toEqual([-121.5, 49.0, -121.5, 49.2]);
    expect(vm.map.unplaced).toBeNull();
    // COLOUR MATCH: the tone index a row is drawn in is the tone index of its pin and of its route.
    const pinRow = new Map(vm.map.pins.map((p) => [p.lat, p.row]));
    expect(vm.rows.map((r) => r.row)).toEqual(vm.rows.map((r) => pinRow.get(LAT[r.station]!)));
  });

  it("is named under the table with its last data time — one line, the whole string", () => {
    const vm = donorView(answer(), opts());
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.silent).toBe("Not reporting: Liard River at Upper Crossing (last 14 Oct, 09:00 PDT) · "
      + "Coal River (not a real-time gauge) · Gone Creek (last report not known)");
  });

  it("takes its last data from its own file, and a file with nothing says the station never reported", () => {
    const a = answer();
    const vm = donorView({ ...a, rows: withLastReports(a.rows, new Map([["GONE", null], ["COAL", "2024-03-03"]])) },
                         opts());
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.silent).toBe("Not reporting: Liard River at Upper Crossing (last 14 Oct, 09:00 PDT) · "
      + "Coal River (not a real-time gauge; last 3 Mar 2024) · Gone Creek (has never reported)");
  });

  it("a file that could not be read (404, offline) is not known — never \"has never reported\"", () => {
    const a = answer();
    const rows = withLastReports(a.rows, new Map([["GONE", undefined]]));
    expect(rows).toBe(a.rows);
    const vm = donorView({ ...a, rows }, opts());
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.silent).toBe("Not reporting: Liard River at Upper Crossing (last 14 Oct, 09:00 PDT) · "
      + "Coal River (not a real-time gauge) · Gone Creek (last report not known)");
  });

  it("on a day ahead, a member with no forecast is silent however fresh its last report", () => {
    // +3: only LEAD carries a forecast.
    const ahead = { ...INDEX, stations: { ...INDEX.stations,
      LEAD: { ...INDEX.stations.LEAD, ahead: { "3": { percentile: 0.4, discharge: 0.4 } } } } };
    const vm = donorView(routed(answerFrom(PANEL, ahead as never, "discharge", 3)), opts(3));
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.rows.map((r) => [r.station, r.row])).toEqual([["LEAD", 0]]);
    // Weight order: LEAD, then every weight-0 member in panel order.
    expect(vm.silent).toBe("No forecast this far ahead: Liard River at Upper Crossing (last 14 Oct, 09:00 PDT) · "
      + "Second Creek (last 20 Oct, 21:00 PDT) · Coal River (not a real-time gauge) · "
      + "Rankless Creek (last 20 Oct, 21:35 PDT) · Gone Creek (last report not known)");
  });

  it("when every member is silent: today's no-record refusal, and the line naming all of them", () => {
    const quiet = { listed: INDEX.listed, stations: { STALE: INDEX.stations.STALE } };
    const vm = donorView(routed(answerFrom(PANEL, quiet, "discharge", 0)), opts());
    expect(vm).toEqual({
      ok: false, heading: "NO ESTIMATE",
      reason: "None of the gauges that speak for this spot has a reading today that can be ranked against its record.",
      horizons: null,
      silent: "Not reporting: Lead Creek (last report not known) · Liard River at Upper Crossing (last 14 Oct, 09:00 PDT) · "
        + "Second Creek (last report not known) · Coal River (not a real-time gauge) · "
        + "Rankless Creek (last report not known) · Gone Creek (last report not known)",
    });
  });

  it("a refusal with no members says no line at all", () => {
    expect(donorView(answerFrom(undefined, INDEX), opts())).toEqual({
      ok: false, heading: "NO ESTIMATE", reason: "No gauge on this water is close enough in size to speak for it.",
      horizons: null, silent: null });
  });

  it("MUTATION PIN: a silent member drawn as a row would be caught — every row speaks, every silent one is named", () => {
    const vm = donorView(answer(), opts());
    if (!vm.ok) throw new Error("expected an estimate");
    const shown = new Set(vm.rows.map((r) => r.station));
    for (const s of ["STALE", "COAL", "GONE"]) {
      expect(shown.has(s)).toBe(false);
      expect(vm.silent).toContain(NAME[s]!);
    }
    for (const s of ["LEAD", "SECOND", "RANKLESS"]) expect(vm.silent).not.toContain(NAME[s]!);
    expect(vm.rows.length + 3).toBe(answer().rows.length);
  });
});
