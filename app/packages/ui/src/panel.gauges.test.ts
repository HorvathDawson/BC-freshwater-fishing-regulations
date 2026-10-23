/**
 * Owner 2026-10-21: "the gauges that say not reporting today should say when last report was if possible", and the
 * gauge switch belongs by the plot. Both are decided here, in @app/ui, from core's words — pinned whole (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import type { Panel, StationId } from "@app/data";
import { answerFrom, donorView, gaugeChoices, lastReportWanted, withLastReports } from "./panel";

const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true },
  { station: "STALE" as never, role: "up", areaKm2: 55, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true },
  { station: "RANKLESS" as never, role: "down", areaKm2: 52, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true },
  { station: "GONE" as never, role: "up", areaKm2: 58, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true },
] };

const INDEX = { stations: {
  LEAD: { percentile: 0.2, parameter: "discharge" as const, discharge: 0.2, observedAt: "2026-10-20T20:30:00-08:00" },
  STALE: { percentile: null, parameter: "discharge" as const, observedAt: "2026-10-17T09:00:00-08:00" },
  RANKLESS: { percentile: null, parameter: "discharge" as const, observedAt: "2026-10-20T20:35:00-08:00" },
} };

const DAY = new Date("2026-10-21T05:00:00Z");
const VAN = "America/Vancouver";
const view = (horizon: 0 | 3 = 0) => donorView(answerFrom(PANEL, INDEX, "discharge", horizon),
  { day: DAY, horizon, from: null, size: { width: 360, height: 210 }, selected: null, pickable: true,
    zone: VAN });

describe("a quiet gauge says when it last reported", () => {
  it("carries the feed's last observation onto the row, and nothing where the feed has none", () => {
    const rows = answerFrom(PANEL, INDEX).rows;
    expect(Object.fromEntries(rows.map((r) => [r.station, r.observedAt ?? null]))).toEqual({
      LEAD: "2026-10-20T20:30:00-08:00", STALE: "2026-10-17T09:00:00-08:00",
      RANKLESS: "2026-10-20T20:35:00-08:00", GONE: null });
  });

  it("a quiet row that still reports says its last report; the silent ones are named under the table (2026-10-22)", () => {
    const vm = view();
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.rows.map((r) => [r.station, r.reads, r.lastReport])).toEqual([
      ["LEAD", "Reads lower than 8 days in 10 at this time of year.", null],
      ["RANKLESS", "Reporting, but its reading cannot be ranked today.", "Last reported 2026-10-20 at 21:35 PDT."],
    ]);
    expect(vm.silent).toBe("Not reporting: STALE (last 17 Oct, 10:00 PDT) · GONE (last report not known)");
  });

  it("a gauge the index dropped takes its last report from its own file — and only a quiet one without one", () => {
    const rows = answerFrom(PANEL, INDEX).rows;
    const got = withLastReports(rows, new Map([["GONE", "2026-09-25T21:35:00-08:00"],
                                               ["LEAD", "1999-01-01T00:00:00Z"], ["STALE", "1999-01-01T00:00:00Z"]]));
    expect(Object.fromEntries(got.map((r) => [r.station, r.observedAt ?? null]))).toEqual({
      LEAD: "2026-10-20T20:30:00-08:00", STALE: "2026-10-17T09:00:00-08:00",
      RANKLESS: "2026-10-20T20:35:00-08:00", GONE: "2026-09-25T21:35:00-08:00" });
    expect(lastReportWanted(rows)).toEqual(["GONE"]);
    // Nothing to fill: the same rows, so a memo downstream does not churn.
    expect(withLastReports(rows, new Map())).toBe(rows);
    const vm = donorView({ ...answerFrom(PANEL, INDEX), rows: got },
      { day: DAY, horizon: 0, from: null, size: { width: 360, height: 210 }, selected: null, pickable: true,
        zone: VAN });
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.silent).toBe("Not reporting: STALE (last 17 Oct, 10:00 PDT) · GONE (last 25 Sep, 22:35 PDT)");
  });
});

describe("a gauge ECCC does not list as real-time (index.json `listed`)", () => {
  // GONE is a panel member but not on ECCC's real-time list this run; STALE is listed and silent.
  const LISTED = { ...INDEX, listed: ["LEAD", "RANKLESS", "STALE"] };
  const at = (index: typeof INDEX & { listed?: string[] }) => donorView(answerFrom(PANEL, index),
    { day: DAY, horizon: 0, from: null, size: { width: 360, height: 210 }, selected: null, pickable: false,
      zone: VAN });

  it("marks only the unlisted member, and only when the index carries a list", () => {
    expect(Object.fromEntries(answerFrom(PANEL, LISTED).rows.map((r) => [r.station, r.realtime ?? null])))
      .toEqual({ LEAD: null, STALE: null, RANKLESS: null, GONE: false });
    expect(answerFrom(PANEL, INDEX).rows.map((r) => r.realtime ?? null)).toEqual([null, null, null, null]);
  });

  it("says 'not a real-time gauge' for the unlisted member and 'not reporting' for a listed silent one", () => {
    const vm = at(LISTED);
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.rows.map((r) => [r.station, r.reads, r.label])).toEqual([
      ["LEAD", "Reads lower than 8 days in 10 at this time of year.",
       "LEAD, upstream, same size, 100 per cent of the answer"],
      ["RANKLESS", "Reporting, but its reading cannot be ranked today.",
       "RANKLESS, downstream, same size, reporting but not ranked today, so it counts for nothing"],
    ]);
    expect(vm.silent).toBe("Not reporting: STALE (last 17 Oct, 10:00 PDT) · GONE (not a real-time gauge)");
  });

  it("an older index without `listed` keeps today's wording", () => {
    const vm = at(INDEX);
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.rows.map((r) => [r.station, r.reads])).toEqual([
      ["LEAD", "Reads lower than 8 days in 10 at this time of year."],
      ["RANKLESS", "Reporting, but its reading cannot be ranked today."],
    ]);
    expect(vm.silent).toBe("Not reporting: STALE (last 17 Oct, 10:00 PDT) · GONE (last report not known)");
  });
});

describe("the chart's gauge switcher", () => {
  it("lists every donor in weight order, the charted one on, with the share the list says", () => {
    const rows = answerFrom(PANEL, INDEX).rows;
    expect(gaugeChoices(rows, "STALE" as StationId).map((g) => [g.station, g.share, g.on, g.quiet, g.row, g.label]))
      .toEqual([
        ["LEAD", "100%", false, false, 0, "Chart LEAD, 100% of this estimate"],
        ["STALE", "quiet", true, true, 1, "Showing STALE, not counted today"],
        ["RANKLESS", "quiet", false, true, 2, "Chart RANKLESS, not counted today"],
        ["GONE", "quiet", false, true, 3, "Chart GONE, not counted today"],
      ]);
  });

  it("offers nothing when there is nothing to switch between", () => {
    const one = answerFrom({ ...PANEL, members: PANEL.members.slice(0, 1) }, INDEX).rows;
    expect(gaugeChoices(one, "LEAD" as StationId)).toEqual([]);
    expect(gaugeChoices([], null)).toEqual([]);
  });
});
