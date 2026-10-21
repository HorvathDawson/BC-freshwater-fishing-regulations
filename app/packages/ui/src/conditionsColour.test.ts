/**
 * THE CONDITIONS COLOURING'S VALUES (2026-10-21): what each renderer is handed, whole. The per-tile map's table is one
 * value per class in the ramp's units — and each class's value is exactly the standing a section of it gets on the
 * pushed path, through the one arithmetic (`sectionStanding`); the pushed path's data is what the Shell used to build.
 */
import { describe, expect, it } from "vitest";
import { buildStandingClasses, sectionStanding, type PanelMember, type SectionId, type StationId } from "@app/data";
import { classMarks, pushedStandings, standingValues } from "./conditionsColour";

const st = (s: string) => s as StationId;
const member = (station: string, areaKm2: number, role: "up" | "down" = "up"): PanelMember =>
  ({ station: st(station), role, areaKm2, years: 40, regulated: false, sameRiver: true });
/** Panel 1: two donors. Sections 5 and 6 share it at one area; 7 at another; 9 is a lake; 8 is nothing. */
const MEMBERS = new Map([[1, [member("A", 100), member("B", 400, "down")]]]);
const CLASSES = buildStandingClasses("r-test",
  [{ sid: 7, panel: 1, areaKm2: 900 }, { sid: 5, panel: 1, areaKm2: 120 }, { sid: 6, panel: 1, areaKm2: 120 }],
  // 5 and 6 lie in gauged lakes AND have a panel: the panel wins, so neither lake becomes a class of its own
  MEMBERS, new Map([[9, st("L")], [5, st("L")], [6, st("L2")]]),
  new Map([["08MF", [{ station: st("A"), areaKm2: 100, years: 40 }]], ["08LG", [{ station: st("Q"), areaKm2: 5, years: 3 }]]]));
const IDX = { stations: {
  A: { percentile: 0.2, discharge: 0.2, parameter: "discharge" as const },
  B: { percentile: 0.6, discharge: 0.6, parameter: "discharge" as const },
  L: { percentile: null, level: 0.7, parameter: "level" as const },
} };

describe("the per-tile path's values", () => {
  it("one class per (panel, target area), a lake class for a lake no panel speaks for; a panel wins over a lake", () => {
    expect([5, 6, 7, 8, 9].map((s) => CLASSES.classOf(s))).toEqual([1, 1, 0, null, 2]);
    expect(CLASSES.classes.map((c) => (c.kind === "panel" ? `panel@${c.panel.areaKm2}` : `lake ${c.station}`)))
      .toEqual(["panel@900", "panel@120", "lake L"]);
  });

  it("each class's value is the section's own standing ×100; nothing to say is NaN, the sentinel -1", () => {
    for (const q of ["discharge", "level"] as const)
      for (const h of [0, 1] as const) {
        const v = standingValues(CLASSES, IDX, q, h);
        for (const s of [5, 6, 7, 9]) {
          const want = sectionStanding(s === 9 ? undefined : { areaKm2: s === 7 ? 900 : 120, members: MEMBERS.get(1)! },
                                       s === 9 || s === 5 ? st("L") : undefined, IDX, q, h);
          const got = v.section(CLASSES.classOf(s)!);
          expect([q, h, s, Number.isNaN(got) ? null : got]).toEqual([q, h, s, want === null ? null : want * 100]);
        }
      }
    const now = standingValues(CLASSES, IDX, "level", 0);
    // the lake reads its own level (70); the panels have no level donor: the sentinel, dashed
    expect([0, 1, 2, 3].map((k) => now.section(k))).toEqual([-1, -1, 70, NaN]);
    expect(now.basins).toEqual({});
    // the field: one gauge reporting (Q has no reading, so 08LG is left out), through the probit round trip
    const field = standingValues(CLASSES, IDX, "discharge", 0).basins;
    expect(Object.keys(field)).toEqual(["08MF"]);
    expect(field["08MF"]).toBeCloseTo(20, 4);
    const off = standingValues(CLASSES, null, "discharge", 0);
    expect([0, 1, 2].map((k) => off.section(k)).every(Number.isNaN)).toBe(true);
  });

  it("the legend's marks are the visible sections' values on the 0-1 scale, sentinels and nothing left out", () => {
    const v = { section: (k: number) => [-1, 31.5, 70][k] ?? NaN, basins: {} };
    expect(classMarks(CLASSES, v, [5, 6, 7, 8, 9].map((s) => s as SectionId))).toEqual([0.315, 0.315, 0.7]);
  });
});

describe("the pushed path's data (the native map)", () => {
  it("every visible section on streams, lakes AND the marsh a stream flows through, and the field, in the ramp units", () => {
    expect(pushedStandings(new Map([[5 as SectionId, 0.25], [9 as SectionId, -0.01]]), new Map([["08MF", 0.5]])))
      .toEqual({ stream: { 5: { standing: 25 }, 9: { standing: -1 } }, lake: { 5: { standing: 25 }, 9: { standing: -1 } },
                 wetland_flowing: { 5: { standing: 25 }, 9: { standing: -1 } },
                 basin: { "08MF": { standing: 50 } } });
  });
});
