/**
 * ONE SPEAKER (T105): "which gauge speaks for this section?" — `speakerFor`, and the two things built on it: whether a
 * station is the speaker's own (`speaks`) and which of them the sheet leads with (`sheetLead`). Whole values.
 *
 * Mutations each test kills:
 *   - `speakerFor` checks the lake before the panel                 -> "a panel wins over the lake station"
 *   - `sheetLead` keeps a pick the speaker does not hold            -> "never leads with a station the speaker does not hold"
 *   - `sheetLead` drops the offline member-0 fallback               -> "offline: the panel's first member"
 *   - `buildStandingClasses` gives a panelled lake section a lake class (the guard skipped)
 *                                                                   -> "the map's classes ask speakerFor"
 */
import { describe, expect, it } from "vitest";
import { buildStandingClasses, sheetLead, speakerFor, speaks, type Panel, type StationId } from "./index";

const st = (s: string) => s as StationId;
const member = (station: string) => ({ station: st(station), role: "up" as const, areaKm2: 100, years: 30,
                                       regulated: false, sameRiver: true });
const PANEL: Panel = { areaKm2: 40, members: [member("A"), member("B")] };

describe("speakerFor: the panel, else the station in the lake, else nothing", () => {
  it("a panel wins over the lake station; a lake alone speaks; nothing is null", () => {
    expect([
      speakerFor(PANEL, st("LAKE")),
      speakerFor(PANEL, undefined),
      speakerFor(undefined, st("LAKE")),
      speakerFor(undefined, undefined),
    ]).toEqual([
      { kind: "panel", panel: PANEL },
      { kind: "panel", panel: PANEL },
      { kind: "lake", station: "LAKE" },
      null,
    ]);
  });

  it("speaks: a panel's members, a lake's own station, nothing else", () => {
    const panel = speakerFor(PANEL, undefined), lake = speakerFor(undefined, st("LAKE"));
    expect([speaks(panel, st("A")), speaks(panel, st("B")), speaks(panel, st("LAKE")), speaks(panel, null),
            speaks(lake, st("LAKE")), speaks(lake, st("A")), speaks(null, st("A"))])
      .toEqual([true, true, false, false, true, false, false]);
  });
});

describe("sheetLead: the sheet's station is always one of the speaker's", () => {
  const panel = speakerFor(PANEL, undefined);
  const rows = [{ station: st("B"), percentile: null }, { station: st("A"), percentile: 0.4 }];

  it("the heaviest reporting donor, any donor, then a pick the speaker holds", () => {
    expect([
      sheetLead(panel, rows, null),
      sheetLead(panel, [{ station: st("B"), percentile: null }], null),
      sheetLead(panel, rows, st("B")),
    ]).toEqual(["A", "B", "B"]);
  });

  it("never leads with a station the speaker does not hold (the section_gauge match the old sheet fell back to)", () => {
    expect([sheetLead(panel, rows, st("MATCH")), sheetLead(null, [], st("MATCH")),
            sheetLead(speakerFor(undefined, st("LAKE")), [], st("MATCH"))])
      .toEqual(["A", null, "LAKE"]);
  });

  it("offline (no rows): the panel's first member; a lake: its own station; nothing: null", () => {
    expect([sheetLead(panel, [], null), sheetLead(speakerFor(undefined, st("LAKE")), [], null),
            sheetLead(null, [], null)])
      .toEqual(["A", "LAKE", null]);
  });
});

describe("the map's classes ask speakerFor", () => {
  it("a lake section a panel speaks for is the panel's class; a lake section alone is the lake's", () => {
    const c = buildStandingClasses("r", [{ sid: 5, panel: 1, areaKm2: 40 }], new Map([[1, PANEL.members]]),
                                   new Map([[5, st("LAKE")], [9, st("LAKE")]]), new Map());
    expect([c.classOf(5), c.classOf(9), c.classOf(7), c.classes]).toEqual([0, 1, null, [
      speakerFor(PANEL, undefined), speakerFor(undefined, st("LAKE"))]]);
  });
});
