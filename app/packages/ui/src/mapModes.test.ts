/** P12 (c): which layer is coloured by what, decided once here and drawn by the Shell. */
import { describe, expect, it } from "vitest";
import { mapModes, type MapModeInput } from "./mapModes";

const base: MapModeInput = {
  tab: "map", flowParam: "discharge", horizon: 3, zoomedOut: false, paired: true, statusOk: true,
  lake: { mode: "plain" }, groups: { depth: false, admin: true },
};
const at = (over: Partial<MapModeInput>) => mapModes({ ...base, ...over });

describe("mapModes (P12): the Shell's colouring decision, pure", () => {
  it("the Map tab with the day's status in: every water wears the status", () => {
    expect(at({})).toEqual({
      onConditions: false, quantity: "flow", standingParam: "discharge", standingHorizon: 3, fieldOn: false,
      modes: { stream: "status", lake: "status", lake_flowing: "status", wetland_flowing: "status",
               gauges: "standing", basin: "plain" },
      activeGroups: { depth: false, admin: true },
    });
  });

  it("the Map tab before the index is in: plain, never 'base'", () => {
    expect(at({ statusOk: false }).modes).toEqual({
      stream: "plain", lake: "plain", lake_flowing: "plain", wetland_flowing: "plain", gauges: "standing",
      basin: "plain" });
  });

  it("a lake choice the reader asked for beats the status, and switches its group on", () => {
    const m = at({ lake: { mode: "surveyed", group: "depth" } });
    expect(m.modes.lake).toBe("surveyed");
    expect(m.modes.stream).toBe("status");
    expect(m.activeGroups).toEqual({ depth: true, admin: true });
  });

  it("Conditions, flow, zoomed out: standings everywhere, the field on", () => {
    expect(at({ tab: "conditions", zoomedOut: true })).toEqual({
      onConditions: true, quantity: "flow", standingParam: "discharge", standingHorizon: 3, fieldOn: true,
      modes: { stream: "standing", lake: "standing", lake_flowing: "standing", wetland_flowing: "standing",
               gauges: "standing", basin: "standing" },
      activeGroups: { depth: false, admin: true },
    });
  });

  it("Conditions, level, zoomed in: standings on the level question, no field", () => {
    const m = at({ tab: "conditions", flowParam: "level" });
    expect([m.quantity, m.standingParam, m.fieldOn, m.modes.stream, m.modes.basin])
      .toEqual(["level", "level", false, "standing", "standing"]);
  });

  it("Conditions under temperature: the water plain, the dots by band, today only, no field", () => {
    expect(at({ tab: "conditions", flowParam: "temperature", zoomedOut: true })).toEqual({
      onConditions: true, quantity: "temperature", standingParam: "discharge", standingHorizon: 0, fieldOn: false,
      modes: { stream: "plain", lake: "plain", lake_flowing: "plain", wetland_flowing: "plain",
               gauges: "temperature", basin: "plain" },
      activeGroups: { depth: false, admin: true },
    });
  });

  it("a pair not PROVEN colours nothing, on either tab — a mixed pair, and one not yet established (P11)", () => {
    // `ok === null` used to colour: the window between the map appearing and the pair being checked is exactly when
    // a mixed pair paints real readings onto the wrong rivers
    for (const tab of ["map", "conditions"] as const)
      expect(at({ tab, paired: false, zoomedOut: true }).modes).toEqual({
        stream: "plain", lake: "plain", lake_flowing: "plain", wetland_flowing: "plain", gauges: "standing",
        basin: "plain" });
  });
});
