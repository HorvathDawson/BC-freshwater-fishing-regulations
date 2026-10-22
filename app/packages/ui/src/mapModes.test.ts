/** P12 (c): which layer is coloured by what, decided once here and drawn by the Shell. */
import { describe, expect, it } from "vitest";
import { mapModes, statusFeed, type MapModeInput } from "./mapModes";

const base: MapModeInput = {
  tab: "map", flowParam: "discharge", horizon: 3, zoomedOut: false, paired: true, statusOk: true, perTile: true,
  lake: { mode: "plain" }, groups: { depth: false, admin: true }, basemap: "map", imageryUp: true,
};
const at = (over: Partial<MapModeInput>) => mapModes({ ...base, ...over });

describe("mapModes (P12): the Shell's colouring decision, pure", () => {
  it("the Map tab with the day's status in: every water wears it, through the profile mode (colour-latency AP)", () => {
    expect(at({})).toEqual({
      onConditions: false, quantity: "flow", standingParam: "discharge", standingHorizon: 3, fieldOn: false,
      horizonsShown: false, asksVisible: false,
      modes: { stream: "profile", lake: "profile", lake_flowing: "profile", wetland_flowing: "profile",
               gauges: "standing", basin: "plain" },
      activeGroups: { depth: false, admin: true },
      basemap: "map", basemapNote: null,
    });
  });

  it("the ground drawn (owner 2026-10-21, satellite online-only): the choice while imagery is up; the drawn map and the reason while it is not", () => {
    const ground = (basemap: "map" | "satellite", imageryUp: boolean) => {
      const m = at({ basemap, imageryUp });
      return { basemap: m.basemap, basemapNote: m.basemapNote };
    };
    expect([ground("satellite", true), ground("satellite", false), ground("map", true), ground("map", false)])
      .toEqual([
        { basemap: "satellite", basemapNote: null },
        { basemap: "map", basemapNote:
            "Satellite needs a connection. Showing the map until you are back online; satellite returns by itself." },
        { basemap: "map", basemapNote: null },
        { basemap: "map", basemapNote: null },
      ]);
  });

  it("the ground changes no colouring: the same modes and groups on satellite, offline or not (one colouring pathway)", () => {
    const look = (m: ReturnType<typeof at>) => ({ modes: m.modes, activeGroups: m.activeGroups });
    expect([look(at({ basemap: "satellite" })), look(at({ basemap: "satellite", imageryUp: false }))])
      .toEqual([look(at({})), look(at({}))]);
  });

  it("a renderer that cannot colour each tile (the phone, until N2's follow-up) keeps the pushed 'status' mode", () => {
    // colour-ap review: N2's native map reads only `data` in the `status` mode and ignores `status`; told 'profile' it
    // painted every water plain with no error
    expect(at({ perTile: false }).modes).toEqual({
      stream: "status", lake: "status", lake_flowing: "status", wetland_flowing: "status", gauges: "standing",
      basin: "plain" });
    expect(at({ perTile: false, statusOk: false }).modes.stream).toBe("plain");
  });

  it("the map is asked what is on screen only where something reads it (the one-pathway audit, 2026-10-21)", () => {
    // web Map tab: the engine colours each tile, nothing reads the visible list — no `idle` -> React round trip per pan;
    // Conditions: the legend's marks; the phone: its pushed colours on both tabs
    expect([at({}), at({ tab: "conditions" }), at({ perTile: false }), at({ perTile: false, tab: "conditions" })]
      .map((m) => m.asksVisible)).toEqual([false, true, true, true]);
  });

  it("the Map tab before the index is in: plain, never 'base'", () => {
    expect(at({ statusOk: false }).modes).toEqual({
      stream: "plain", lake: "plain", lake_flowing: "plain", wetland_flowing: "plain", gauges: "standing",
      basin: "plain" });
  });

  it("a lake choice the reader asked for beats the status, and switches its group on", () => {
    const m = at({ lake: { mode: "surveyed", group: "depth" } });
    expect(m.modes.lake).toBe("surveyed");
    expect(m.modes.stream).toBe("profile");
    expect(m.activeGroups).toEqual({ depth: true, admin: true });
  });

  it("Conditions, flow, zoomed out: standings everywhere, the field on, the map's horizons offered", () => {
    expect(at({ tab: "conditions", zoomedOut: true })).toEqual({
      onConditions: true, quantity: "flow", standingParam: "discharge", standingHorizon: 3, fieldOn: true,
      horizonsShown: true, asksVisible: true,
      modes: { stream: "standing", lake: "standing", lake_flowing: "standing", wetland_flowing: "standing",
               gauges: "standing", basin: "standing" },
      activeGroups: { depth: false, admin: true },
      basemap: "map", basemapNote: null,
    });
  });

  it("Conditions, level, zoomed in: standings on the level question, no field", () => {
    const m = at({ tab: "conditions", flowParam: "level" });
    expect([m.quantity, m.standingParam, m.fieldOn, m.modes.stream, m.modes.basin])
      .toEqual(["level", "level", false, "standing", "standing"]);
  });

  it("Conditions under temperature: the water plain, the dots by band, today only, no field, no horizons", () => {
    expect(at({ tab: "conditions", flowParam: "temperature", zoomedOut: true })).toEqual({
      onConditions: true, quantity: "temperature", standingParam: "discharge", standingHorizon: 0, fieldOn: false,
      horizonsShown: false, asksVisible: true,
      modes: { stream: "plain", lake: "plain", lake_flowing: "plain", wetland_flowing: "plain",
               gauges: "temperature", basin: "plain" },
      activeGroups: { depth: false, admin: true },
      basemap: "map", basemapNote: null,
    });
  });

  it("paints the map at the chosen horizon, ten days included (owner 2026-10-20)", () => {
    expect(at({ tab: "conditions", horizon: 10 }).standingHorizon).toBe(10);
    expect(at({ tab: "conditions", horizon: 10, flowParam: "level" }).standingHorizon).toBe(10);
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

describe("statusFeed: how the day's status reaches the map, per renderer (colour-ap review)", () => {
  const index = {} as never;
  const day = new Date(2026, 9, 21);
  it("per-tile (web): the index and the day as MapProps.status, on both tabs; nothing pushed", () => {
    expect(statusFeed({ perTile: true, statusOk: true, index, tab: "map", visible: [5], day }))
      .toEqual({ status: { index, day }, data: undefined });
    expect(statusFeed({ perTile: true, statusOk: true, index, tab: "conditions", visible: [5], day }).status)
      .toEqual({ index, day });
  });
  it("no per-tile event (native): the visible sections pushed as `data` on the Map tab, no status prop", () => {
    const f = statusFeed({ perTile: false, statusOk: true, index, tab: "map", visible: [], day });
    expect(f.status).toBeNull();
    expect(f.data).toEqual({ stream: {}, lake: {}, wetland: {} });
    expect(statusFeed({ perTile: false, statusOk: true, index, tab: "conditions", visible: [], day }).data)
      .toBeUndefined();
  });
  it("not a proven pair, or no index: nothing either way", () => {
    for (const perTile of [true, false]) {
      expect(statusFeed({ perTile, statusOk: false, index, tab: "map", visible: [], day }))
        .toEqual({ status: null, data: undefined });
      expect(statusFeed({ perTile, statusOk: true, index: null, tab: "map", visible: [], day }))
        .toEqual({ status: null, data: undefined });
    }
  });
});
