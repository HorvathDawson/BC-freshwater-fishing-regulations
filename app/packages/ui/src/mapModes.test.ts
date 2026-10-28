/** P12 (c): which layer is coloured by what, decided once here and drawn by the Shell. */
import { describe, expect, it } from "vitest";
import { mapModes, statusFeed, type MapModeInput } from "./mapModes";

const base: MapModeInput = {
  tab: "map", flowParam: "discharge", horizon: 3, zoomedOut: false, paired: true, statusOk: true,
  water: "regulations", groups: { depth: false, admin: true }, basemap: "map", imageryUp: true,
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

  it("the map is asked what is on screen only where something reads it (the one-pathway audit, 2026-10-21)", () => {
    // Map tab: the engine colours each tile, nothing reads the visible list — no `idle` -> React round trip per pan;
    // Conditions: the legend's marks
    expect([at({}), at({ tab: "conditions" })].map((m) => m.asksVisible)).toEqual([false, true]);
  });

  it("the Map tab before the index is in: plain, never 'base'", () => {
    expect(at({ statusOk: false }).modes).toEqual({
      stream: "plain", lake: "plain", lake_flowing: "plain", wetland_flowing: "plain", gauges: "standing",
      basin: "plain" });
  });

  it("ONE WATER COLOURING (owner 2026-10-21 late): a lake measure colours the lakes by it and EVERY other water neutral", () => {
    const look = (water: "regulations" | "stocked" | "depth", over: Partial<MapModeInput> = {}) => {
      const m = at({ water, ...over }).modes;
      return `${m.stream} ${m.lake} ${m.lake_flowing} ${m.wetland_flowing}`;
    };
    expect([look("regulations"), look("stocked"), look("depth"),
            look("depth", { statusOk: false }), look("stocked", { paired: false })]).toEqual([
      "profile profile profile profile",
      "plain stocked plain plain",
      "plain surveyed plain plain",
      "plain surveyed plain plain",
      "plain plain plain plain",
    ]);
  });

  it("MUTATION PIN: no rule lets a stream keep the day's status while the lakes show a measure", () => {
    for (const water of ["stocked", "depth"] as const) {
      const m = at({ water }).modes;
      expect([water, m.stream, m.lake_flowing, m.wetland_flowing]).toEqual([water, "plain", "plain", "plain"]);
    }
  });

  it("the water colouring is the Map tab's: Conditions answers its own question for every water", () => {
    expect(at({ tab: "conditions", water: "stocked" }).modes).toEqual(at({ tab: "conditions" }).modes);
  });

  it("the groups are the registry's, passed through untouched", () => {
    expect(at({ groups: { depth: true, ownership: true } }).activeGroups).toEqual({ depth: true, ownership: true });
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

describe("statusFeed: how the day's status reaches the map (colour-ap review)", () => {
  const index = {} as never;
  const day = new Date(2026, 9, 21);
  it("the index and the day as MapProps.status", () => {
    expect(statusFeed({ statusOk: true, index, day })).toEqual({ index, day });
  });
  it("not a proven pair, or no index: nothing", () => {
    expect(statusFeed({ statusOk: false, index, day })).toBeNull();
    expect(statusFeed({ statusOk: true, index: null, day })).toBeNull();
  });
});
