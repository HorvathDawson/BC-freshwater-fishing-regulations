/**
 * THE LAYERS SHEET, direction B (owner 2026-10-21 late) — checked two ways.
 *
 * 1. THE REGISTRY AGAINST THE STYLE. A menu is a list of promises about what the map can do (a choice that named a
 *    colour mode the style lacked once threw from a render effect and blanked the app). Every group an entry sets is a
 *    toggleable style group, every toggleable group has a switch (private land had none), every always-shown entry
 *    names real, NON-toggleable layers — Indigenous land among them — and each water colouring exists as a lake mode.
 * 2. THE SHEET, MOUNTED: it draws the registry's VM whole — three sections, greyed tiles with their tags, the legend
 *    under the chosen colouring, the switches, the always-shown line — and nothing it used to carry (sources, live
 *    data, credits, the palette).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { STYLE_META } from "@app/map";
import { LAYER_DEFAULTS, LAYERS, STOCK_RECENCY, chooseLayer, layersVM, type LayerChoice, type LayerFacts }
  from "@app/core";
import { LayersSheet } from "./LayersSheet";
import { LIGHT } from "./theme";

const groups = new Map(STYLE_META.groups.map((g) => [g.id, g]));
const layerGroup = (STYLE_META as unknown as { layerGroup: Record<string, string> }).layerGroup;

describe("the layer registry against the style", () => {
  it("every group an entry sets is a TOGGLEABLE style group — never protected, access or admin", () => {
    const set = LAYERS.flatMap((e) => (e.sets?.groups ?? []).map((g) => `${e.id}:${g}:${groups.get(g)?.toggleable}`));
    expect(set).toEqual(["water.depth:depth:true", "overlay.contours:depth:true", "overlay.private:ownership:true"]);
  });

  it("every toggleable style group has a switch (a group with no switch can never be seen)", () => {
    const toggleable = STYLE_META.groups.filter((g) => g.toggleable).map((g) => g.id).sort();
    const switched = [...new Set(LAYERS.filter((e) => e.kind === "overlay").flatMap((e) => e.sets?.groups ?? []))].sort();
    expect(switched).toEqual(toggleable);
  });

  it("ALWAYS SHOWN names real layers in their own, non-toggleable groups — Indigenous land (reserves) included", () => {
    const always = LAYERS.filter((e) => e.kind === "always").map((e) =>
      `${e.label}: ${(e.draws?.layers ?? []).map((l) => `${l}/${layerGroup[l]}`).join(" ")}`
      + ` [${(e.draws?.groups ?? []).map((g) => `${g}:${groups.get(g)?.toggleable}`).join(" ")}]`);
    expect(always).toEqual([
      "parks and ecological reserves: park/protected park_closed/protected eco_reserve/protected [protected:false]",
      "Indigenous land (reserves): indigenous_land/access [access:false]",
      "no-access and permit-only land: no_access/access no_access_permit/access [access:false]",
      "region and management-unit lines: region/admin mu/admin [admin:false]",
    ]);
  });

  it("each water colouring is a lake mode the style has (Regulations = the day's status)", () => {
    const lake = Object.keys(STYLE_META.colorModes.lake ?? {});
    expect(["status", "profile", "stocked", "surveyed"].filter((m) => lake.includes(m)))
      .toEqual(["status", "profile", "stocked", "surveyed"]);
  });

  it("the Stocked key's bands are the style's (`stock_recency`), in order", () => {
    expect(STOCK_RECENCY.map((b) => b.band)).toEqual(
      (STYLE_META as unknown as { enums: Record<string, string[]> }).enums.stock_recency!.filter((x) => x !== "never"));
  });
});

const NONE: LayerFacts = { carries: { stocked: false, surveyed: false }, online: true };
const FED: LayerFacts = { carries: { stocked: true, surveyed: true }, online: true };

/** The Shell's wiring of the sheet, and nothing else: the choice, the VM over the facts, a press through chooseLayer. */
function Wired({ facts, start = LAYER_DEFAULTS, out }: { facts: LayerFacts; start?: LayerChoice; out: { c?: LayerChoice } }) {
  const [c, setC] = useState(start);
  out.c = c;
  return <LayersSheet open onClose={() => {}} palette={LIGHT} vm={layersVM("map", c, facts)}
                      onChoose={(id) => setC((x) => chooseLayer(x, id, facts))} />;
}

const radios = () => screen.getAllByRole("radio").map((r) =>
  `${r.getAttribute("aria-label")}${r.getAttribute("aria-checked") === "true" ? " *" : ""}`
  + `${r.getAttribute("aria-disabled") === "true" ? " (off)" : ""}`);
const switches = () => screen.getAllByRole("switch").map((r) =>
  `${r.getAttribute("aria-label")}${r.getAttribute("aria-checked") === "true" ? " on" : ""}`
  + `${r.getAttribute("aria-disabled") === "true" ? " (off)" : ""}`);
const text = (id: string) => screen.queryByTestId(id)?.textContent ?? null;

describe("the Layers sheet, mounted", () => {
  afterEach(cleanup);

  it("draws B: Map, Colour the water by, On the map, then the always-shown line — whole", () => {
    render(<Wired facts={NONE} out={{}} />);
    expect(screen.getAllByRole("heading").map((h) => h.textContent)).toEqual(
      ["Layers", "MAP", "COLOUR THE WATER BY", "ON THE MAP"]);
    expect(radios()).toEqual(["Map *", "Satellite", "Corridors (off)", "Regulations *", "Stocked (off)", "Depth (off)"]);
    expect(switches()).toEqual(["Depth contours", "Private land", "Pools and runs (off)", "Relief (off)"]);
    expect(text("layers-always")).toBe("Always shown: parks and ecological reserves, Indigenous land (reserves), "
      + "no-access and permit-only land, and region and management-unit lines. They are never switched off.");
  });

  it("each greyed tile and switch says why in its own tag", () => {
    render(<Wired facts={NONE} out={{}} />);
    const tile = (name: string) => screen.getByRole("radio", { name }).textContent;
    const row = (name: string) => screen.getByRole("switch", { name }).textContent;
    expect([tile("Corridors"), tile("Stocked"), tile("Depth"), row("Pools and runs"), row("Relief")]).toEqual([
      "CorridorsDetail imagery along a streamCOMING SOON",
      "StockedWhen a lake was last stockedNO DATA YET",
      "DepthWhich lakes were surveyedNO DATA YET",
      "Pools and runsSlow and fast water, read from lidarCOMING SOON",
      "ReliefLidar hillshadeCOMING SOON",
    ]);
    expect(text("lake-modes-note")).toBe(
      "Stocked and Depth have no data yet. They turn on by themselves when a release carries their values.");
  });

  it("the legend sits under the chosen colouring: the day's three statuses", () => {
    render(<Wired facts={NONE} out={{}} />);
    expect(text("water-legend")).toBe("ClosedHas its own regulationsBase regulations only");
  });

  it("no lake values: pressing a greyed tile or a coming-soon switch changes nothing", () => {
    const out: { c?: LayerChoice } = {};
    render(<Wired facts={NONE} out={out} />);
    fireEvent.click(screen.getByRole("radio", { name: "Stocked" }));
    fireEvent.click(screen.getByRole("radio", { name: "Corridors" }));
    fireEvent.click(screen.getByRole("switch", { name: "Pools and runs" }));
    expect(out.c).toEqual(LAYER_DEFAULTS);
  });

  it("a release carrying values: Stocked chooses, and its key says streams and wetlands draw plain", () => {
    const out: { c?: LayerChoice } = {};
    render(<Wired facts={FED} out={out} />);
    fireEvent.click(screen.getByRole("radio", { name: "Stocked" }));
    expect([out.c!.water, radios().slice(3), text("lake-modes-note")]).toEqual(
      ["water.stocked", ["Regulations", "Stocked *", "Depth"], null]);
    expect(text("water-legend")).toBe("Stocked this seasonStocked this yearStocked 1–3 years agoStocked 3–10 years ago"
      + "Stocked over 10 years agoNot stocked, or no record. Streams and wetlands draw plain: stocking is recorded for "
      + "lakes only.");
  });

  it("the switches flip: private land on, then off", () => {
    const out: { c?: LayerChoice } = {};
    render(<Wired facts={NONE} out={out} />);
    fireEvent.click(screen.getByRole("switch", { name: "Private land" }));
    expect([switches()[1], out.c!.on["overlay.private"]]).toEqual(["Private land on", true]);
    fireEvent.click(screen.getByRole("switch", { name: "Private land" }));
    expect([switches()[1], out.c!.on["overlay.private"]]).toEqual(["Private land", false]);
  });

  it("satellite offline: Map SHOWING, Satellite kept chosen but greyed, and the reason line", () => {
    render(<Wired facts={{ ...NONE, online: false }} start={{ ...LAYER_DEFAULTS, ground: "ground.satellite" }} out={{}} />);
    expect([radios().slice(0, 2), screen.getByRole("radio", { name: "Map" }).textContent,
            screen.getByRole("radio", { name: "Satellite" }).textContent, text("basemap-note")]).toEqual([
      ["Map", "Satellite * (off)"], "MapDrawn · works offlineSHOWING", "SatelliteEsri imagery · online onlyNEEDS A CONNECTION",
      "Satellite needs a connection. Showing the map until you are back online; satellite returns by itself."]);
  });

  it("carries no data sources, live data, credits or palette any more (they moved to the menu and the map)", () => {
    render(<Wired facts={NONE} out={{}} />);
    const all = document.body.textContent ?? "";
    for (const gone of ["Live data", "Stream gauges", "Palette", "Colour-blind", "OpenStreetMap", "Esri, Vantor",
                        "Data sources", "REACHES"])
      expect([gone, all.includes(gone)]).toEqual([gone, false]);
  });
});
