/**
 * (T180) THE WORDS THAT LEFT THE COMPONENTS, pinned whole (AGENTS 33): every sentence composed from parts here reads
 * exactly as it did when it was typed in the component, and the publishers shared by the map's credits and the
 * Sources page read the same in both.
 */
import { describe, expect, it } from "vitest";
import { CONDITIONS_WORDS, FLOW_LEGEND } from "./conditions";
import { MAP_CREDITS } from "./menu/sources";
import { MENU_WORDS } from "./menu/words";
import { SPOT_CAPTURE_WORDS, SPOT_CONFIRM_WORDS } from "./spotWords";
import { TRACE_WORDS } from "./trace";

describe("(T180) words moved from components to core, composed whole", () => {
  it("the map's credits, line for line as App.tsx typed them (the Province's forecast line verbatim)", () => {
    expect(MAP_CREDITS).toEqual([
      "Basemap © OpenStreetMap contributors · © Protomaps",
      "Hydrometric data: Environment and Climate Change Canada",
      "Forecast data provided by the BC River Forecast Centre, Province of British Columbia, and used under the "
        + "Province's copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright). Users should use the "
        + "information on this website with caution and at their own risk.",
      "Stocking: Province of British Columbia, Fisheries Inventory Data Queries",
      "Weather from Open-Meteo.com (CC BY 4.0)",
    ]);
  });

  it("the Sources page's publisher lines read the same publishers, unchanged", () => {
    const f = MENU_WORDS.sources.from;
    expect([f.synopsis, f.inseason, f.fwa, f.basemap, f.hydat, f.feed, f.forecast, f.weather]).toEqual([
      "Province of British Columbia. The printed regulations this app reads.",
      "Province of British Columbia. Shown on the waters they apply to, as posted.",
      "Province of British Columbia. Contains information licensed under the Open Government Licence – British Columbia.",
      "© OpenStreetMap contributors. © Protomaps.",
      "Environment and Climate Change Canada. Historical hydrometric data, under the Open Government Licence – Canada.",
      "Environment and Climate Change Canada. Real-time hydrometric data, under the Open Government Licence – Canada.",
      "BC River Forecast Centre. The CLEVER, COFFEE and ELF river forecasts, used with permission.",
      "Weather data by Open-Meteo.com, CC BY 4.0.",
    ]);
  });

  it("the legend ramp's lines aloud", () => {
    expect(FLOW_LEGEND.near(0.2)).toBe("about 8 in 10 days higher");
    expect(FLOW_LEGEND.scale(FLOW_LEGEND.title.discharge, FLOW_LEGEND.low, FLOW_LEGEND.high, FLOW_LEGEND.mid))
      .toBe("Flow vs. past years, this date: colours from low to high; normal is the middle half of past years");
  });

  it("the chart legend's model entry and the trace panel's values", () => {
    expect([CONDITIONS_WORDS.key.model("GEM", true), CONDITIONS_WORDS.key.model("ELF", false)])
      .toEqual(["GEM forecast, drawn. Tap to hide it", "ELF forecast. Tap to draw it"]);
    expect(TRACE_WORDS.magnitude(4)).toBe("magnitude 4");
  });

  it("the spot screens' composed lines", () => {
    expect([SPOT_CAPTURE_WORDS.tapPoint("Chilliwack River"), SPOT_CAPTURE_WORDS.tapPoint(null)])
      .toEqual(["Tap the point on Chilliwack River.", "Tap the point on this reach."]);
    expect(SPOT_CONFIRM_WORDS.delete.detail).toBe(
      "The gauge reading, the weather and the rules that were in force are recorded nowhere else. This cannot be undone.");
  });
});
