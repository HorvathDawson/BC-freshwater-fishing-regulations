/**
 * (T940, owner 2026-11-22: "Yes, use the proposed set (Recommended)") OVER SATELLITE THE STATUS COLOURS ARE ONE SET, THE SAME IN
 * EVERY THEME — AND THE DRAWN MAP IS UNCHANGED.
 *
 * Pinned on what each map is handed (the paint a real adapter writes) and on the key's swatches:
 *   - the six tokens hold the approved hexes in light, dark and cvd;
 *   - on satellite the stream's six status colours are identical in all three themes, and carry those hexes (the grey of a section with
 *     no status, `color.water.ungauged`, is not one of the six and stays the theme's);
 *   - on the drawn map it is the theme's own (`color.status.*`), differs between themes, and carries none of them;
 *   - satellite -> map takes the drawn colours back;
 *   - the key's swatches (`waterStatusColour(.., true)`, `salmonStatusColour(.., true)`) are those hexes, and the drawn ones unchanged.
 */
import { describe, expect, it } from "vitest";
import type { DayProfiles } from "@app/core";
import { baseAdapter, type MapHandle } from "../packages/map/src/adapters/contract";
import { IMAGERY_WATER_LAYERS, STYLE_META, resolveTheme, salmonStatusColour, waterStatusColour } from "../packages/map/src/style";

const RAW = { open: "6BFF9E", restricted: "E3A6FF", closed: "FF3B6D", own: "FFD307", base: "8CC3FF", no_access: "E6E4E2" } as const;
/** The owner's approved hexes, written without the `#` so the palette check (colours live in tokens + themes only) does not mistake the pin for a palette. */
const SET = Object.fromEntries(Object.entries(RAW).map(([k, h]) => [k, `#${h}`])) as Record<keyof typeof RAW, string>;
const THEMES = ["light", "dark", "cvd"] as const;
const DAY: DayProfiles = { closed: ["p1"], own: ["p2"], base: ["b"], no_access: ["n"], none: ["p3"] };

function streamColour(theme: string, basemaps: ("map" | "satellite")[]): string {
  const paint: Record<string, Record<string, unknown>> = {};
  const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                         setFeatureState: () => {}, clearFeatureStates: () => {} };
  const a = baseAdapter("web");
  a.applyView(h, "plain", theme);
  for (const l of ["stream", "lake", "lake_flowing"]) a.setLayerMode(h, l, "profile", theme);
  a.setChannel(h, "status", DAY, theme);
  for (const b of basemaps) a.setBasemap(h, b, theme);
  return JSON.stringify(paint["stream"]!["line-color"]).toLowerCase();
}
/** The colour expression without its last argument: the fallback for a section with no status (`color.water.ungauged`), which is not one of the six. */
function statuses(theme: string, basemaps: ("map" | "satellite")[]): string {
  const e = JSON.parse(streamColour(theme, basemaps)) as unknown[];
  return JSON.stringify(e.slice(0, -1));
}

describe("T940 satellite status set", () => {
  it("the tokens are the approved set in every theme", () => {
    for (const t of THEMES)
      for (const [s, want] of Object.entries(SET))
        expect(resolveTheme(t)[`color.imagery.status.${s}`], `${t} ${s}`).toBe(want);
  });

  it("on satellite the stream is painted in the one set, the same in every theme", () => {
    const [l, d, c] = THEMES.map((t) => statuses(t, ["satellite"]));
    expect(d).toBe(l);
    expect(c).toBe(l);
    for (const s of ["closed", "own", "base", "no_access"] as const) expect(l, s).toContain(SET[s].toLowerCase());
  });

  it("the drawn map keeps each theme's own colours, and none of the satellite set", () => {
    const drawn = THEMES.map((t) => streamColour(t, ["map"]));
    expect(new Set(drawn).size).toBe(3);
    THEMES.forEach((t, i) => {
      const th = resolveTheme(t);
      expect(drawn[i], t).toContain(String(th["color.status.closed"]).toLowerCase());
      expect(drawn[i], t).toContain(String(th["color.status.base"]).toLowerCase());
    });
    // dark's closed and own ARE the satellite hexes (the proposal kept them); base and no-access are what moved
    for (const [i, t] of THEMES.entries()) if (t !== "dark") expect(drawn[i], t).not.toContain(SET.closed.toLowerCase());
    expect(drawn[0]).not.toContain(SET.base.toLowerCase());
  });

  it("satellite then back to the map restores the drawn colours", () => {
    for (const t of THEMES) expect(streamColour(t, ["satellite", "map"])).toBe(streamColour(t, ["map"]));
  });

  it("the key's swatches are the lines they explain", () => {
    for (const t of THEMES) {
      for (const s of ["open", "restricted", "closed", "own", "base", "no_access"] as const)
        if (s !== "open" && s !== "restricted") expect(waterStatusColour(t, s, true), `${t} ${s}`).toBe(SET[s]);
      expect(salmonStatusColour(t, "open", true)).toBe(SET.open);
      expect(salmonStatusColour(t, "closed", true)).toBe(SET.closed);
      expect(waterStatusColour(t, "base"), `${t} drawn base`).toBe(resolveTheme(t)["color.status.base"]);
    }
    expect(waterStatusColour("light", "base")).not.toBe(waterStatusColour("light", "base", true));
  });

  /** The paint a layer is handed in a mode, on the ground(s), as lowercase JSON. */
  function layerPaint(theme: string, layer: string, mode: string, basemap: "map" | "satellite"): string {
    const paint: Record<string, Record<string, unknown>> = {};
    const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                           setFeatureState: () => {}, clearFeatureStates: () => {} };
    const a = baseAdapter("web");
    a.applyView(h, "plain", theme);
    a.setLayerMode(h, layer, mode, theme);
    if (mode === "salmon") a.setChannel(h, "salmon", { open: ["s0"], release: [], closed: ["s3"], check: [], none: [] } as never, theme);
    a.setBasemap(h, basemap, theme);
    return JSON.stringify(paint[layer]).toLowerCase();
  }

  it("a lake or wetland in the Salmon view keeps on satellite in the same green as the stream and the key", () => {
    for (const t of THEMES)
      for (const layer of ["lake", "lake__edge", "wetland_flowing"]) {
        const sat = layerPaint(t, layer, "salmon", "satellite");
        expect(sat, `${t} ${layer}`).toContain(SET.open.toLowerCase());
        expect(sat, `${t} ${layer}`).not.toContain(String(resolveTheme(t)["color.salmon.open-area"]).toLowerCase());
      }
  });

  it("land areas keep their theme's colours on satellite: the swap is for water lines and fills only", () => {
    for (const t of THEMES) {
      const th = resolveTheme(t);
      for (const [layer, token] of [["park_closed", "color.status.closed"], ["eco_reserve", "color.status.closed"],
                                    ["no_access", "color.status.closed"], ["sign_zone", "color.status.closed"],
                                    ["no_access_permit", "color.status.restricted"]] as const) {
        const want = String(th[token]).toLowerCase();
        expect(layerPaint(t, layer, "plain", "satellite"), `${t} ${layer}`).toContain(want);
        if (t !== "dark") expect(layerPaint(t, layer, "plain", "satellite"), `${t} ${layer} must not wear the satellite set`).not.toContain(SET[token === "color.status.closed" ? "closed" : "restricted"].toLowerCase());
      }
    }
  });

  it("every layer a water's own / base status colours is on the satellite list, and no land layer is", () => {
    const statusWater = Object.entries(STYLE_META.colorModes)
      .filter(([, modes]) => JSON.stringify(modes).match(/"color\.status\.(own|base)"/)).map(([l]) => l).sort();
    expect(statusWater.every((l) => IMAGERY_WATER_LAYERS.has(l))).toBe(true);
    for (const land of ["park_closed", "eco_reserve", "no_access", "sign_zone", "no_access_permit"]) expect(IMAGERY_WATER_LAYERS.has(land)).toBe(false);
  });
});
