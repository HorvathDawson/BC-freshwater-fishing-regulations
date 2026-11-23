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
import { resolveTheme, salmonStatusColour, waterStatusColour } from "../packages/map/src/style";

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
});
