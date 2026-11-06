/**
 * (T424) Web and phone draw the SAME credits and the corners never meet — pinned on the sources: both maps draw the one
 * `MapCredits` from the one look, neither renderer's own attribution control is on, the scale is bottom-LEFT on both and
 * the credits bottom-RIGHT, at the narrowest width with Esri's badge showing.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { GEOMETRY } from "../packages/map/src/chrome";

const read = (p: string) => readFileSync(new URL(`../${p}`, import.meta.url), "utf8");

const web = read("packages/map/src/Map.web.tsx"), native = read("packages/map/src/Map.native.tsx");
const instance = read("packages/map/src/web/useMapInstance.ts"), css = read("packages/map/src/controls.css");

describe("web and phone draw the same credits, and the corners never meet (T424)", () => {
  it("both maps draw MapCredits from the same look, and neither renderer's own attribution control is on", () => {
    expect(web).toContain("<MapCredits look={{ view, modes, groups, basemap }}");
    expect(native).toContain("<MapCredits look={{ view, modes, groups, basemap }}");
    expect(instance).toMatch(/attributionControl:\s*false/);
    expect(native).toMatch(/attribution=\{false\}/);
    // the renderer's own control has no styling left, and nothing but the one component draws an Esri badge
    expect(css).not.toContain(".maplibregl-ctrl-attrib {");
    expect(web).not.toContain("ESRI_BADGE");
    expect(native).not.toContain("ESRI_BADGE");
  });

  it("the scale bar is bottom-LEFT on both, the credits bottom-RIGHT", () => {
    expect(instance).toMatch(/ScaleControl\([^)]*\),\s*"bottom-left"/);
    expect(native).toMatch(/left: GEOMETRY\.scaleLeft/);
    expect(native).not.toMatch(/right: 10, bottom: 24/);
  });

  it("at the narrowest width (320) and wider, the scale ends well left of the credits, Esri's badge showing", () => {
    const G = GEOMETRY;
    for (const width of [320, 360, 390, 1440]) {
      const scaleEnd = G.scaleLeft + G.scaleMax;
      const creditsStart = width - G.right - G.chip - G.gap - G.esriBadgeMax;
      expect(scaleEnd).toBeLessThan(creditsStart);
    }
  });
});
