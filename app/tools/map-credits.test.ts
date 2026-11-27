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
/** (T1154) the native map element itself: the engine's tree */
const nativeView = read("packages/map/src/native/maplibre/View.tsx");
const instance = read("packages/map/src/web/useMapInstance.ts"), css = read("packages/map/src/controls.css");

describe("web and phone draw the same credits, and the corners never meet (T424)", () => {
  it("both maps draw MapCredits from the same look, and neither renderer's own attribution control is on", () => {
    expect(web).toContain("<MapCredits look={{ view, modes, groups, basemap }}");
    expect(native).toContain("<MapCredits look={{ view, modes, groups, basemap }}");
    expect(instance).toMatch(/attributionControl:\s*false/);
    expect(nativeView).toMatch(/attribution=\{false\}/);
    // the renderer's own control has no styling left, and nothing but the one component draws an Esri badge
    expect(css).not.toContain(".maplibregl-ctrl-attrib {");
    expect(web).not.toMatch(/IMAGERY\.badge|esri-badge/);
    expect(native).not.toMatch(/IMAGERY\.badge|esri-badge/);
    expect(nativeView).not.toMatch(/IMAGERY\.badge|esri-badge/);
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
      // (T1212) Esri's badge stands over the ©, so the widest thing in the corner is the badge
      const creditsStart = width - G.right - Math.max(G.chip, G.esriBadgeMax);
      expect(scaleEnd).toBeLessThan(creditsStart);
    }
  });
});

describe("(T1212) the © and the scale on one grid; + / − for wide windows only", () => {
  it("both maps hand MapCredits the ©'s own inset (insetCredits, else insetBottom); the scale stays over the foot", () => {
    expect(web).toContain("insetBottom={insetCredits ?? insetBottom}");
    expect(native).toContain("insetBottom={insetCredits ?? insetBottom}");
    expect(native).toMatch(/left: GEOMETRY\.scaleLeft, bottom: GEOMETRY\.scaleBottom \+ insetBottom/);
  });

  it("the web's scale is 14 px in (GEOMETRY.scaleLeft), past MapLibre's 10 px corner margin", () => {
    expect(GEOMETRY.scaleLeft).toBe(14);
    expect(css).toMatch(/\.maplibregl-ctrl-bottom-left \.maplibregl-ctrl\.maplibregl-ctrl-scale \{ margin-left: 14px; \}/);
  });

  it("a phone-wide web map hides MapLibre's + / − (zoomButtons false); wide windows and bare maps keep theirs", () => {
    expect(web).toContain('data-zoom-buttons={zoomButtons ? undefined : "off"}');
    expect(web).toMatch(/zoomButtons = true/);
    expect(css).toMatch(/\[data-zoom-buttons="off"\] \.maplibregl-ctrl-top-right \.maplibregl-ctrl-group \{ display: none; \}/);
  });

  it("(T1212) both maps hand MapCredits the foot block as footBottom, and insetCredits (else insetBottom) as the © block", () => {
    for (const src of [web, native]) {
      const call = /<MapCredits [^]*?\/>/.exec(src)?.[0] ?? "";
      expect([call.includes("insetBottom={insetCredits ?? insetBottom}"), call.includes("footBottom={insetBottom}")]).toEqual([true, true]);
    }
  });
});
