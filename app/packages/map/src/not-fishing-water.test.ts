/**
 * Q125 (owner 2026-10-24): sewage basins, tailing ponds and dumps — T152's `not_fishing_water` tile layer — DRAW as a
 * muted fill, never as a lake, and are not a fishing water anyone can tap. Held on the web style, the native style
 * and the tap/visible sets, so a later edit cannot paint them lake-blue or make them answer a tap.
 */
import { describe, expect, it } from "vitest";
import { paintFor } from "./paint";
import { nativeStyle } from "./native-style";
import { runtimeStyle } from "./runtime-style";
import { SECTION_LAYERS, TAP_GROUPS, VISIBLE_LAYERS } from "./sections";
import { STYLE_META, resolveTheme } from "./style";

type L = { id: string; type: string; source?: string; "source-layer"?: string; paint?: Record<string, unknown> };
type S = { layers: L[]; sources: Record<string, { promoteId?: Record<string, string> }> };

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
const base = { at: AT, theme: "light", view: "plain", modes: {}, hide: [], groups: {}, basemap: "map" as const,
               gauges: '{"type":"FeatureCollection","features":[]}' };
const ID = "not_fishing_water";
/** Every colour a water is drawn in: the lake fill, the water ramp, statuses, flow. */
const WATER = /^color\.(lake|water|wetland|status|flow|stock|change)\./;

describe("not fishing water (Q125)", () => {
  it("is one plain fill over its own source-layer, in a non-water token", () => {
    expect(STYLE_META.colorModes[ID]).toEqual({
      plain: { label: "Not fishing water", scale: "static", color: { token: "color.notfishing.fill" } } });
    expect(WATER.test("color.notfishing.fill")).toBe(false);
    for (const theme of ["light", "dark", "cvd"]) {
      const t = resolveTheme(theme);
      expect(paintFor(ID, "plain", t)["fill-color"], theme).toBe(t["color.notfishing.fill"]);
      expect(t["color.notfishing.fill"], theme).not.toEqual(t["color.lake.fill"]);
    }
  });

  it.each(["web", "native"] as const)("draws on the %s map as a fill, never the lake's colour", (platform) => {
    for (const theme of ["light", "dark"]) {
      const s = (platform === "web" ? runtimeStyle(AT, theme)
                                    : nativeStyle({ ...base, theme }).style) as unknown as S;
      const l = s.layers.find((x) => x.id === ID)!;
      expect(l, `${platform} ${theme}`).toBeDefined();
      expect([l.type, l.source, l["source-layer"]]).toEqual(["fill", "atlas", ID]);
      const lake = s.layers.find((x) => x.id === "lake")!;
      if (platform === "native")
        expect(l.paint?.["fill-color"]).not.toEqual(lake.paint?.["fill-color"]);
      // never a lake: no layer over the `lake` source-layer draws it, and it carries no lake filter
      expect(s.layers.filter((x) => x["source-layer"] === ID).map((x) => x.id)).toEqual([ID]);
    }
  });

  it("is not a fishing water: no id, no tap, no highlight, not in the visible set", () => {
    expect(STYLE_META.featureIds[ID]).toBeUndefined();
    expect(SECTION_LAYERS).not.toContain(ID);
    expect(VISIBLE_LAYERS).not.toContain(ID);
    expect(TAP_GROUPS.flatMap((g) => g.layers)).not.toContain(ID);
    const web = runtimeStyle(AT, "light") as unknown as S;
    expect(Object.keys(web.sources.atlas!.promoteId ?? {})).not.toContain(ID);
  });
});
