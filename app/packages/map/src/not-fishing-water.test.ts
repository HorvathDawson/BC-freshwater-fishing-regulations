/**
 * Q125 (owner 2026-10-24): sewage basins, tailing ponds and dumps — T152's `not_fishing_water` tile layer — DRAW as a
 * muted fill, never as a lake. T292 (owner 2026-10-29, "they should be an off colour"): a near-opaque off colour with a
 * dashed edge, so the basemap's own water under them cannot show through, and a tap names what it is (never a section,
 * never regulations). Held on the web style, the native style and the tap/visible sets, so a later edit cannot paint
 * them lake-blue or make them answer as a water.
 */
import { describe, expect, it } from "vitest";
import { paintFor } from "./paint";
import { nativeStyle } from "./native-style";
import { runtimeStyle } from "./runtime-style";
import { firstNotFishing, NOT_FISHING_LAYERS, SECTION_LAYERS, TAP_GROUPS, VISIBLE_LAYERS } from "./sections";
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
      // (T292) near-opaque: the basemap's OSM water under a tailing pond must not show through as blue
      expect(t["opacity.notfishing"] as number, theme).toBeGreaterThanOrEqual(0.85);
      expect(typeof t["color.notfishing.edge"], theme).toBe("string");
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
      // never a lake: only its own fill and its dashed edge draw the source-layer, and no lake layer does
      const own = s.layers.filter((x) => x["source-layer"] === ID);
      expect(own.map((x) => [x.id, x.type])).toEqual([[ID, "fill"], [`${ID}__edge`, "line"]]);
      expect(own[1]!.paint?.["line-dasharray"], `${platform} ${theme}: the edge is dashed`).toBeDefined();
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

  it("(T292) a tap on one is its own, after the water, the last thing a tap asks: the kind, never a section", () => {
    expect(NOT_FISHING_LAYERS).toEqual([ID]);
    expect(firstNotFishing([{ properties: { kind: "tailings" } }, { properties: { kind: "sewage" } }]))
      .toEqual({ kind: "tailings" });
    // an older atlas (no `kind`) or a value this app does not know: still the sheet, unnamed
    expect(firstNotFishing([{ properties: {} }])).toEqual({ kind: null });
    expect(firstNotFishing([{ properties: { kind: "lake" } }])).toEqual({ kind: null });
    expect(firstNotFishing([])).toBeNull();
  });
});
