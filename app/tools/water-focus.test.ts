/**
 * (T459, owner 2026-11-06: "in the section navigation with up/downstream all other rivers should be greyed out") A WATER'S
 * MAP VIEW, FOCUSED: the style a phone is handed (`nativeStyle`, through the one `applyLook`) draws this water (`part`,
 * `water`) in its colours, the water it flows into (`near`) in its colours muted, and every other section in the one
 * context grey — and nothing changes on a map that is not focused (the browse map). The grey is no status: no colour mode
 * paints it, so no legend lists it, and it stands apart from every Map-tab status and from "unknown" (rule 23).
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { STYLE_META, resolveTheme } from "@app/map";
import { nativeStyle } from "../packages/map/src/native-style";
import { deltaE } from "./cvd";

const AT = { atlas: "file:///a.pmtiles", basemap: "file:///b.pmtiles", glyphs: "file:///g/{fontstack}/{range}.pbf",
             sprite: "file:///s", assets: "file:///x" } as never;
type L = { id: string; type: string; paint?: Record<string, unknown> };
const style = (theme: string, focus: boolean) => nativeStyle({ at: AT, theme, view: "plain",
  modes: { stream: "plain", lake: "plain" }, basemap: "map", gauges: '{"type":"FeatureCollection","features":[]}', focus });
const evalPaint = (l: L, prop: string, state: Record<string, unknown>) => {
  const spec = (latest as unknown as Record<string, Record<string, { default?: unknown }>>)[`paint_${l.type}`]![prop]!;
  const e = createPropertyExpression(l.paint?.[prop] ?? spec.default, prop as never, spec as never);
  if (e.result !== "success") throw new Error(`${l.id} ${prop} does not parse`);
  return e.value.evaluate({ zoom: 12 } as never, { type: 2, properties: {} } as never, state as never);
};
const hex = (c: unknown) => {
  const { r, g, b } = c as { r: number; g: number; b: number };
  return "#" + [r, g, b].map((v) => Math.round(v * 255).toString(16).padStart(2, "0")).join("").toUpperCase();
};

describe("(T459) a water's map view, focused", () => {
  for (const theme of ["light", "dark"]) it(`${theme}: this water in colour, the receiving water muted, the rest grey`, () => {
    const t = resolveTheme(theme);
    const on = style(theme, true), off = style(theme, false);
    expect([...on.errors, ...off.errors]).toEqual([]);
    for (const [id, cprop, oprop] of [["stream", "line-color", "line-opacity"], ["lake", "fill-color", "fill-opacity"]] as const) {
      const l = (on.style.layers as L[]).find((x) => x.id === id)!, o = (off.style.layers as L[]).find((x) => x.id === id)!;
      const own = hex(evalPaint(o, cprop, {})), base = Number(evalPaint(o, oprop, {}));
      expect(hex(evalPaint(l, cprop, { part: true }))).toBe(own);
      expect(hex(evalPaint(l, cprop, { water: true }))).toBe(own);
      expect(hex(evalPaint(l, cprop, { near: true }))).toBe(own);
      expect(hex(evalPaint(l, cprop, {}))).toBe(String(t["color.water.context"]).toUpperCase());
      expect(Number(evalPaint(l, oprop, { part: true }))).toBeCloseTo(base);
      expect(Number(evalPaint(l, oprop, { near: true }))).toBeCloseTo(base * Number(t["opacity.water.near"]));
      expect(Number(evalPaint(l, oprop, {}))).toBeCloseTo(base * Number(t["opacity.water.context"]));
      // MUTATION PIN: not focused, an unlit section keeps its own colour (the browse map is untouched)
      expect(hex(evalPaint(o, cprop, {}))).toBe(own);
    }
  });

  it("the context grey is no status: no colour mode paints it, and it stands apart from every Map-tab status and unknown", () => {
    const painted = JSON.stringify(STYLE_META.colorModes);
    expect(painted.includes("color.water.context")).toBe(false);
    for (const theme of ["light", "dark", "cvd"]) {
      const t = resolveTheme(theme);
      for (const k of ["color.status.closed", "color.status.own", "color.status.base", "color.status.no_access",
                       "color.water.unmapped"])
        expect(deltaE(String(t["color.water.context"]), String(t[k])), `${theme} ${k}`).toBeGreaterThanOrEqual(12);
    }
  });
});
