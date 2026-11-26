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
import { maplibreDialect } from "../packages/map/src/native/maplibre/dialect";
import { deltaE } from "./cvd";

const AT = { atlas: "file:///a.pmtiles", basemap: "file:///b.pmtiles", glyphs: "file:///g/{fontstack}/{range}.pbf",
             sprite: "file:///s", assets: "file:///x" } as never;
type L = { id: string; type: string; paint?: Record<string, unknown> };
const style = (theme: string, focus: boolean) => nativeStyle({ at: AT, theme, view: "plain",
  modes: { stream: "plain", lake: "plain" }, basemap: "map", gauges: '{"type":"FeatureCollection","features":[]}', focus }, maplibreDialect);
/** (T848) the stream as drawn: the lit sections are its heavy layer's (`stream-heavy`), `stream` hidden under it there */
const drawn = (layers: L[], id: string) => (state: Record<string, unknown>): L => {
  const own = layers.find((x) => x.id === id)!, heavy = layers.find((x) => x.id === `${id}-heavy`);
  return heavy && Number(evalPaint(heavy, "line-opacity", state)) > 0 ? heavy : own;
};
const evalPaint = (l: L, prop: string, state: Record<string, unknown>): unknown => {
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
      const [lOn, lOff] = [drawn(on.style.layers as L[], id), drawn(off.style.layers as L[], id)];
      const ev = (which: typeof lOn, prop: string, s: Record<string, unknown>) => evalPaint(which(s), prop, s);
      const own = hex(ev(lOff, cprop, {})), base = Number(ev(lOff, oprop, {}));
      expect(hex(ev(lOn, cprop, { part: true }))).toBe(own);
      expect(hex(ev(lOn, cprop, { water: true }))).toBe(own);
      expect(hex(ev(lOn, cprop, { near: true }))).toBe(own);
      expect(hex(ev(lOn, cprop, {}))).toBe(String(t["color.water.context"]).toUpperCase());
      expect(Number(ev(lOn, oprop, { part: true }))).toBeCloseTo(base);
      expect(Number(ev(lOn, oprop, { near: true }))).toBeCloseTo(base * Number(t["opacity.water.near"]));
      expect(Number(ev(lOn, oprop, {}))).toBeCloseTo(base * Number(t["opacity.water.context"]));
      // MUTATION PIN: not focused, an unlit section keeps its own colour (the browse map is untouched)
      expect(hex(ev(lOff, cprop, {}))).toBe(own);
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
