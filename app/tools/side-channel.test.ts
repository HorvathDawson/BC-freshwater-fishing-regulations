/**
 * (T891, owner 2026-11-18: "side channels should be less bold too") A braid's SIDE CHANNEL — a stream feature carrying the
 * tile attribute `sc` (pipeline export.is_side_channel_piece; absent on the main channel) — is drawn THINNER than the main
 * channel, in the SAME colour, and the main channel keeps its width. Checked by EVALUATING the runtime style's paint
 * (MapLibre's own evaluator) in every theme and every stream mode (Trout/regs `profile`, `salmon`, Conditions `standing`,
 * `runs`, `plain`): every layer sized from the stream's width follows it, no layer is added for it, and a selected side
 * channel still takes the highlight's floor, under a selection casing.
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { STYLE_META, resolveTheme } from "../packages/map/src/style";

type Layer = { id: string; type: string; "source-layer"?: string; paint?: Record<string, unknown> };
const THEMES = ["light", "dark", "cvd"] as const;
const MODES = Object.keys(STYLE_META.colorModes["stream"] ?? {});
const ZOOMS = [6, 8, 10, 12, 14, 16];
const ORDS = [1, 4, 7];

const specOf = (prop: string) =>
  (latest as unknown as Record<string, Record<string, { default?: unknown }>>)[`paint_line`]![prop]!;
/** `prop` of `layer` evaluated at a zoom, for a feature with `props`, in feature-state `state`. */
function value(layer: Layer, prop: string, zoom: number, props: Record<string, unknown>, state: Record<string, unknown> = {}) {
  const v = layer.paint?.[prop] ?? specOf(prop).default;
  const e = createPropertyExpression(v, prop as never, specOf(prop) as never);
  if (e.result !== "success") throw new Error(`${layer.id} ${prop} does not parse: ${JSON.stringify(e.value)}`);
  return e.value.evaluate({ zoom } as never, { type: 2, properties: props } as never, state as never) as unknown;
}
const streamLines = (theme: string, mode: string) =>
  ((runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never, theme, { stream: mode }) as
    { layers: Layer[] }).layers).filter((l) => l.type === "line" && l["source-layer"] === "stream");

describe("(T891) side channels draw thinner, in the same colour", () => {
  it("the style reads the tile's `sc` through a width factor, not a layer of its own", () => {
    const w = STYLE_META.widths["stream"] as { sideChannel?: { has: string; token: string } };
    expect(w.sideChannel).toEqual({ has: "sc", token: "width.stream.side" });
    for (const theme of THEMES) {
      const f = Number(resolveTheme(theme)["width.stream.side"]);
      expect(f > 0 && f < 1, `${theme}: width.stream.side ${f}`).toBe(true);
      // the same stream layers with or without side channels: no layer was added for them
      expect(streamLines(theme, "plain").map((l) => l.id).filter((id) => /side|sc\b/.test(id))).toEqual([]);
    }
  });

  for (const theme of THEMES) {
    const f = Number(resolveTheme(theme)["width.stream.side"]);
    for (const mode of MODES) {
      it(`${theme} / ${mode}: the side channel at the factor, the main channel untouched, same colour; every mark follows`, () => {
        const layers = streamLines(theme, mode);
        const stream = layers.find((l) => l.id === "stream")!;
        for (const z of ZOOMS) for (const ord of ORDS) {
          const main = Number(value(stream, "line-width", z, { ord }));
          const side = Number(value(stream, "line-width", z, { ord, sc: 1 }));
          expect(side, `z${z} ord ${ord}`).toBeCloseTo(main * f, 6);
          expect(JSON.stringify(value(stream, "line-color", z, { ord, sc: 1 })), `colour z${z} ord ${ord}`)
            .toBe(JSON.stringify(value(stream, "line-color", z, { ord })));
          // every other stream-source line: never wider on a side channel than on the main channel
          for (const l of layers) {
            if (!l.paint?.["line-width"]) continue;
            for (const state of [{}, { selected: true }, { part: true }]) {
              const [m, s] = [Number(value(l, "line-width", z, { ord }, state)),
                              Number(value(l, "line-width", z, { ord, sc: 1 }, state))];
              expect(s, `${l.id} z${z} ord ${ord} ${JSON.stringify(state)}`).toBeLessThanOrEqual(m + 1e-9);
            }
          }
        }
      });

      it(`${theme} / ${mode}: a selected side channel still reads as selected (the highlight's floor, under a casing)`, () => {
        const layers = streamLines(theme, mode);
        const heavy = layers.find((l) => l.id === "stream-heavy")!;
        const casing = layers.find((l) => l.id === "selection-casing")!;
        const floor = Number(resolveTheme(theme)["width.stream.highlight"]);
        for (const z of ZOOMS) {
          const sel = { selected: true }, props = { ord: 4, sc: 1 };
          expect(Number(value(heavy, "line-opacity", z, props, sel)), `heavy shows z${z}`).toBeGreaterThan(0);
          expect(Number(value(heavy, "line-width", z, props, sel)), `floor z${z}`).toBeGreaterThanOrEqual(floor - 1e-9);
          expect(Number(value(casing, "line-opacity", z, props, sel)), `casing z${z}`)
            .toBe(Number(value(casing, "line-opacity", z, { ord: 4 }, sel)));
          expect(Number(value(casing, "line-width", z, props, sel)), `casing wider z${z}`)
            .toBeGreaterThan(Number(value(heavy, "line-width", z, props, sel)));
        }
      });
    }
  }
});
