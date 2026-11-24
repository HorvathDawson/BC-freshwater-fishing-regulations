/**
 * (T1029, draft, look B of the v2 pictures) THE ONE-BANK CUE in the runtime style: the stretch drawn wider and split down
 * the middle — the bank's half in the bank's answer (`bks`), the other half in the line's own (`bko`) — and the closed cue
 * on the bank's half only, only when that answer is closed. Checked by EVALUATING the runtime style's paint (MapLibre's
 * own evaluator) at a feature-state, in every theme, in the two colourings that show it.
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { BANK_CUE_LAYERS, BANK_LAYERS, BANK_MIN_PX, BANK_MINZOOM, BANK_WIDEN } from "../packages/map/src/paint";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { resolveTheme } from "../packages/map/src/style";

type Layer = { id: string; type: string; "source-layer"?: string; paint?: Record<string, unknown>; layout?: Record<string, unknown> };
const specOf = (prop: string) =>
  (latest as unknown as Record<string, Record<string, { default?: unknown }>>)[`paint_line`]![prop]!;
function value(layer: Layer, prop: string, zoom: number, props: Record<string, unknown>, state: Record<string, unknown> = {}) {
  const v = layer.paint?.[prop] ?? specOf(prop).default;
  const e = createPropertyExpression(v, prop as never, specOf(prop) as never);
  if (e.result !== "success") throw new Error(`${layer.id} ${prop} does not parse: ${JSON.stringify(e.value)}`);
  return e.value.evaluate({ zoom } as never, { type: 2, properties: props } as never, state as never) as unknown;
}
const layers = (theme: string, mode: string) =>
  ((runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never, theme, { stream: mode }) as
    { layers: Layer[] }).layers);
const colour = (v: unknown) => String((v as { toString(): string }).toString());
const WEST_CLOSED = { bk: -1, bks: "salmon.closed", bko: "salmon.open" };
const ORD7 = { ord: 7 };

describe("(T1029) one bank: a wider line split down the middle", () => {
  for (const theme of ["light", "dark", "cvd"]) {
    const all = layers(theme, "salmon");
    const get = (id: string) => all.find((l) => l.id === id)!;
    const stream = get("stream");
    it(`${theme}: each half is half the widened stream, a quarter of it to its own side`, () => {
      for (const [side, sign] of [["right", 1], ["left", -1]] as const) {
        const half = get(BANK_LAYERS[side]);
        for (const z of [12, 14, 16]) for (const ord of [{ ord: 1 }, ORD7]) {
          const w = Number(value(stream, "line-width", z, ord));
          const t = Math.max(w * BANK_WIDEN, BANK_MIN_PX);
          expect(Number(value(half, "line-width", z, ord, WEST_CLOSED))).toBeCloseTo(t / 2, 4);
          expect(Number(value(half, "line-offset", z, ord, WEST_CLOSED))).toBeCloseTo(sign * t / 4, 4);
        }
        // the floor is reached: a small creek at z12 is drawn at the minimum, not 1.5 times its hairline
        expect(Number(value(stream, "line-width", 12, { ord: 1 })) * BANK_WIDEN).toBeLessThan(BANK_MIN_PX);
      }
    });
    it(`${theme}: the bank's half wears the bank's answer, the other half the line's own; no bank, nothing`, () => {
      const t = resolveTheme(theme);
      const left = get(BANK_LAYERS.left), right = get(BANK_LAYERS.right);
      const closed = colour(value({ id: "x", type: "line", paint: { "line-color": t["color.salmon.closed"] } }, "line-color", 14, {}));
      const open = colour(value({ id: "x", type: "line", paint: { "line-color": t["color.salmon.open"] } }, "line-color", 14, {}));
      expect(colour(value(left, "line-color", 14, ORD7, WEST_CLOSED))).toBe(closed);
      expect(colour(value(right, "line-color", 14, ORD7, WEST_CLOSED))).toBe(open);
      expect(value(left, "line-opacity", 14, ORD7, WEST_CLOSED)).toBe(1);
      expect(value(left, "line-opacity", 14, ORD7, {})).toBe(0);
      expect(value(right, "line-opacity", 14, ORD7, {})).toBe(0);
    });
    it(`${theme}: the closed cue rides the bank's half only, and only when its answer is closed`, () => {
      const left = get(BANK_CUE_LAYERS.left), right = get(BANK_CUE_LAYERS.right);
      expect(value(left, "line-opacity", 14, ORD7, WEST_CLOSED)).toBe(1);
      expect(value(right, "line-opacity", 14, ORD7, WEST_CLOSED)).toBe(0);
      expect(value(left, "line-opacity", 14, ORD7, { ...WEST_CLOSED, bks: "salmon.release" })).toBe(0);
      expect(value(left, "line-opacity", 14, ORD7, { ...WEST_CLOSED, bks: "status.closed" })).toBe(1);
      expect(Number(value(left, "line-offset", 14, ORD7, WEST_CLOSED)))
        .toBeCloseTo(Number(value(get(BANK_LAYERS.left), "line-offset", 14, ORD7, WEST_CLOSED)), 4);
      expect(left.paint?.["line-dasharray"]).toEqual(get("stream-closed").paint?.["line-dasharray"]);
    });
  }
  it("drawn from zoom 13 up only (owner 2026-11-24): every bank layer starts there", () => {
    expect(BANK_MINZOOM).toBe(13);
    for (const id of [...Object.values(BANK_LAYERS), ...Object.values(BANK_CUE_LAYERS)])
      expect((layers("light", "salmon").find((l) => l.id === id) as { minzoom?: number } | undefined)?.minzoom).toBe(13);
  });
  it("hidden on the plain map (no bucket of its own there)", () => {
    for (const id of [...Object.values(BANK_LAYERS), ...Object.values(BANK_CUE_LAYERS)])
      expect(layers("light", "plain").find((l) => l.id === id)?.layout?.["visibility"]).toBe("none");
  });
});
