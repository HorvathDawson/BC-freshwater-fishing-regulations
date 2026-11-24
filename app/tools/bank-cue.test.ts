/**
 * (T1029, look B, owner 2026-11-24) THE ONE-BANK CUE in the style a map is handed: the stretch drawn wider and split down
 * the middle — the bank's half in the BANK's chain's answer that day (feature-state `bk` "Ls1": left of the drawn line,
 * chain s1), the other half in the line's own (`sp`) — and the closed cue on the bank's half only, only when the bank's
 * answer is closed. Checked by EVALUATING (MapLibre's own evaluator) the style `nativeStyle` builds with the day
 * installed — the adapter's own paint, the call every later day repaints with — in every theme; and the first frame
 * (`runtimeStyle`, no day) draws none.
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { BANK_CUE_LAYERS, BANK_LAYERS, BANK_MIN_PX, BANK_MINZOOM, BANK_WIDEN, bankCuePaint, bankHalfPaint } from "../packages/map/src/paint";
import { nativeStyle } from "../packages/map/src/native-style";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { resolveTheme, type ChannelValues } from "../packages/map/src/style";

type Layer = { id: string; type: string; "source-layer"?: string; paint?: Record<string, unknown>; layout?: Record<string, unknown>; minzoom?: number };
const specOf = (prop: string) =>
  (latest as unknown as Record<string, Record<string, { default?: unknown }>>)[`paint_line`]![prop]!;
function value(layer: Layer, prop: string, zoom: number, props: Record<string, unknown>, state: Record<string, unknown> = {}) {
  const v = layer.paint?.[prop] ?? specOf(prop).default;
  const e = createPropertyExpression(Array.isArray(v) && typeof v[0] === "number" ? ["literal", v] : v, prop as never, specOf(prop) as never);
  if (e.result !== "success") throw new Error(`${layer.id} ${prop} does not parse: ${JSON.stringify(e.value)}`);
  return e.value.evaluate({ zoom } as never, { type: 2, properties: props } as never, state as never) as unknown;
}
const colour = (v: unknown) => String((v as { toString(): string }).toString());
const hex = (h: unknown) => colour(value({ id: "x", type: "line", paint: { "line-color": h } }, "line-color", 14, {}));

// the day: chain s0 open, the bank's chain s1 closed; the status day marks "n" no access
const SALMON = { open: ["s0"], closed: ["s1"], release: ["s2"], check: ["s3"], none: ["s4"] };
const STATUS = { closed: ["p0"], own: ["p1"], base: ["b"], no_access: ["n"], none: ["p2"] };
const VALUES = { status: STATUS, salmon: SALMON, method: null, standing: null, stocked: null, survey: null, runs: null,
                 issue: null, bank: null } as unknown as ChannelValues;
const AT = { atlas: "file:///d/a.pmtiles", basemap: "file:///d/b.pmtiles", outside: "file:///d/o.geojson", assets: "file:///d/m/" };
function handed(theme: string, mode: string, values: ChannelValues = VALUES): Layer[] {
  const { style, errors } = nativeStyle({ at: AT, theme, view: "plain", modes: { stream: mode }, hide: [], groups: {},
    basemap: "map" as const, gauges: '{"type":"FeatureCollection","features":[]}', values } as never);
  expect(errors).toEqual([]);
  return (style as unknown as { layers: Layer[] }).layers;
}
const firstFrame = (theme: string, mode: string) =>
  (runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never, theme, { stream: mode }) as { layers: Layer[] }).layers;

const WEST = { bk: "Ls1", sp: "s0", p: "p1" };
const ORD7 = { ord: 7 };
const ALL = [...Object.values(BANK_LAYERS), ...Object.values(BANK_CUE_LAYERS)];

describe("(T1029) one bank: a wider line split down the middle", () => {
  for (const theme of ["light", "dark", "cvd"]) {
    const all = handed(theme, "salmon");
    const get = (id: string) => all.find((l) => l.id === id)!;
    const stream = get("stream");
    const t = resolveTheme(theme);
    it(`${theme}: each half is half the widened stream, a quarter of it to its own side`, () => {
      for (const [side, sign] of [["right", 1], ["left", -1]] as const) {
        const half = get(BANK_LAYERS[side]), cue = get(BANK_CUE_LAYERS[side]);
        for (const z of [13, 14, 16]) for (const ord of [{ ord: 1 }, ORD7]) {
          const w = Number(value(stream, "line-width", z, ord));
          const full = Math.max(w * BANK_WIDEN, BANK_MIN_PX);
          expect(Number(value(half, "line-width", z, ord, WEST))).toBeCloseTo(full / 2, 4);
          expect(Number(value(half, "line-offset", z, ord, WEST))).toBeCloseTo(sign * full / 4, 4);
          expect(Number(value(cue, "line-offset", z, ord, WEST))).toBeCloseTo(sign * full / 4, 4);
        }
      }
      // the floor is reached: a small creek at z13 is drawn at the minimum, not 1.5 times its hairline
      expect(Number(value(stream, "line-width", 13, { ord: 1 })) * BANK_WIDEN).toBeLessThan(BANK_MIN_PX);
    });
    it(`${theme}: the bank's half wears the bank chain's answer, the other half the line's own; no bank, nothing`, () => {
      const left = get(BANK_LAYERS.left), right = get(BANK_LAYERS.right);
      const closed = hex(t["color.salmon.closed"]), open = hex(t["color.salmon.open"]);
      expect(closed).not.toBe(open);
      expect(colour(value(left, "line-color", 14, ORD7, WEST))).toBe(closed);          // s1, the bank's chain: closed
      expect(colour(value(right, "line-color", 14, ORD7, WEST))).toBe(open);           // s0, the line's own: open
      expect(colour(value(right, "line-color", 14, ORD7, WEST))).toBe(colour(value(stream, "line-color", 14, ORD7, WEST)));
      // the bank on the other side: the halves swap
      const east = { ...WEST, bk: "Rs1" };
      expect(colour(value(right, "line-color", 14, ORD7, east))).toBe(closed);
      expect(colour(value(left, "line-color", 14, ORD7, east))).toBe(open);
      // the bank's chain open that day: both halves open
      expect(colour(value(left, "line-color", 14, ORD7, { ...WEST, bk: "Ls0" }))).toBe(open);
      for (const half of [left, right]) {
        expect(value(half, "line-opacity", 14, ORD7, WEST)).toBe(1);
        expect(value(half, "line-opacity", 14, ORD7, { sp: "s0", p: "p1" })).toBe(0);   // no bank: nothing drawn
        expect(value(half, "line-opacity", 14, ORD7, { ...WEST, p: "n" })).toBe(0);     // no access: the dots alone
      }
    });
    it(`${theme}: the closed cue rides the bank's half only, and only when the bank's answer is closed`, () => {
      const left = get(BANK_CUE_LAYERS.left), right = get(BANK_CUE_LAYERS.right);
      expect(value(left, "line-opacity", 14, ORD7, WEST)).toBe(1);
      expect(value(right, "line-opacity", 14, ORD7, WEST)).toBe(0);
      expect(value(right, "line-opacity", 14, ORD7, { ...WEST, bk: "Rs1" })).toBe(1);
      expect(value(left, "line-opacity", 14, ORD7, { ...WEST, bk: "Ls0" })).toBe(0);   // the bank open: no cue
      expect(value(left, "line-opacity", 14, ORD7, { ...WEST, sp: "s1", bk: "Ls0" })).toBe(0);   // the LINE closed is not the bank's
      expect(value(left, "line-opacity", 14, ORD7, { sp: "s1" })).toBe(0);
      expect(value(left, "line-opacity", 14, ORD7, { ...WEST, p: "n" })).toBe(0);
      expect(left.paint?.["line-dasharray"]).toEqual(get("stream-closed").paint?.["line-dasharray"]);
    });
  }
  it("Standard (profile) shows the layers but draws nothing on them yet, whatever the label", () => {
    // the paint itself, with every day installed (on a map the salmon day may arrive before or after the status day)
    const t = resolveTheme("light");
    for (const side of ["left", "right"] as const) {
      expect(bankHalfPaint(side, "profile", t, VALUES, 2)["line-opacity"]).toBe(0);
      expect(bankCuePaint(side, "profile", t, VALUES, 2)["line-opacity"]).toBe(0);
    }
    const all = handed("light", "profile");
    for (const id of ALL) {
      const l = all.find((x) => x.id === id)!;
      expect(l.layout?.["visibility"]).not.toBe("none");
      expect(value(l, "line-opacity", 14, ORD7, WEST)).toBe(0);
    }
  });
  it("no salmon day installed, and the first frame: nothing drawn", () => {
    const noDay = handed("light", "salmon", { ...VALUES, salmon: null });
    for (const layers of [noDay, firstFrame("light", "salmon")])
      for (const id of ALL) expect(value(layers.find((x) => x.id === id)!, "line-opacity", 14, ORD7, WEST)).toBe(0);
  });
  it("drawn from zoom 13 up only (owner 2026-11-24): every bank layer starts there", () => {
    expect(BANK_MINZOOM).toBe(13);
    for (const id of ALL) expect(handed("light", "salmon").find((l) => l.id === id)?.minzoom).toBe(13);
  });
  it("hidden on the plain map (no bucket of its own there)", () => {
    for (const layers of [handed("light", "plain"), firstFrame("light", "plain")])
      for (const id of ALL) expect(layers.find((l) => l.id === id)?.layout?.["visibility"]).toBe("none");
  });
  it("no label test is an `in` scan over the bank's label (T903): a `match`, the same answer", () => {
    // a finfish closure that day puts a label test over the chain into the line's colour (the no-access grey held off it)
    const fin = { ...VALUES, status: { ...STATUS, finfish: ["s3"] } } as unknown as ChannelValues;
    const all = handed("light", "salmon", fin);
    const stream = all.find((l) => l.id === "stream")!, left = all.find((l) => l.id === BANK_LAYERS.left)!;
    expect(JSON.stringify(stream.paint?.["line-color"])).toMatch(/\["in",\["coalesce",\["feature-state","sp"\]/);
    for (const id of ALL) expect(JSON.stringify(all.find((l) => l.id === id)!.paint)).not.toMatch(/\["in",\["slice"/);
    // no access on a finfish-closed chain is not grey: the bank's half reads the BANK's chain for it
    const t = resolveTheme("light");
    expect(colour(value(left, "line-color", 14, ORD7, { bk: "Ls3", sp: "s0", p: "n" }))).not.toBe(hex(t["color.status.no_access"]));
    expect(colour(value(left, "line-color", 14, ORD7, { bk: "Ls1", sp: "s3", p: "n" }))).toBe(hex(t["color.status.no_access"]));
  });
});
