/**
 * (T594, owner 2026-11-11: look C) OVER SATELLITE THE STREAM LINE OPENS — AND NOTHING ELSE MOVES.
 *
 * `on-imagery.ts`: zoomed in over imagery, a stream line becomes two thin edges with the water clear between; a lake's
 * fill goes light. Pinned here by EVALUATING what each map is handed (MapLibre's own evaluator, as midzoom.test.ts does)
 * over zooms, stream orders and feature-states:
 *   - the drawn map is painted exactly as before (no gap, the same width and opacity) — the owner's street map;
 *   - over imagery, below z11 every stream layer paints what the drawn map paints (the overview is unchanged);
 *   - from z14 the line is two `width.imagery.edge` edges round a clear middle at least `IMAGERY_GAP_FLOOR` wide, so a
 *     creek a few pixels wide is between the edges, never under one;
 *   - THE TAP: MapLibre hit-tests a line over its whole band — `gap + 2 × width` when it has a gap, `width` when not
 *     (GL JS `getLineWidth`, MapLibre Native `RenderLineLayer::queryIntersectsFeature`) — and the band is never narrower
 *     than the solid line the drawn map hands the same tap, at any zoom, order or state;
 *   - the satellite → map switch takes the gap back on a live map (the web restyles in place);
 *   - the phone is handed the same opened paint (`nativeStyle`, one style source).
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import type { DayProfiles } from "@app/core";
import { baseAdapter, type MapHandle } from "../packages/map/src/adapters/contract";
import { nativeStyle } from "../packages/map/src/native-style";
import { IMAGERY_GAP_FLOOR } from "../packages/map/src/on-imagery";
import { rampAt } from "../packages/map/src/paint";
import { resolveTheme } from "../packages/map/src/style";

const DAY: DayProfiles = { closed: ["p1"], own: ["p2"], base: ["b"], no_access: ["n"], none: ["p3"] };
const THEMES = ["light", "dark", "cvd"] as const;
const ORDS = [1, 2, 3, 4, 6, 8, 10];
const STATES = [{ p: "p1" }, { p: "p2" }, { p: "b" }, { p: "p2", selected: true }, { p: "b", water: true }];
const ZOOMS = [6, 8, 10, 10.5, 11, 11.5, 12, 12.5, 13, 13.5, 14, 15, 16, 17, 18];
/** The layers drawn with the stream's width. */
const STREAM_LAYERS = ["stream", "stream-closed", "stream-gear-fly", "stream-gear-bait", "stream-runs-pass", "selection-casing"];

type Paint = Record<string, Record<string, unknown>>;
function painted(theme: string, basemaps: ("map" | "satellite")[]): Paint {
  const paint: Paint = {};
  const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                         setFeatureState: () => {}, clearFeatureStates: () => {} };
  const a = baseAdapter("web");
  a.applyView(h, "plain", theme);
  for (const l of ["stream", "lake", "lake_flowing"]) a.setLayerMode(h, l, "profile", theme);
  a.setChannel(h, "status", DAY, theme);
  for (const b of basemaps) a.setBasemap(h, b, theme);
  return paint;
}

const specOf = (prop: string) =>
  (latest as unknown as Record<string, Record<string, { default?: unknown }>>)[`paint_${prop.split("-")[0]}`]![prop]!;
const evaluator = (prop: string, value: unknown) => {
  const spec = specOf(prop);
  const e = createPropertyExpression(value ?? spec.default, prop as never, spec as never);
  if (e.result !== "success") throw new Error(`${prop} does not parse: ${JSON.stringify(e.value)}`);
  return (zoom: number, ord: number, state: Record<string, unknown>) =>
    e.value.evaluate({ zoom } as never, { type: 2, properties: { ord, area_m2: 1e6 } } as never, state as never) as number;
};
/** What a tap's query box meets: the band MapLibre tests a line with. */
const band = (w: number, g: number) => (g > 0 ? g + 2 * w : w);

describe("over satellite the stream opens; the drawn map does not move (T594)", () => {
  for (const theme of THEMES) {
    const map = painted(theme, ["map"]);
    const sat = painted(theme, ["satellite"]);
    const back = painted(theme, ["satellite", "map"]);
    const fresh = painted(theme, []);
    const e = Number(resolveTheme(theme)["width.imagery.edge"]);

    it(`${theme}: the drawn map is painted exactly as a map that never saw satellite`, () => {
      // every layer the adapter painted, before the ground's own (the basemap's names, which only a ground paints)
      expect(Object.fromEntries(Object.keys(fresh).map((l) => [l, map[l]]))).toEqual(fresh);
      for (const l of STREAM_LAYERS) expect(map[l]?.["line-gap-width"], l).toBeUndefined();
    });

    it(`${theme}: back from satellite, every stream layer's gap is taken back and its paint is the drawn map's`, () => {
      for (const l of STREAM_LAYERS) {
        expect(back[l]?.["line-gap-width"], l).toBe(0);
        const { ["line-gap-width"]: _gap, ...rest } = back[l]!;
        expect(rest, l).toEqual(map[l]);
      }
      for (const l of ["lake", "lake_flowing", "lake__edge"]) expect(back[l], l).toEqual(map[l]);
    });

    it(`${theme}: overview zooms are unchanged; from z14 two ${e} px edges round a clear middle a creek fits in`, () => {
      const problems: string[] = [];
      for (const l of STREAM_LAYERS) {
        const wMap = evaluator("line-width", map[l]!["line-width"]);
        const wSat = evaluator("line-width", sat[l]!["line-width"]);
        const gSat = evaluator("line-gap-width", sat[l]!["line-gap-width"]);
        for (const z of ZOOMS) for (const ord of ORDS) for (const s of STATES) {
          const [wm, ws, gs] = [wMap(z, ord, s), wSat(z, ord, s), gSat(z, ord, s)];
          const where = `${l} z${z} ord ${ord} ${JSON.stringify(s)}`;
          if (z <= 11 && (gs !== 0 || Math.abs(ws - wm) > 1e-6)) problems.push(`${where}: changed at overview (${wm} -> ${ws} gap ${gs})`);
          // THE TAP: never a narrower band than the drawn map's line
          if (band(ws, gs) < wm - 1e-6) problems.push(`${where}: band ${band(ws, gs)} < the drawn line ${wm}`);
          if (l === "stream" && z >= 14) {
            if (Math.abs(ws - e) > 1e-6) problems.push(`${where}: edge ${ws}, not ${e}`);
            if (gs < rampAt(IMAGERY_GAP_FLOOR, z) - 1e-6) problems.push(`${where}: clear middle ${gs} under the floor`);
          }
        }
      }
      expect(problems, problems.slice(0, 8).join("\n")).toEqual([]);
    });

    it(`${theme}: the gear edges and the selection's outline sit outside the stream's edges by what they showed before`, () => {
      const problems: string[] = [];
      const wStream = evaluator("line-width", sat["stream"]!["line-width"]);
      const gStream = evaluator("line-gap-width", sat["stream"]!["line-gap-width"]);
      for (const l of ["stream-gear-fly", "selection-casing"]) {
        const pad = (z: number, o: number, s: Record<string, unknown>) =>
          (evaluator("line-width", map[l]!["line-width"])(z, o, s) - evaluator("line-width", map["stream"]!["line-width"])(z, o, s)) / 2;
        const w = evaluator("line-width", sat[l]!["line-width"]), g = evaluator("line-gap-width", sat[l]!["line-gap-width"]);
        for (const z of [14, 16]) for (const ord of ORDS) for (const s of STATES) {
          if (Math.abs(g(z, ord, s) - gStream(z, ord, s)) > 1e-6) problems.push(`${l} z${z} ord ${ord}: its gap is not the stream's`);
          if (Math.abs(w(z, ord, s) - wStream(z, ord, s) - pad(z, ord, s)) > 1e-6)
            problems.push(`${l} z${z} ord ${ord}: shows ${w(z, ord, s) - wStream(z, ord, s)} outside, not ${pad(z, ord, s)}`);
        }
      }
      expect(problems, problems.slice(0, 8).join("\n")).toEqual([]);
    });

    it(`${theme}: a lake's fill is light close in, its outline heavier; unchanged at the overview`, () => {
      for (const l of ["lake", "lake_flowing"]) {
        const fm = evaluator("fill-opacity", map[l]!["fill-opacity"]), fs = evaluator("fill-opacity", sat[l]!["fill-opacity"]);
        expect(fs(10, 1, { p: "b" })).toBeCloseTo(fm(10, 1, { p: "b" }), 6);
        expect(fs(14, 1, { p: "b" })).toBeCloseTo(fm(14, 1, { p: "b" }) * Number(resolveTheme(theme)["opacity.imagery.fill"]), 6);
      }
      const em = evaluator("line-width", map["lake__edge"]!["line-width"]), es = evaluator("line-width", sat["lake__edge"]!["line-width"]);
      expect(es(10, 1, { p: "b" })).toBeCloseTo(em(10, 1, { p: "b" }), 6);
      expect(es(14, 1, { p: "b" })).toBeCloseTo(em(14, 1, { p: "b" }) * 1.8, 6);
    });

    it(`${theme}: the phone is handed the same opened paint, and its drawn map has no gap`, () => {
      const modes = { stream: "profile", lake: "profile", lake_flowing: "profile" };
      const layers = (b: "map" | "satellite") => Object.fromEntries((nativeStyle({ at: { atlas: "a", basemap: "b" } as never,
        theme, view: "plain", modes, basemap: b, gauges: '{"type":"FeatureCollection","features":[]}',
        values: { status: DAY } as never }).style as { layers: { id: string; paint?: Record<string, unknown> }[] }).layers
        .map((x) => [x.id, x.paint ?? {}]));
      const [nm, ns] = [layers("map"), layers("satellite")];
      for (const l of STREAM_LAYERS) {
        expect(nm[l]?.["line-gap-width"], l).toBeUndefined();
        expect(ns[l]?.["line-gap-width"], l).toEqual(sat[l]!["line-gap-width"]);
        expect(ns[l]?.["line-width"], l).toEqual(sat[l]!["line-width"]);
      }
    });
  }
});
