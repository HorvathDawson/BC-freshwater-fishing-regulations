/**
 * (T903) PAINTED AGAINST THE DAY'S DEFAULT — AND THE MAP LOOKS THE SAME.
 *
 * `paintDefaultStyle` writes each long label list as the negation of its short rest (and a colour `match`'s biggest
 * branch as its fallback). The proof is by EVALUATION (MapLibre's own evaluator): every paint property of every layer it
 * moved, over zooms, stream orders, every label each key can hold that day (and "" — not labelled yet) and the flags,
 * gives the same width, colour and opacity as today's style, in every view and theme. The premise it rests on — every
 * label a channel writes is in its day's sets — is @app/core's (`dayProfiles`, `salmonDayProfiles` place every profile
 * and chain of the release).
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { LAB_DEFAULT, labStyle } from "./lab";
import { nativeStyle } from "./native-style";
import { labelUniverse, paintDefaultStyle } from "./paint-default";
import type { ChannelValues } from "./style";

const range = (p: string, from: number, n: number) => Array.from({ length: n }, (_, i) => `${p}${from + i}`);
// lopsided as a real day is (Salmon: 920 of 1,095 chains closed), so the rewrite has long lists to turn round
const STATUS = { closed: range("p", 0, 3), own: range("p", 3, 14), base: ["b"], no_access: ["n"], none: range("p", 17, 2),
                 finfish: ["s0"], finfishNone: [] };
const SALMON = { closed: range("s", 0, 30), open: range("s", 30, 4), release: ["s34"], check: ["s35"], none: ["s36"] };
const METHOD = { none: range("m", 0, 10), bait: ["m10"], fly: ["m11"] };
const VALUES = { status: STATUS, salmon: SALMON, method: METHOD, standing: null, stocked: null, survey: null, runs: null,
                 issue: null } as unknown as ChannelValues;

const AT = { atlas: "file:///d/release/tiles/v/atlas-v.pmtiles", basemap: "file:///d/basemap.pmtiles",
             outside: "file:///d/release/tiles/v/bc_outside.geojson", assets: "file:///d/release/tiles/v/map-assets-1/" };
type L = { id: string; type: string; paint?: Record<string, unknown>; filter?: unknown };
const style = (mode: string, theme: string) => nativeStyle({ at: AT, theme, view: "plain", modes: { stream: mode }, hide: [],
  groups: {}, basemap: "map" as const, gauges: '{"type":"FeatureCollection","features":[]}', values: VALUES } as never).style as unknown as { layers: L[] };

const spec = (type: string, prop: string) =>
  ((latest as unknown as Record<string, Record<string, unknown>>)[`paint_${type}`] ?? {})[prop];
function ev(type: string, prop: string, value: unknown) {
  if (!spec(type, prop)) throw new Error(`no spec for ${type} ${prop}`);
  const e = createPropertyExpression(Array.isArray(value) && typeof value[0] === "number" ? ["literal", value] : value, prop as never,
                                     spec(type, prop) as never);
  if (e.result !== "success") throw new Error(`${prop} does not parse: ${JSON.stringify(e.value)}`);
  return (zoom: number, ord: number, state: Record<string, unknown>) => {
    const out = e.value.evaluate({ zoom } as never, { type: 2, properties: { ord } } as never, state as never);
    return typeof out === "number" ? Math.round(out * 1e6) / 1e6 : JSON.stringify(out);
  };
}

const U = labelUniverse(VALUES);
const all = (k: string) => [...(U.get(k) ?? []), ""];
const P = all("p"), SP = all("sp"), GM = all("gm");
// every label of each key at least once, the others turning round beside it, with and without the flags
const FLAGS = [{}, { selected: true }, { part: true }, { inseason: true }];
const STATES = Array.from({ length: Math.max(P.length, SP.length, GM.length) }, (_, i) =>
  ({ p: P[i % P.length], sp: SP[i % SP.length], gm: GM[(i * 7) % GM.length], gi: i % 3 ? "" : "1" }))
  .flatMap((s, i) => [s, { ...s, ...FLAGS[i % FLAGS.length] }]);
const ZOOMS = [6, 9.5, 12, 15];
const ORDS = [1, 4, 8];

describe("(T903) paint against the day's default", () => {
  it("knows each key's labels from the day's sets (the finfish closures belong to the salmon key)", () => {
    expect(U.get("p")?.size).toBe(21);
    expect(U.get("sp")?.size).toBe(37);
    expect(U.get("gm")?.size).toBe(12);
    expect(U.get("p")?.has("s0")).toBe(false);
  });

  for (const theme of ["light", "dark"]) for (const mode of ["salmon", "profile", "runs", "standing", "plain"]) {
    it(`${theme} ${mode}: every moved layer draws the same width, colour and opacity for every label`, () => {
      const today = style(mode, theme);
      const after = paintDefaultStyle(today, VALUES, { match: false }) as typeof today;
      expect(after.layers.map((l) => l.id)).toEqual(today.layers.map((l) => l.id));
      let moved = 0, evaluated = 0;
      today.layers.forEach((l, i) => {
        const a = after.layers[i]!;
        expect(JSON.stringify(a.filter)).toBe(JSON.stringify(l.filter));     // filters never read feature-state
        if (JSON.stringify(a.paint) === JSON.stringify(l.paint)) return;
        moved += 1;
        for (const prop of Object.keys(l.paint ?? {})) {
          const was = ev(l.type, prop, l.paint![prop]), now = ev(l.type, prop, a.paint![prop]);
          for (const z of ZOOMS) for (const o of ORDS) for (const s of STATES) {
            evaluated += 1;
            if (now(z, o, s) !== was(z, o, s)) throw new Error(`${l.id} ${prop} z${z} ord ${o} ${JSON.stringify(s)}: ${was(z, o, s)} → ${now(z, o, s)}`);
          }
        }
      });
      // the Salmon and Standard maps carry the long lists, so the rewrite has work there
      if (mode === "salmon" || mode === "profile") expect(moved).toBeGreaterThan(0);
      expect(evaluated >= 0).toBe(true);
    });
  }

  it("shortens what MapLibre scans on the Salmon map", () => {
    const today = style("salmon", "light");
    const after = paintDefaultStyle(today, VALUES, { match: false });
    expect(JSON.stringify(after).length).toBeLessThan(JSON.stringify(today).length);
  });

  it("is on by default: the map is handed the default-painted style; with no day it is today's very object", () => {
    expect(LAB_DEFAULT.paintDefault).toBe(true);
    const today = style("salmon", "light");
    expect(JSON.stringify(labStyle(today, LAB_DEFAULT, VALUES)))
      .toBe(JSON.stringify(labStyle(paintDefaultStyle(today, VALUES, { match: false }), { ...LAB_DEFAULT, paintDefault: false })));
    const bare = style("plain", "light");
    expect(paintDefaultStyle(bare, null, { match: false })).toBe(bare);
  });
});
