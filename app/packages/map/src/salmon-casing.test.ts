/**
 * (T887 option B) THE KEEP CASING AND THE GEAR MARKS. A cased keep section in the Salmon view still shows its fly-only
 * (solid) and bait-ban (dashed) edges: the casing layer sits UNDER the gear edges (and the line over both),
 * and on a kept section the edge is pushed OUTSIDE the casing, so no part of the mark is drawn under it at any zoom.
 * Independent review 2026-11-18: the first B drew the casing over the edges and hid them below about z13.
 */
import { describe, expect, it } from "vitest";
import { GEAR_LAYERS, SALMON_CASING_LAYER, gearPaint, salmonCasingPaint, streamWidths } from "./paint";
import { runtimeStyle } from "./runtime-style";
import { PMTILES } from "./tile-access";
import { NO_VALUES, resolveTheme, type ChannelValues } from "./style";

const tk = resolveTheme("light");
const values = {
  ...NO_VALUES,
  salmon: { open: ["s1"], release: ["s2"], closed: [], check: [], none: [] },
  method: { fly: ["g1"], bait: ["g2"] },
} as unknown as ChannelValues;
/** a paint expression evaluated for a feature with this state, at one zoom (the pieces these paints use) */
function evaluate(e: unknown, state: Record<string, string>, zoom: number): unknown {
  if (!Array.isArray(e)) return e;
  const [op, ...a] = e as [string, ...unknown[]];
  const ev = (x: unknown) => evaluate(x, state, zoom);
  switch (op) {
    case "interpolate": {
      const stops = a.slice(2) as unknown[];
      for (let i = 0; i < stops.length; i += 2)
        if (zoom <= (stops[i] as number) || i + 2 >= stops.length) {
          if (i === 0 || zoom >= (stops[i] as number)) return ev(stops[i + 1]);
          const [z0, z1] = [stops[i - 2] as number, stops[i] as number];
          const [v0, v1] = [ev(stops[i - 1]) as number, ev(stops[i + 1]) as number];
          return v0 + (v1 - v0) * (zoom - z0) / (z1 - z0);
        }
      return undefined;
    }
    case "zoom": return zoom;
    case "+": return a.reduce((s: number, x) => s + (ev(x) as number), 0);
    case "*": return a.reduce((s: number, x) => s * (ev(x) as number), 1);
    case "max": return Math.max(...a.map((x) => ev(x) as number));
    case "case": { for (let i = 0; i + 1 < a.length; i += 2) if (ev(a[i])) return ev(a[i + 1]); return ev(a[a.length - 1]); }
    case "in": return (ev(a[1]) as unknown[]).includes(ev(a[0]));
    case "literal": return a[0];
    case "coalesce": { for (const x of a) { const v = ev(x); if (v !== null && v !== undefined) return v; } return null; }
    case "feature-state": return state[a[0] as string] ?? null;
    case "get": return a[0] === "ord" ? 4 : undefined;   // a mid-order stream
    case "has": return false;                             // (T891) the main channel: no `sc`
    case "to-number": { const v = ev(a[0]); return v === null || v === undefined ? Number(ev(a[1]) ?? 0) : Number(v); }
    case "!": return !ev(a[0]);
    case "all": return a.every((x) => ev(x));
    case "any": return a.some((x) => ev(x));
    case "boolean": return Boolean(ev(a[0]) ?? a[1]);
    case "==": return ev(a[0]) === ev(a[1]);
    default: throw new Error(`unhandled ${op}`);
  }
}

describe("(T887) the keep casing never hides a gear mark", () => {
  it("light has a casing (option B): the layer is under both gear edges, which are under the line", () => {
    expect(Number(tk["width.salmon.casing"])).toBeGreaterThan(0);
    const ids = (runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, "light", { stream: "salmon" }, PMTILES) as
      { layers: { id: string }[] }).layers.map((l) => l.id);
    const at = (id: string) => ids.indexOf(id);
    expect(at(SALMON_CASING_LAYER) < at(GEAR_LAYERS.stream.fly) && at(SALMON_CASING_LAYER) < at(GEAR_LAYERS.stream.bait)
           && at(GEAR_LAYERS.stream.bait) < at("stream")).toBe(true);
  });

  it("on a kept, fly-only section the edge shows and reaches past the casing at every zoom; elsewhere it is unchanged", () => {
    const sw = streamWidths("salmon", tk, values);
    const casing = salmonCasingPaint("salmon", tk, values, sw.base);
    for (const mark of ["fly", "bait"] as const) {
      const gear = gearPaint("stream", "salmon", mark, tk, values, sw.base);
      const label = mark === "fly" ? "g1" : "g2";
      for (const z of [9, 10, 11, 12, 13, 14, 16]) {
        const kept = { sp: "s1", gm: label }, released = { sp: "s2", gm: label };
        expect(evaluate(casing["line-opacity"], kept, z)).toBe(1);
        expect([mark, z, (evaluate(gear["line-opacity"], kept, z) as number) > 0]).toEqual([mark, z, true]);
        const edgeW = evaluate(gear["line-width"], kept, z) as number, casingW = evaluate(casing["line-width"], kept, z) as number;
        expect([mark, z, edgeW > casingW]).toEqual([mark, z, true]);
        // an uncased (release) section: the edge hugs the line as before, and no casing is drawn
        expect(evaluate(casing["line-opacity"], released, z)).toBe(0);
        expect(evaluate(gear["line-width"], released, z)).toBeLessThan(edgeW);
      }
    }
  });
});
