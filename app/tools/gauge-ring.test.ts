/**
 * (T636, owner 2026-11-11: "the level only tag in the conditions map is a lot it should be a border color or something
 * instead") A GAUGE REPORTING ONLY THE OTHER QUANTITY TODAY is a grey dot with a RING on the map, never words:
 *   - both maps are handed the ring on a muted dot (`gauge-dot` stroke: `color.gauge.only`, `GAUGE_ONLY_RING_PX`) and
 *     the paper stroke on every coloured one, and the label layer takes only features that carry a label;
 *   - colour-blind check (worst of normal vision and protan / deutan / tritan, Viénot and Machado): the ring against
 *     every stop of the flow and change ramps (the coloured dots beside it) and against its own grey fill, held at the
 *     floors below; over satellite imagery it is RECORDED, not held: no single ring colour stands off both the dark and
 *     the pale imagery (the T636 shots show it; the owner picks the colour).
 */
import { describe, expect, it } from "vitest";
import { runtimeStyle } from "@app/map";
import { nativeStyle } from "../packages/map/src/native-style";
import { GAUGE_ONLY_RING_PX } from "../packages/map/src/gaugeDots";
import { resolveTheme, STYLE_META } from "../packages/map/src/style";
import { MODELS, worstSeparation } from "./cvd";

const de = <T>(x: T): T => JSON.parse(JSON.stringify(x).replace(/~/g, "#")) as T;
const THEMES = ["light", "dark", "cvd"] as const;
const AT = { atlas: "a.pmtiles", basemap: "b.pmtiles", gauges: '{"type":"FeatureCollection","features":[]}' };
/** the imagery grounds of tools/status-colours-distinct.test.ts, plus T594's dark forest and brown river */
const SATELLITE = de(["~28290d", "~353b1a", "~1c1a0b", "~61512c", "~2A333B", "~e6e8ea", "~0a1b11", "~a7a385"]);
/** the ring against the nearest ramp stop / its own grey fill, per theme: what `color.gauge.only` (ink) reaches, rounded down */
const FLOOR = { light: { ramp: 18.5, fill: 76 }, dark: { ramp: 14.5, fill: 58 }, cvd: { ramp: 10, fill: 76 } } as const;

type Layer = { id: string; filter?: unknown; paint?: Record<string, unknown> };
const layer = (style: unknown, id: string) => (style as { layers: Layer[] }).layers.find((l) => l.id === id)!;
const MUTED = ["==", ["get", "muted"], true];

function ringFailures(th: (typeof THEMES)[number], t: Record<string, unknown>): string[] {
  const ring = t["color.gauge.only"] as string, out: string[] = [];
  const stops = (STYLE_META.colorModes.stream!.standing as { stops: [number, { token: string }][] }).stops.filter(([at]) => at >= 0);
  const ramp = Math.min(...stops.map(([, r]) => worstSeparation(ring, t[r.token] as string, MODELS).dE));
  const fill = worstSeparation(ring, t["color.water.ungauged"] as string, MODELS).dE;
  if (ramp < FLOOR[th].ramp) out.push(`${th}: ring ${ring} vs the ramp ${ramp.toFixed(1)} < ${FLOOR[th].ramp}`);
  if (fill < FLOOR[th].fill) out.push(`${th}: ring ${ring} vs its grey fill ${fill.toFixed(1)} < ${FLOOR[th].fill}`);
  return out;
}

describe("a gauge reporting only the other quantity: a ringed grey dot, no words (T636)", () => {
  for (const th of THEMES) {
    it(`${th}: both maps ring the muted dot and keep the paper stroke on the coloured ones; no label without words`, () => {
      const t = resolveTheme(th) as Record<string, unknown>;
      for (const style of [runtimeStyle(AT as never, th, { gauges: "standing" }),
                           nativeStyle({ at: AT as never, theme: th, view: "conditions", modes: { gauges: "standing" },
                                         basemap: "satellite", gauges: AT.gauges }).style]) {
        const dot = layer(style, "gauge-dot").paint!;
        expect(dot["circle-stroke-color"]).toEqual(["case", MUTED, t["color.gauge.only"], expect.any(String)]);
        expect(dot["circle-stroke-width"]).toEqual(["case", MUTED, GAUGE_ONLY_RING_PX, 2]);
        expect(JSON.stringify(layer(style, "gauge-label").filter)).toContain('["has","label"]');
      }
    });

    it(`${th}: the ring stands off every ramp colour and its own fill, for every reader`, () => {
      expect(ringFailures(th, resolveTheme(th) as Record<string, unknown>)).toEqual([]);
    });
  }

  it("fails a ring in a ramp colour (mutation pin)", () => {
    const t = resolveTheme("light") as Record<string, unknown>;
    expect(ringFailures("light", { ...t, "color.gauge.only": t["color.flow.f6"] })[0]).toBe(`light: ring ${t["color.flow.f6"]} vs the ramp 0.0 < 18.5`);
  });

  it("over satellite: recorded, not held — the ink ring is lost on dark imagery, seen on pale", () => {
    const sep = (th: string) => SATELLITE.map((g) => worstSeparation(resolveTheme(th)["color.gauge.only"] as string, g, MODELS).dE);
    expect(Math.min(...sep("light"))).toBeLessThan(5);                  // conifer shadow: the ring is gone, the grey fill stays
    expect(Math.max(...sep("light"))).toBeGreaterThan(60);              // gravel, snow: plain
  });
});
