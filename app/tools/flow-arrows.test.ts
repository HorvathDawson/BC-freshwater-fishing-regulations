/**
 * WHICH WAY THE WATER GOES, on the Conditions map (owner 2026-10-21: "add arrows to the map streams in the condition
 * tab to show flow direction").
 *
 * Three claims, each pinned against an OUTPUT rather than the input that made it:
 *   1. The style draws the arrows for the Conditions view and for nothing else — the Map tab, the mini map and the
 *      spot picker never ask for them, so they never draw there (both renderers: `applyLook` and `nativeStyle`).
 *   2. An arrow points DOWNSTREAM. The tiles carry stream lines digitised MOUTH TO SOURCE (the tile contract's
 *      `flows`, measured 2026-10-21 on the live atlas: 342 of 347 junctions on the Chilliwack/Vedder at z13, 539 of 542
 *      on the lower Fraser at z9 and 62 of 62 on the Burnaby/Port Moody municipal streams at z14 have the mouth at the
 *      first vertex; every exception is a side channel meeting its own river at its upstream end). So the glyph has to
 *      point AGAINST the line's own direction. Worked out from the generated layout, the way MapLibre places it, and
 *      checked on a real Vedder section whose mouth is known.
 *   3. An arrow is the stream's own colour: it follows the stream's colour mode, so it cannot disagree with its line.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { baseAdapter, type MapHandle } from "../packages/map/src/adapters/contract";
import { paintFor } from "../packages/map/src/paint";
import { applyLook } from "../packages/map/src/look";
import { nativeStyle } from "../packages/map/src/native-style";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { MAP_STYLE, STYLE_META, colorExpression, resolveTheme, shownLayers } from "../packages/map/src/style";

const ARROWS = "stream__arrows";
const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const contract = JSON.parse(readFileSync(join(ROOT, "pipeline/deliver/tiles/tile-contract.json"), "utf8")) as
  { layers: Record<string, { flows?: string | null }> };

type L = { id: string; type: string; "source-layer"?: string; minzoom?: number; filter?: unknown;
           layout?: Record<string, unknown>; paint?: Record<string, unknown> };
const arrowLayer = () => (MAP_STYLE.layers as L[]).find((l) => l.id === ARROWS);

describe("the flow arrows are the Conditions view's own", () => {
  it("one symbol layer over the stream lines, hidden until a view asks for it", () => {
    const l = arrowLayer()!;
    expect({ type: l.type, sourceLayer: l["source-layer"], minzoom: l.minzoom, visibility: l.layout?.visibility,
             placement: l.layout?.["symbol-placement"] })
      .toEqual({ type: "symbol", sourceLayer: "stream", minzoom: 10, visibility: "none", placement: "line" });
  });

  it("only the Conditions view draws them", () => {
    expect(Object.fromEntries(STYLE_META.views.map((v) => [v.id, shownLayers(v.id)])))
      .toEqual({ conditions: [ARROWS], temperature: [], stocked: [], plain: [] });
  });

  /** A handle that records the last visibility each layer was given. */
  const recorder = () => {
    const vis: Record<string, boolean> = {};
    const h: MapHandle = { setVisibility: (id, v) => { vis[id] = v; }, setPaint: () => {},
                           setFeatureState: () => {}, clearFeatureStates: () => {}, setZoomRange: () => {} };
    return { h, vis };
  };
  const has = (id: string) => MAP_STYLE.layers.some((l) => l.id === id);

  it("web: drawn on the Conditions tab, taken back on the Map tab (one map, both tabs, in turn)", () => {
    const { h, vis } = recorder();
    const errors: Error[] = [];
    const adapter = baseAdapter("web");
    applyLook(adapter, h, has, { view: "plain", theme: "light", modes: { stream: "standing" },
                                 hide: ["mu", "region"], show: shownLayers("conditions") }, (e) => errors.push(e));
    const onConditions = vis[ARROWS];
    applyLook(adapter, h, has, { view: "plain", theme: "light", modes: { stream: "profile" }, hide: [] },
              (e) => errors.push(e));
    expect({ onConditions, onMap: vis[ARROWS], errors }).toEqual({ onConditions: true, onMap: false, errors: [] });
  });

  it("native: the style a phone is handed carries the same answer", () => {
    const at = { atlas: "file:///a.pmtiles", basemap: "file:///b.pmtiles" };
    const common = { at, theme: "light", view: "plain", groups: {}, basemap: "map" as const,
                     gauges: '{"type":"FeatureCollection","features":[]}' };
    const vis = (s: unknown) => ((s as { layers: L[] }).layers.find((l) => l.id === ARROWS)!.layout!.visibility);
    const cond = nativeStyle({ ...common, modes: { stream: "standing" }, hide: ["mu", "region"],
                               show: shownLayers("conditions") });
    // A phone's Map tab colours by `status` (it has no per-tile `profile` colouring — `coloursEachTile`).
    const map = nativeStyle({ ...common, modes: { stream: "status" }, hide: [] });
    expect({ conditions: vis(cond.style), map: vis(map.style), errors: [...cond.errors, ...map.errors] })
      .toEqual({ conditions: "visible", map: "none", errors: [] });
  });
});

/**
 * Where a glyph points in its own frame, screen-style (y down). MapLibre lays a line-placed glyph with its +x along the
 * line, in the direction the line was digitised (with `text-keep-upright: false`, never flipped).
 */
const GLYPH: Record<string, [number, number]> = { "▶": [1, 0], "◀": [-1, 0], "▲": [0, -1], "▼": [0, 1] };
/** The glyph's direction in the LINE's frame (+x = first vertex towards last), after `text-rotate` (clockwise). */
function arrowInLineFrame(layout: Record<string, unknown>): [number, number] {
  const g = GLYPH[String(layout["text-field"])];
  if (!g) throw new Error(`no direction known for glyph ${JSON.stringify(layout["text-field"])}`);
  const r = (Number(layout["text-rotate"] ?? 0) * Math.PI) / 180;
  const [x, y] = g;
  return [Math.round(x * Math.cos(r) - y * Math.sin(r)), Math.round(x * Math.sin(r) + y * Math.cos(r))];
}

describe("an arrow points downstream", () => {
  it("the tiles say which way their lines run: mouth to source", () => {
    expect(contract.layers.stream!.flows).toBe("mouth_to_source");
  });

  it("so the glyph points against the line, and MapLibre is told never to turn it round", () => {
    const layout = arrowLayer()!.layout!;
    expect({ arrow: arrowInLineFrame(layout), font: layout["text-font"], keepUpright: layout["text-keep-upright"],
             rotationAlignment: layout["text-rotation-alignment"], pitchAlignment: layout["text-pitch-alignment"] })
      // Regular: Protomaps' Medium has no geometric shapes, and a missing glyph is silently not drawn.
      .toEqual({ arrow: [-1, 0], font: ["Noto Sans Regular"], keepUpright: false, rotationAlignment: "map",
                 pitchAlignment: "map" });
  });

  it("every arrow on the map — the Conditions arrows, the route overlay's, the highlighted route's — agrees", () => {
    const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", gauges: '{"type":"FeatureCollection","features":[]}' },
                           "light") as unknown as { layers: L[] };
    const arrowsOn = (id: string) => {
      const l = s.layers.find((x) => x.id === id)!.layout!;
      return { arrow: arrowInLineFrame(l), font: l["text-font"], keepUpright: l["text-keep-upright"],
               turned: l["text-rotate"] ?? 0 };
    };
    // `turned: 0`: a 180° text-rotate pivots the glyph off its centre, so the arrow sat beside the river (Vedder, z12).
    const want = { arrow: [-1, 0], font: ["Noto Sans Regular"], keepUpright: false, turned: 0 };
    expect({ conditions: arrowsOn(ARROWS), trace: arrowsOn("trace-arrows"), route: arrowsOn("route-arrows") })
      .toEqual({ conditions: want, trace: want, route: want });
  });

  it("the model is not a tautology: an unrotated ▲ points across the river, a ▶ upstream", () => {
    expect([arrowInLineFrame({ "text-field": "▲" }), arrowInLineFrame({ "text-field": "▲", "text-rotate": 180 }),
            arrowInLineFrame({ "text-field": "▶" })]).toEqual([[0, -1], [0, 1], [1, 0]]);
  });

  it("on a known river: the Vedder above the canal flows WEST, and its arrow points west", () => {
    // Section 313838 as the live atlas carries it (z13 tiles 1318/2810 → 1320/2809, 2026-10-21): first vertex at the
    // Vedder Canal end, last vertex up towards Vedder Crossing. The river runs from Vedder Crossing to the canal.
    const first = [-122.06522, 49.09067], last = [-121.96732, 49.09755];
    const [ax] = arrowInLineFrame(arrowLayer()!.layout!);
    const east = ax * (last[0]! - first[0]!);       // the arrow's world x: along (last - first) scaled by its sign
    expect({ arrowHeads: east < 0 ? "west" : "east", towardsTheCanal: east * (first[0]! - last[0]!) > 0 })
      .toEqual({ arrowHeads: "west", towardsTheCanal: true });
  });
});

describe("an arrow is its stream's colour", () => {
  it("follows the stream's colour mode, under every mode the stream has", () => {
    expect(STYLE_META.follows[ARROWS]).toBe("stream");
    const t = resolveTheme("dark");
    for (const mode of Object.keys(STYLE_META.colorModes.stream!))
      expect(paintFor(ARROWS, mode, t, null)["text-color"], mode)
        // a profile mode with no day installed (the status's, and T211's salmon) is the plain map
        .toEqual(STYLE_META.colorModes.stream![mode]!.scale === "profile" ? colorExpression("stream", "plain", t)
          : colorExpression("stream", mode, t));
  });

  it("setting the stream's mode paints the arrows too, with the paper as their halo", () => {
    const paint: Record<string, Record<string, unknown>> = {};
    const h: MapHandle = { setVisibility: () => {}, setFeatureState: () => {}, clearFeatureStates: () => {},
                           setPaint: (id, p, v) => { (paint[id] ??= {})[p] = v; } };
    baseAdapter("web").setLayerMode(h, "stream", "standing", "light");
    const t = resolveTheme("light");
    expect(paint[ARROWS]).toEqual({ "text-color": colorExpression("stream", "standing", t),
                                    "text-halo-color": t["color.outside"], "text-halo-width": 1.2,
                                    "text-halo-blur": 0.5 });
  });
});
