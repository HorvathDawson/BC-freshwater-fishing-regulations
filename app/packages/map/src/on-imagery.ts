/**
 * (T594, owner 2026-11-11: look C) THE WATER OVER SATELLITE IMAGERY IS NOT HIDDEN BY ITS OWN COLOUR.
 *
 * The owner: "we need to figure out a way to have better satellite where it isn't blocked by the streams lines". On the
 * drawn map a stream is a solid line as wide as its order says (the Fraser ~25 px at z14); over imagery that band covered
 * the very channel a reader turns satellite on to see. So, on satellite only (`MapAdapter.setBasemap`; the street map is
 * painted exactly as before):
 *
 *   overview zooms  unchanged: the solid line, today's colour and weight, up to z11.
 *   closing in      the line OPENS into two thin edges in the status colour with the water clear between
 *                   (`line-gap-width`), eased in over z11-z14 (`IMAGERY_OPEN`). Each edge is `width.imagery.edge`; the
 *                   clear middle is the line's own width less the edges — and never narrower than `IMAGERY_GAP_FLOOR`,
 *                   so a creek a few pixels wide sits BETWEEN the edges rather than under them (a small stream's line is
 *                   thinner than its channel close in; the owner's condition: "it must not block small streams").
 *   opacity         the edges ease from full strength at z13 to `opacity.imagery.edge` at z15 (owner: "try lower opacity
 *                   when zoomed in too"), held by the colour-blind gate (tools/status-colours-distinct.test.ts).
 *   the marks       every line drawn with the stream's width opens the same way, round the same clear middle: the gear
 *                   edges and the selection's outline sit just outside the coloured edges, a passing run's dash and
 *                   closed's paper dash break the edges themselves (so closed still reads as broken), the water copy
 *                   opens as the stream does. The no-access dots are thin already and unchanged.
 *   lakes, rivers   a lake's, a river polygon's and a flowing wetland's fill fades to `opacity.imagery.fill` of itself by
 *                   z13; the lake's outline, which carries its status, is drawn heavier there (`IMAGERY_LAKE_EDGE`).
 *
 * THE TAP IS UNCHANGED: MapLibre (GL JS and Native) hit-tests a line with a gap over its whole band, gap + both edges,
 * and the band here is never narrower than the solid line it replaces (tools/imagery-casing.test.ts measures it).
 *
 * Every transform keeps MapLibre's one rule for zoom expressions: a single zoom curve, outermost. The inputs' own zoom
 * curves are merged with the ramps' stops into one curve (`compose`), each stop's value computed from the inputs' values
 * at that zoom (linear between their stops, as the renderer interpolates), with `let` so a width is written once.
 */
import { rampAt } from "./paint";
import type { Tokens } from "./style";

type Ramp = readonly (readonly [number, number])[];

/** How open the line is (0 solid, 1 two edges), by zoom. */
const IMAGERY_OPEN: Ramp = [[11, 0], [12, 0.5], [14, 1]];
/**
 * The narrowest clear middle, px, by zoom: about 12 m of ground at z16 near 49°N (1.57 m/px), doubling per zoom as the
 * ground does — a creek's own width, so it is never under an edge.
 */
export const IMAGERY_GAP_FLOOR: Ramp = [[12, 0], [13, 2], [14, 3], [15, 5], [16, 8], [17, 14], [18, 24]];
/** The edges' opacity: full to z13, `opacity.imagery.edge` (k) from z15. */
const edgeFade = (k: number): Ramp => [[13, 1], [15, k]];
/** A lake's fill, times this (k = `opacity.imagery.fill`) from z13; its outline times `IMAGERY_LAKE_EDGE`. */
const fillFade = (k: number): Ramp => [[11, 1], [13, k]];
const IMAGERY_LAKE_EDGE: Ramp = [[11, 1], [13, 1.8]];

/** The fill layers that fade over imagery, and the lake outline drawn heavier. */
const IMAGERY_FILLS: readonly string[] = ["lake", "lake_flowing", "wetland_flowing"];
const IMAGERY_LAKE_OUTLINE = "lake__edge";

type Stops = [number, unknown][];
const isZoomCurve = (e: unknown): e is unknown[] => Array.isArray(e)
  && (e[0] === "interpolate" || e[0] === "step") && JSON.stringify(e[e[0] === "step" ? 1 : 2]) === '["zoom"]';

/** A zoom curve's stops; null for anything that is not one. Only a linear curve can be merged (it is all we draw). */
function stopsOf(e: unknown): Stops | null {
  if (!isZoomCurve(e)) return null;
  if (e[0] !== "interpolate" || JSON.stringify(e[1]) !== '["linear"]')
    throw new Error(`on imagery: only a linear zoom curve can be merged, got ${JSON.stringify(e.slice(0, 2))}`);
  const out: Stops = [];
  for (let i = 3; i < e.length; i += 2) out.push([e[i] as number, e[i + 1]]);
  return out;
}

const round = (n: number) => Math.round(n * 1e4) / 1e4;
const mix = (a: unknown, b: unknown, f: number): unknown =>
  typeof a === "number" && typeof b === "number" ? round(a + (b - a) * f) : ["+", ["*", a, round(1 - f)], ["*", b, round(f)]];

/** What `e` evaluates to at zoom `z`, as an expression: linear between a curve's stops, held outside them. */
function at(e: unknown, z: number): unknown {
  const s = stopsOf(e);
  if (!s) return e;
  if (z <= s[0]![0]) return s[0]![1];
  for (let i = 1; i < s.length; i++) {
    const [z1, v1] = s[i]!, [z0, v0] = s[i - 1]!;
    if (z === z1) return v1;
    if (z < z1) return mix(v0, v1, (z - z0) / (z1 - z0));
  }
  return s[s.length - 1]![1];
}

/** ONE zoom curve over the inputs' stops and `zooms`, each stop's output `f(the inputs at z, z)`. */
function compose(inputs: readonly unknown[], zooms: readonly number[], f: (v: unknown[], z: number) => unknown): unknown {
  const all = new Set(zooms);
  for (const e of inputs) for (const [z] of stopsOf(e) ?? []) all.add(z);
  const zs = [...all].sort((a, b) => a - b);
  return ["interpolate", ["linear"], ["zoom"], ...zs.flatMap((z) => [z, f(inputs.map((e) => at(e, z)), z)])];
}

const zoomsOf = (...ramps: Ramp[]) => ramps.flatMap((r) => r.map(([z]) => z));
/** `e` times a ramp, as one zoom curve. */
const timesRamp = (e: unknown, ramp: Ramp): unknown =>
  compose([e], zoomsOf(ramp), ([v], z) => {
    const k = round(rampAt(ramp, z));
    return k === 1 ? v : typeof v === "number" ? round(v * k) : ["*", v, k];
  });

const num = (tokens: Tokens, key: string): number => {
  const v = Number(tokens[key]);
  if (!Number.isFinite(v)) throw new Error(`on imagery: token ${key} missing from the theme`);
  return v;
};

/**
 * A line drawn with the stream's width, OPENED: its `line-width` and `line-gap-width` over imagery.
 *
 * `own` is the layer's own solid width (the stream's `stream`, a gear edge's stream + its edges, the selection's
 * stream + its outline), `stream` the stream line's solid width, both as `paintFor` makes them. At each zoom, with
 * t how open, e the edge, F the floor of the clear middle, W the stream and X the layer:
 *   gap   t · max(W - 2e, F)                        the clear middle, the same for every layer
 *   band  (1 - t) · W + t · max(W, F + 2e)          the stream's whole band (never narrower than W: the tap)
 *   width gap > 0 ? (band - gap) / 2 + (X - W) / 2 : the layer's own solid width
 * so every layer's edge starts at the same inner line and a layer wider than the stream shows outside its edges by
 * what it showed each side before. `openAs` is X where a layer opens to another width than its solid one (closed's
 * stripe: 0.4 W solid, the stream's own W opened, so its dash breaks the edges).
 */
function openedLine(own: unknown, stream: unknown, tokens: Tokens, openAs: unknown = own):
    { "line-width": unknown; "line-gap-width": unknown } {
  const e = num(tokens, "width.imagery.edge");
  const zooms = zoomsOf(IMAGERY_OPEN, IMAGERY_GAP_FLOOR);
  const gapAt = (w: unknown, z: number): unknown => {
    const t = round(rampAt(IMAGERY_OPEN, z));
    if (t === 0) return 0;
    return ["*", t, ["max", ["-", w, 2 * e], round(rampAt(IMAGERY_GAP_FLOOR, z))]];
  };
  return {
    "line-gap-width": compose([stream], zooms, ([w], z) => gapAt(w, z)),
    "line-width": compose([own, stream, openAs], zooms, ([solid, w, x], z) => {
      const t = round(rampAt(IMAGERY_OPEN, z));
      if (t === 0) return solid;
      const f = round(rampAt(IMAGERY_GAP_FLOOR, z));
      return ["let", "w", w, "x", x, "s", solid, ["let", "g", gapAt(["var", "w"], z),
        ["case", [">", ["var", "g"], 0],
          ["+", ["/", ["-", ["+", ["*", round(1 - t), ["var", "w"]], ["*", t, ["max", ["var", "w"], f + 2 * e]]], ["var", "g"]], 2],
                ["/", ["-", ["var", "x"], ["var", "w"]], 2]],
          ["var", "s"]]]];
    }),
  };
}

/** A line's opacity eased to `opacity.imagery.edge` close in. */
function edgeOpacity(opacity: unknown, tokens: Tokens): unknown {
  return timesRamp(opacity ?? 1, edgeFade(num(tokens, "opacity.imagery.edge")));
}

/**
 * The stream-width layers' paint over imagery: `paint` (the layer's plain-map paint) with its width opened round the
 * stream's (`stream`, the stream line's solid width) and — `fade` — its opacity eased close in.
 */
export function openedPaint(paint: Record<string, unknown>, stream: unknown, tokens: Tokens, fade = true,
                            openAs: unknown = paint["line-width"]): Record<string, unknown> {
  const out: Record<string, unknown> = { ...paint, ...openedLine(paint["line-width"], stream, tokens, openAs) };
  if (fade) out["line-opacity"] = edgeOpacity(paint["line-opacity"], tokens);
  return out;
}

/** A lake's, a river polygon's or a flowing wetland's paint over imagery (fill faded), and the lake outline (heavier). */
export function waterOnImagery(layerId: string, paint: Record<string, unknown>, tokens: Tokens): Record<string, unknown> {
  if (IMAGERY_FILLS.includes(layerId) && paint["fill-opacity"] !== undefined)
    return { ...paint, "fill-opacity": timesRamp(paint["fill-opacity"], fillFade(num(tokens, "opacity.imagery.fill"))) };
  if (layerId === IMAGERY_LAKE_OUTLINE && paint["line-width"] !== undefined)
    return { ...paint, "line-width": timesRamp(paint["line-width"], IMAGERY_LAKE_EDGE) };
  return paint;
}
