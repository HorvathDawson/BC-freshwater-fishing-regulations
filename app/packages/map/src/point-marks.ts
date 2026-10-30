/**
 * (T317) THE MAP'S OWN POINT MARKS, DRAWN BY THE MAP ENGINE — the boundary marks (`MapProps.marks`), the "where I tapped"
 * marker (`marker`) and a panel's named points (`pins`). They were view annotations / DOM markers: React views (or DOM
 * nodes) the SDK moved over the map every frame, a step behind the camera on a phone's pan. Now each is a feature in a
 * GeoJSON source with circle / symbol layers over every map layer, so they move WITH the map and cost no JS while it does.
 *
 * Platform-free (both maps read it): the sources' data, the layers' specs, the label box image and what a tap hit.
 * The phone's `Map.native.tsx` mounts them as `GeoJSONSource` + `Layer`; the web's `web/markers.ts` adds them at runtime.
 */
import type { MapMark } from "./map-props";
import { SCALE, rgb } from "./raster";
import { labelShift } from "./marks";

export const MARK_SOURCE = "t317-marks";
export const MARK_RING = "t317-marks-ring";
export const MARK_LABEL = "t317-marks-label";
export const PIN_SOURCE = "t317-pins";
/** The phone's pins: a dot in the tone with a ring in the card colour (the view annotations' `dot`). */
export const PIN_DOT = "t317-pins-dot";
/** The web's pins: MapLibre's default teardrop look, a generated pin image per tone (`here-mark.ts`). */
export const PIN_ICON = "t317-pins-icon";
/** The label's box: card fill, ink edge, square corners; stretched round the text (`icon-text-fit`). */
export const MARK_BOX_IMAGE = "t317-mark-box";
/** Half the 44 px target a mark answered a finger in. */
export const MARK_HIT = 22;
/** The label's bottom edge above the point: the old button's `bottom: 38` inside a 44 px box centred on it. */
const LABEL_LIFT = 16;
const LABEL_PAD_Y = 3, LABEL_PAD_X = 7, LABEL_EDGE = 1.5, LABEL_SIZE = 12, LABEL_LINE = 15;
/** The label's band (`MARK_LABEL_HALF` × 2): its text wraps inside it, padding and edge off. */
const LABEL_MAX_PX = 180 - 2 * (LABEL_PAD_X + LABEL_EDGE);
/** The map glyph stack nearest the app's Archivo regular (a map can only draw the fonts its glyph server has). */
export const MARK_LABEL_FONT_STACK = ["Noto Sans Regular"];

type Fc = { type: "FeatureCollection"; features: unknown[] };
const point = (lon: number, lat: number, properties: Record<string, unknown>) =>
  ({ type: "Feature", properties, geometry: { type: "Point", coordinates: [lon, lat] } });

/**
 * The marks' source: one ring per mark, and one label per labelled mark at `labelAt[i]` when given — the point the label
 * is centred over, moved sideways so the label stays inside the map (`labelShift`, measured where the camera settles).
 */
export function marksData(marks: readonly MapMark[] | undefined,
                          labelAt?: readonly (readonly [number, number] | null | undefined)[]): Fc {
  const features: unknown[] = [];
  (marks ?? []).forEach((k, i) => {
    features.push(point(k.lon, k.lat, { i, kind: "ring", on: k.on }));
    if (k.label) {
      const at = labelAt?.[i] ?? [k.lon, k.lat];
      features.push(point(at[0], at[1], { i, kind: "label", label: k.label }));
    }
  });
  return { type: "FeatureCollection", features };
}

/** Where each labelled mark's label goes, from its screen point and the map's width: unchanged, or moved by `labelShift`. */
export async function shiftedLabels(marks: readonly MapMark[], width: number,
                                    project: (lon: number, lat: number) => Promise<[number, number] | null> | [number, number] | null,
                                    unproject: (x: number, y: number) => Promise<[number, number] | null> | [number, number] | null) {
  return Promise.all(marks.map(async (k): Promise<[number, number] | null> => {
    if (!k.label) return null;
    const p = await project(k.lon, k.lat);
    if (!p) return null;
    const dx = labelShift(p[0], width);
    return dx === 0 ? [k.lon, k.lat] : await unproject(p[0] + dx, p[1]);
  }));
}

/** The two marks layers: the 16 px ring (filled when open) and the label in its box above it. `box`: the image's name. */
export function marksLayers(c: { ink: string; card: string }, box: string) {
  return [
    {
      id: MARK_RING, type: "circle" as const, source: MARK_SOURCE, filter: ["==", ["get", "kind"], "ring"],
      paint: {
        // 16 px across with its 3 px edge: the stroke is drawn OUTSIDE the radius
        "circle-radius": 5, "circle-stroke-width": 3, "circle-stroke-color": c.ink,
        "circle-color": ["case", ["==", ["get", "on"], true], c.ink, c.card],
        "circle-pitch-alignment": "map",
      },
    },
    {
      id: MARK_LABEL, type: "symbol" as const, source: MARK_SOURCE, filter: ["==", ["get", "kind"], "label"],
      layout: {
        "text-field": ["get", "label"], "text-font": MARK_LABEL_FONT_STACK, "text-size": LABEL_SIZE,
        "text-line-height": LABEL_LINE / LABEL_SIZE, "text-max-width": LABEL_MAX_PX / LABEL_SIZE,
        "text-anchor": "bottom", "text-justify": "center",
        "text-offset": [0, -(LABEL_LIFT + LABEL_PAD_Y + LABEL_EDGE) / LABEL_SIZE],
        "icon-image": box, "icon-text-fit": "both",
        "icon-text-fit-padding": [LABEL_PAD_Y, LABEL_PAD_X, LABEL_PAD_Y, LABEL_PAD_X],
        // every mark drawn, always: they were views, never collided away
        "text-allow-overlap": true, "icon-allow-overlap": true, "text-ignore-placement": true,
        "icon-ignore-placement": true,
      },
      paint: { "text-color": c.ink },
    },
  ];
}

/** The label's box, as RGBA pixels (2×): a square card with a 1.5 px ink edge; only the middle stretches. */
export function markBoxImage(fill: string, edge: string) {
  const s = 12 * SCALE, b = LABEL_EDGE * SCALE;
  const [fr, fg, fb] = rgb(fill, [255, 255, 255]);
  const [er, eg, eb] = rgb(edge, [0, 0, 0]);
  const data = new Uint8Array(s * s * 4);
  for (let y = 0; y < s; y++)
    for (let x = 0; x < s; x++) {
      const onEdge = Math.min(x, y, s - 1 - x, s - 1 - y) < b;
      const i = (y * s + x) * 4;
      data[i] = onEdge ? er : fr; data[i + 1] = onEdge ? eg : fg; data[i + 2] = onEdge ? eb : fb; data[i + 3] = 255;
    }
  const inner = Math.ceil(b);
  return { width: s, height: s, data, pixelRatio: SCALE,
           stretchX: [[inner, s - inner]] as [number, number][], stretchY: [[inner, s - inner]] as [number, number][],
           content: [inner, inner, s - inner, s - inner] as [number, number, number, number] };
}

/** The mark a tap landed on — the first rendered marks feature's `i` — or null. */
export function markHit(features: readonly { properties?: unknown }[]): number | null {
  for (const f of features) {
    const i = (f.properties as { i?: unknown } | null | undefined)?.i;
    if (typeof i === "number" && Number.isInteger(i)) return i;
    if (typeof i === "string" && /^\d+$/.test(i)) return Number(i);
  }
  return null;
}

/** The 44 px box round a press that a mark answers in, `[[left, top], [right, bottom]]` (as `tapBox`). */
export const markBox = (x: number, y: number): [[number, number], [number, number]] =>
  [[x - MARK_HIT, y - MARK_HIT], [x + MARK_HIT, y + MARK_HIT]];

export interface PinPoint { lat: number; lon: number; tone?: string; title?: string }

/**
 * The pins' source: the marker (`kind: "marker"`, drawn bigger) and each named point (`kind: "pin"`, its `i` in `pins`),
 * each with its colour (`tone`, the accent when it has none) and its words (`title`).
 */
export function pinsData(marker: { lat: number; lon: number } | null | undefined, pins: readonly PinPoint[] | undefined,
                         accent: string): Fc {
  const features: unknown[] = (pins ?? []).map((p, i) =>
    point(p.lon, p.lat, { kind: "pin", i, tone: p.tone ?? accent, img: pinImageName(p.tone ?? accent), title: p.title ?? "" }));
  if (marker) features.push(point(marker.lon, marker.lat, { kind: "marker", tone: accent, img: pinImageName(accent), title: "" }));
  return { type: "FeatureCollection", features };
}

/** The phone's pins: the dots the view annotations drew — 16 px for the marker, 12 px for a pin, a 2 px card ring. */
export function pinDotLayer(card: string) {
  return {
    id: PIN_DOT, type: "circle" as const, source: PIN_SOURCE,
    paint: {
      "circle-radius": ["case", ["==", ["get", "kind"], "marker"], 6, 4],
      "circle-color": ["get", "tone"], "circle-stroke-color": card, "circle-stroke-width": 2,
      "circle-pitch-alignment": "map",
    },
  };
}

/** A web pin's image name for a tone. */
export const pinImageName = (tone: string): string => `t317-pin-${tone.replace(/[^0-9a-zA-Z]/g, "")}`;

/** The web's pins: MapLibre's default marker's teardrop (27 × 41 px) from the generated pin (22 × 30), a pin at 0.72. */
export function pinIconLayer() {
  return {
    id: PIN_ICON, type: "symbol" as const, source: PIN_SOURCE,
    layout: {
      "icon-image": ["get", "img"], "icon-anchor": "bottom",
      "icon-size": ["case", ["==", ["get", "kind"], "marker"], 1.35, 0.97],
      "icon-allow-overlap": true, "icon-ignore-placement": true,
      // the marker over the pins
      "symbol-sort-key": ["case", ["==", ["get", "kind"], "marker"], 1, 0],
    },
  };
}
