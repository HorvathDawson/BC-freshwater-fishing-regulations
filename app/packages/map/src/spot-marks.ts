/**
 * (T316) YOUR SPOTS, DRAWN BY THE MAP ENGINE (owner 2026-10-31: "The spot icon … when a user pans it lags behind the map?
 * Shouldn't it just be rendered in the actual map engine instead?? Like using geojson"). The spot marks were view
 * annotations (phone) / DOM boxes moved on every `move` (web): React views the SDK moved over the map a step behind the
 * camera. Now each mark `spotMarkers` gives is a feature in one GeoJSON source with symbol layers over every map layer,
 * so they move WITH the map and cost no JS while it does. The marks are still `@app/core` `spotMarkers` at the zoom the
 * camera SETTLED at (its grouping, names and parting zoom are the tested ones; the source's own `cluster` would group by
 * a different rule and lose "the zoom that parts it"), so JS runs once per settle, never per frame.
 *
 * SAME LOOK as `@app/ui-native` `SpotPin`: a small square of the spot mark framed in the spot ground; selected, ringed
 * in the mark after a 2 px gap; a group larger with its count; parking a P; the name above on a ground strip. Every size
 * is `SPOT_PIN` / `spotFace`. Taps are `spotUnder` (the 44 px target, platform-free) → `pressSpot`.
 *
 * Platform-free (both maps read it): the source data, the layer specs, the face images as RGBA.
 */
import { SPOT_PIN, spotFace, type SpotMarker } from "@app/core";
import { SCALE, rgb } from "./raster";

export const SPOT_SOURCE = "t316-spots";
export const SPOT_FACE = "t316-spots-face";
export const SPOT_TEXT = "t316-spots-text";
export const SPOT_LABEL = "t316-spots-label";
/** The map glyph face nearest the pin's bold (the glyph host has no Bold: Regular, Medium, Italic). */
export const SPOT_FONT = ["Noto Sans Medium"];

/** The colours a spot mark is drawn in: the app's spot tokens (`palette.spot`). */
export interface SpotColours { mark: string; ground: string }

const hex = (c: string) => c.replace(/[^0-9a-zA-Z]/g, "");
/** A face image's name: its size, selection and colours (an image name is added once, never replaced). */
export const spotImageName = (face: number, selected: boolean, c: SpotColours): string =>
  `t316-spot-${face}${selected ? "s" : ""}-${hex(c.mark)}-${hex(c.ground)}`;
/** The name strip's box image. */
export const spotBoxName = (c: SpotColours): string => `t316-spot-box-${hex(c.ground)}`;

/** A name cut to the strip's widest (`SPOT_PIN.labelMaxWidth`; ~6.5 px a character, as `spotMarkers` measures it). */
export function spotLabelText(title: string): string {
  const max = Math.floor((SPOT_PIN.labelMaxWidth - 8) / 6.5);
  return title.length <= max ? title : `${title.slice(0, max - 1).trimEnd()}…`;
}

type Fc = { type: "FeatureCollection"; features: unknown[] };

/** The marks' source: one feature per mark, in `marks` order (`i`), with its face image, its text and its name. */
export function spotsData(marks: readonly SpotMarker[], c: SpotColours): Fc {
  return {
    type: "FeatureCollection",
    features: marks.map((m, i) => {
      const { face, extent } = spotFace(m);
      const text = m.parking ? "P" : m.count > 1 ? String(m.count) : "";
      return {
        type: "Feature",
        properties: { i, key: m.key, img: spotImageName(face, m.selected, c), sel: m.selected ? 1 : 0, ext: extent,
                      text, small: m.parking ? 0 : 1, label: m.label === null ? "" : spotLabelText(m.label) },
        geometry: { type: "Point", coordinates: [m.lon, m.lat] },
      };
    }),
  };
}

/** Every face image the marks can use (two sizes × selected), as RGBA at 2×. */
export function spotImages(c: SpotColours) {
  const out: { name: string; width: number; height: number; data: Uint8Array; pixelRatio: number }[] = [];
  for (const face of [SPOT_PIN.face, SPOT_PIN.groupFace])
    for (const selected of [false, true]) out.push({ name: spotImageName(face, selected, c), ...spotFaceImage(face, selected, c) });
  return out;
}

/**
 * One face, as `SpotPin` draws it (square corners: `palette.r.chip` is 0): the mark filled inside a `frame` of the
 * ground; selected, a `frame`-wide ring of the mark round it after a 2 px gap (`SPOT_PIN.ring` out each side).
 */
export function spotFaceImage(face: number, selected: boolean, c: SpotColours) {
  const extent = face + (selected ? 2 * SPOT_PIN.ring : 0);
  const s = extent * SCALE, f = SPOT_PIN.frame * SCALE, inset = (extent - face) / 2 * SCALE;
  const mark = rgb(c.mark, [0, 0, 0]), ground = rgb(c.ground, [255, 255, 255]);
  const data = new Uint8Array(s * s * 4);
  for (let y = 0; y < s; y++)
    for (let x = 0; x < s; x++) {
      const edge = Math.min(x, y, s - 1 - x, s - 1 - y);         // px in from the image's edge
      let px: [number, number, number] | null = null;
      if (selected && edge < f) px = mark;                      // the ring
      else if (edge >= inset) px = edge < inset + f ? ground : mark;   // the face: its frame, then the mark
      if (!px) continue;                                        // the gap: clear
      const i = (y * s + x) * 4;
      data[i] = px[0]; data[i + 1] = px[1]; data[i + 2] = px[2]; data[i + 3] = 255;
    }
  return { width: s, height: s, data, pixelRatio: SCALE };
}

/** The name strip: the ground, square, stretched round the text (`icon-text-fit`). */
export function spotBoxImage(c: SpotColours) {
  const s = 4 * SCALE;
  const [r, g, b] = rgb(c.ground, [255, 255, 255]);
  const data = new Uint8Array(s * s * 4);
  for (let i = 0; i < s * s; i++) { data[i * 4] = r; data[i * 4 + 1] = g; data[i * 4 + 2] = b; data[i * 4 + 3] = 255; }
  return { width: s, height: s, data, pixelRatio: SCALE };
}

const always = { "icon-allow-overlap": true, "icon-ignore-placement": true, "text-allow-overlap": true,
                 "text-ignore-placement": true } as const;
/** The selected mark over the rest (it was the view drawn on top). */
const sortKey = ["get", "sel"];

/** How far a name's text sits above the point, px: half the face, the gap, the strip's padding. */
const labelLift = (selected: boolean) => spotFace({ selected, count: 1 }).extent / 2 + SPOT_PIN.labelGap + 1;

/** The three layers: the face, its count / P, and the name on its strip above it (`spotMarkers` placed the names). */
export function spotsLayers(c: SpotColours) {
  return [
    {
      id: SPOT_FACE, type: "symbol" as const, source: SPOT_SOURCE,
      layout: { "icon-image": ["get", "img"], "symbol-sort-key": sortKey, ...always },
    },
    {
      id: SPOT_TEXT, type: "symbol" as const, source: SPOT_SOURCE, filter: ["!=", ["get", "text"], ""],
      layout: { "text-field": ["get", "text"], "text-font": SPOT_FONT,
                "text-size": ["case", ["==", ["get", "small"], 1], 10, 11], "symbol-sort-key": sortKey, ...always },
      paint: { "text-color": c.ground },
    },
    {
      id: SPOT_LABEL, type: "symbol" as const, source: SPOT_SOURCE, filter: ["!=", ["get", "label"], ""],
      layout: {
        "text-field": ["get", "label"], "text-font": SPOT_FONT, "text-size": 11, "text-line-height": 13 / 11,
        "text-max-width": 1000, "text-anchor": "bottom",
        // its bottom `labelGap` above the face's top (the strip's 1 px padding below the text), in ems of 11 px
        "text-offset": ["case", ["==", ["get", "sel"], 1], ["literal", [0, -labelLift(true) / 11]],
                        ["literal", [0, -labelLift(false) / 11]]],
        "icon-image": spotBoxName(c), "icon-text-fit": "both", "icon-text-fit-padding": [1, 4, 1, 4],
        "symbol-sort-key": sortKey, ...always,
      },
      paint: { "text-color": c.mark },
    },
  ];
}
