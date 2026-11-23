/**
 * (T812 / T885 item 2) THE CLOSED-AREA CIRCLES ON THE SALMON MAP (owner 2026-11-16: "we want the circles to just be circles
 * on the map not separate sections of the lake"; "Salmon map only (Recommended)", "Follow the day (Recommended)", tap:
 * "Opens the lake's card (Recommended)"). Babine Lake's "within a 400 m radius of the mouth of …" areas: one GeoJSON
 * source, a fill and its edge, drawn over the lake and under the streams and every label.
 *
 * NOTHING IS COMPUTED HERE: the rings are the pipeline's (`deliver.salmon.closed_area_circles`, true ground radius), the
 * days and the status @app/core `salmonCirclesOn`'s (the piece's own chain, the live feed's composition when there is
 * one). A circle never colours a section (AGENTS rule 68: what a circle binds is the reach run's).
 *
 * A TAP GOES THROUGH: these layers are never asked for a feature (`onPressFeature` queries the atlas layers), so a tap
 * inside a circle lands on the lake under it and opens the lake's card, as the owner chose.
 *
 * Platform-free (both maps read it): the source's data and the layer specs.
 */
import { salmonCirclesOn, salmonDayIndex, type SalmonMapStatus } from "@app/core";
import type { SalmonFeed } from "./colouring";
import { resolveTheme } from "./style";

export const SALMON_CIRCLE_SOURCE = "t812-salmon-circles";
export const SALMON_CIRCLE_FILL = "t812-salmon-circles-fill";
export const SALMON_CIRCLE_CASING = "t812-salmon-circles-casing";
export const SALMON_CIRCLE_EDGE = "t812-salmon-circles-edge";
/** the layer they go under: the streams (so a creek's own line still shows across its mouth) and so every label */
export const SALMON_CIRCLE_BEFORE = "stream";
/** the mode the circles are drawn in: the Salmon view's (never the Standard map, owner 2026-11-16) */
export const SALMON_CIRCLE_MODE = "salmon";

/** the colour token of each status: the lake's own salmon colours (`layers.source.json` lake `colorModes.salmon`) */
export const SALMON_CIRCLE_TOKENS: Readonly<Record<SalmonMapStatus, string>> = {
  open: "color.salmon.open-area", release: "color.salmon.release", closed: "color.salmon.closed", check: "color.salmon.check",
};

type Fc = { type: "FeatureCollection"; features: unknown[] };
export const NO_CIRCLES: Fc = { type: "FeatureCollection", features: [] };

/**
 * The circles' source on the map now: the day's circles (`salmonCirclesOn`) while the stream layer is in the Salmon mode,
 * else none. Each a polygon with its `status` and its piece's id.
 */
export function salmonCirclesData(salmon: SalmonFeed | null | undefined, streamMode: string | undefined): Fc {
  if (!salmon || streamMode !== SALMON_CIRCLE_MODE) return NO_CIRCLES;
  return {
    type: "FeatureCollection",
    features: salmonCirclesOn(salmon.facts, salmonDayIndex(salmon.day))
      .filter((c) => !salmon.only || c.status === salmon.only)
      .map((c) => ({ type: "Feature", properties: { id: c.id, status: c.status },
                     geometry: { type: "Polygon", coordinates: [c.ring.map(([x, y]) => [x, y])] } })),
  };
}

/** the status colour as one `match` over the feature's `status`, in this theme */
function statusColour(theme: string): unknown {
  const t = resolveTheme(theme) as Record<string, string>;
  return ["match", ["get", "status"],
          ...(Object.keys(SALMON_CIRCLE_TOKENS) as SalmonMapStatus[]).flatMap((s) => [s, t[SALMON_CIRCLE_TOKENS[s]]]),
          t[SALMON_CIRCLE_TOKENS.check]];
}

/**
 * The three layers, in draw order: the area in its status colour, half see-through so the lake's own colour reads beside
 * it; the salmon casing round it (`color.salmon.casing`, the Salmon view's own edge), so a closed circle on a closed lake
 * still shows; the edge in the status colour on it. A PROPOSAL for the owner's look (pictures: T812-shots/).
 */
export function salmonCircleLayers(theme: string): { id: string; type: "fill" | "line"; source: string; paint: Record<string, unknown> }[] {
  const t = resolveTheme(theme) as Record<string, string>;
  const colour = statusColour(theme);
  return [
    { id: SALMON_CIRCLE_FILL, type: "fill", source: SALMON_CIRCLE_SOURCE, paint: { "fill-color": colour, "fill-opacity": 0.45 } },
    { id: SALMON_CIRCLE_CASING, type: "line", source: SALMON_CIRCLE_SOURCE,
      paint: { "line-color": t["color.salmon.casing"], "line-width": ["interpolate", ["linear"], ["zoom"], 8, 1.5, 14, 3.5] } },
    { id: SALMON_CIRCLE_EDGE, type: "line", source: SALMON_CIRCLE_SOURCE,
      paint: { "line-color": colour, "line-width": ["interpolate", ["linear"], ["zoom"], 8, 0.75, 14, 1.75] } },
  ];
}
