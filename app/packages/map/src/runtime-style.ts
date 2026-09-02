/**
 * Composing the style the renderer actually gets: a basemap underneath, our water on top.
 *
 * Two archives, deliberately:
 *   basemap  a Protomaps build of OpenStreetMap for BC (data/bc.pmtiles) — earth, roads,
 *            landcover, places. Generic, replaceable, and NOT ours to style; the flavour
 *            comes from @protomaps/basemaps so it looks like every other OSM map, which is
 *            the point. A fisherman should recognise the ground before they read our water.
 *   atlas    our pipeline's tiles (output/tiles/atlas.pmtiles) — every stream, lake,
 *            wetland and administrative boundary in the province, carrying identity but no
 *            regulation. Colour arrives at runtime by feature-state.
 *
 * Our layers are appended AFTER the basemap's, so regulated water always draws over roads
 * and landcover. The generated style (`style.json`) is the source of truth for that half
 * and is never edited here — this only stacks it on a ground and points the source at a
 * real URL.
 */
import { layers as basemapLayers, namedFlavor } from "@protomaps/basemaps";
import { MAP_STYLE, STYLE_META, defaultView, resolveTheme } from "./style";
import { paintFor } from "./adapters/contract";

export interface TileEndpoints {
  /** Where the two archives are served from, with byte-range support. */
  atlas: string;
  basemap: string;
  /**
   * Label glyphs and icons. Protomaps hosts these; a shipped app needs its own copy, since
   * a map that silently loses every label when offline is worse than one with no labels.
   */
  glyphs?: string;
  sprite?: string;
  /**
   * The outside-BC mask: a rectangle with the province punched out, as GeoJSON.
   * ~61 KB, and changing how it looks never needs a tile rebuild.
   */
  outside?: string;
  /**
   * The gauges themselves, as GeoJSON points — one per station on screen, each with a
   * `label` the map draws beside it ("15.7 m³/s · p4th").
   *
   * A RUNTIME SOURCE, not a tile layer, because these move every half hour and the tiles
   * move once a build. It is also only ever a few hundred points, so the whole set can be
   * handed over on each refresh rather than diffed.
   */
  gauges?: string;
}

const PM_ASSETS = "https://protomaps.github.io/basemaps-assets";

/**
 * The flow ramp, as an expression over a feature's own `percentile` property.
 *
 * Shares the stream layer's stops by construction — read from the same style metadata —
 * so a gauge dot and the river under it are painted from one scale. Two ramps would drift.
 */
function rampExpression(theme: string): unknown {
  const t = resolveTheme(theme) as Record<string, string>;
  const mode = STYLE_META.colorModes.stream?.standing as
    { stops?: [number, { token: string }][] } | undefined;
  const stops = mode?.stops ?? [];
  const out: unknown[] = ["interpolate", ["linear"],
                          ["*", ["coalesce", ["get", "percentile"], -0.01], 100]];
  for (const [at, ref] of stops) out.push(at, t[ref.token]);
  return out;
}

export function runtimeStyle(at: TileEndpoints, theme: string,
                             modes: Record<string, string> = {}) {
    // The BASEMAP has two flavours; ours has three. A colour-blind reader needs our outcome
  // hues changed, not the ground under them, so cvd rides on the light ground.
  const flavor = namedFlavor(theme === "dark" ? "black" : "light");
  const paper = resolveTheme(theme)["color.outside"] as string;

  return {
    version: 8 as const,
    glyphs: at.glyphs ?? `${PM_ASSETS}/fonts/{fontstack}/{range}.pbf`,
    sprite: at.sprite ?? `${PM_ASSETS}/sprites/v4/${theme === "dark" ? "dark" : "light"}`,
    sources: {
      basemap: { type: "vector" as const, url: `pmtiles://${at.basemap}`, attribution:
        '<a href="https://openstreetmap.org/copyright">© OpenStreetMap</a> · © Protomaps' },
      ...(at.outside ? { outside: { type: "geojson" as const, data: at.outside } } : {}),
      ...(at.gauges ? { gauges: { type: "geojson" as const, data: at.gauges } } : {}),
      atlas: {
        type: "vector" as const,
        url: `pmtiles://${at.atlas}`,
        /**
         * WITHOUT THIS NOTHING WORKS, and nothing says so.
         *
         * MapLibre does not lift a tile property into `feature.id` on its own. Our ids are
         * strings (`{blk}:{measure}`, an area name), which is what `promoteId` is for.
         * Absent it, `feature.id` is `undefined` — so `setFeatureState` addresses nothing
         * and every colour-by-rule silently fails, and `queryRenderedFeatures(...)[0].id`
         * is undefined so a TAP NEVER OPENS A SHEET. No error is raised at any point.
         *
         * Built from `STYLE_META.featureIds` rather than typed here, so the property named
         * in `layers.source.json` and the property MapLibre promotes cannot disagree.
         */
        promoteId: Object.fromEntries(
          Object.entries(STYLE_META.featureIds)
            .filter(([layerId]) => MAP_STYLE.layers.some((l) => l.id === layerId))
            .map(([layerId, prop]) => [
              (MAP_STYLE.layers.find((l) => l.id === layerId) as
                 { "source-layer"?: string })["source-layer"] ?? layerId,
              prop,
            ])),
      },
    },
    layers: [
      /**
       * A flat ground under everything, so no tile gap ever shows as bare canvas.
       *
       * The basemap archive is clipped to the province, so there are no tiles beyond it —
       * everything outside BC is already empty. A background under the whole map is
       * therefore the mask: BC is painted over it by the basemap, and what shows through
       * is exactly what is not British Columbia.
       *
       * This is NOT the outside-BC mask — that is the `outside` fill near the top of the
       * stack, drawn from a GeoJSON cookie cutter. This is only the colour behind the
       * basemap, so a tile that has not arrived yet is not a black hole.
       */
      { id: "outside-bc", type: "background" as const,
        paint: { "background-color": resolveTheme(theme)["color.outside"] as string } },
      ...basemapLayers("basemap", flavor, { lang: "en" }),
      /**
       * A SHEET OF PAPER OVER THE GROUND, for the Conditions view only.
       *
       * The flow ramp has to carry a seven-step ordered scale on 1.4-pixel lines, over an
       * OpenStreetMap basemap built to be looked at: green parks, tan landcover, white
       * roads with casings, every one of them saturated enough to compete. Rewriting the
       * ramp fixed the stop that was literally the paper's colour, but no palette wins that
       * fight on its own — the answer is to stop the fight, by muting everything the
       * question is not about.
       *
       * A fill over the basemap and UNDER our water, not a change to the basemap's own
       * paint: the flavour is Protomaps' and is deliberately not ours to restyle (see the
       * file header), and a wash is one layer to turn on rather than forty to recolour.
       *
       * Opacity 0 by default. `Map.web` raises it when the stream layer is in `standing`
       * mode, which is exactly when the Conditions ramp is on screen.
       */
      { id: "basemap-wash", type: "background" as const,
        paint: {
          "background-color": resolveTheme(theme)["color.outside"] as string,
          "background-opacity": 0,
        } },
      // PAINTED HERE, not on `load`. Shipping the style unpainted meant MapLibre drew its
      // own defaults — black lines across the province — for the frame or two before the
      // adapter ran, which reads as the map flashing.
      // THE MASK, over the basemap and over our water alike, under the labels. A fill of
      // a rectangle-minus-BC: everything it covers is somewhere we have no answers for.
      // A background cannot do this job — it sits under the basemap, so the far bank of a
      // border river would still be drawn as ordinary ground.
      ...(at.outside ? [{
        id: "outside", type: "fill" as const, source: "outside",
        paint: {
          "fill-color": resolveTheme(theme)["color.outside"] as string,
          "fill-opacity": resolveTheme(theme)["opacity.outside"] as number,
        },
      }] : []),
      ...MAP_STYLE.layers.map((l) => {
        const view = defaultView();
        const mode = modes[l.id] ?? view?.modes[l.id];
        if (!mode) return l;
        return { ...l, paint: paintFor(l.id, mode, resolveTheme(theme)) };
      }),
      /**
       * THE GAUGES, LAST — which is what puts them ON TOP.
       *
       * They were written above this line and drew UNDERNEATH every river, because a
       * MapLibre style is painted in array order and the comment saying "above the water"
       * was describing an intention rather than the code. A gauge dot under a two-pixel
       * river line is a dot you cannot see and cannot tap.
       *
       * Drawn as a dot plus its own number — never a dot alone, because a bare dot says
       * "something is here" and the number is the point. The halo and the dot's stroke are
       * the PAPER the mark sits on — the same colour the map already uses for ground
       * outside the province. A second near-white token would only ever hold the same
       * value, and two names for one meaning is how a palette drifts.
       */
      ...(at.gauges ? [
        // THE DOT IS THE SAME COLOUR AS ITS RIVER. It reads the percentile off the
        // feature and runs it through the same ramp the stream layer uses, so a gauge and
        // the water it measures can never disagree on screen. A dot in a colour of its own
        // would be a second scale the reader has to learn.
        //
        // THE FILTER IS THE ZOOM LADDER. Tile features thin out on their own because
        // tippecanoe stamped a minzoom on each; a GeoJSON source does not, so every station
        // in the province drew at every zoom and a creek gauge sat over country where its
        // creek disappeared four zooms ago. `minz` comes from `@app/core/ladder`, which a
        // test holds equal to the pipeline's — so a dot appears exactly when its water does.
        { id: "gauge-dot", type: "circle" as const, source: "gauges",
          filter: ["<=", ["get", "minz"], ["zoom"]],
          paint: {
            "circle-radius": 5,
            "circle-color": rampExpression(theme),
            "circle-stroke-width": 2,
            "circle-stroke-color": paper,
          } },
        { id: "gauge-label", type: "symbol" as const, source: "gauges",
          /**
           * READ IT BEFORE YOU CAN FISH IT. A reading is a fact about one point on one
           * river, and at the zoom that river first appears the map is a province — the
           * number is unreadable clutter over country you are not standing in. Four zooms
           * past the dot is roughly "a valley on screen", which is where the number starts
           * to mean something, and never below z8 whatever the river's size, so the Fraser
           * does not label itself from orbit.
           */
          filter: ["all",
                   [">=", ["zoom"], 8],
                   ["<=", ["+", ["get", "minz"], 4], ["zoom"]]],
          layout: {
            "text-field": ["get", "label"],
            "text-font": ["Noto Sans Medium"],
            "text-size": 12,
            // THE PILL. An SDF-free stretchable image added at runtime (see `pillImage`),
            // fitted around whatever the text turns out to be — which is what the design
            // draws and what a halo could only approximate. `icon-text-fit` is the whole
            // reason this is an icon rather than a background colour: MapLibre has no such
            // thing for text, and a halo on a busy basemap reads as a smudge.
            "icon-image": "gauge-pill",
            "icon-text-fit": "both" as const,
            "icon-text-fit-padding": [3, 9, 3, 9],
            "icon-anchor": "left" as const,
            "icon-offset": [10, 0],
            "text-offset": [1.05, 0],
            "text-anchor": "left" as const,
            // Let a crowded valley drop labels rather than overlap them; the dots stay.
            "text-allow-overlap": false,
            "icon-allow-overlap": false,
            "text-optional": true,
          },
          paint: {
            "text-color": resolveTheme(theme)["color.gauge.dot"] as string,
          } },
      ] : []),
    ],
  };
}
