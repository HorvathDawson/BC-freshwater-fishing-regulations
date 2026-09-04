/**
 * Composing the style the renderer actually gets: a basemap underneath, our water on top.
 *
 * Two archives, deliberately:
 *   basemap  a Protomaps build of OpenStreetMap for BC (data/bc.pmtiles) — earth, roads,
 *            landcover, places. Generic, replaceable, and NOT ours to style; the flavour
 *            comes from @protomaps/basemaps so it looks like every other OSM map, which is
 *            the point. A fisherman should recognise the ground before they read our water.
 *   atlas    our pipeline's tiles (data/generated/tiles/atlas.pmtiles) — every stream, lake,
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
 * GeoJSON for a `geojson` source's `data`, parsed.
 *
 * MAPLIBRE TREATS A STRING AS A URL. Handing it the serialised FeatureCollection made the
 * renderer fetch `/%7B%22type%22:%22FeatureCollection%22...%7D` as a relative path; the dev
 * server answered with index.html, and the console filled with
 * `Unexpected token '<', "<!DOCTYPE "... is not valid JSON` — once per map mount, so the
 * count climbed on every tab switch while the app looked fine. The gauge source then began
 * empty and was only rescued by the imperative `setData` in Map.web.tsx.
 *
 * The value stays a STRING across the props boundary on purpose: it changes every half hour
 * and a string is what makes React's identity check cheap. Parsing belongs here, at the one
 * point where it meets the renderer.
 *
 * A malformed payload yields an empty collection rather than throwing: a bad feed must not
 * take the whole map down, and an empty source draws nothing, which is the honest picture.
 */
function geojsonData(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return { type: "FeatureCollection", features: [] };
  }
}

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
      // `outside` IS a URL — the mask is fetched. `gauges` is not: it is the GeoJSON
      // itself, so it has to be parsed. See `geojsonData` below for why that distinction
      // cost us an error on every map mount.
      ...(at.outside ? { outside: { type: "geojson" as const, data: at.outside } } : {}),
      ...(at.gauges
        ? { gauges: { type: "geojson" as const, data: geojsonData(at.gauges),
                      // ATTRIBUTION RIDES ON THE SOURCE, so MapLibre's own control
                      // aggregates it with the basemap's rather than the app maintaining a
                      // second list. It used to appear NOWHERE on the map — the whole list
                      // lived behind the Layers button, two taps from the data it covers.
                      //
                      // This shows on every view, not only Conditions: `Map.web` hands the
                      // style an empty FeatureCollection when there are no gauges, so the
                      // source exists from the first frame (which is deliberate — see
                      // EMPTY_FC there). The credit is therefore always on, which is the
                      // safe direction to be wrong in.
                      attribution: "Hydrometric data: Environment and Climate Change Canada" } }
        : {}),
      atlas: {
        type: "vector" as const,
        url: `pmtiles://${at.atlas}`,
        // Every reach, lake and boundary in this archive is derived from the Province's
        // Freshwater Atlas, and the rules painted over them from the Province's synopsis.
        // Both are open data with an attribution condition, and the map is where they are
        // actually drawn.
        attribution:
          '<a href="https://www2.gov.bc.ca/gov/content/data/open-data">Province of British ' +
          "Columbia</a> \u00b7 Freshwater Atlas &amp; Freshwater Fishing Synopsis",
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
          // `color.mask`, NOT `color.outside`. They were one token and it had two jobs:
          // the PAPER (the ground under a tile that has not arrived, and the fill of a
          // gauge label's pill) and the "we have no answers here" fill beyond the
          // province. Darkening one darkened the other, so the mask could not be made to
          // read without turning the loading state grey and the pills with it.
          "fill-color": resolveTheme(theme)["color.mask"] as string,
          /*
           * OPAQUE WHEN ZOOMED OUT, a tint when zoomed in.
           *
           * The basemap archive is clipped to tiles that TOUCH British Columbia, and at low
           * zoom one tile is enormous — so the coverage ends on a tile boundary somewhere
           * out over Washington, and the mask was painting a 72% wash over ground that
           * stopped abruptly mid-screen. Beyond it sat the bare `color.outside` paper,
           * which is near-white, next to a dark washed slab. Two different "not British
           * Columbia" greys with a straight edge between them, and neither edge was a real
           * boundary.
           *
           * Carrying the mask to full opacity by z6 hides the seam completely: zoomed out,
           * the province reads as a clean silhouette on flat ground, which is the honest
           * picture — we have no answers out there and no obligation to draw it. By z8 the
           * tint is back, and that is where it earns its keep: close in you want to see the
           * far bank of a border river, faintly, so the water does not just stop.
           */
          "fill-opacity": ["interpolate", ["linear"], ["zoom"],
                           6, 1,
                           8, resolveTheme(theme)["opacity.outside"] as number],
        },
      }] : []),
      ...MAP_STYLE.layers.map((l) => {
        const view = defaultView();
        const mode = modes[l.id] ?? view?.modes[l.id];
        if (!mode) return l;
        return { ...l, paint: paintFor(l.id, mode, resolveTheme(theme)) };
      }),
      /**
       * WHICH WAY THE WATER GOES, on a highlighted route only.
       *
       * The route panel's job is to show the chain of reaches between where you are and
       * the station that speaks for you. Colouring them says WHICH; it does not say which
       * END is the gauge, and on a braided lowland river that is genuinely ambiguous.
       *
       * FWA BLUE LINES RUN MOUTH TO SOURCE — verified against the Fraser, whose first
       * vertex is at Vancouver and whose last is above Prince George — so the line's own
       * direction points UPSTREAM. `text-rotate: 180` turns the arrowhead around to point
       * downstream, and `text-keep-upright: false` stops MapLibre helpfully flipping it
       * back on west-flowing rivers, which would make half the province's arrows lie.
       *
       * Drawn from feature-state rather than a filter: a `filter` cannot read feature
       * state, so the layer covers every stream and its opacity is 0 unless selected.
       */
      ...(at.gauges ? [{
        id: "route-arrows", type: "symbol" as const, source: "atlas",
        "source-layer": "stream",
        minzoom: 7,
        layout: {
          "symbol-placement": "line" as const,
          "symbol-spacing": 70,
          "text-field": "▲",
          "text-font": ["Noto Sans Medium"],
          "text-size": 11,
          "text-rotate": 180,
          "text-keep-upright": false,
          "text-allow-overlap": true,
          "text-ignore-placement": true,
          "text-rotation-alignment": "map" as const,
          "text-pitch-alignment": "map" as const,
        },
        paint: {
          "text-color": resolveTheme(theme)["color.highlight"] as string,
          "text-opacity": ["case",
                           ["boolean", ["feature-state", "selected"], false], 0.95, 0],
          "text-halo-color": paper,
          "text-halo-width": 1.4,
        },
      }] : []),
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
        /**
         * THE PROVINCE, BELOW THE ZOOM WHERE INDIVIDUAL RIVERS MEAN ANYTHING.
         *
         * The atlas thins itself out as you zoom away — by z6 most of BC's water is gone,
         * which is correct for a map of rivers and useless for a map of CONDITIONS. What
         * is left is a handful of mainstems and a scatter of dots too small to read, and
         * the question at that zoom is not "what is this creek doing" but "is the country
         * I am driving to wet or dry".
         *
         * So below z7 the rivers fade out and each station becomes a soft disc of its own
         * colour. It is NOT an interpolation and does not pretend to be one: every disc is
         * centred on a real gauge and coloured by that gauge's own reading, and where they
         * overlap they simply blend. Nothing is claimed about the country between two
         * stations except that two stations are near it.
         *
         * Discs shrink to nothing by z7.5, exactly as the dots and the rivers come in, so
         * the two never argue on screen.
         */
        { id: "gauge-haze", type: "circle" as const, source: "gauges",
          maxzoom: 7.5,
          paint: {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              4, 30, 5.5, 26, 7, 12, 7.5, 0],
            "circle-color": rampExpression(theme),
            "circle-blur": 0.85,
            "circle-opacity": ["interpolate", ["linear"], ["zoom"],
                               4, 0.5, 6.5, 0.42, 7.5, 0],
          } },
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
          // Nothing below z7: at that scale a 5 px dot is noise, and the haze above is
          // saying the same thing in a form a person can actually read.
          minzoom: 7,
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
