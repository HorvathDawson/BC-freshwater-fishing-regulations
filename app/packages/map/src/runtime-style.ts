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
import { MAP_STYLE, STYLE_META, colorExpression, defaultView, resolveTheme } from "./style";
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
/**
 * Water temperature, in the colours this map already uses for "may I fish here".
 *
 * TWO THINGS ARE DELIBERATE AND BOTH ARE THE OPPOSITE OF THE FLOW SCALE.
 *
 * IT IS ABSOLUTE. Every other scale here is relative to a river's own record, because 12
 * m³/s is a flood on one creek and a drought on another. Temperature is not: 20 °C is 20
 * °C on every river in the province, it is where British Columbia closes them, and it is
 * where a released fish starts dying. There is also no history to rank it against — HYDAT
 * carries level, flow and sediment and no temperature at all.
 *
 * IT REUSES THE STATUS COLOURS, and that is not laziness. `open`, `restricted` and
 * `closed` already mean "fishable", "caution" and "don't" on this map. Water too warm to
 * release a fish into means exactly those three things, so giving it a fourth palette
 * would ask a reader to learn a second word for an idea they have. A temperature ramp of
 * its own was written and deleted: the palette guard caught it colliding with the flow
 * ramp in four places, which was the right complaint about inventing a scale.
 *
 * The thresholds come from the publisher (`_TEMP_BANDS`) and are POLICY awaiting curation,
 * not physics — the band is carried on the feature rather than recomputed here, so there
 * is one place to change when the real per-river numbers are curated.
 */
function tempExpression(theme: string): unknown {
  const t = resolveTheme(theme) as Record<string, string>;
  return ["match", ["coalesce", ["get", "temperatureBand"], "unknown"],
          "cool", t["color.status.open"],
          "warm", t["color.status.restricted"],
          "critical", t["color.status.closed"],
          t["color.water.ungauged"]];
}

/**
 * The gauge dots' colour for a mode, so a view change can repaint them.
 *
 * THE STYLE IS BUILT ONCE, at map creation, and the recolour effect afterwards walks
 * `modes` calling `setLayerMode` per layer. That works for every layer in the generated
 * catalog and silently does nothing for these, because there is no layer whose id is
 * "gauges" — the dots are `gauge-dot` and `gauge-label`, added here from a live feed. So
 * switching from flow to temperature changed which stations were fetched and left them
 * painted on the old scale, which is the most confusing possible half-state: the right
 * roster, in the wrong language.
 */
export function gaugeDotColour(theme: string, mode: string | undefined): unknown {
  return mode === "temperature" ? tempExpression(theme) : rampExpression(theme);
}

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

/** The basemap's layers, cut at its first symbol layer so ours can be placed first. */
function splitBasemap(flavor: Parameters<typeof basemapLayers>[1]) {
  /*
   * THE BASEMAP'S OWN PARKS COME OUT — AND NOTHING ELSE DOES.
   *
   * We draw parks from the province's own boundaries now, knowing which are national
   * (closed), which are provincial (open) and which are ecological reserves. The basemap
   * cannot tell those apart, so its green competes with ours and its labels used to win
   * every collision against ours.
   *
   * BUT `landuse_park` IS NOT A PARK LAYER. It is Protomaps' green-space layer, and its
   * filter takes wood, forest, scrub, grassland, glacier and sand along with the parks.
   * Hiding it — which is what this did first — removed about 80% of British Columbia's
   * ground colour to suppress 3%: measured on our own extract, `wood` is 62.6% of the
   * landuse layer's bytes and every park kind together is 3.1%. The map went beige, and
   * `landcover` cannot fill in for it because Protomaps ships that only to z7.
   *
   * So the filter is narrowed rather than the layer dropped. Out go the kinds we now draw
   * ourselves and draw better — the parks, and `military`/`naval_base`, which are
   * `no_access` on our side and must read as closed rather than as green space.
   */
  /*
   * AND ITS WATER LABELS GO, because we draw those now — from the FWA, which is the same
   * gazette the regulations name water by. Two sets of river and lake names on one map is
   * worse than either alone: OSM and the FWA disagree about spellings, about which channel
   * of a braid carries the name, and about whether a widening is a lake at all, so a reader
   * gets a river labelled twice, slightly differently, and no way to tell which name a
   * regulation means. `water_label_ocean` and the island labels stay — we do not draw the
   * sea, and nobody is fishing a regulation on the Strait of Georgia.
   */
  const BASEMAP_NAMES_WATER = new Set(["water_label_lakes", "water_waterway_label"]);
  const OURS_NOW = ["national_park", "park", "protected_area", "nature_reserve",
                    "military", "naval_base"];
  const all = (basemapLayers("basemap", flavor, { lang: "en" }) as
               { id: string; type?: string; filter?: unknown[] }[])
    .filter((l) => !BASEMAP_NAMES_WATER.has(l.id))
    .map((l) => (l.id === "landuse_park" && Array.isArray(l.filter)
      ? { ...l, filter: (l.filter as unknown[]).filter(
            (v) => typeof v !== "string" || !OURS_NOW.includes(v)) }
      : l));
  const i = all.findIndex((l) => l.type === "symbol");
  return i < 0 ? { below: all, labels: [] as typeof all }
               : { below: all.slice(0, i), labels: all.slice(i) };
}

export function runtimeStyle(at: TileEndpoints, theme: string,
                             modes: Record<string, string> = {}) {
    // The BASEMAP has two flavours; ours has three. A colour-blind reader needs our outcome
  // hues changed, not the ground under them, so cvd rides on the light ground.
  const flavor = namedFlavor(theme === "dark" ? "black" : "light");
  const { below: baseBelowLabels, labels: baseLabels } = splitBasemap(flavor);
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
      // THE BASEMAP, SPLIT AT ITS FIRST LABEL — and our own labels go in the seam.
      //
      // MapLibre places symbols in layer order and the FIRST one to claim a spot keeps it.
      // Spread whole, the basemap's own park and place labels are all ahead of ours, so
      // every name this app adds loses the collision and is silently dropped: measured,
      // `park_closed__label` rendered ZERO features over Glacier National Park while
      // Protomaps' pale-green "Glacier National Park of Canada" sat on top of it. v1 hit
      // exactly this and its comment says so — waterbody names losing to land-use labels.
      //
      // Ours are the ones that carry a consequence: which water is closed, which land you
      // may not cross, which river you are standing on. They go first; the basemap's
      // decorative labels fill in around them.
      ...baseBelowLabels,
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
      ...MAP_STYLE.layers.flatMap((l) => {
        const view = defaultView();
        const mode = modes[l.id] ?? view?.modes[l.id];
        const painted = mode ? { ...l, paint: paintFor(l.id, mode, resolveTheme(theme)) } : l;
        /*
         * THE GLOW IS GONE, AND THE FIELD IS WHY.
         *
         * It was a wide blurred line under each river, drawn below z9, colouring the
         * province by flow at the zooms where a river is a hairline. That is exactly the
         * job the basin field now does — same question, same band (the glow stopped at z9,
         * the field runs z4–8), same ramp — and two answers to one question stacked on one
         * another is the failure this app keeps producing. The field does it better: a
         * catchment is the shape a percentile is a claim about, where a blurred line is a
         * claim about a corridor of arbitrary width.
         *
         * It was also on EVERY view, so the Regulations map was lit by flow it was not
         * showing a legend for.
         */
        if (l.id !== "stream") return [painted];
        return [painted,
        /*
         * "MEASURED HERE, BUT NOTHING TO COMPARE IT TO" — DRAWN AS A DASH.
         *
         * That state was a purple on the flow ramp, which is the one thing a sequential
         * scale must not carry: every other colour on it means a POSITION between low and
         * high, and this one means the axis does not exist here. A reader with any form of
         * colour blindness had no way to tell it from a value at all, and a reader without
         * had to learn a hue that appears nowhere else.
         *
         * A DASH IS NOT A COLOUR. It reads as "this line is provisional" without competing
         * for a place on the ramp, it survives any palette, and it needs no legend entry to
         * be understood.
         *
         * ITS OWN LAYER, because `line-dasharray` is not data-driven in MapLibre — it
         * cannot be varied per feature. What CAN be is opacity, so this layer covers every
         * stream and shows only where the feature-state says -1. The base `stream` layer
         * paints that same state the neutral ungauged grey underneath, so the dash carries
         * the whole distinction and the colour carries none of it.
         */
        {
          id: "stream-nobaseline", type: "line" as const, source: "atlas",
          "source-layer": "stream",
          layout: { "line-cap": "butt" as const, "line-join": "round" as const },
          paint: {
            "line-color": resolveTheme(theme)["color.water.ungauged"] as string,
            // Slightly heavier than the river under it, so the gaps read as gaps rather
            // than as a thin line that failed to draw.
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           6, 1.2, 10, 2, 14, 3.2],
            "line-dasharray": [2.5, 2],
            "line-opacity": ["case",
                             ["==", ["to-number", ["feature-state", "standing"], 0], -1],
                             1, 0],
          }}];
      }),
      // The basemap's own labels, AFTER ours — see the split above. They still draw; they
      // just no longer get first refusal on every position on the map.
      ...baseLabels,
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
         * The question at that zoom is not "what is this creek doing" but "is the country
         * I am driving to wet or dry". The first answer to that was a soft disc per
         * station, blurred and blended, scattered over the map. It was wrong in three ways
         * at once and it looked it:
         *
         *   · IT WAS ON LAND. A disc around a gauge paints country the gauge says nothing
         *     about, and the claim a percentile actually makes is about a CATCHMENT being
         *     wet — which is a shape, not a radius.
         *   · OVERLAPS INVENTED COLOURS. Two discs blending produced a hue that is not on
         *     the scale at all, so the reader was shown a value nobody computed.
         *   · A BLURRED EDGE IS A LIGHTER SHADE, and on a sequential ramp a lighter shade
         *     is a different number. Every disc faded through half the legend on its way
         *     out.
         *
         * So the light moved onto the water. This is the same ramp, the same values, drawn
         * as a wide soft line along the RIVER — which is the only thing we have a reading
         * for. Where there is no water there is no colour, which is the honest picture:
         * the atlas at z5 keeps the mainstems and the mainstems are exactly what carries a
         * gauge, so the province reads as a lit river network on quiet ground.
         *
         * It is drawn UNDER `stream` so the crisp line stays crisp on top of its own glow,
         * and it is gone by z9, where the rivers are wide enough to carry the colour
         * themselves.
         */
        { id: "gauge-dot", type: "circle" as const, source: "gauges",
          // Nothing below z7: at that scale a 5 px dot is noise, and the haze above is
          // saying the same thing in a form a person can actually read.
          minzoom: 7,
          filter: ["<=", ["get", "minz"], ["zoom"]],
          paint: {
            "circle-radius": 5,
            "circle-color": modes.gauges === "temperature"
              ? tempExpression(theme) : rampExpression(theme),
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
