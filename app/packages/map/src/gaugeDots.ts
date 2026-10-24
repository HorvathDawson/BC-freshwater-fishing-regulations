/**
 * THE GAUGE DOTS: the live feed's runtime source and the two layers drawn from it (`gauge-dot`, `gauge-label`), and the
 * colour a dot takes for a mode — composed into the style by `runtime-style.ts`, repainted on a view change by `look.ts`.
 */
import { TEMP_BANDS } from "@app/core";
import { STYLE_META, resolveTheme, tempBandColour } from "./style";

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
 * COOL AND CRITICAL REUSE THE STATUS COLOURS, and that is not laziness. `open` and `closed`
 * already mean "fishable" and "don't" on this map, and water too warm to release a fish into
 * means exactly that. A temperature ramp of its own was written and deleted: the palette
 * guard caught it colliding with the flow ramp in four places, which was the right complaint
 * about inventing a scale.
 *
 * WARM DOES NOT (T83). It wore `restricted`, and restricted is not "caution" on this map — it
 * is the permit-only hatch, so a warm river's dot said "you need a permit here". It has its
 * own hue, `color.temp.warm`; the band -> token table is @app/core `TEMP_BAND_TOKEN`, read
 * here and by the legend, so the dot and its swatch cannot disagree.
 *
 * The thresholds come from the publisher (`_TEMP_BANDS`) and are POLICY awaiting curation,
 * not physics — the band is carried on the feature rather than recomputed here, so there
 * is one place to change when the real per-river numbers are curated.
 */
function tempExpression(theme: string): unknown {
  const t = resolveTheme(theme) as Record<string, string>;
  return ["match", ["coalesce", ["get", "temperatureBand"], "unknown"],
          ...TEMP_BANDS.flatMap(([band]) => [band, tempBandColour(theme, band)]),
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

/**
 * The flow ramp, as an expression over a feature's own `ramp` property (its standing in the channel's units).
 *
 * Shares the stream layer's stops by construction — read from the same style metadata —
 * so a gauge dot and the river under it are painted from one scale. Two ramps would drift.
 */
function rampExpression(theme: string): unknown {
  const t = resolveTheme(theme) as Record<string, string>;
  const mode = STYLE_META.colorModes.stream?.standing as
    { stops?: [number, { token: string }][] } | undefined;
  const stops = mode?.stops ?? [];
  // `ramp` is the dot's value already in the standing channel's units (`toRamp` in @app/ui: a percentile x 100, or the
  // Change domain 200-300), so the dot reads the river's own stops at every horizon (T126).
  const out: unknown[] = ["interpolate", ["linear"], ["coalesce", ["get", "ramp"], -1]];
  for (const [at, ref] of stops) out.push(at, t[ref.token]);
  return out;
}

/**
 * The gauges' runtime source: the GeoJSON itself, parsed (`geojsonData`).
 *
 * ATTRIBUTION RIDES ON THE SOURCE, so MapLibre's own control
 * aggregates it with the basemap's rather than the app maintaining a
 * second list. It used to appear NOWHERE on the map — the whole list
 * lived behind the Layers button, two taps from the data it covers.
 *
 * This shows on every view, not only Conditions: `Map.web` hands the
 * style an empty FeatureCollection when there are no gauges, so the
 * source exists from the first frame (which is deliberate — see
 * EMPTY_FC there). The credit is therefore always on, which is the
 * safe direction to be wrong in.
 */
export function gaugeSource(raw: string) {
  return { type: "geojson" as const, data: geojsonData(raw),
           attribution: "Hydrometric data: Environment and Climate Change Canada" };
}

/** The dot and its number, over the gauges source, on the paper (`paper`: the halo and the dot's stroke). */
export function gaugeLayers(theme: string, modes: Record<string, string>, paper: string) {
  return [
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
  ];
}
