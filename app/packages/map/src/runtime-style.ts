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
import { ATLAS_MAX_ZOOM, BASEMAP_MAX_ZOOM, TEMP_BANDS } from "@app/core";
import { MAP_STYLE, STYLE_META, colorExpression, defaultView, resolveTheme, tempBandColour, withFollowers,
         type Tokens } from "./style";
import { casingPaint, nobaselineOpacity, paintFor, ringPaint, SELECTION_CASING, SELECTION_RINGS } from "./adapters/contract";
import { traceLayers, traceSources } from "./trace-overlay";
import { BASEMAP_LABELS, basemapLabelPaint } from "./labels";

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
   * The release's map assets folder (P11/P18, ending in "/"): `fonts/{fontstack}/{range}.pbf` and
   * `sprites/v4/<light|dark>` under it. The installed folder on a phone (always offline), the
   * release url on the web. `glyphs`/`sprite` win when given; absent, the Protomaps CDN.
   */
  assets?: string;
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
  /**
   * The satellite imagery's tile template (`imageryTiles`), when the build names one: an ArcGIS Location Platform key
   * makes it Esri's keyed endpoint. Absent, the style's own (`layers.source.json`, source `imagery`).
   */
  imagery?: string;
}

/** Esri's keyed World Imagery tiles (ArcGIS Location Platform), the same MapServer tile scheme under a token. */
const ESRI_KEYED = "https://ibasemaps-api.arcgis.com/arcgis/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}";

/**
 * THE SATELLITE TILE TEMPLATE for a build: the style's own (Esri's ArcGIS Online MapServer, as the original app used)
 * with no key, or the Location Platform endpoint with the build's key (`EXPO_PUBLIC_ARCGIS_API_KEY`, set by the owner
 * in the build environment and restricted by referrer/app in the ArcGIS dashboard — never written into the code).
 */
export function imageryTiles(key: string | null): string {
  const own = (MAP_STYLE.sources as unknown as Record<string, { tiles?: string[] }>).imagery?.tiles?.[0] ?? "";
  if (!key) return own;
  const query = own.includes("?") ? own.slice(own.indexOf("?")) : "";
  return `${ESRI_KEYED}${query}${query ? "&" : "?"}token=${encodeURIComponent(key)}`;
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
 * The flow ramp, as an expression over a feature's own `ramp` property (its standing in the channel's units).
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

/** The basemap's layers, cut at its first symbol layer so ours can be placed first. */
function splitBasemap(flavor: Parameters<typeof basemapLayers>[1], tokens: Tokens) {
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
  /*
   * AND ITS NAMES WEAR OUR INKS (`labels.ts`): one label style across the map, the basemap's included. Painted for the
   * drawn ground here; `setBasemap` repaints them for imagery and back.
   */
  const named = all.map((l) => (BASEMAP_LABELS.includes(l.id)
    ? { ...l, paint: { ...(l as { paint?: Record<string, unknown> }).paint, ...basemapLabelPaint(l.id, tokens, "map") } }
    : l));
  const i = named.findIndex((l) => l.type === "symbol");
  return i < 0 ? { below: named, labels: [] as typeof named }
               : { below: named.slice(0, i), labels: named.slice(i) };
}

/**
 * THE EXTERNAL RASTER SOURCES the generated style declares — the satellite imagery (Esri World Imagery, online-only).
 *
 * Copied from `style.json` rather than restated here, so the URL, tile size and the
 * attribution Esri requires live in ONE place (`layers.source.json`), where the builder
 * refuses an external source without a credit. Our own authoring keys (`external`,
 * `$comment`) are stripped: MapLibre's validator rejects properties it does not know.
 * `imagery` (a keyed template, `imageryTiles`) replaces the imagery source's tiles and nothing else.
 */
function externalRasters(imagery?: string): Record<string, Record<string, unknown>> {
  return Object.fromEntries(Object.entries(MAP_STYLE.sources)
    .filter(([, v]) => (v as { type?: string }).type === "raster")
    .map(([id, v]) => [id, {
      ...Object.fromEntries(Object.entries(v as Record<string, unknown>)
        .filter(([k]) => k !== "external" && !k.startsWith("$"))),
      ...(id === "imagery" && imagery ? { tiles: [imagery] } : {}),
    }]));
}

/**
 * Every vector source the generated style declares besides `atlas` — the field's `basins` — pointed at the atlas archive
 * with its own maxzoom, and promoting its layers' ids as `atlas` does (feature-state is how the field is coloured).
 */
function fieldSources(atlasUrl: string): Record<string, Record<string, unknown>> {
  return Object.fromEntries(Object.entries(MAP_STYLE.sources as Record<string, { type?: string; url?: string; maxzoom?: number }>)
    .filter(([id, s]) => id !== "atlas" && s.type === "vector" && s.url === "pmtiles://atlas")
    .map(([id, s]) => [id, {
      type: "vector" as const, url: `pmtiles://${atlasUrl}`, maxzoom: s.maxzoom,
      promoteId: Object.fromEntries((MAP_STYLE.layers as { id: string; source?: string; "source-layer"?: string }[])
        .filter((l) => l.source === id && STYLE_META.featureIds[l.id] !== undefined)
        .map((l) => [l["source-layer"] ?? l.id, STYLE_META.featureIds[l.id]!])),
    }]));
}

/** Raster layers (imagery) go UNDER everything of ours; see the layer list below. */
const isRaster = (l: { type?: string }) => l.type === "raster";

/** The last geometry layer (not a name) drawing a source-layer: where a selection ring over its polygons goes. */
const lastDrawing = (sourceLayer: string): string | undefined =>
  (MAP_STYLE.layers as { id: string; type?: string; "source-layer"?: string }[])
    .filter((l) => l["source-layer"] === sourceLayer && l.type !== "symbol").at(-1)?.id;

export function runtimeStyle(at: TileEndpoints, theme: string,
                             modes: Record<string, string> = {}) {
    // The BASEMAP has two flavours; ours has three. A colour-blind reader needs our outcome
  // hues changed, not the ground under them, so cvd rides on the light ground.
  const flavor = namedFlavor(theme === "dark" ? "black" : "light");
  const { below: baseBelowLabels, labels: baseLabels } = splitBasemap(flavor, resolveTheme(theme));
  const paper = resolveTheme(theme)["color.outside"] as string;
  // A lake's outline takes the lake's mode on the FIRST frame too — see `withFollowers`.
  const layerModes = withFollowers(modes);

  return {
    version: 8 as const,
    glyphs: at.glyphs ?? `${at.assets ?? `${PM_ASSETS}/`}fonts/{fontstack}/{range}.pbf`,
    sprite: at.sprite ?? `${at.assets ?? `${PM_ASSETS}/`}sprites/v4/${theme === "dark" ? "dark" : "light"}`,
    sources: {
      // EACH SOURCE STOPS WHERE ITS ARCHIVE STOPS, so the camera can go past it (CAMERA_MAX_ZOOM, z18) and MapLibre
      // overzooms the last real tile instead of asking the archive for tiles it does not hold — a blank map. Stated
      // rather than left to the archive's header because a style's own `maxzoom` wins over the header's (GL JS
      // `loadTileJson`), and `tools/overzoom.test.ts` holds both numbers to the archives the pipeline writes.
      basemap: { type: "vector" as const, url: `pmtiles://${at.basemap}`, maxzoom: BASEMAP_MAX_ZOOM, attribution:
        '<a href="https://openstreetmap.org/copyright">© OpenStreetMap</a> · © Protomaps' },
      ...externalRasters(at.imagery),
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
        maxzoom: ATLAS_MAX_ZOOM,
        // Every reach, lake and boundary in this archive is derived from the Province's
        // Freshwater Atlas, and the rules painted over them from the Province's synopsis.
        // Both are open data with an attribution condition, and the map is where they are
        // actually drawn.
        attribution:
          '<a href="https://www2.gov.bc.ca/gov/content/data/open-data">Province of British ' +
          "Columbia</a> \u00b7 Freshwater Atlas &amp; Freshwater Fishing Synopsis",
        /**
         * WITHOUT AN ID NOTHING WORKS, and nothing says so.
         *
         * A feature with no `feature.id` cannot take feature-state — every colour-by-rule silently fails — and a tap
         * on it returns no id, so a TAP NEVER OPENS A SHEET. No error is raised at any point.
         *
         * MapLibre does not lift a tile property into `feature.id` on its own, which is what `promoteId` is for. Built
         * from `STYLE_META.featureIds` rather than typed here, so the property named in `layers.source.json` and the
         * property MapLibre promotes cannot disagree.
         *
         * THE SECTIONS ARE PROMOTED TOO, ON PURPOSE (N2 review). The tiles also carry `section_id` as each section's MVT
         * feature id (P18, `pipeline/tests/test_tile_mvt_ids.py`), because MapLibre Native ignores `promoteId` — that id
         * is the only one a phone sees, and `nativeStyle` drops this object. The web keeps promoting the property: the
         * value is the same integer, and an atlas of an older vintage (no MVT ids) still colours and taps on the web.
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
      /*
       * THE FIELD'S OWN SOURCE: the same archive, stopped at the last zoom it carries basins at, so MapLibre overzooms
       * those tiles up to the handover instead of asking for deeper ones that have none (`sources.basins` in
       * layers.source.json says why). Only the basin layer reads it, and only while the field is drawn.
       */
      ...fieldSources(at.atlas),
      // THE ROUTE OVERLAY's two sources, empty until a panel fills them — see trace-overlay.ts.
      ...traceSources(),
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
      /*
       * THE SATELLITE GROUND, directly over the drawn one and under everything else: the
       * Conditions wash, the outside-BC mask (so the province still reads as the place we
       * answer for), our water, and every label. Hidden until the map's `basemap` prop asks
       * for it — see `setBasemap` in the adapter contract.
       */
      ...(MAP_STYLE.layers as { id: string; type?: string }[]).filter(isRaster),
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
      ...MAP_STYLE.layers.filter((l) => !isRaster(l as { type?: string })).flatMap((l) => {
        const view = defaultView();
        const mode = layerModes[l.id] ?? view?.modes[l.id];
        const coloured = mode ? { ...l, paint: paintFor(l.id, mode, resolveTheme(theme)) } : l;
        // THE FIELD IS NOT DRAWN outside the modes it draws in — on the first frame too (`fieldMoved` in the adapter)
        const painted = l.id === STYLE_META.field?.layer && !(STYLE_META.field.drawnIn).includes(mode ?? "")
          ? { ...coloured, layout: { ...(coloured as { layout?: Record<string, unknown> }).layout, visibility: "none" } } as unknown as typeof coloured
          : coloured;
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
        /*
         * THE SELECTION'S OUTLINE (owner 2026-10-21): what is selected keeps its colour and is ringed — a ring over a
         * lake's / wetland's own polygons (after the last layer drawing that source-layer), a casing directly UNDER the
         * stream line so the line's colour stays on top. Shown by feature-state `selected` only; see `SELECTION_CASING`.
         */
        const ring = (id: string, sourceLayer: string) => ({
          id, type: "line" as const, source: "atlas", "source-layer": sourceLayer,
          layout: { "line-join": "round" as const }, paint: ringPaint(resolveTheme(theme)) });
        // FOUND, NOT NAMED: the lake's last layer moved from `lake_flowing` to `lake__edge` when the shoreline was drawn
        // over the boundaries (T83), and a ring placed by name would have slid under the shoreline it rings.
        const ringed = l.id === lastDrawing("lake") ? ring(SELECTION_RINGS.lake, "lake")
          : l.id === lastDrawing("wetland") ? ring(SELECTION_RINGS.wetland, "wetland") : undefined;
        if (ringed) return [painted, ringed];
        if (l.id !== "stream") return [painted];
        const casing = {
          id: SELECTION_CASING, type: "line" as const, source: "atlas", "source-layer": "stream",
          layout: { "line-cap": "round" as const, "line-join": "round" as const },
          paint: casingPaint((painted as { paint?: Record<string, unknown> }).paint?.["line-width"]
                             ?? paintFor("stream", "plain", resolveTheme(theme))["line-width"], resolveTheme(theme)),
        };
        /*
         * ONLY IN THE MODE THAT MEANS ANYTHING BY IT.
         *
         * This overlay was emitted for every mode, and its opacity reads `feature-state`
         * "standing" — which MapLibre keeps until something clears it. So leaving the
         * Conditions map for the Regulations map left the states behind, and rivers with no
         * baseline stayed dashed on a map where "no baseline" is not a thing being asked
         * about. Tapping one rewrote its state and the dash vanished, which is what made it
         * look like a rendering glitch rather than a stale layer.
         *
         * It was then built only for the standing mode — at map CREATION. The map is created on
         * whichever tab the app opens and is not remounted between tabs, so a reader who
         * started on the Map tab never saw a dash on Conditions (the one-pathway audit,
         * 2026-10-21). Now it is ALWAYS built and its opacity is the adapter's, painted with
         * the stream's own mode (`nobaselineOpacity`): every flow mode reads the number its
         * colour reads, every other mode is 0 — so nothing can leak, and nothing is missing.
         */
        return [casing, painted,
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
            "line-opacity": nobaselineOpacity(mode ?? "plain"),
          }}];
      }),
      /*
       * The basemap's own labels, AFTER ours — see the split above. They still draw; they
       * just no longer get first refusal on every position on the map.
       *
       * CONFINING THEM TO BC IS NOT DONE HERE. It needs the mask as a literal (a `within`
       * expression cannot reference a source) and the mask is fetched, so it is applied in
       * `Map.web.tsx` once it lands — in ONE place, through `toExpression`, because
       * Protomaps writes most of these filters in the legacy syntax and combining a legacy
       * filter with an expression under `all` makes MapLibre parse the whole thing as
       * legacy.
       *
       * This file used to do it too, with the raw filter and no conversion. Two
       * implementations of one behaviour, and only one of them was fixed when the legacy
       * syntax bit — so the map threw `filter[1][0]: "!" found` the moment a remount found
       * the mask already cached and took the constructor path instead of the effect.
       */
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
       * direction points UPSTREAM, and a line-placed ◀ points downstream. `text-keep-upright:
       * false` stops MapLibre helpfully flipping it on west-flowing rivers, which would make
       * half the province's arrows lie. ◀ in Regular, not ▲ in Medium turned 180
       * (2026-10-21): Medium has no geometric shapes, so the arrows were never drawn; a
       * line-placed ▲ points across the line; and the 180° turn pivots it off the line.
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
          "text-field": "◀",
          "text-font": ["Noto Sans Regular"],
          "text-size": 11,
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
      /*
       * THE ROUTE OVERLAY, LAST OF ALL — over every river and every gauge dot. Present in
       * every view and empty unless a route map fills it (`setTrace`), so declaring it costs
       * a frame nothing and a route never needs the style rebuilt to appear.
       */
      ...traceLayers(theme),
    ],
  };
}
