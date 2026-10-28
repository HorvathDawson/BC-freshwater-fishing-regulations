/**
 * The seam between the pipeline and the app.
 *
 * The map style names a `sourceLayer` and a `featureIdProperty` per layer. Those are
 * strings, and until this file existed nothing checked them: the style said
 * `sections` / `section_id` while the tiles carried `stream` / `id`. That combination
 * draws NOTHING and errors NOWHERE — the map simply comes up empty, and every debugging
 * instinct sends you looking at the tiles, the network, the camera, anywhere but a
 * mismatched string.
 *
 * `pipeline/deliver/tiles/tile-contract.json` is generated from pipeline/deliver/tiles/layers.py by
 * `python -m pipeline.deliver.tiles --write-contract`. Both sides read it, so drift is a failing
 * test rather than a blank screen.
 */
import { describe, expect, it } from "vitest";
import { MAGNITUDE_LADDER, zoomForMagnitude } from "@app/core";
import { runtimeStyle } from "@app/map";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const contract = JSON.parse(
  readFileSync(join(ROOT, "pipeline/deliver/tiles/tile-contract.json"), "utf8"),
) as { layers: Record<string, { geometry: string; attrs: string[];
                              featureId: string | null; decorative: boolean; mvtId?: boolean }>;
        magnitudeLadder: [number, number][] };

const readStyle = (f: string) =>
  JSON.parse(readFileSync(join(ROOT, "app/packages/map/style", f), "utf8"));
const MAP_STYLE = readStyle("style.json") as
  { layers: ({ id: string } & Record<string, unknown>)[] };
const STYLE_META = readStyle("style.meta.json") as
  { colorModes: Record<string, unknown>; featureIds: Record<string, string> };

const source = JSON.parse(
  readFileSync(join(ROOT, "app/packages/map/style/layers.source.json"), "utf8"),
) as {
  layers: { id: string; sourceLayer: string; geometry: string; featureIdProperty?: string;
            outline?: unknown; label?: { priority?: number }; highlightable?: boolean;
            colorModes: Record<string, { data?: { field: string } }> }[];
  providers: Record<string, { key?: string }>;
};
/**
 * The layers drawn FROM THE ATLAS — the only ones the tile contract speaks for. The
 * satellite imagery is a third-party raster with no source-layer, no attributes and no
 * id; holding it to our tile contract would be asking Esri's JPEGs for a `section_id`.
 *
 * EVERY SOURCE ON THE ATLAS ARCHIVE, not only the one named `atlas`: the Conditions field reads the same archive through
 * `basins`, a second source stopped at the last zoom the tiles carry basins at so it overzooms (layers.source.json).
 */
const sources = (source as unknown as { sources: Record<string, { url?: string }> }).sources;
source.layers = source.layers.filter((l) =>
  sources[(l as { source?: string }).source ?? ""]?.url === "pmtiles://atlas");

describe("the style and the tiles agree", () => {
  it("every layer the style draws exists in the tiles", () => {
    for (const l of source.layers) {
      expect(contract.layers[l.sourceLayer], `style layer "${l.id}" -> sourceLayer`).toBeDefined();
    }
  });

  it("every featureIdProperty is a property the tile actually carries", () => {
    // The one that was wrong. A feature id naming a property that is not on the feature
    // means setFeatureState silently addresses nothing.
    for (const l of source.layers) {
      const t = contract.layers[l.sourceLayer]!;
      // A DECORATIVE LAYER MUST NOT NAME ONE. It ships no attributes at all, so promoting
      // a property would give every feature `id: undefined` — and MapLibre reports that as
      // a hit like any other, so a tap would resolve to nothing with no error anywhere.
      if (t.decorative) {
        expect(l.featureIdProperty,
               `${l.id} draws a decorative tile layer, which carries no attributes, but ` +
               `promotes "${l.featureIdProperty}"`).toBeUndefined();
        continue;
      }
      // A SECTION'S ID IS ITS MVT FEATURE ID ONLY (T202): the tile carries no `section_id` property, and the contract
      // says so with `mvtId` — the id the style names is the feature's own, which both renderers read as `feature.id`.
      if (t.mvtId && t.featureId === l.featureIdProperty) {
        expect(t.attrs, `${l.id}: an MVT-id layer must not also ship its id as a property`)
          .not.toContain(l.featureIdProperty);
        continue;
      }
      expect(t.attrs, `${l.id}.featureIdProperty`).toContain(l.featureIdProperty);
    }
  });

  it("nothing decorative is tappable, highlightable, or given an id", () => {
    // Three separate places have to agree and they are in three files. The route through a
    // lake was second in the tap order for months while its own comment said it must never
    // be tapped as though it were open water.
    for (const l of source.layers) {
      if (!contract.layers[l.sourceLayer]?.decorative) continue;
      expect(STYLE_META.featureIds[l.id], `${l.id} is decorative but promotes an id`)
        .toBeUndefined();
      expect(l.highlightable ?? false, `${l.id} is decorative but is highlightable`)
        .toBe(false);
    }
  });

  it("geometry types match", () => {
    for (const l of source.layers) {
      expect(contract.layers[l.sourceLayer]!.geometry, l.id).toBe(l.geometry);
    }
  });

  it("a provider either shares the layer's id, or declares how it bridges to it", () => {
    // Values arrive keyed one way and the geometry is addressed another. Either they match,
    // or the provider names a fanout — an undeclared gap means the colour never lands and
    // the map just looks unregulated.
    for (const l of source.layers) {
      for (const [modeName, mode] of Object.entries(l.colorModes)) {
        const provider = (mode as { data?: { provider?: string } }).data?.provider;
        if (!provider) continue;
        const p = source.providers[provider] as { key?: string; fanout?: string } | undefined;
        if (!p?.key) continue;
        const ok = p.key === l.featureIdProperty || Boolean(p.fanout);
        expect(ok, `${l.id}/${modeName} via ${provider}: key "${p.key}" vs feature id ` +
                   `"${l.featureIdProperty}" and no fanout declared`).toBe(true);
      }
    }
  });

  it("the generated style carries the same layer set as the authored source", () => {
    const built = new Set(MAP_STYLE.layers.map((l) => l.id));
    for (const l of source.layers) expect(built.has(l.id), l.id).toBe(true);

    // Plus one companion line layer per polygon that declares an outline. MapLibre fills
    // cannot be stroked, so an edge is a second layer — generated, never authored, so it
    // cannot drift from the fill it belongs to.
    const outlined = source.layers.filter((l) => l.outline);
    for (const l of outlined)
      expect(built.has(`${l.id}__edge`), `${l.id} declares an outline but has no edge layer`)
        .toBe(true);

    // And one companion SYMBOL layer per layer that declares a label — the water's own
    // name, drawn along the line or set inside the polygon. Same reason as the edge:
    // generated, so a name cannot end up over a different source-layer than its geometry.
    const labelled = source.layers.filter((l) => l.label);
    for (const l of labelled)
      expect(built.has(`${l.id}__label`), `${l.id} declares a label but has no symbol layer`)
        .toBe(true);
    // And one FLOW-ARROW symbol layer per line that declares arrows (the Conditions view's).
    const arrowed = source.layers.filter((l) => (l as { arrows?: unknown }).arrows);
    for (const l of arrowed)
      expect(built.has(`${l.id}__arrows`), `${l.id} declares arrows but has no arrow layer`).toBe(true);
    expect(arrowed.map((l) => l.id)).toEqual(["stream"]);
    expect(Object.keys(STYLE_META.colorModes).length)
      .toBe(source.layers.length + outlined.length + labelled.length + arrowed.length);
  });

  it("the layers that say where you may not go cannot be switched off", () => {
    /*
     * A CLOSURE IS NOT A PREFERENCE.
     *
     * `protected`, `access` and `admin` were all toggleable AND hidden by default, so the
     * map's normal state showed no national park, no ecological reserve, no Indian reserve,
     * no land the public may not enter — and no region or unit boundary, which is the line
     * that decides WHICH REGULATION TABLE applies. Every one of those is a reason a person
     * can be doing something they should not be, and a reader who never opens the layer
     * menu is exactly the reader who most needs to see them.
     *
     * `ownership` is deliberately the other way round — title is context, not an
     * instruction, and most of the province is Crown land — so it is checked here too, to
     * pin the asymmetry rather than leave it to whoever edits the file next.
     */
    const byId = new Map((source as unknown as { groups: { id: string; toggleable: boolean;
                          defaultVisible: boolean }[] }).groups.map((g) => [g.id, g]));
    for (const id of ["water", "protected", "access", "admin"]) {
      const g = byId.get(id)!;
      expect(g, `group "${id}" is gone`).toBeDefined();
      expect(g.toggleable, `group "${id}" may be switched off`).toBe(false);
      expect(g.defaultVisible, `group "${id}" is hidden by default`).toBe(true);
    }
    const own = byId.get("ownership")!;
    expect(own.toggleable).toBe(true);
    expect(own.defaultVisible).toBe(false);

    // and the generated style must actually paint them, not just declare them visible
    const drawn = new Map(MAP_STYLE.layers.map((l) => [l.id, l]));
    for (const l of source.layers) {
      const grp = (l as unknown as { group: string }).group;
      if (!["protected", "access", "admin"].includes(grp)) continue;
      const layer = drawn.get(l.id) as { layout?: { visibility?: string } } | undefined;
      expect(layer?.layout?.visibility, `${l.id} is not visible in the built style`)
        .toBe("visible");
    }
  });

  it("every label is drawn after every piece of geometry", () => {
    // Draw order IS array order, and it decides two things at once: what paints over what,
    // and which symbol layer wins a collision. A label emitted beside its own geometry sits
    // eighth of ten, so every river drawn afterwards crosses out the lake names underneath.
    const ids = MAP_STYLE.layers.map((l) => l.id);
    const firstLabel = ids.findIndex((id) => id.endsWith("__label"));
    expect(firstLabel, "no label layer in the built style").toBeGreaterThan(-1);
    // The flow arrows are symbols too, and come after EVERY name: a name placed first keeps
    // its spot, and an arrow that would land on it is the one dropped.
    const firstArrow = ids.findIndex((id) => id.endsWith("__arrows"));
    for (const id of ids.slice(firstLabel))
      expect(id.endsWith("__label") || id.endsWith("__arrows"), `${id} is drawn after a label`).toBe(true);
    expect(ids.slice(firstArrow).every((id) => id.endsWith("__arrows")), "an arrow is drawn before a name")
      .toBe(true);

    // And among themselves, in DECLARED priority order — not in whatever order the source
    // happens to list the geometry. A river name beats a lake name, as v1 had it.
    const prio = new Map(source.layers.filter((l) => l.label)
      .map((l) => [`${l.id}__label`, (l.label as { priority?: number }).priority ?? 0]));
    const drawn = ids.slice(firstLabel, firstArrow).map((id) => prio.get(id) ?? 0);
    expect(drawn, "labels are not in priority order")
      .toEqual([...drawn].sort((a, b) => a - b));
  });

  it("every generated edge draws over the same source-layer as its fill", () => {
    // An edge over the wrong source-layer is an outline of a different thing, and looks
    // entirely plausible until you notice the shoreline does not match the lake.
    const byId = new Map(MAP_STYLE.layers.map((l) => [l.id, l as Record<string, unknown>]));
    for (const l of source.layers.filter((x) => x.outline)) {
      const edge = byId.get(`${l.id}__edge`)!;
      expect(edge["source-layer"]).toBe(byId.get(l.id)!["source-layer"]);
      expect(edge.type, `${l.id}__edge must be a line`).toBe("line");
    }
  });

  it("no tile layer is silently unused", () => {
    // Not a failure — the pipeline may build a layer before the app draws it — but it
    // should be a deliberate, visible list rather than something nobody notices.
    //
    //   wma  built for regulation scopes; only Creston Valley ships (T202, owner 2026-10-27), and nothing in the app
    //        draws it yet. `sign_zone` (dashed closure ring) and `watershed` (the Liard wash) are drawn since T202.
    const drawn = new Set(source.layers.map((l) => l.sourceLayer));
    const undrawn = Object.keys(contract.layers).filter((n) => !drawn.has(n));
    expect(undrawn).toEqual(["wma"]);
  });
});


/**
 * THE CHECK THAT WAS MISSING.
 *
 * Everything above verifies that a feature id was NAMED. Nothing verified that the runtime
 * USES it — and it did not: the composed style had no `promoteId`, so MapLibre never lifted
 * the property into `feature.id`. `setFeatureState` addressed nothing and a tap returned an
 * undefined id, so the map could not be coloured and could not be tapped. Every test here
 * passed the whole time, because they all asked the same question of the same two files.
 *
 * These assertions go to the composed style — the object the renderer is actually handed.
 */
describe("the runtime uses the feature id, not just names it", () => {
  const style = runtimeStyle(
    { atlas: "https://example.invalid/atlas.pmtiles",
      basemap: "https://example.invalid/basemap.pmtiles" },
    "light",
  ) as unknown as {
    sources: Record<string, { promoteId?: Record<string, string> }>;
    layers: { id: string; "source-layer"?: string; source?: string }[];
  };

  /*
   * THE ATLAS PROMOTES NOTHING (T202, owner 2026-10-27). A section's id is its MVT feature id
   * (`pipeline/tests/test_tile_mvt_ids.py`), and the tiles no longer repeat it as a `section_id` property. An OBJECT
   * `promoteId` is applied by GL JS to EVERY source-layer of the source — one not listed reads `properties[undefined]`
   * and loses its MVT id — so promoting the area layers alone left every stream with `id: undefined`: no colour, no tap.
   * The area layers on the atlas are static (no feature-state), as on the phone, which never had a promoteId.
   */
  it("the atlas source carries no promoteId, so every section keeps its MVT id", () => {
    expect(style.sources.atlas?.promoteId).toBeUndefined();
    for (const [layerId, prop] of Object.entries(STYLE_META.featureIds)) {
      const layer = MAP_STYLE.layers.find((l) => l.id === layerId) as { source?: string } | undefined;
      if (layer?.source !== "atlas" || prop === "section_id") continue;
      // an area layer on the atlas may name an id (its values reach the style), but nothing may key feature-state on it
      const modes = Object.values(STYLE_META.colorModes[layerId] ?? {}) as { scale?: string }[];
      expect(modes.every((m) => m.scale === "static"), `${layerId} needs feature-state but its id is not promoted`)
        .toBe(true);
    }
  });

  it("the sections' id is the MVT id, on exactly these source-layers", () => {
    const mvt = Object.entries(contract.layers)
      .filter(([, l]) => (l as { mvtId?: boolean }).mvtId).map(([n]) => n).sort();
    expect(mvt).toEqual(["lake", "stream", "wetland"]);
    for (const n of mvt) {
      expect(contract.layers[n]!.featureId, n).toBe("section_id");
      expect(contract.layers[n]!.attrs, n).not.toContain("section_id");
    }
  });

  it("promotes by SOURCE-LAYER, which is what MapLibre keys promoteId on", () => {
    // Keying by our layer id instead is the quiet version of the same failure: the object
    // is present, looks right in a diff, and matches nothing at runtime.
    const promote = style.sources.atlas?.promoteId ?? {};
    const sourceLayers = new Set(MAP_STYLE.layers.map(
      (l) => (l as { "source-layer"?: string })["source-layer"]).filter(Boolean));
    for (const key of Object.keys(promote))
      expect(sourceLayers, `promoteId key "${key}" is not a source-layer`).toContain(key);
  });

  it("promotes nothing the tiles do not carry", () => {
    const promote = style.sources.atlas?.promoteId ?? {};
    for (const [sourceLayer, prop] of Object.entries(promote))
      expect(contract.layers[sourceLayer]?.attrs,
             `${sourceLayer} has no attribute "${prop}"`).toContain(prop);
  });
});


/**
 * The gauge dots are the one thing on this map with NO tile behind them.
 *
 * Every water is a tile feature carrying the minzoom tippecanoe stamped on it, so the atlas
 * thins out on its own. The gauges are a GeoJSON source refreshed every half hour, and a
 * GeoJSON source carries no ladder — so the app has to apply the same one, and "the same
 * one" is a copy, and a copy is a thing that drifts. It drifted the only way it could: not
 * at all at first, and silently later.
 */
describe("the gauge dots climb the same ladder as the water", () => {
  it("the app's ladder IS the pipeline's, stop for stop", () => {
    expect(MAGNITUDE_LADDER.map((p) => [...p])).toEqual(contract.magnitudeLadder);
  });

  it("a dot appears at the zoom its own river appears at", () => {
    // The Fraser at Hope (magnitude 273,576) is on screen from z4; a creek gauge of
    // magnitude 12 waits until z11, by which time its creek is drawn too.
    expect(zoomForMagnitude(273576)).toBe(4);
    expect(zoomForMagnitude(12)).toBe(11);
    // No magnitude is not a small magnitude — it goes to the bottom, never hidden.
    expect(zoomForMagnitude(null)).toBe(13);   // the bottom rung is z13 since T85
  });
});

describe("the gauges draw on top of the water", () => {
  const style = runtimeStyle(
    { atlas: "https://example.invalid/atlas.pmtiles",
      basemap: "https://example.invalid/basemap.pmtiles",
      gauges: "https://example.invalid/gauges.geojson" },
    "light",
  ) as unknown as { layers: ({ id: string; filter?: unknown } & Record<string, unknown>)[] };
  const at = (id: string) => style.layers.findIndex((l) => l.id === id);

  it("puts every gauge layer after every atlas layer", () => {
    // They were written first and therefore drawn UNDERNEATH, while the comment above them
    // said "above the water". A dot under a river line is a dot you cannot see or tap.
    const lastAtlas = Math.max(...MAP_STYLE.layers.map((l) => at(l.id)));
    expect(at("gauge-dot")).toBeGreaterThan(lastAtlas);
    expect(at("gauge-label")).toBeGreaterThan(at("gauge-dot"));
  });

  it("filters both on the ladder, so a dot cannot outlive its river", () => {
    for (const id of ["gauge-dot", "gauge-label"]) {
      const f = JSON.stringify(style.layers[at(id)]!.filter);
      expect(f, `${id} draws at every zoom`).toContain("minz");
      expect(f, `${id} ignores the camera`).toContain("zoom");
    }
  });

  it("hands the low zooms to the field and the high ones to the dots, with no gap", () => {
    /*
     * Two answers to one question on screen at once is one failure; a zoom with NEITHER
     * answer is the other. The basin field carries the regional picture where individual
     * rivers have thinned out of the atlas; the dots take over where they are back.
     *
     * THE FIELD REPLACED A GLOW, which replaced a blurred disc per station. The disc was
     * drawn over LAND — country no gauge speaks for — and its overlaps blended into colours
     * that were not on the scale at all. The glow fixed the first by painting along the
     * water and kept the second problem in a smaller form: a blurred line is a claim about
     * a corridor of arbitrary width, and it was drawn in EVERY view, so the Regulations map
     * was lit by flow it showed no legend for. The field is the shape a percentile is
     * actually a claim about, and it belongs to one view.
     */
    const field = style.layers[at("basin")] as
      { maxzoom?: number; type?: string; source?: string; "source-layer"?: string };
    const dot = style.layers[at("gauge-dot")] as { minzoom?: number };
    expect(field.type).toBe("fill");
    // its own source on the atlas archive, which overzooms the tiles' last basin zoom up to the handover
    expect(field.source).toBe("basins");
    expect(field["source-layer"]).toBe("basin");
    expect(field.maxzoom).toBeDefined();
    expect(dot.minzoom).toBeDefined();
    expect(dot.minzoom!).toBeLessThanOrEqual(field.maxzoom! + 1);

    // and it is UNDER the water it explains, or the rivers disappear into it
    expect(at("basin")).toBeLessThan(at("stream"));
    expect(at("basin")).toBeLessThan(at("lake"));
  });

  it("has no glow left to light the province twice", () => {
    // Same question, same band, same ramp as the field — and on every view, including the
    // one with no flow legend on it.
    expect(style.layers.some((l: { id: string }) => l.id === "stream-glow")).toBe(false);
  });

  it("no layer claims a condition for country that has no water in it", () => {
    // The rule the haze broke. Anything painted from the flow ramp must be drawn over the
    // atlas's own water, never over the ground between two stations.
    const ramp = JSON.stringify(STYLE_META.colorModes.stream?.standing ?? {});
    expect(ramp).toContain("flow.");
    for (const l of style.layers as { id: string; source?: string; paint?: unknown }[]) {
      const paint = JSON.stringify(l.paint ?? {});
      if (!paint.includes("flow.f")) continue;
      expect(["atlas", "gauges"], `${l.id} paints a flow colour from "${l.source}"`)
        .toContain(l.source);
    }
  });

  it("draws the route arrows above the water and below the gauges", () => {
    // They annotate the highlighted reach, so they cannot be under it; and they must not
    // sit over a reading, which is the one thing on the map that carries a number.
    expect(at("route-arrows")).toBeGreaterThan(
      Math.max(...MAP_STYLE.layers.map((l) => at(l.id))));
    expect(at("route-arrows")).toBeLessThan(at("gauge-dot"));
  });
});
