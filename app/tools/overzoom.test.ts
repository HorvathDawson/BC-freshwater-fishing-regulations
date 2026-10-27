/**
 * OVERZOOM, ACROSS THE SEAM (owner 2026-10-21): "allow over zooming (more than the pmtile) where it just keeps the
 * resolution of the lower zoom."
 *
 * The camera goes to `CAMERA_MAX_ZOOM` (z18); every SOURCE stops at its own archive's deepest zoom and MapLibre
 * overzooms from there. Three things break that, all silently, and this file pins each:
 *
 *   a source's maxzoom raised to the camera's — it asks for z15-18 tiles that do not exist: a BLANK map, which reads
 *     exactly like water with no rules on it. A tiled source with no maxzoom at all defaults to 22: the same, plus a
 *     404 per tile for a raster.
 *   a source's maxzoom BELOW its archive's — it throws away detail the archive holds. So each number is held to what
 *     the pipeline writes (its constants and, with TILES_DIR, the archive headers themselves).
 *   a layer that stops drawing past z14, or a size that shrinks — the closer view loses what the z14 view showed.
 */
import { describe, expect, it } from "vitest";
import { existsSync, openSync, readSync, closeSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { ATLAS_MAX_ZOOM, BASEMAP_MAX_ZOOM, CAMERA_MAX_ZOOM, FIT_MAX_ZOOM } from "@app/core";
import { STYLE_META, runtimeStyle } from "@app/map";
import { nativeStyle } from "../packages/map/src/native-style";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");
const pyConst = (file: string, name: string) =>
  Number(new RegExp(`^${name}\\s*=\\s*(\\d+)`, "m").exec(read(file))?.[1]);

const AT = {
  atlas: "https://example.invalid/atlas.pmtiles",
  basemap: "https://example.invalid/basemap.pmtiles",
  outside: "https://example.invalid/outside.geojson",
  gauges: '{"type":"FeatureCollection","features":[]}',
};

type Src = { type: string; maxzoom?: number };
type Layer = { id: string; type: string; source?: string; minzoom?: number; maxzoom?: number;
               paint?: Record<string, unknown>; layout?: Record<string, unknown> };
const THEMES = Object.keys(STYLE_META.themes);

describe("each source stops where its archive stops", () => {
  it("the atlas and basemap ceilings are the pipeline's own numbers", () => {
    const contract = JSON.parse(read("pipeline/deliver/tiles/tile-contract.json")) as
      { layers: Record<string, { maxzoom: number }> };
    expect({
      atlas: ATLAS_MAX_ZOOM,
      contract: Math.max(...Object.values(contract.layers).map((l) => l.maxzoom)),
      ladder: pyConst("pipeline/deliver/tiles/ladder.py", "MAX_ZOOM"),
      basemap: BASEMAP_MAX_ZOOM,
      basemapBuild: pyConst("pipeline/deliver/tiles/basemap.py", "MAX_ZOOM"),
    }).toEqual({ atlas: 13, contract: 13, ladder: 13, basemap: 13, basemapBuild: 13 });
  });

  for (const theme of THEMES)
    it(`every tiled source declares a maxzoom short of the camera's, on both maps (${theme})`, () => {
      const web = runtimeStyle(AT, theme).sources as Record<string, Src>;
      const phone = nativeStyle({ at: AT, theme, view: "plain", modes: {}, basemap: "satellite",
                                  gauges: AT.gauges }).style.sources as Record<string, Src>;
      const tiled = (s: Record<string, Src>) => Object.fromEntries(Object.entries(s)
        .filter(([, v]) => v.type === "vector" || v.type === "raster")
        .map(([k, v]) => [k, v.maxzoom ?? "none"]));
      const imagery = (JSON.parse(read("app/packages/map/style/layers.source.json")) as
        { sources: Record<string, Src> }).sources.imagery.maxzoom;
      // THE FIELD'S SOURCE stops where the archive's BASINS stop (`basin` maxzoom = BASIN_HANDOVER_Z - 1 in the tile
      // builder), so MapLibre overzooms the last tile that carries them up to the field's handover rather than asking for
      // deeper tiles with no basins in them — a field that vanished a zoom early (refinement pass, 2026-10-22).
      const basins = pyConst("pipeline/deliver/tiles/layers.py", "BASIN_HANDOVER_Z") - 1;
      const want = { atlas: ATLAS_MAX_ZOOM, basemap: BASEMAP_MAX_ZOOM, basins, imagery };
      expect({ web: tiled(web), phone: tiled(phone) }).toEqual({ web: want, phone: want });
      // OUR ARCHIVES stop short of the camera. The satellite imagery is Esri's live service (owner 2026-10-21), not an
      // archive of ours: its maxzoom is Esri's (z19; its tiles are 256 px, so the z18 camera asks for z19 tiles —
      // measured in the web check, handoff/PLAN/app-data/satellite/esri/results.json), and where it has none the source's `blankTile=false` answers 404 and MapLibre overzooms the parent
      // (tools/imagery.test.ts pins the URL).
      expect({ atlas: want.atlas < CAMERA_MAX_ZOOM, basemap: want.basemap < CAMERA_MAX_ZOOM,
               basins: basins < STYLE_META.field!.handover, imagery: imagery >= CAMERA_MAX_ZOOM })
        .toEqual({ atlas: true, basemap: true, basins: true, imagery: true });
    });

  it("a fit stops at z14, one past the atlas's z13 (T85 kept the fits); the camera goes five past the atlas", () => {
    expect({ fit: FIT_MAX_ZOOM - ATLAS_MAX_ZOOM, camera: CAMERA_MAX_ZOOM - ATLAS_MAX_ZOOM }).toEqual({ fit: 1, camera: 5 });
  });
});

/** The archive's own header: PMTiles v3, min_zoom at byte 100, max_zoom at 101. */
function header(path: string): { magic: string; min: number; max: number } {
  const b = Buffer.alloc(127);
  const fd = openSync(path, "r");
  try { readSync(fd, b, 0, 127, 0); } finally { closeSync(fd); }
  return { magic: b.subarray(0, 7).toString("latin1"), min: b[100], max: b[101] };
}
const TILES = process.env.TILES_DIR ? resolve(ROOT, process.env.TILES_DIR) : null;
describe.skipIf(!TILES || !existsSync(TILES!))("the live archives' headers (Tier 2: TILES_DIR)", () => {
  it("atlas.pmtiles and basemap.pmtiles end at the zooms the sources declare", () => {
    expect({ atlas: header(join(TILES!, "atlas.pmtiles")), basemap: header(join(TILES!, "basemap.pmtiles")) })
      .toEqual({ atlas: { magic: "PMTiles", min: 4, max: ATLAS_MAX_ZOOM },
                 basemap: { magic: "PMTiles", min: 0, max: BASEMAP_MAX_ZOOM } });
  });
});

describe("what z14 draws, z15-18 draws too", () => {
  for (const theme of THEMES)
    it(`no layer drawn at z14 stops before the camera's ceiling (${theme})`, () => {
      const layers = runtimeStyle(AT, theme).layers as Layer[];
      const cut = layers.filter((l) => (l.minzoom ?? 0) <= ATLAS_MAX_ZOOM && l.maxzoom !== undefined
                                       && l.maxzoom > ATLAS_MAX_ZOOM && l.maxzoom <= CAMERA_MAX_ZOOM)
        .map((l) => `${l.id} maxzoom ${l.maxzoom}`);
      expect(cut).toEqual([]);
    });

  it("none of OUR layers waits for a zoom past the tiles (the basemap's own styling may)", () => {
    const ours = (runtimeStyle(AT, "light").layers as Layer[]).filter((l) => l.source !== "basemap");
    expect(ours.filter((l) => (l.minzoom ?? 0) > ATLAS_MAX_ZOOM).map((l) => l.id)).toEqual([]);
  });

  /**
   * EVERY ZOOM-DEPENDENT SIZE OF OURS, at z14 and at each overzoom: defined (MapLibre holds an interpolate's last stop
   * past it, so a curve ending at z14 is defined to z18 and keeps the z14 width — "the resolution of the lower zoom")
   * and never smaller than at z14, so a river, a label or a gauge dot does not thin out as the reader closes in.
   */
  const SIZES = ["line-width", "line-gap-width", "text-size", "icon-size", "circle-radius", "circle-stroke-width",
                 "fill-extrusion-height"];
  const isZoomCurve = (v: unknown): boolean => JSON.stringify(v ?? null).includes('["zoom"]');
  const specOf = (layerType: string, prop: string) =>
    ((latest as unknown as Record<string, Record<string, unknown>>)[`paint_${layerType}`]?.[prop]
     ?? (latest as unknown as Record<string, Record<string, unknown>>)[`layout_${layerType}`]?.[prop]);
  const FEATURE = { type: 2, properties: { magnitude: 120, status: "open", name: "Vedder River" } };
  const STATE = { status: "open", selected: true };

  for (const theme of THEMES)
    it(`sizes are defined to z${CAMERA_MAX_ZOOM} and none shrinks past z14 (${theme})`, () => {
      const layers = (runtimeStyle(AT, theme).layers as Layer[]).filter((l) => l.source !== "basemap");
      const bad: string[] = [];
      let checked = 0;
      for (const l of layers)
        for (const [prop, v] of Object.entries({ ...l.paint, ...l.layout })) {
          if (!SIZES.includes(prop) || !isZoomCurve(v)) continue;
          const spec = specOf(l.type, prop);
          if (!spec) { bad.push(`${l.id} ${prop}: not a ${l.type} property`); continue; }
          const parsed = createPropertyExpression(v, prop, spec as never);
          if (parsed.result !== "success") { bad.push(`${l.id} ${prop}: does not parse`); continue; }
          const at = (zoom: number) => parsed.value.evaluate({ zoom } as never, FEATURE as never, STATE as never);
          const base = at(ATLAS_MAX_ZOOM) as number;
          checked += 1;
          for (let z = ATLAS_MAX_ZOOM + 1; z <= CAMERA_MAX_ZOOM; z++) {
            const got = at(z) as number;
            if (typeof got !== "number" || !Number.isFinite(got)) bad.push(`${l.id} ${prop} z${z}: ${String(got)}`);
            else if (got < base) bad.push(`${l.id} ${prop} z${z}: ${got} < z14's ${base}`);
          }
        }
      // a guard on the guard: the river, the lake edge and the labels at least
      expect(checked).toBeGreaterThan(10);
      expect(bad).toEqual([]);
    });
});
