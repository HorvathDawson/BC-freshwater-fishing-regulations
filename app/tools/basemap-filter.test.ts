/**
 * THE BASEMAP SHIPS ONLY WHAT THE STYLE DRAWS (T85, decisions D32) — and nothing the style draws is cut.
 *
 * `pipeline/deliver/tiles/basemap.py` filters the Protomaps `landuse` and `water` layers down to the kinds the app's
 * style draws (`LANDUSE_DRAWN`, `WATER_LINES_KEPT`): 26 MB of undrawn landuse and 242 MB of OSM stream lines. That list
 * is a claim about THIS style, so it is held to it here: every landuse kind and every water-line kind a basemap layer
 * of the composed style can draw must be one the filter keeps (or one it drops on purpose, `WATER_LINES_DROPPED`). A
 * Protomaps upgrade that starts drawing a new kind fails here, before a basemap build ships ground without it.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { STYLE_META, runtimeStyle } from "@app/map";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const SRC = readFileSync(join(ROOT, "pipeline/deliver/tiles/basemap.py"), "utf8");
/** A Python tuple of strings, `NAME = ("a", "b", ...)`, possibly over several lines. */
const pyTuple = (name: string): string[] => {
  const m = new RegExp(`^${name}\\s*=\\s*\\(([^)]*)\\)`, "m").exec(SRC);
  if (!m) throw new Error(`basemap.py declares no ${name}`);
  return [...m[1].matchAll(/"([^"]+)"/g)].map((x) => x[1]);
};

const AT = {
  atlas: "https://example.invalid/atlas.pmtiles",
  basemap: "https://example.invalid/basemap.pmtiles",
  outside: "https://example.invalid/outside.geojson",
  gauges: '{"type":"FeatureCollection","features":[]}',
};
type Layer = { id: string; type: string; source?: string; "source-layer"?: string; filter?: unknown };

/** The kind values a filter can match: every string literal that is not an operator, a key or a geometry type. */
const NOT_A_KIND = new Set(["in", "==", "!=", "any", "all", "!", "none", "!in", "kind", "$type", "get", "literal",
                            "match", "Polygon", "LineString", "Point"]);
const kinds = (f: unknown): string[] =>
  Array.isArray(f) ? f.flatMap(kinds) : typeof f === "string" && !NOT_A_KIND.has(f) ? [f] : [];

describe("the basemap filter keeps every kind the style draws", () => {
  for (const theme of Object.keys(STYLE_META.themes)) {
    const layers = (runtimeStyle(AT, theme).layers as Layer[]).filter((l) => l.source === "basemap");

    it(`landuse (${theme})`, () => {
      const keep = new Set([...pyTuple("LANDUSE_DRAWN"), ...pyTuple("LANDUSE_STANDIN")]);
      const drawn = layers.filter((l) => l["source-layer"] === "landuse" && l.type !== "symbol");
      expect(drawn.length).toBeGreaterThan(5);
      expect([...new Set(drawn.flatMap((l) => kinds(l.filter)))].filter((k) => !keep.has(k))).toEqual([]);
    });

    it(`water lines (${theme})`, () => {
      const keep = new Set([...pyTuple("WATER_LINES_KEPT"), ...pyTuple("WATER_LINES_DROPPED")]);
      const lines = layers.filter((l) => l["source-layer"] === "water" && l.type === "line");
      expect(lines.length).toBeGreaterThan(0);
      expect([...new Set(lines.flatMap((l) => kinds(l.filter)))].filter((k) => !keep.has(k))).toEqual([]);
    });
  }

  // T202: the build also drops lake/river label points, earth lines and `sort_rank` — none of which a layer may read.
  for (const theme of Object.keys(STYLE_META.themes)) {
    const layers = (runtimeStyle(AT, theme).layers as Layer[]).filter((l) => l.source === "basemap");

    it(`no drawn water label reads a dropped point kind, and nothing draws earth lines (${theme})`, () => {
      const dropped = new Set(pyTuple("WATER_POINTS_DROPPED"));
      const labels = layers.filter((l) => l["source-layer"] === "water" && l.type === "symbol");
      expect(labels.length).toBeGreaterThan(0);              // the sea's names stay
      expect([...new Set(labels.flatMap((l) => kinds(l.filter)))].filter((k) => dropped.has(k))).toEqual([]);
      expect(layers.filter((l) => l["source-layer"] === "earth" && l.type === "line").map((l) => l.id)).toEqual([]);
    });

    it(`no basemap layer reads a key the build drops (${theme})`, () => {
      const unread = pyTuple("UNREAD_KEYS");
      expect(unread).toContain("sort_rank");
      expect(layers.filter((l) => unread.some((k) => JSON.stringify(l).includes(`"${k}"`))).map((l) => l.id))
        .toEqual([]);
    });
  }

  it("the parks and military the app draws itself are not kept for the basemap", () => {
    const keep = new Set(pyTuple("LANDUSE_DRAWN"));
    expect(["national_park", "park", "protected_area", "nature_reserve", "military", "naval_base"]
      .filter((k) => keep.has(k))).toEqual([]);
  });
});
