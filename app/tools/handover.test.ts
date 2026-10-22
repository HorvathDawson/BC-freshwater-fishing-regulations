/**
 * ONE ZOOM, HELD EQUAL BY THIS TEST.
 *
 * Below the handover the Conditions map is a field of catchments; above it, rivers. The
 * places that have to agree about where that line is:
 *
 *   the STYLE         ends the field there and stands the rivers down below it
 *                     (the basin layer's maxzoom, `minzoomByMode`, `field.handover`)
 *   the APP           refuses taps below it                   (HANDOVER_Z)
 *   the TILE BUILDER  writes basin polygons up to a zoom AT OR BELOW it (BASIN_HANDOVER_Z - 1),
 *                     and the field's own source stops exactly there, so MapLibre overzooms
 *                     the last tile that carries basins up to the handover. z10 since the
 *                     refinement pass (owner 2026-10-22: the watershed colouring "at a slightly
 *                     higher zoom"); the tiles still stop at z8.
 *
 * Disagree and the map has a zoom where nothing is drawn, or one where two answers are
 * drawn over each other and the smaller wins by being on top. Neither failure throws.
 */
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { STYLE_META, baseAdapter, resolveTheme, type MapHandle } from "@app/map";
import { paintFor } from "../packages/map/src/adapters/contract";
// The style itself is not exported — it is the generated artifact, and reading
// it here is reading what actually ships rather than a re-export of it.
const MAP_STYLE = JSON.parse(
  readFileSync(new URL("../packages/map/style/style.json", import.meta.url)
    .pathname, "utf8")) as { layers: { id: string; maxzoom?: number }[]; sources: Record<string, unknown> };

const ROOT = new URL("../../", import.meta.url).pathname;
const PY = `${ROOT}.venv/bin/python`;

/** What the tile builder thinks, read from the tile builder. */
function fromPipeline(): { handover: number; basinMax: number; basinMin: number; simplifyM: number } {
  const script = `
import json, sys
sys.path.insert(0, ${JSON.stringify(ROOT)})
from pipeline.deliver.tiles.layers import BASIN_HANDOVER_Z, BY_NAME
from pipeline.deliver.tiles.basins import SIMPLIFY_M
b = BY_NAME["basin"]
print(json.dumps({"handover": BASIN_HANDOVER_Z, "basinMax": b.maxzoom,
                  "basinMin": b.minzoom, "simplifyM": SIMPLIFY_M}))
`;
  return JSON.parse(execFileSync(PY, ["-c", script], { encoding: "utf8" }));
}

/** What the app thinks, read from the app — the constant, not a copy of it. */
function fromShell(): number {
  const src = readFileSync(`${ROOT}app/packages/ui-native/src/Shell.tsx`, "utf8");
  const m = src.match(/const HANDOVER_Z = (\d+);/);
  if (!m) throw new Error("HANDOVER_Z not found in Shell.tsx");
  return Number(m[1]);
}

const has = existsSync(PY);

describe("the basin/river handover", () => {
  it("stands the rivers down while they are coloured by flow, and not otherwise", () => {
    /*
     * KEYED ON THE MODE, NOT THE VIEW. The app renders one view and sets each layer's
     * colour mode, so a rule keyed on "the conditions view" is a rule that never fires —
     * which is exactly what happened: the field was built, the tiles carried it, and the
     * rivers stayed drawn over the top of it at every zoom.
     */
    expect(STYLE_META.minzoomByMode.stream).toEqual({ standing: 10 });
    expect(STYLE_META.minzoomByMode.lake).toEqual({ standing: 10 });
    // Under closure or plain they keep the style's own floor — switching to Regulations at
    // z6 must not show a province with no rivers on it.
    expect(STYLE_META.minzoomByMode.stream!.closure).toBeUndefined();
    expect(STYLE_META.minzoomByMode.stream!.plain).toBeUndefined();
  });

  it("draws the field in the Conditions view and hides it in every other", () => {
    for (const v of STYLE_META.views)
      expect(v.modes.basin).toBe(v.id === "conditions" ? "standing" : "plain");
  });

  it("never lets the field be tapped", () => {
    // It is a region, not a reach. A sheet for it would answer a question about one river
    // with a number about a valley.
    expect(STYLE_META.highlightable.some((h) => h.id === "basin")).toBe(false);
  });

  it("colours the field from the same ramp as the rivers", () => {
    // A second legend for one scale is how a reader learns to distrust both.
    type Stop = [number, unknown];
    const river = (STYLE_META.colorModes.stream?.standing as { stops?: Stop[] }).stops ?? [];
    const field = (STYLE_META.colorModes.basin?.standing as { stops?: Stop[] }).stops ?? [];
    // Every REAL stop, identically. The field drops the -1 sentinel and keeps nothing else
    // to itself.
    expect(field).toEqual(river.filter(([at]) => at >= 0));
  });

  it("keeps the no-record sentinel off the field", () => {
    /*
     * On a reach, "a gauge reports here and has no record to rank it against" is worth
     * drawing — you can tap it and be told. On a 3,600 km2 watershed group it is a purple
     * blotch the size of a valley that a reader cannot interrogate, and it reads as chaos
     * across the province. Such a group falls through to the unmeasured grey, which is what
     * a group with no gauge gets: at a province on screen, "we cannot say" is one answer.
     */
    const field = (STYLE_META.colorModes.basin?.standing as
                   { stops?: [number, unknown][] }).stops ?? [];
    expect(field.some(([at]) => at < 0)).toBe(false);
    const river = (STYLE_META.colorModes.stream?.standing as
                   { stops?: [number, unknown][] }).stops ?? [];
    expect(river.some(([at]) => at < 0)).toBe(true);
  });

  it("stops the field exactly where the rivers start", () => {
    const basin = MAP_STYLE.layers.find((l) => l.id === "basin")!;
    const app = fromShell();
    /*
     * MAXZOOM IS EXCLUSIVE. This asserted `app - 1`, under a comment reading "maxzoom is
     * inclusive of the zoom BELOW the handover" — and so the test agreed with the bug and
     * reported it fixed. MapLibre hides a layer at zoom >= maxzoom, so `maxzoom: 8` against
     * a river minzoom of 9 left the whole [8, 9) band drawing NOTHING: zoom out of
     * Conditions and the rivers vanished a full zoom level before the field appeared.
     *
     * The two numbers are the SAME number. The field draws below the handover, the rivers
     * from it, and there is no zoom that is both or neither.
     */
    expect(basin.maxzoom).toBe(app);
    expect(STYLE_META.field!.handover).toBe(app);
    expect(STYLE_META.minzoomByMode.stream!.standing).toBe(app);
  });

  it("leaves no zoom drawing neither the field nor the rivers", () => {
    // The property the numbers above exist for, stated directly and checked across the
    // band rather than inferred from two constants that were once off by one.
    const basin = MAP_STYLE.layers.find((l) => l.id === "basin")!;
    const riversFrom = STYLE_META.minzoomByMode.stream!.standing!;
    for (let z = 4; z <= 14; z += 0.5) {
      const field = z < (basin.maxzoom ?? 24) && z >= (basin.minzoom ?? 0);
      const rivers = z >= riversFrom;
      expect(field || rivers, `z${z} in Conditions draws neither`).toBe(true);
    }
  });

  it.skipIf(!has)("agrees with the tile builder, which decides what exists at all", () => {
    const py = fromPipeline();
    const sources = MAP_STYLE.sources as Record<string, { url?: string; maxzoom?: number }>;
    const basin = MAP_STYLE.layers.find((l) => l.id === "basin") as { source?: string };
    // THE FIELD'S SOURCE STOPS WHERE THE BASINS STOP: one zoom deeper and MapLibre asks the archive for tiles with no
    // basins in them, and the field vanishes before the rivers come back.
    expect(sources[basin.source!]).toMatchObject({ url: "pmtiles://atlas", maxzoom: py.basinMax });
    expect(py.basinMax).toBe(py.handover - 1);
    // and the handover is at or past the tiles' own: overzooming carries the field forward, never back
    expect(fromShell()).toBeGreaterThanOrEqual(py.handover);
    // And the field must reach the zoom the map opens at its widest.
    expect(py.basinMin).toBeLessThanOrEqual(4);
    /*
     * AND ITS EDGES ARE FINE ENOUGH FOR THE DEEPEST ZOOM IT IS DRAWN AT. The basins were simplified at 500 m — one pixel
     * at z8, the old handover — and overzoomed to z10 that is 5 px straight runs: the sawtooth edge on the right of
     * after-conditions-z9 (review 2026-10-22). One pixel at the handover, at 49 N.
     */
    const pxAtHandover = (156543.03 * Math.cos((49 * Math.PI) / 180)) / 2 ** fromShell();
    expect(py.simplifyM).toBeLessThanOrEqual(pxAtHandover);
  });

  /** A handle that records each layer's floor and visibility as the adapter sets them. */
  const recorder = () => {
    const floor: Record<string, number> = {};
    const shown: Record<string, boolean> = {};
    const h: MapHandle = {
      setVisibility: (id, v) => { shown[id] = v; },
      setPaint: () => {},
      setFeatureState: () => {},
      clearFeatureStates: () => {},
      setZoomRange: (id, z) => { floor[id] = z; },
    };
    return { h, floor, shown };
  };

  it("draws nothing under the field: every water layer, name, outline and dash starts where it stops", () => {
    // owner 2026-10-22: "make it so the lakes and streams etc disappear when it is visible". Under the field before:
    // the lake outlines, the lake names from z8, the no-baseline dashes and the basemap's rivers.
    const a = baseAdapter("web");
    const { h, floor, shown } = recorder();
    a.setLayerMode(h, "stream", "standing", "light");
    a.setLayerMode(h, "lake", "standing", "light");
    a.setLayerMode(h, "basin", "standing", "light");
    expect(shown.basin).toBe(true);
    expect(floor).toEqual({
      stream: 10, "selection-casing": 10, stream__arrows: 10, lake: 10, lake__edge: 10,
      "stream-nobaseline": 10, water_river: 10, stream__label: 11, lake__label: 10, lake_flowing: 10,
      wetland: 10, wetland_flowing: 10, under_lake: 10, contour: 10,
      // and the areas and the gauges (review 2026-10-22: hatches at Liumchen and gauge dots drawn on the field)
      indigenous_land: 10, indigenous_land__edge: 11, indigenous_land__label: 10,
      no_access: 10, no_access__edge: 10, no_access__label: 10,
      no_access_permit: 10, no_access_permit__edge: 10, no_access_permit__label: 10,
      eco_reserve: 10, eco_reserve__edge: 10, eco_reserve__label: 10,
      park: 10, park__label: 10, park_closed: 10, park_closed__edge: 10, park_closed__label: 10,
      "gauge-dot": 10, "gauge-label": 10,
    });
  });

  it("releases every floor with the field, and does not draw it in plain", () => {
    // `plain` is the Map tab and the temperature view: the basin layer's plain colour was a #D5D5D5 wash at 0.55 over
    // the whole province below z9 on the Map tab. Not drawn, and not fetched.
    const a = baseAdapter("web");
    const { h, floor, shown } = recorder();
    a.setLayerMode(h, "basin", "standing", "light");
    a.setLayerMode(h, "stream", "plain", "light");
    a.setLayerMode(h, "basin", "plain", "light");
    expect(shown.basin).toBe(false);
    expect(shown.basin__edge).toBe(false);
    expect(floor).toMatchObject({ stream: 0, "stream-nobaseline": 0, water_river: 9, lake__label: 8, wetland: 0,
                                  no_access__label: 9, park_closed__label: 7, "gauge-dot": 7, "gauge-label": 0 });
  });
});

/**
 * THE FIELD IS ONE SOLID SHEET (review 2026-10-22). The basins are simplified one polygon at a time in the tile build, so
 * neighbours do not share an edge: 3,552 slivers open between them and 3,694 overlap, up to ~700 m across
 * (measured on data/generated/tiles/layers/basin.geojsonl). Overzoomed from z8 to z10 those are white and black wedges a
 * valley long, and with the field at 0.55 every overlap was a darker seam and the basemap's lakes and the Fraser showed
 * through it. So the field is painted OPAQUE, each colour pre-mixed on the paper at `opacity.basin` (the same colour the
 * translucent field had on blank paper), and a seam of the basin's own colour is drawn UNDER it, wide enough to close the
 * widest gap at every zoom it is drawn.
 */
describe("the field is one solid sheet", () => {
  const STYLE_LAYERS = MAP_STYLE.layers as { id: string; type?: string }[];
  /** `a` laid over `b` at `alpha`, as the renderer blends: per channel, in sRGB. */
  const over = (a: string, b: string, alpha: number) => {
    const p = (h: string) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16));
    const [x, y] = [p(a), p(b)];
    return "#" + x.map((v, i) => Math.round(v * alpha + y[i]! * (1 - alpha)).toString(16).padStart(2, "0")).join("")
      .toUpperCase();
  };
  /** Every hex colour in an expression, in order. */
  const colours = (e: unknown): string[] =>
    Array.isArray(e) ? e.flatMap(colours) : typeof e === "string" && /^#[0-9a-f]{6}$/i.test(e) ? [e.toUpperCase()] : [];
  /** Metres per CSS pixel at zoom z, at the latitude of the southern interior (49 N). */
  const mpp = (z: number) => (156543.03 * Math.cos((49 * Math.PI) / 180)) / 2 ** z;
  /** The widest sliver between two simplified basins (`basin.geojsonl`, maximum inscribed circle). */
  const WIDEST_GAP_M = 700;
  /** A zoom curve's value at z (linear between stops, clamped). */
  const at = (curve: unknown, z: number): number => {
    if (typeof curve === "number") return curve;
    const c = curve as unknown[];
    const stops: [number, number][] = [];
    for (let i = 3; i < c.length; i += 2) stops.push([c[i] as number, c[i + 1] as number]);
    if (z <= stops[0]![0]) return stops[0]![1];
    for (let i = 1; i < stops.length; i++)
      if (z <= stops[i]![0]) {
        const [[z0, v0], [z1, v1]] = [stops[i - 1]!, stops[i]!];
        return v0 + ((v1 - v0) * (z - z0)) / (z1 - z0);
      }
    return stops[stops.length - 1]![1];
  };

  for (const theme of ["light", "dark", "cvd"]) {
    it(`paints the field opaque, each colour mixed on the paper (${theme})`, () => {
      const t = resolveTheme(theme) as Record<string, string | number>;
      const k = Number(t["opacity.basin"]);
      const paper = String(t["color.outside"]);
      const plainly = paintFor("basin", "standing", { ...t, "opacity.basin": 1 });
      const solid = paintFor("basin", "standing", t);
      expect(solid["fill-opacity"]).toBe(1);
      const want = colours(plainly["fill-color"]).map((c) => over(c, paper, k));
      expect(colours(solid["fill-color"])).toEqual(want);
      expect(want.length).toBeGreaterThan(5);
    });

    it(`draws a seam under the field in the field's own colour (${theme})`, () => {
      const t = resolveTheme(theme);
      const ids = STYLE_LAYERS.map((l) => l.id);
      expect(ids.indexOf("basin__edge")).toBe(ids.indexOf("basin") - 1);
      expect(STYLE_META.follows.basin__edge).toBe("basin");
      const seam = paintFor("basin__edge", "standing", t);
      expect(seam["line-color"]).toEqual(paintFor("basin", "standing", t)["fill-color"]);
      expect(seam["line-opacity"]).toBe(1);
      for (let z = 4; z < STYLE_META.field!.handover; z += 0.25)
        expect(at(seam["line-width"], z), `z${z}`).toBeGreaterThanOrEqual(WIDEST_GAP_M / mpp(z));
    });
  }
});
