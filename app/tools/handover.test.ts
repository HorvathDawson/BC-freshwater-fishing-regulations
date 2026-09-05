/**
 * ONE ZOOM, THREE PLACES, held equal by this test.
 *
 * Below the handover the Conditions map is a field of catchments; above it, rivers. Three
 * separate things have to agree about where that line is:
 *
 *   the TILE BUILDER  stops writing basin polygons above it   (BASIN_HANDOVER_Z)
 *   the STYLE         stands the rivers down below it         (minzoomByView)
 *   the APP           refuses taps below it                   (HANDOVER_Z)
 *
 * Disagree by one and the map has a zoom where nothing is drawn, or one where two answers
 * are drawn over each other and the smaller wins by being on top. Neither failure throws.
 */
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { STYLE_META } from "@app/map";
// The style itself is not exported — it is the generated artifact, and reading
// it here is reading what actually ships rather than a re-export of it.
const MAP_STYLE = JSON.parse(
  readFileSync(new URL("../packages/map/style/style.json", import.meta.url)
    .pathname, "utf8")) as { layers: { id: string; maxzoom?: number }[] };

const ROOT = new URL("../../", import.meta.url).pathname;
const PY = `${ROOT}.venv/bin/python`;

/** What the tile builder thinks, read from the tile builder. */
function fromPipeline(): { handover: number; basinMax: number; basinMin: number } {
  const script = `
import json, sys
sys.path.insert(0, ${JSON.stringify(ROOT)})
from pipeline.deliver.tiles.layers import BASIN_HANDOVER_Z, BY_NAME
b = BY_NAME["basin"]
print(json.dumps({"handover": BASIN_HANDOVER_Z, "basinMax": b.maxzoom,
                  "basinMin": b.minzoom}))
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
    expect(STYLE_META.minzoomByMode.stream).toEqual({ standing: 9 });
    expect(STYLE_META.minzoomByMode.lake).toEqual({ standing: 9 });
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
    expect(py.handover).toBe(fromShell());
    expect(py.basinMax).toBe(py.handover - 1);
    // And the field must reach the zoom the map opens at its widest.
    expect(py.basinMin).toBeLessThanOrEqual(4);
  });
});
