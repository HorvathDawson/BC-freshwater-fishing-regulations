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
  it("stands the rivers down in the Conditions view and nowhere else", () => {
    // Every other view must leave them alone, or switching to Regulations at z6 would show
    // a province with no rivers on it.
    expect(STYLE_META.minzoomByView.stream).toEqual({ conditions: 9 });
    expect(STYLE_META.minzoomByView.lake).toEqual({ conditions: 9 });
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
    const river = STYLE_META.colorModes.stream?.standing as { stops?: unknown[] };
    const field = STYLE_META.colorModes.basin?.standing as { stops?: unknown[] };
    expect(field?.stops).toEqual(river?.stops);
  });

  it("stops the field exactly where the rivers start", () => {
    const basin = MAP_STYLE.layers.find((l) => l.id === "basin")!;
    const app = fromShell();
    // maxzoom is inclusive of the zoom BELOW the handover: the field draws z4–8, the
    // rivers from z9. One zoom with both, or one with neither, is the bug.
    expect(basin.maxzoom).toBe(app - 1);
    expect(STYLE_META.minzoomByView.stream!.conditions).toBe(app);
  });

  it.skipIf(!has)("agrees with the tile builder, which decides what exists at all", () => {
    const py = fromPipeline();
    expect(py.handover).toBe(fromShell());
    expect(py.basinMax).toBe(py.handover - 1);
    // And the field must reach the zoom the map opens at its widest.
    expect(py.basinMin).toBeLessThanOrEqual(4);
  });
});
