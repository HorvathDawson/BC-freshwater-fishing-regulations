/**
 * (T1210) THE TILES DECLARE THE LABEL LAYERS LOOK A DRAWS: `stream_name` from z4 and `lake_name`, a point with its name and
 * area, to z7 (pipeline/deliver/tiles/tile-contract.json). Here, not in packages/map/src/look-a.test.ts, because a package
 * reads no files (tools/check-boundaries.mjs).
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const contract = JSON.parse(readFileSync(new URL("../../pipeline/deliver/tiles/tile-contract.json", import.meta.url), "utf8")) as
  { layers: Record<string, { geometry: string; attrs: string[]; minzoom: number; maxzoom: number }> };

describe("(T1210) the tiles' label layers, as look A draws them", () => {
  it("the tiles declare them: `stream_name` from z4, `lake_name` (a point with name and area) to z7", () => {
    expect(contract.layers["stream_name"]!.minzoom).toBe(4);
    expect(contract.layers["lake_name"]).toMatchObject({ geometry: "point", attrs: ["name", "area_m2"], minzoom: 4, maxzoom: 7 });
  });
});
