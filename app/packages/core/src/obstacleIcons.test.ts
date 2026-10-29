import { describe, expect, it } from "vitest";
import { OBSTACLE_TYPES } from "./obstacles";
import { OBSTACLE_ICON_GRID, OBSTACLE_ICON_KINDS, OBSTACLE_ICON_PRIORITY, OBSTACLE_ICON_SHAPES, obstacleIconKind, obstacleIconSvg } from "./obstacleIcons";

describe("(T173) obstacle icons", () => {
  it("one drawing per tile type, plus the dot; an unknown or missing type is the dot", () => {
    expect([...OBSTACLE_ICON_KINDS]).toEqual([...OBSTACLE_TYPES, "other"]);
    expect(Object.keys(OBSTACLE_ICON_SHAPES).sort()).toEqual([...OBSTACLE_ICON_KINDS].sort());
    expect([obstacleIconKind("falls"), obstacleIconKind("weir"), obstacleIconKind(undefined)]).toEqual(["falls", "other", "other"]);
    expect([...OBSTACLE_ICON_PRIORITY].sort()).toEqual([...OBSTACLE_ICON_KINDS].sort());
  });

  it("every point sits inside the grid with room for the halo", () => {
    for (const kind of OBSTACLE_ICON_KINDS)
      for (const s of OBSTACLE_ICON_SHAPES[kind]) {
        const pts = "stroke" in s ? s.stroke : "fill" in s ? s.fill : null;
        for (const [x, y] of pts ?? []) for (const v of [x, y]) expect(v).toBeGreaterThanOrEqual(1.5), expect(v).toBeLessThanOrEqual(OBSTACLE_ICON_GRID - 1.5);
      }
  });

  it("the SVG carries the ink, and the halo under it when asked", () => {
    const a = obstacleIconSvg("dam", "INK"), b = obstacleIconSvg("dam", "INK", "PAPER");
    expect(a).toContain('viewBox="0 0 16 16"');
    expect(a).not.toContain("PAPER");
    expect(b.indexOf("PAPER")).toBeLessThan(b.indexOf("INK"));
  });
});
