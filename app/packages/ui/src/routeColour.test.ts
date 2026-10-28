/**
 * The route map's colouring, whole (`routeColour.ts`): the chain as the engine's classes, its values in ramp units.
 */
import { describe, expect, it } from "vitest";
import type { SectionId } from "@app/data";
import { routeLabels, routeValues } from "./routeColour";

const sid = (n: number) => n as SectionId;

describe("the route map's colouring", () => {
  it("a chain section's class is its first place on the chain; anything else has none", () => {
    const l = routeLabels([sid(5), sid(9), sid(5), sid(2)]);
    expect([5, 9, 2, 7].map((s) => l.classOf(s))).toEqual([0, 1, 3, null]);
  });

  it("values per class in the ramp's units; a section nothing speaks for is NaN; no answers yet is null", () => {
    const l = routeLabels([sid(5), sid(9), sid(2)]);
    expect(routeValues(l, new Map())).toBeNull();
    const v = routeValues(l, new Map([[sid(5), 0.25], [sid(2), -0.01]]))!;
    expect([0, 1, 2, 3].map((k) => v.section(k))).toEqual([25, NaN, -1, NaN]);
    expect(v.basins).toEqual({});
  });
});
