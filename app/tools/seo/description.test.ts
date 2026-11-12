/** (T672 W4, owner 2026-11-12) the meta description names EVERY region a water runs through. */
import { describe, expect, it } from "vitest";
import { descriptionOf, waterRegions } from "./html";
import type { WaterModel } from "./model";

const m = (regions: number[], focus: string | null = null) => ({ name: "Fraser River", regions, region: regions[0] ?? 0, focus }) as unknown as WaterModel;

describe("descriptionOf regions", () => {
  it("a multi-region water names all of them, ascending, once each", () => {
    expect(waterRegions(m([7, 2, 5, 3, 2], "2"))).toBe("Regions 2, 3, 5, 7");
    expect(descriptionOf(m([2, 3, 5, 7], "2"), "Fraser River", "Quesnel")).toContain("(near Quesnel, Regions 2, 3, 5, 7)");
  });
  it("a one-region water keeps its own code", () => {
    expect(waterRegions(m([7], "7a"))).toBe("Region 7A");
    expect(descriptionOf(m([7], "7a"))).toContain("(Region 7A)");
  });
  it("a one-region water without a focus says the hub's number", () => expect(waterRegions(m([4]))).toBe("Region 4"));
});
