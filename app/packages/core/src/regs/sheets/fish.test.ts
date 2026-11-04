// (T344, owner 2026-10-31) a group the book names is one page per member, swiped between; one member is its own sheet
import { describe, expect, it } from "vitest";
import { FISH } from "../fish.generated";
import type { RegsInputs } from "../inputs";
import { fishSheet } from "./fish";

const inp = (groups: Record<string, { name: string; members: string[] }>) => ({ statics: { exportSpecies: {
  fish: { LW: { name: "Lake whitefish" }, MW: { name: "Mountain whitefish" } }, groups } } }) as unknown as RegsInputs;

describe("T344: a group's fish sheet", () => {
  it("Whitefish: two pages, lake then mountain, each the member's own sheet", () => {
    const s = fishSheet(inp({ WHITEFISH: { name: "Whitefish", members: ["LW", "MW"] } }), "sp:WHITEFISH", "WHITEFISH", FISH)!;
    expect([s.title, s.blocks.map((b) => b.k)]).toEqual(["Whitefish", ["swipe"]]);
    const b = s.blocks[0]! as Extract<(typeof s.blocks)[number], { k: "swipe" }>;
    expect(b.pages.map((p) => [p.key, p.title, p.blocks[0]!.k])).toEqual([["sp:LW", "Lake whitefish", "image"], ["sp:MW", "Mountain whitefish", "image"]]);
  });
  it("a group with one member that has a sheet is that sheet; a species is unchanged", () => {
    const one = fishSheet(inp({ G: { name: "G", members: ["LW", "NOPE"] } }), "sp:G", "G", FISH)!;
    expect([one.key, one.title, one.blocks.some((b) => b.k === "swipe")]).toEqual(["sp:G", "Lake whitefish", false]);
    expect(fishSheet(inp({}), "sp:LW", "LW", FISH)!.blocks.some((b) => b.k === "swipe")).toBe(false);
  });
});
