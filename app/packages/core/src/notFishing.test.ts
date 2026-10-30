/**
 * T292 — the not-fishing-water tap sheet: the whole composed words (AGENTS.md rule 33).
 */
import { describe, expect, it } from "vitest";
import { NOT_FISHING_KINDS, notFishingKind, notFishingSheet } from "./notFishing";

describe("notFishingSheet", () => {
  it("names each kind and says it is not a fishing water", () => {
    expect(NOT_FISHING_KINDS.map((k) => notFishingSheet(k))).toEqual([
      { title: "Sewage lagoon — not a fishing water",
        body: ["The Freshwater Atlas records this as a sewage settling basin: man-made, and not a place to fish. It has no fishing regulations, and search does not list it."] },
      { title: "Mine tailings pond — not a fishing water",
        body: ["The Freshwater Atlas records this as a mine tailings pond: man-made, and not a place to fish. It has no fishing regulations, and search does not list it."] },
      { title: "Dump — not a fishing water",
        body: ["The Freshwater Atlas records this as a dump: man-made, and not a place to fish. It has no fishing regulations, and search does not list it."] },
    ]);
  });

  it("a tile with no kind (an older atlas) still says it, without naming one", () => {
    expect(notFishingSheet(null)).toEqual({
      title: "Not a fishing water",
      body: ["The Freshwater Atlas records this as a sewage lagoon, mine tailings pond or dump: man-made, and not a place to fish. It has no fishing regulations, and search does not list it."],
    });
  });

  it("reads only the three tile values", () => {
    expect(["sewage", "tailings", "dump", "lake", "", undefined, 3].map(notFishingKind))
      .toEqual(["sewage", "tailings", "dump", null, null, null, null]);
  });
});
