import { describe, expect, it } from "vitest";
import { freshness } from "./index";

describe("freshness", () => {
  it("never reports a never-fetched value as live", () => {
    expect(freshness(null, 1000, 100)).toEqual({ state: "unknown" });
  });
  it("distinguishes stale from live", () => {
    expect(freshness(0, 50, 100).state).toBe("live");
    expect(freshness(0, 500, 100).state).toBe("stale");
  });
});
