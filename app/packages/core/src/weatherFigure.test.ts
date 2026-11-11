import { describe, expect, it } from "vitest";
import { latLon } from "./grouped";
import { grouped } from "./grouped";

describe("(T620 G25, G26) shared number words", () => {
  it("a point is four decimals everywhere", () => {
    expect(latLon({ lat: 49.097441, lon: -121.967519 })).toBe("49.0974, -121.9675");
  });
  it("grouping is commas, negative and fractions kept", () => {
    expect([grouped(60648), grouped(999), grouped(-1234.5), grouped(0)]).toEqual(["60,648", "999", "−1,234.5", "0"]);
  });
});
