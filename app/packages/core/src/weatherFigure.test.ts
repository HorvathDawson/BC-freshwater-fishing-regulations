import { describe, expect, it } from "vitest";
import { latLon } from "./grouped";
import { grouped } from "./grouped";
import { weatherFigure } from "./weatherFigure";

describe("(T620 G25, G26) shared number words", () => {
  it("a point is four decimals everywhere", () => {
    expect(latLon({ lat: 49.097441, lon: -121.967519 })).toBe("49.0974, -121.9675");
  });
  it("grouping is commas, negative and fractions kept", () => {
    expect([grouped(60648), grouped(999), grouped(-1234.5), grouped(0)]).toEqual(["60,648", "999", "−1,234.5", "0"]);
  });
});

describe("(T1039) humidity, as a spot's weather window and the gauge page say it", () => {
  it("whole percent", () => {
    expect([weatherFigure("humidity", 91.2), weatherFigure("humidity", 91.5), weatherFigure("cloud", 91.5)]).toEqual(["91%", "92%", "92%"]);
  });
});
