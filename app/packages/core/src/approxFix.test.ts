/** (T723 G5) the rough-fix sentence, whole, and where it does not appear. */
import { describe, expect, it } from "vitest";
import { captureAtDetail, roughFixLine } from "./approxFix";

describe("roughFixLine", () => {
  it("says how rough, in metres or kilometres", () => {
    expect(roughFixLine(250)).toBe("Your phone placed you only to about 250 m, so this pin may not be where you are.");
    expect(roughFixLine(3240)).toBe("Your phone placed you only to about 3.2 km, so this pin may not be where you are.");
    expect(roughFixLine(14800)).toBe("Your phone placed you only to about 15 km, so this pin may not be where you are.");
  });
  it("is silent for a good fix, the bar itself, and an unknown accuracy", () => {
    expect([roughFixLine(5), roughFixLine(100), roughFixLine(undefined), roughFixLine(null)]).toEqual([null, null, null, null]);
  });
});

describe("captureAtDetail", () => {
  it("a good fix keeps the capture's own sentence byte for byte; a rough one adds the second", () => {
    expect(captureAtDetail(20)).toBe("Tap the stream or lake your spot is on, near the pin.");
    expect(captureAtDetail(3240)).toBe(
      "Tap the stream or lake your spot is on, near the pin. Your phone placed you only to about 3.2 km, so this pin may not be where you are.");
  });
});
