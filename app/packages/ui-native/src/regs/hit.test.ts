/**
 * T40 (A) — the hit-area arithmetic every regs press goes through: the slop that grows a drawn box to 44 × 44, and the
 * `data-hitslop` record the a11y render test reads back.
 */
import { describe, expect, it } from "vitest";
import { hitArea, hitData, hitProps, hitSlopFor, MIN_HIT } from "./hit";

describe("hitSlopFor", () => {
  it("grows a 24 × 21 inline link to 44 × 44, split evenly, the odd pixel above", () => {
    expect(hitSlopFor(24, 21)).toEqual({ top: 12, bottom: 11, left: 10, right: 10 });
  });

  it("a box already big enough gets no slop", () => {
    expect(hitSlopFor(50, 50)).toEqual({ top: 0, bottom: 0, left: 0, right: 0 });
    expect(hitSlopFor(44, 44)).toEqual({ top: 0, bottom: 0, left: 0, right: 0 });
  });

  it("a fractional line height rounds the slop up, never short of 44", () => {
    // TYPE.metaBold's line height
    expect(hitSlopFor(44, 16.22)).toEqual({ top: 14, bottom: 14, left: 0, right: 0 });
    expect(16.22 + 14 + 14).toBeGreaterThanOrEqual(MIN_HIT);
  });

  it("one axis small, the other not: only that axis grows", () => {
    expect(hitSlopFor(28, 34)).toEqual({ top: 5, bottom: 5, left: 8, right: 8 });
  });
});

describe("the data-hitslop record", () => {
  it("is the box then the slop in CSS order, and reads back to the area reached", () => {
    expect(hitData({ w: 24, h: 21 })).toBe("24 21 12 10 11 10");
    expect(hitArea("24 21 12 10 11 10")).toEqual({ w: 44, h: 44 });
    expect(hitArea("80 44 0 0 0 0")).toEqual({ w: 80, h: 44 });
    expect(hitArea("80 44")).toBeNull();
    expect(hitArea("a b c d e f")).toBeNull();
  });

  it("hitProps hands a Pressable the slop and the record together", () => {
    expect(hitProps({ w: 28, h: 34 })).toEqual({ hitSlop: { top: 5, bottom: 5, left: 8, right: 8 }, dataSet: { hitslop: "28 34 5 8 5 8" } });
  });
});
