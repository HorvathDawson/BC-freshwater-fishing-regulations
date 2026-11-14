import { describe, expect, it } from "vitest";
import { returnedFromBackground } from "./returnedFromBackground";

describe("returnedFromBackground (T723 G2)", () => {
  it("is true only for background -> active", () => {
    expect(returnedFromBackground("background", "active")).toBe(true);
  });
  it("is false for an iOS inactive -> active (Control Center, permission alert)", () => {
    expect(returnedFromBackground("inactive", "active")).toBe(false);
  });
  it("is false for active -> active and for any move away", () => {
    expect(returnedFromBackground("active", "active")).toBe(false);
    expect(returnedFromBackground("active", "background")).toBe(false);
    expect(returnedFromBackground("active", "inactive")).toBe(false);
    expect(returnedFromBackground("background", "inactive")).toBe(false);
  });
  it("Android path (active <-> background only) behaves as before", () => {
    const seq = ["active", "background", "active", "background", "active"];
    const applied = seq.slice(1).filter((st, i) => returnedFromBackground(seq[i]!, st)).length;
    expect(applied).toBe(2);
  });
});
