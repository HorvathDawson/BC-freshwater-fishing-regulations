import { describe, expect, it } from "vitest";
import { SCREEN_FAULT_ACTIONS, screenFaultLine } from "./screenFault";

describe("T1164 a screen that could not be drawn", () => {
  it("each screen says so in one whole line", () => {
    expect(screenFaultLine("water")).toBe("This water hit a problem and could not be drawn.");
    expect(screenFaultLine("spots")).toBe("Spots hit a problem and could not be drawn.");
    expect(screenFaultLine("newSpot")).toBe("Adding a spot hit a problem and could not be drawn.");
  });
  it("the ways on", () => { expect(SCREEN_FAULT_ACTIONS).toEqual({ retry: "Try again", back: "Go back" }); });
});
