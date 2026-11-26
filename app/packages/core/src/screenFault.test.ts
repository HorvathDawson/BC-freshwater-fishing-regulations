import { describe, expect, it } from "vitest";
import { SCREEN_FAULT_ACTIONS, screenFaultLine } from "./screenFault";

describe("T1164 a screen that could not be drawn", () => {
  it("each screen says so in one whole line", () => {
    expect(screenFaultLine("water")).toBe("This water hit a problem and could not be drawn.");
    expect(screenFaultLine("spots")).toBe("Spots hit a problem and could not be drawn.");
    expect(screenFaultLine("page")).toBe("This page hit a problem and could not be drawn.");
    expect(screenFaultLine("sheet")).toBe("A panel hit a problem and could not be drawn.");
    expect(screenFaultLine("search")).toBe("Search hit a problem and could not be drawn.");
    expect(screenFaultLine("key")).toBe("The map key hit a problem and could not be drawn.");
    expect(screenFaultLine("app")).toBe("The app hit a problem and could not be drawn.");
    expect(screenFaultLine("newSpot")).toBe("Adding a spot hit a problem and could not be drawn.");
  });
  it("the ways on", () => { expect(SCREEN_FAULT_ACTIONS).toEqual({ retry: "Try again", back: "Go back", close: "Close" }); });
});
