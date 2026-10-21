/** P11: the start-up and first-run words, composed here and pinned whole (AGENTS 33). */
import { describe, expect, it } from "vitest";
import { INSTALL_WORDS, installLine, startStage } from "./install";

describe("the first-run download line", () => {
  it("says how much of how much, in MB to one decimal, grouped", () => {
    expect(installLine({ bytes: 0, of: 1_461_200_000 })).toBe("0.0 of 1,461.2 MB downloaded");
    expect(installLine({ bytes: 12_460_000, of: 1_461_200_000 })).toBe("12.5 of 1,461.2 MB downloaded");
    expect(installLine({ bytes: 1_461_200_000, of: 1_461_200_000 })).toBe("1,461.2 of 1,461.2 MB downloaded");
  });
  it("says nothing when there is nothing to fetch (every file copied from the held set)", () => {
    expect(installLine({ bytes: 0, of: 0 })).toBe("");
  });
});

describe("the start-up stages", () => {
  it("has a line for every stage, and an unknown stage says nothing rather than a made-up one", () => {
    expect(startStage("manifest")).toBe("Checking for the latest regulations…");
    expect(startStage("install")).toBe("Downloading the map and regulations for offline use");
    expect(startStage("paint")).toBe("Opening the map…");
    expect(startStage("fonts")).toBe("Loading type…");
    expect(startStage("bundle")).toBe("Opening the map data…");
    expect(startStage("nope")).toBe("");
  });
  it("the words a failed start shows", () => {
    expect(INSTALL_WORDS.failed).toBe("The map data could not be loaded.");
    expect(INSTALL_WORDS.firstRunOffline)
      .toBe("The first start needs a connection: the map and regulations download once, then work offline.");
  });
});
