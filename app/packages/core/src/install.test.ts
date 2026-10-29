/** P11: the start-up and first-run words, composed here and pinned whole (AGENTS 33). */
import { describe, expect, it } from "vitest";
import { downloadFraction, INSTALL_WORDS, installLine, rolledBackLine, SETUP_WORDS, setupFraction, setupLine, startStage } from "./install";

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
    expect(startStage("nope")).toBe("");
  });
  it("the words a failed start shows", () => {
    expect(INSTALL_WORDS.failed).toBe("The map data could not be loaded.");
    expect(INSTALL_WORDS.firstRunOffline)
      .toBe("The first start needs a connection: the map and regulations download once, then work offline.");
  });
});

describe("the rollback line (a new set would not open, so the one it replaced is used)", () => {
  it("names the set refused and the set in use, whole", () => {
    expect(rolledBackLine({ refused: "r-2b2b2b2b2b2b", using: "r-1a1a1a1a1a1a" }))
      .toBe("The newest map data (r-2b2b2b2b2b2b) would not open, so the app is using the data it had before " +
            "(r-1a1a1a1a1a1a).");
  });
  it("leaves out a name it does not have rather than making one up", () => {
    expect(rolledBackLine({ refused: null, using: null }))
      .toBe("The newest map data would not open, so the app is using the data it had before.");
  });
});

describe("the second stage of a first install (T272)", () => {
  it("has its own words and a bar that moves through its steps, never past 100 %", () => {
    expect(SETUP_WORDS.title).toBe("Setting up on this device");
    expect(setupLine({ step: "verify", done: 0, of: 100 })).toBe("Checking the downloaded files · 0 %");
    expect(setupLine({ step: "verify", done: 50, of: 100 })).toBe("Checking the downloaded files · 42 %");
    expect(setupLine({ step: "assets", done: 0, of: 2 })).toBe("Fetching the map's labels and symbols · 85 %");
    expect(setupLine({ step: "open", done: 3, of: 3 })).toBe("Opening the map · 100 %");
    const steps = [{ step: "verify", done: 100, of: 100 }, { step: "assets", done: 0, of: 9 }, { step: "open", done: 0, of: 3 }] as const;
    const f = steps.map(setupFraction);
    expect(f).toEqual([...f].sort((a, b) => a - b));
    expect(downloadFraction({ bytes: 5, of: 10 })).toBe(0.5);
    expect(downloadFraction({ bytes: 11, of: 10 })).toBe(1);
    expect(downloadFraction({ bytes: 0, of: 0 })).toBe(0);
  });
});
