/** P11: the words for an UNPROVEN pair (no tile version known), pinned whole (AGENTS 33). */
import { describe, expect, it } from "vitest";
import { unprovenLine } from "./pairing";

describe("the unproven-pair line (tiles with no version: nothing is coloured, and the reader is told why)", () => {
  it("names the data the tiles could not be matched to, whole", () => {
    expect(unprovenLine("r-6960fe8201e3"))
      .toBe("Status colours are off: these map tiles carry no version, so they cannot be proven to match the data " +
            "(r-6960fe8201e3).");
  });
  it("leaves out a data name it does not have rather than making one up", () => {
    expect(unprovenLine(null))
      .toBe("Status colours are off: these map tiles carry no version, so they cannot be proven to match the data.");
  });
});
