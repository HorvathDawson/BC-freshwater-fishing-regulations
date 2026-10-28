/**
 * T166 — the tidal water tap sheet: the whole composed words (AGENTS.md rule 33).
 */
import { describe, expect, it } from "vitest";
import { tidalSheet } from "./tidal";

describe("tidalSheet", () => {
  it("says the freshwater regulations do not apply, and where the tidal rules are", () => {
    expect(tidalSheet()).toEqual({
      title: "Tidal water — DFO rules",
      body: [
        "Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp.",
        "See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required.",
      ],
    });
  });
});
