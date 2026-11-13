/**
 * T166 / T692 — the tidal water words: the whole composed lines (AGENTS.md rule 33). No tap sheet: the fill is inert.
 */
import { describe, expect, it } from "vitest";
import { TIDAL_SHEET_WORDS } from "./tidal";

describe("TIDAL_SHEET_WORDS", () => {
  it("says the freshwater regulations do not apply, and where the tidal rules are", () => {
    expect(TIDAL_SHEET_WORDS.body).toEqual([
      "Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp.",
      "See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required.",
    ]);
  });
});
