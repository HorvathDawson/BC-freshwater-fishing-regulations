/**
 * One outcome, one colour, everywhere.
 *
 * `theme.ts` and the map themes used to declare these separately, with DIFFERENT values —
 * `#D81E1E` against `#c0392b` for "closed" — under a comment saying the two agreed because
 * they named the same outcomes. Naming the same outcome is not agreeing about it, and the
 * visible result was a status pill in a different red from the river beside it.
 */
import { describe, expect, it } from "vitest";
import { resolveTheme, themeNames } from "@app/map";
import { OUTCOMES, statusWord } from "@app/core";
import { CVD, DARK, LIGHT, outcomeColour, THEMES } from "./theme";

describe("outcome colour", () => {
  it("matches the map, hex for hex, in every theme", () => {
    for (const [name, palette] of [["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const) {
      const map = resolveTheme(name) as Record<string, string>;
      for (const o of OUTCOMES)
        expect(outcomeColour(palette, o).toLowerCase(),
               `${name}/${o}: chrome and map disagree`)
          .toBe(map[`color.status.${o}`]!.toLowerCase());
    }
  });

  it("has a map theme for every palette the app offers", () => {
    // The colour-blind palette existed for weeks with no map theme behind it, so choosing
    // it swapped the chrome and left the map red/green — for the one person who chose it
    // because they cannot tell those apart.
    for (const name of Object.keys(THEMES))
      expect(themeNames(), `no map theme named "${name}"`).toContain(name);
  });

  it("changes the red/green pair in the colour-blind theme, and nothing else", () => {
    expect(CVD.closed).not.toBe(LIGHT.closed);
    expect(CVD.open).not.toBe(LIGHT.open);
    // the ground stays put: only the pair a deuteranope cannot separate moves
    expect(CVD.card).toBe(LIGHT.card);
    expect(CVD.ink).toBe(LIGHT.ink);
  });

  it("keeps the word regardless of the colour", () => {
    for (const o of OUTCOMES)
      expect(statusWord({ outcome: o, provenance: "specific", from: [] })).toBeTruthy();
  });
});
