import { describe, expect, it } from "vitest";
import { CVD, DARK, LIGHT, THEMES, outcomeColour, outcomeDash } from "./theme";

const OUTCOMES = ["closed", "restricted", "open", "unknown"] as const;

describe("themes", () => {
  it("every theme colours every outcome", () => {
    // Rule 29: an outcome with no colour renders as something it is not, and `unknown`
    // silently becoming `open` is the one failure with real consequences.
    for (const [name, p] of Object.entries(THEMES)) {
      for (const o of OUTCOMES) {
        expect(outcomeColour(p, o), `${name}.${o}`).toMatch(/^#[0-9A-Fa-f]{6}$/);
      }
    }
  });

  it("no theme reuses one colour for two outcomes", () => {
    for (const [name, p] of Object.entries(THEMES)) {
      const used = OUTCOMES.map((o) => outcomeColour(p, o).toLowerCase());
      expect(new Set(used).size, name).toBe(used.length);
    }
  });

  it("the colour-blind theme drops the red/green pair", () => {
    // Deuteranopia cannot separate LIGHT's closed from its open.
    expect(CVD.closed).not.toBe(LIGHT.closed);
    expect(CVD.open).not.toBe(LIGHT.open);
  });

  it("outcome is also carried by a dash pattern, so hue is never the only channel", () => {
    expect(outcomeDash("unknown")).toBeDefined();
    expect(outcomeDash("restricted")).toBeDefined();
    expect(outcomeDash("unknown")).not.toEqual(outcomeDash("restricted"));
  });

  it("dark is a real theme, not an inversion", () => {
    for (const o of OUTCOMES) expect(outcomeColour(DARK, o)).not.toBe(outcomeColour(LIGHT, o));
  });
});
