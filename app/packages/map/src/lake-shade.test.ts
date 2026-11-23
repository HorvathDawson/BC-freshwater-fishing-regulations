/**
 * (T467, owner 2026-11-22: "Yes, the shaded band") THE LAKE'S SHORELINE IS A SHADE OF ITS FILL: every colour the edge
 * FOLLOWS from the fill (`outline.follow`) is drawn `shade.lake.edge` off its token — the status modes, the Flow standing,
 * stocked, surveyed, runs, salmon — while plain keeps the outline's own colour and the colour-blind theme its solid edge.
 * The colour gate for the shaded edges is tools/status-colours-distinct.test.ts ("(T467)").
 */
import { describe, expect, it } from "vitest";
import { colorExpression, resolveTheme, shadeHex, STYLE_META } from "./style";

/** Every colour string an expression paints. */
const colours = (e: unknown): string[] =>
  Array.isArray(e) ? e.flatMap(colours) : typeof e === "string" && /^#[0-9a-f]{6}$/i.test(e) ? [e.toUpperCase()] : [];

/** Colours written ENCODED, `~` for the hash (check-colours.test.ts's house trick): measurement inputs, never paint. */
const de = (x: string): string => x.replace(/~/g, "#");

describe("(T467) shadeHex", () => {
  it("mixes toward black below 0 and white above, per channel; 0 is the colour itself", () => {
    expect([shadeHex(de("~808080"), -0.5), shadeHex(de("~808080"), 0.5), shadeHex(de("~d08404"), 0), shadeHex(de("~fff"), -1)])
      .toEqual(["~404040", "~C0C0C0", "~D08404", "~000000"].map(de));
  });
  it("throws on what it cannot shade (no silent fallback)", () => {
    expect(() => shadeHex("transparent", 0.2)).toThrow("not a #rgb / #rrggbb colour");
    expect(() => shadeHex(de("~000000"), 1.5)).toThrow("not in [-1, 1]");
  });
});

describe("(T467) the lake's edge paints a shade of every colour it follows", () => {
  const followed = Object.keys(STYLE_META.colorModes["lake__edge"]!).filter((m) => m !== "plain");

  it("in light and dark the followed modes are shaded — Flow's standing, stocked, surveyed", () => {
    expect(followed).toEqual(expect.arrayContaining(["standing", "profile", "stocked"]));
    for (const th of ["light", "dark"]) {
      const t = resolveTheme(th);
      const k = t["shade.lake.edge"] as number;
      expect([th, k !== 0]).toEqual([th, true]);
      for (const m of ["standing", "stocked", "surveyed"]) {
        const fill = colours(colorExpression("lake", m, t));
        const edge = colours(colorExpression("lake__edge", m, t));
        // each fill colour the edge follows is drawn shaded (the fill's plain water is swapped for the outline's own)
        const plainFill = String(t["color.lake.fill"]).toUpperCase();
        expect([th, m, edge]).toEqual([th, m, fill.map((c) => (c === plainFill ? String(t["color.water.mapped"]).toUpperCase() : shadeHex(c, k)))]);
      }
    }
  });

  it("plain keeps the outline's own colour; the colour-blind theme keeps its solid edge (shade 0)", () => {
    const t = resolveTheme("light");
    expect(colorExpression("lake__edge", "plain", t)).toBe(t["color.water.mapped"]);
    const cvd = resolveTheme("cvd");
    expect(cvd["shade.lake.edge"]).toBe(0);
    expect(colours(colorExpression("lake__edge", "standing", cvd))).toEqual(colours(colorExpression("lake", "standing", cvd)));
  });
});
