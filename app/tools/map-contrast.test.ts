/**
 * MAP LINE WORK MEETS WCAG NON-TEXT CONTRAST (T470, owner 2026-11-09: "what ever is industry standard").
 *
 * The standard is WCAG 2.x SC 1.4.11 Non-text Contrast: a graphical object a reader needs to understand the content has
 * at least 3:1 (relative luminance) against the colours next to it. On this map those objects are the water lines and
 * lake outlines that carry an ANSWER — the Standard view's statuses (closed / own / base / no access, and the grey of a
 * water not answered yet), the Salmon view's four (open / release / closed / check), and the selected part's ring
 * (`color.highlight`) — and what is next to them is the basemap ground they are drawn on. The same colours are the
 * legend's swatches, which sit on the card. Words on the legend are text: SC 1.4.3, 4.5:1.
 *
 * The grounds are READ, never copied: the Protomaps flavour each theme draws (`runtime-style.ts`: dark rides on "black",
 * light and cvd on "light") — its land, water, and the green-space kinds `landuse_park` paints at full strength from z11
 * (wood, forest, scrub/grass, glacier). Our own translucent area tints (parks, watersheds, closures) are not grounds
 * here: they are washes over these, measured in the ticket.
 *
 * This measures the solid colour, as a main river and every stream from z12 are drawn. The mid-zoom order fade
 * (T470 B, `fadeByOrder`) thins small own / base streams on purpose at z7-z11; that is held by its own block in
 * status-colours-distinct.test.ts.
 *
 * What it does not change: the closed stripe, the dotted no-access line and the gear edges are non-colour cues and stay.
 * And it is not the colour-blind gate: separations BETWEEN statuses are held in ΔE2000 by status-colours-distinct,
 * water-status-cvd and cvd.test.ts — 1.4.11 asks that each mark be seen, those that the marks be told apart.
 */
import { describe, expect, it } from "vitest";
import { namedFlavor } from "@protomaps/basemaps";
import { SALMON_MAP_STATUSES, WATER_STATUSES } from "@app/core";
import { resolveTheme, waterStatusColour } from "@app/map";
import { salmonStatusColour } from "@app/map/colours";
import { CVD, DARK, LIGHT, type Palette } from "../packages/ui-native/src/theme";
import { contrast } from "./cvd";

/** Encoded colours (`~` for the hash), so this file holds no colour literal for the one-palette gate (check-colours). */
const de = (s: string) => s.replace(/~/g, "#");

type Theme = "light" | "dark" | "cvd";
const THEMES: { th: Theme; flavor: string; palette: Palette }[] = [
  { th: "light", flavor: "light", palette: LIGHT },
  { th: "dark", flavor: "black", palette: DARK },
  { th: "cvd", flavor: "light", palette: CVD },
];

/** SC 1.4.11 and SC 1.4.3. */
const NON_TEXT = 3;
const TEXT = 4.5;

/** The ground a water line is drawn on: the basemap flavour's land, water and green-space fills. */
function grounds(flavor: string): [string, string][] {
  const f = namedFlavor(flavor) as unknown as Record<string, string>;
  return [["land", "earth"], ["water", "water"], ["wood", "wood_b"], ["forest", "park_b"], ["scrub", "scrub_b"],
          ["glacier", "glacier"]].map(([name, key]) => [name!, f[key!]!]);
}

/** Every colour the map draws as a mark that carries an answer, by name. */
function marks(th: Theme): [string, string][] {
  const t = resolveTheme(th) as Record<string, string>;
  return [
    ...WATER_STATUSES.map((s): [string, string] => [`status ${s}`, waterStatusColour(th, s)]),
    ["status missing", t["color.water.unmapped"]!],
    ...SALMON_MAP_STATUSES.map((s): [string, string] => [`salmon ${s}`, salmonStatusColour(th, s)]),
    ["selection", t["color.highlight"]!],
  ];
}

/** A mark's worst ratio over the grounds, and the ground that binds. */
function worstOnGround(colour: string, flavor: string) {
  return grounds(flavor).map(([g, hex]) => ({ g, hex, r: contrast(colour, hex) })).sort((a, b) => a.r - b.r)[0]!;
}

/**
 * THE MARKS UNDER 3:1 ON THE GROUND, RECORDED (T470). Listed whole: a new one fails, one lifted to 3:1 fails until it is
 * taken off, and each may not slip under its recorded ratio (its measured worst, rounded down to 0.05).
 *
 * Light and cvd own / base (and the Salmon view's check, which IS own) cannot reach 3:1 by colour without undoing T47.
 * On this light basemap (green-space fills at relative luminance ~0.57) a 3:1 line needs luminance <= 0.155, about
 * L* 46. Searched over sRGB (step 3-5) with the hue kept: the best amber at 3:1 stands ΔE2000 14.7 off the closed red for
 * the worst reader, against T47's 29 (the owner's own complaint, "has regs and closed are very similar", 2026-10-21);
 * the best blue at 3:1 (#0060FA-ish) sits 14.3 from `status.open` in normal vision (one-meaning floor 20) and under the
 * closed / base and no-access floors. Darkening them brings back the bug T47 fixed. The remedy that keeps both is a
 * casing — a contrasting edge drawn under the line, the way roads and transit lines meet 1.4.11 on light basemaps —
 * which is a new layer and a visual change to the map T470 just calmed: the owner's call, raised in the ticket.
 *
 * The missing grey is the same neutral as `water.unmapped` everywhere (tidal water, water outside B.C., a section
 * with no answer pushed yet); a 3:1 grey on this ground collides with the dotted no-access grey (T165).
 */
const UNDER: Record<string, number> = {
  "light status own": 1.75, "light status base": 1.95, "light status missing": 2,
  "light salmon check": 1.75,
  "cvd status own": 1.75, "cvd status base": 1.9, "cvd status missing": 2,
  "cvd salmon check": 1.75,
};

describe("map marks against the ground they are drawn on (WCAG 1.4.11, 3:1)", () => {
  it("every answer-carrying mark reaches 3:1 on every basemap ground, or is a recorded exception that has not slipped", () => {
    const under: string[] = [];
    const slipped: string[] = [];
    for (const { th, flavor } of THEMES)
      for (const [name, colour] of marks(th)) {
        const w = worstOnGround(colour, flavor);
        if (w.r >= NON_TEXT) continue;
        const key = `${th} ${name}`;
        under.push(key);
        if (UNDER[key] !== undefined && w.r < UNDER[key]!)
          slipped.push(`${key} ${colour} on ${w.g} ${w.hex}: ${w.r.toFixed(2)} < recorded ${UNDER[key]}`);
      }
    expect(slipped).toEqual([]);
    expect(under).toEqual(Object.keys(UNDER));
  });

  it("dark reaches 3:1 everywhere: every mark, every ground", () => {
    // The dark theme is the one the owner reads at night; it has no recorded exception and may not gain one.
    expect(Object.keys(UNDER).filter((k) => k.startsWith("dark "))).toEqual([]);
    const fails = marks("dark").map(([n, c]) => [n, c, worstOnGround(c, "black")] as const)
      .filter(([, , w]) => w.r < NON_TEXT).map(([n, c, w]) => `${n} ${c} on ${w.g}: ${w.r.toFixed(2)}`);
    expect(fails).toEqual([]);
  });

  it("closed, the one answer that sends a reader home, reaches 3:1 in every theme on every ground", () => {
    for (const { th, flavor } of THEMES)
      for (const c of [waterStatusColour(th, "closed"), salmonStatusColour(th, "closed")])
        expect([th, c, worstOnGround(c, flavor).r >= NON_TEXT]).toEqual([th, c, true]);
  });

  it("fails the dark base before T470 on the dark water (mutation pin)", () => {
    const before = de("~0368EB");
    const w = worstOnGround(before, "black");
    expect([w.g, w.r.toFixed(2)]).toEqual(["water", "2.52"]);
  });
});

describe("the legend (WCAG 1.4.11 for its swatches, 1.4.3 for its words)", () => {
  for (const { th, palette } of THEMES)
    it(`every swatch reaches 3:1 on the card, and its words 4.5:1 — ${th}`, () => {
      const bad: string[] = [];
      for (const [name, colour] of marks(th)) {
        if (name === "selection") continue;   // the selection is not a legend row
        const r = contrast(colour, palette.card);
        if (r < NON_TEXT) bad.push(`swatch ${name} ${colour} on the card ${palette.card}: ${r.toFixed(2)}`);
      }
      // LegendCount (Chrome.tsx): the count in ink, the label in sub (ink when it stands alone)
      for (const [name, ink] of [["ink", palette.ink], ["sub", palette.sub]] as const) {
        const r = contrast(ink, palette.card);
        if (r < TEXT) bad.push(`legend words in ${name} ${ink} on the card ${palette.card}: ${r.toFixed(2)}`);
      }
      expect(bad).toEqual([]);
    });
});
