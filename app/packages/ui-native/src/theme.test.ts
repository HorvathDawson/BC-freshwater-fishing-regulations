/**
 * Colours that must not be mistaken for each other.
 *
 * The style build already runs a ΔE guard over the MAP tokens; these are the app's own
 * palette and were outside it. The failure this exists for: a donor pin at ΔE 11 from the
 * "you are here" marker, on a map whose whole job is to say which of five pins is you.
 */
import { describe, expect, it } from "vitest";
import { CVD, DARK, LIGHT, type Palette } from "./theme";

/** CIE L*a*b* via sRGB, matching `tools/build-style.mjs` so the two guards agree. */
function lab(hex: string): [number, number, number] {
  const h = hex.replace("#", "");
  const [r, g, b] = [0, 2, 4]
    .map((i) => parseInt(h.slice(i, i + 2), 16) / 255)
    .map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4)) as
      [number, number, number];
  const f = (t: number) => (t > 0.008856 ? Math.cbrt(t) : 7.787 * t + 16 / 116);
  const X = f((r * 0.4124 + g * 0.3576 + b * 0.1805) / 0.95047);
  const Y = f(r * 0.2126 + g * 0.7152 + b * 0.0722);
  const Z = f((r * 0.0193 + g * 0.1192 + b * 0.9505) / 1.08883);
  return [116 * Y - 16, 500 * (X - Y), 200 * (Y - Z)];
}

const deltaE = (a: string, b: string) => {
  const [x, y] = [lab(a), lab(b)];
  return Math.hypot(x[0] - y[0], x[1] - y[1], x[2] - y[2]);
};

/** The style build's own threshold, so one number governs both guards. */
const MIN_DELTA_E = 12;

describe.each([["light", LIGHT], ["dark", DARK]] as const)("%s theme", (_name, p: Palette) => {
  it("keeps every donor tone clear of the you-are-here marker", () => {
    for (const tone of p.donor)
      expect(deltaE(tone, p.accent)).toBeGreaterThanOrEqual(MIN_DELTA_E);
  });

  it("keeps the donor tones clear of each other", () => {
    // Four pins on one map, four rows in one table, matched by colour and nothing else.
    for (let i = 0; i < p.donor.length; i++)
      for (let j = i + 1; j < p.donor.length; j++)
        expect(deltaE(p.donor[i]!, p.donor[j]!)).toBeGreaterThanOrEqual(MIN_DELTA_E);
  });

  it("keeps the quiet grey clear of every live tone and of the marker", () => {
    // A gauge that is not reporting is drawn grey — the same "nothing to say" the map uses
    // for ungauged water. It must not be mistakable for a gauge that IS carrying the
    // answer, nor for the you-are-here marker standing beside it.
    for (const tone of p.donor)
      expect(deltaE(p.quiet, tone)).toBeGreaterThanOrEqual(MIN_DELTA_E);
    expect(deltaE(p.quiet, p.accent)).toBeGreaterThanOrEqual(MIN_DELTA_E);
  });

  it("keeps every donor tone readable against the card it sits on", () => {
    for (const tone of p.donor)
      expect(deltaE(tone, p.card)).toBeGreaterThanOrEqual(MIN_DELTA_E);
  });
});

/** WCAG relative-luminance contrast. */
function contrast(a: string, b: string): number {
  const L = (hex: string) => {
    const h = hex.replace("#", "");
    const [r, g, b2] = [0, 2, 4]
      .map((i) => parseInt(h.slice(i, i + 2), 16) / 255)
      .map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4)) as
        [number, number, number];
    return 0.2126 * r + 0.7152 * g + 0.0722 * b2;
  };
  const [x, y] = [L(a), L(b)].sort((m, n) => n - m) as [number, number];
  return (x + 0.05) / (y + 0.05);
}

describe.each([["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const)(
  "%s theme: the place colour", (_name, p: Palette) => {
  it("is not the you-chose-this accent, nor the live feed's", () => {
    // A town's pin stands on the same small map as a lit water; a town row sits in the
    // same list as the selected one. Both must read as a different KIND of thing.
    expect(deltaE(p.place, p.accent)).toBeGreaterThanOrEqual(MIN_DELTA_E);
    expect(deltaE(p.place, p.live)).toBeGreaterThanOrEqual(MIN_DELTA_E);
  });

  it("reads as text on the card and on the pressed row", () => {
    // The group heading is 10.5px caps in this colour — small text, so 4.5:1.
    expect(contrast(p.place, p.card)).toBeGreaterThanOrEqual(4.5);
    expect(contrast(p.place, p.wash)).toBeGreaterThanOrEqual(4.5);
  });

  it("gives the places band a ground you can see, and every word on it 4.5:1", () => {
    // Visible against the plain list around it — that is the whole point of the band —
    // and the pressed/selected row inside it (the card) must differ from it too.
    expect(deltaE(p.placeBand, p.card)).toBeGreaterThanOrEqual(3);
    expect(deltaE(p.placeBand, p.wash)).toBeGreaterThanOrEqual(3);
    for (const [name, c] of [["ink", p.ink], ["place", p.place]] as const)
      expect(contrast(c, p.placeBand), `${name} on the band`).toBeGreaterThanOrEqual(4.5);
  });
});
