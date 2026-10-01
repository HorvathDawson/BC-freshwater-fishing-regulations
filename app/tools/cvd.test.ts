/**
 * The colour-blind theme, measured rather than believed.
 *
 * The app offers a `cvd` setting. Whether it WORKS was, before this file, a matter of
 * someone having looked at it once — and the thing about a colour-blind palette is that the
 * person who chose the hexes is, overwhelmingly likely, not the person it is for. Nobody on
 * this project can see the bug by looking.
 *
 * So it is arithmetic. `simulate` projects a colour the way a dichromat's cones do
 * (Viénot–Brettel–Mollon 1999, the model the accessibility tools use), and separations are
 * measured in ΔE2000, because two hexes far apart in RGB can be perceptually identical and
 * a check that cannot tell the difference is theatre.
 *
 * WHAT IT CAUGHT ON THE FIRST RUN, all of it shipped, none of it visible to us:
 *
 *   · `accent` and `status.unknown` were THE SAME HEX. "You are here" and "no rule found"
 *     were one colour, ΔE 0.0.
 *   · `restricted` was #D98C00 — 2.73:1 as pill text. A plain WCAG AA failure.
 *   · the stocking ramp was three greens, ΔE 2.4 apart under protanopia.
 *   · `highlight` (the water you tapped) sat ΔE 3.5 from ordinary mapped water under
 *     protanopia, so selecting a river did nothing a protanope could see.
 *
 * WHY THE NUMBERS ARE WHAT THEY ARE. Two published schemes do the work: Okabe & Ito's
 * Color Universal Design set for the categorical hues, cividis for the sequential ramp.
 * Anchoring on them matters more than any threshold here — a set of hand-picked hexes can
 * pass a test written by the same person who picked them.
 *
 * THE ONE THING THIS TEST CANNOT DO is make colour carry the answer alone, and it must not:
 * every legend swatch sits beside its WORD, and the closed areas carry a HATCH. The
 * thresholds below are what makes colour a good SECOND channel, never the only one.
 */
import { describe, expect, it } from "vitest";
import { resolveTheme } from "@app/map";
import { CVD, DARK, LIGHT, type Palette } from "../packages/ui-native/src/theme";
import { contrast, deltaE, parseHex, simulate } from "./cvd";

/** A status colour drawn as TEXT over a 13% tint of itself — so that is the ground. */
function tintOf(colour: string, card: string): string {
  const [r, g, b] = parseHex(colour), [R, G, B] = parseHex(card);
  const mix = (x: number, y: number) => Math.round(x * 0.133 + y * (1 - 0.133));
  return `#${[mix(r!, R!), mix(g!, G!), mix(b!, B!)]
    .map((v) => v.toString(16).padStart(2, "0")).join("")}`;
}

/**
 * The worst separation across normal vision and all three dichromacies.
 *
 * Red/green (protan, deutan) and blue/yellow (tritan) are reported apart because they are
 * not equally common — roughly 8% of men against 0.01% of people — and a palette that has
 * to serve both perfectly cannot exist: the two demands pull in opposite directions on the
 * hue circle. Red/green carries the higher floor; tritan gets a real but lower one.
 */
function separation(a: string, b: string) {
  return {
    rg: Math.min(deltaE(a, b),
                 deltaE(simulate(a, "protan"), simulate(b, "protan")),
                 deltaE(simulate(a, "deutan"), simulate(b, "deutan"))),
    tritan: deltaE(simulate(a, "tritan"), simulate(b, "tritan")),
  };
}

/** Every pair in a set must clear the floor, and the message must name the pair. */
function allPairs(set: [string, string][], rgFloor: number, tritanFloor: number) {
  for (let i = 0; i < set.length; i++)
    for (let j = i + 1; j < set.length; j++) {
      const [na, ca] = set[i]!, [nb, cb] = set[j]!;
      const s = separation(ca, cb);
      expect(s.rg, `${na}(${ca}) vs ${nb}(${cb}) under red/green deficiency`)
        .toBeGreaterThanOrEqual(rgFloor);
      expect(s.tritan, `${na}(${ca}) vs ${nb}(${cb}) under tritanopia`)
        .toBeGreaterThanOrEqual(tritanFloor);
    }
}

const cvdMap = resolveTheme("cvd") as Record<string, string>;
const status = (p: Palette): [string, string][] =>
  [["closed", p.closed], ["restricted", p.restricted], ["open", p.open]];

describe("the colour-blind theme", () => {
  it("keeps the three status colours apart under every deficiency", () => {
    allPairs(status(CVD), 20, 18);
  });

  it("never paints two different answers the same colour", () => {
    /*
     * THE BUG THIS EXISTS FOR. `accent` was #5F26E0 and so was `status.unknown`, because
     * CVD was `{...LIGHT, ...outcomes("cvd")}` and the cvd theme had picked the light
     * theme's accent for "unknown". Each was defensible alone. Together they meant the
     * marker for the spot you tapped was the colour of "we have no rule for this water".
     */
    const meanings: [string, string][] = [
      ...status(CVD), ["accent", CVD.accent], ["live", CVD.live], ["quiet", CVD.quiet],
    ];
    for (let i = 0; i < meanings.length; i++)
      for (let j = i + 1; j < meanings.length; j++)
        expect(meanings[i]![1].toLowerCase(),
               `${meanings[i]![0]} and ${meanings[j]![0]} are the same colour`)
          .not.toBe(meanings[j]![1].toLowerCase());
  });

  it("draws every status colour as readable text on its own tint", () => {
    /*
     * 4.5:1, WCAG AA — the pill's word is 11.5px bold, which is not "large text" by any
     * definition, so the relaxed 3:1 does not apply. This is the constraint that shapes the
     * whole palette: it caps every outcome colour's lightness, and separation then has to
     * come from spreading them DOWN through the dark half of the range.
     */
    for (const p of [LIGHT, DARK, CVD] as const)
      for (const [name, colour] of status(p))
        expect(contrast(colour, tintOf(colour, p.card)), `${name} (${colour}) as pill text`)
          .toBeGreaterThanOrEqual(4.5);
  });

  it("keeps the donor identity hues distinct from each other and from the marker", () => {
    /*
     * These say "this pin is that row" and nothing else, so being confusable is their only
     * possible failure. `accent` joins the set because it is the "you are here" marker
     * standing among the donor pins on the same small map.
     */
    allPairs([...CVD.donor.map((c, i) => [`donor${i + 1}`, c] as [string, string]),
              ["accent", CVD.accent]], 17, 14);
  });

  it("gives every donor mark enough contrast to be seen at all", () => {
    // Fills, not text: WCAG 1.4.11 non-text contrast, 3:1.
    for (const [i, c] of CVD.donor.entries())
      expect(contrast(c, CVD.card), `donor${i + 1} (${c}) on the card`)
        .toBeGreaterThanOrEqual(3);
    expect(contrast(CVD.quiet, CVD.card), "quiet").toBeGreaterThanOrEqual(3);
  });

  it("orders the stocking ramp by lightness, so no deficiency can flatten it", () => {
    /*
     * A RAMP IS NOT A PALETTE. Recency is ordered, so the reader has to be able to say which
     * of two swatches is more recent — which hue alone cannot do for anybody, colour-blind
     * or not. cividis carries the order in LIGHTNESS, which survives every deficiency
     * because none of them touches luminance much.
     */
    const L = CVD.stock.map((c) => contrast(c, CVD.card));
    for (let i = 0; i < L.length - 1; i++)
      expect(L[i]!, `stock step ${i + 1} must be darker than step ${i + 2}`)
        .toBeGreaterThan(L[i + 1]!);
    for (let i = 0; i < CVD.stock.length - 1; i++) {
      const s = separation(CVD.stock[i]!, CVD.stock[i + 1]!);
      expect(s.rg, `stock ${i + 1}/${i + 2} under red/green`).toBeGreaterThanOrEqual(10);
      expect(s.tritan, `stock ${i + 1}/${i + 2} under tritanopia`).toBeGreaterThanOrEqual(10);
    }
  });

  it("separates the water you tapped from water you did not", () => {
    /*
     * `highlight` at #c2187a sat ΔE 3.5 from `water.mapped` under protanopia. Tapping a
     * river is the app's most basic gesture and it had no visible result for a protanope.
     */
    allPairs([["highlight", cvdMap["color.highlight"]!],
              ["mapped", cvdMap["color.water.mapped"]!],
              ["unmapped", cvdMap["color.water.unmapped"]!],
              ["ungauged", cvdMap["color.water.ungauged"]!]], 15, 12);
  });

  it("keeps the access overlays apart — they are the ones you act on", () => {
    allPairs([["park", cvdMap["color.park.fill"]!],
              ["reserve", cvdMap["color.reserve.fill"]!],
              ["noaccess", cvdMap["color.noaccess.fill"]!],
              ["wetland", cvdMap["color.wetland.fill"]!],
              ["indigenous", cvdMap["color.indigenous.fill"]!]], 15, 15);
  });

  it("keeps the flow ramp readable step to step", () => {
    const flow = [1, 2, 3, 4, 5, 6, 7].map((i) =>
      [`f${i}`, cvdMap[`color.flow.f${i}`]!] as [string, string]);
    for (let i = 0; i < flow.length - 1; i++) {
      const s = separation(flow[i]![1], flow[i + 1]![1]);
      expect(s.rg, `${flow[i]![0]}/${flow[i + 1]![0]} under red/green`)
        .toBeGreaterThanOrEqual(10);
    }
    /* There is no no-baseline colour to hold apart any more. `color.flow.nobaseline` was a
       purple no layer drew — the sentinel became a DASH (`stream-nobaseline`) — and in the
       dark theme it was the accent's exact hex. It was deleted rather than defended. */
  });

  it("holds the light and dark themes to the same text contrast", () => {
    /*
     * Found while measuring the colour-blind theme: `faint` was #99A0A6, 2.65:1 on the card.
     * That is a WCAG AA failure in the DEFAULT theme, affecting every reader — the
     * colour-blind audit was simply the first thing that looked.
     */
    for (const [name, p] of [["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const) {
      expect(contrast(p.ink, p.card), `${name} ink`).toBeGreaterThanOrEqual(4.5);
      expect(contrast(p.sub, p.card), `${name} sub`).toBeGreaterThanOrEqual(4.5);
      expect(contrast(p.faint, p.card), `${name} faint`).toBeGreaterThanOrEqual(4.5);
      expect(contrast(p.accent, p.card), `${name} accent`).toBeGreaterThanOrEqual(4.5);
    }
  });
});
