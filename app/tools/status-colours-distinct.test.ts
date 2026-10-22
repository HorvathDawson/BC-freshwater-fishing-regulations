/**
 * STATUS COLOURS YOU CAN TELL APART, AS DRAWN (T47).
 *
 * tools/water-status-cvd.test.ts holds closed / own / base >= 20 apart AS TOKENS, in light and dark. It passed while the
 * owner could not tell "has its own regulations" from "closed" on the map ("has regs and closed are very similar",
 * 2026-10-21), because a status is not drawn as a token: it is a 1-2 px antialiased line laid over land, forest and
 * water, a lake fill at `opacity.lake` over the basemap's water, or a line over satellite imagery. And it held only three
 * of the five: restricted (the permit-area hatch, drawn over every water) and open (the cool temperature dot beside the
 * warm = restricted and critical = closed ones) were 8-9 apart from their neighbours under deuteranopia.
 *
 * So this measures ALL FIVE as drawn, in all three themes, as the WORST of normal vision and protan / deutan / tritan
 * under Viénot 1999 AND Machado 2009, and holds the reference lines (region and management-unit boundaries) neutral and
 * off every status, so a boundary can never read as a closure. Palette D (handoff/PLAN/app-data/status-colours/
 * T47-colours.md, part 2) was searched to maximise exactly these minima; the floors below are what D reaches, so the
 * palette may move only to something at least as distinct. Where a floor sits under the ticket's 20 it is because no
 * palette inside the house rules reaches 20 there (the search proves it; T47-colours.md "D" says which pair binds).
 */
import { describe, expect, it } from "vitest";
import { resolveTheme } from "@app/map";
import { MODELS, deltaE, parseHex, worstSeparation } from "./cvd";

/**
 * Colours below are written ENCODED, `~` for the hash (the house trick from check-colours.test.ts), so this file holds no
 * colour literal and the one-palette gate needs no exception for it. They are measurement inputs and old palettes, never
 * paint: the grounds were sampled from screenshots, the mutants are the palettes this gate exists to refuse.
 */
const de = <T>(x: T): T => JSON.parse(JSON.stringify(x).replace(/~/g, "#")) as T;

type Theme = "light" | "dark" | "cvd";
const THEMES: Theme[] = ["light", "dark", "cvd"];
const STATUSES = ["closed", "own", "base", "restricted", "open"] as const;
type Status = (typeof STATUSES)[number];
const WATER: readonly Status[] = ["closed", "own", "base"];
/**
 * THE PAIRS A READER SEES SIDE BY SIDE ON THE MAP. Water: closed / own / base as stream lines and lake fills. Permit:
 * restricted is the permit-area hatch laid over whatever water runs through it. Temperature: the gauge dots, cool = open,
 * warm = restricted, critical = closed, side by side. Own and base are never beside open (the temperature view draws no
 * status water), so those two pairs are held only to "one meaning per colour" below.
 */
const ADJACENT: readonly [Status, Status][] = [["closed", "own"], ["closed", "base"], ["own", "base"],
  ["restricted", "closed"], ["restricted", "own"], ["restricted", "base"], ["open", "restricted"], ["open", "closed"]];

/**
 * The grounds a LINE is drawn on, sampled from the T47 baseline screenshots (scripts/colour-math.mjs GROUNDS): light land,
 * forest, river, meadow; dark land, hollow, river; satellite conifer, olive, shadow, scrub, water, snow. The colour-blind
 * palette rides on the light basemap. No status-coloured pixel is a ground.
 */
const GROUND: Record<Theme, string[]> = de({
  light: ["~d9cecc", "~b0caa8", "~82cdd9", "~e9e7e1"],
  dark: ["~372e2f", "~221e1e", "~31353f"],
  cvd: ["~d9cecc", "~b0caa8", "~82cdd9", "~e9e7e1"],
});
const SATELLITE = de(["~28290d", "~353b1a", "~1c1a0b", "~61512c", "~2A333B", "~e6e8ea"]);
/** The ground under a status LAKE: the basemap's own water, which the status lake layer is drawn on top of. */
const LAKE_GROUND: Record<Theme, string> = de({ light: "~82cdd9", dark: "~31353f", cvd: "~82cdd9" });
/** Mean coverage of a thin status line's pixels; a boundary is wider (3 / 1.2 px), so it covers more. */
const LINE = 0.6, EDGE = 0.8;

/**
 * THE FLOORS, per theme: what palette D reaches, rounded down to 0.5. `water` = closed / own / base pairs, `other` = the
 * permit and temperature pairs. Light and the colour-blind palette clear the ticket's 20 for every status pair as tokens;
 * dark does for the water pairs (and holds closed/own where it was before T47), and its restricted/base pair stops at 15.9 — a restricted light enough to read as pill
 * text on the dark card (4.5:1) meets the dark-card base blue under deuteranopia, whatever the hue (search-d.mjs with
 * T47_TOKEN_MIN=20 finds no dark palette at all). No palette reaches 20 as thin lines in any theme.
 */
const FLOOR: Record<Theme, Record<string, number>> = {
  light: { waterToken: 29, waterLine: 19, waterLake: 24, waterSat: 17.5, otherToken: 25.5, otherLine: 18.5, otherSat: 16,
           boundaryToken: 19.5, boundaryLine: 12.5 },
  dark:  { waterToken: 20.5, waterLine: 19.5, waterLake: 20.5, waterSat: 16.5, otherToken: 15.5, otherLine: 13.5, otherSat: 11,
           boundaryToken: 15, boundaryLine: 9.5 },
  cvd:   { waterToken: 29, waterLine: 19, waterLake: 29, waterSat: 17.5, otherToken: 26.5, otherLine: 19, otherSat: 17.5,
           boundaryToken: 18.5, boundaryLine: 14.5 },
};
/** One meaning per colour: every two statuses, adjacent or not, in normal vision. */
const MEANING_FLOOR = 20;
/** A boundary is a neutral: grey or ink, its RGB channels within this of each other. */
const NEUTRAL_SPREAD = 16;

const over = (fill: string, under: string, a: number) => {
  const [f, u] = [parseHex(fill), parseHex(under)];
  return `#${f.map((c, i) => Math.round(c * a + u[i]! * (1 - a)).toString(16).padStart(2, "0")).join("")}`;
};
const worst = (a: string, b: string) => worstSeparation(a, b, MODELS);
/** The worst separation of a and b drawn at coverage ca / cb over every ground. */
const drawn = (a: string, b: string, grounds: string[], ca: number, cb: number) =>
  grounds.map((g) => ({ g, ...worst(over(a, g, ca), over(b, g, cb)) })).sort((x, y) => x.dE - y.dE)[0]!;

/**
 * Every floor a theme's tokens break, one whole sentence each (pair, colours, how it is drawn, the number, the reader).
 * Empty = the palette passes. A pure function of the tokens, so the mutation pins below run it on other palettes.
 */
function failures(th: Theme, t: Record<string, unknown>): string[] {
  const out: string[] = [];
  const f = FLOOR[th];
  const colour = (s: Status) => t[`color.status.${s}`] as string;
  const hold = (what: string, x: { dE: number; under: string; g?: string }, floor: number) => {
    if (x.dE < floor) out.push(`${th}: ${what} ${x.dE.toFixed(1)} < ${floor} (${x.under}${x.g ? ` on ${x.g}` : ""})`);
  };
  const lakeA = t["opacity.lake"] as number;
  for (const [a, b] of ADJACENT) {
    const k = WATER.includes(a) && WATER.includes(b) ? "water" : "other";
    const name = `${a} ${colour(a)} vs ${b} ${colour(b)}`;
    hold(`${name} as tokens`, worst(colour(a), colour(b)), f[`${k}Token`]!);
    hold(`${name} as thin lines`, drawn(colour(a), colour(b), GROUND[th], LINE, LINE), f[`${k}Line`]!);
    hold(`${name} over satellite`, drawn(colour(a), colour(b), SATELLITE, LINE, LINE), f[`${k}Sat`]!);
    if (k === "water") hold(`${name} as lake fills`, drawn(colour(a), colour(b), [LAKE_GROUND[th]], lakeA, lakeA), f.waterLake!);
  }
  for (const edge of ["color.region.edge", "color.mu.edge"])
    for (const s of STATUSES) {
      const [e, c] = [t[edge] as string, colour(s)];
      hold(`${edge} ${e} vs ${s} ${c} as tokens`, worst(e, c), f.boundaryToken!);
      hold(`${edge} vs ${s} as lines`, drawn(e, c, GROUND[th], EDGE, LINE), f.boundaryLine!);
    }
  for (let i = 0; i < STATUSES.length; i++)
    for (let j = i + 1; j < STATUSES.length; j++) {
      const [a, b] = [STATUSES[i]!, STATUSES[j]!];
      const d = deltaE(colour(a), colour(b));
      if (d < MEANING_FLOOR) out.push(`${th}: ${a} ${colour(a)} and ${b} ${colour(b)} are one colour to anybody: ${d.toFixed(1)} < ${MEANING_FLOOR}`);
    }
  for (const edge of ["color.region.edge", "color.mu.edge"]) {
    const v = parseHex(t[edge] as string);
    const spread = Math.max(...v) - Math.min(...v);
    if (spread > NEUTRAL_SPREAD) out.push(`${th}: ${edge} ${t[edge]} is a hue, not a neutral (RGB spread ${spread})`);
  }
  return out;
}

/** Today's palette before T47 (the owner's complaint) — the mutant every floor was raised against. */
const TODAY: Record<Theme, Record<string, string | number | number[]>> = de({
  light: { "color.status.closed": "~940018", "color.status.own": "~B37B00", "color.status.base": "~0072B2",
           "color.status.restricted": "~8f5c00", "color.status.open": "~1D7741", "color.region.edge": "~8E2F52",
           "color.mu.edge": "~8A7A62", "opacity.lake": 0.4 },
  dark:  { "color.status.closed": "~FC4164", "color.status.own": "~F0E442", "color.status.base": "~56B4E9",
           "color.status.restricted": "~e8a733", "color.status.open": "~48b07a", "color.region.edge": "~E29AB4",
           "color.mu.edge": "~A6947A" },
  cvd:   { "color.status.closed": "~C80D00", "color.status.own": "~B37B00", "color.status.base": "~0072B2",
           "color.status.restricted": "~5C0453", "color.status.open": "~0063D2", "color.region.edge": "~8E2F52",
           "color.mu.edge": "~8A7A62" },
});
/** Candidate B (the part-1 recommendation): boundaries fixed, but restricted and open left where they were. */
const B_LIGHT = de({ "color.status.closed": "~940018", "color.status.own": "~D68100", "color.status.base": "~0072B2",
  "color.status.restricted": "~8f5c00", "color.status.open": "~1D7741" });

describe("status colours, as drawn (T47)", () => {
  for (const th of THEMES)
    it(`keeps every status apart from its neighbours and every boundary off every status — ${th}`, () => {
      expect(failures(th, resolveTheme(th))).toEqual([]);
    });

  it("tells the region from the unit by width and dash, never by colour", () => {
    for (const th of THEMES) {
      const t = resolveTheme(th) as Record<string, unknown>;
      expect([th, t["color.mu.edge"]]).toEqual([th, t["color.region.edge"]]);
      expect([th, (t["width.region.edge"] as number) >= 2 * (t["width.admin.edge"] as number)]).toEqual([th, true]);
      expect([th, t["dash.mu"]]).toEqual([th, [1, 2.5]]);
    }
  });

  it("fails today's palette on the owner's pair, as drawn (mutation pin)", () => {
    const light = failures("light", resolveTheme("light", TODAY.light));
    expect(light).toContain(de("light: closed ~940018 vs own ~B37B00 as thin lines 18.9 < 19 (deutan (vienot) on ~d9cecc)"));
    expect(light).toContain(de("light: closed ~940018 vs own ~B37B00 as lake fills 17.0 < 24 (tritan (vienot) on ~82cdd9)"));
    expect(light).toContain(de("light: color.region.edge ~8E2F52 is a hue, not a neutral (RGB spread 95)"));
    expect(failures("cvd", resolveTheme("cvd", TODAY.cvd)))
      .toContain(de("cvd: closed ~C80D00 vs own ~B37B00 as tokens 8.6 < 29 (deutan (vienot))"));
    expect(failures("dark", resolveTheme("dark", TODAY.dark)))
      .toContain(de("dark: open ~48b07a vs closed ~FC4164 as tokens 7.8 < 15.5 (deutan (machado))"));
  });

  it("fails candidate B, which fixed the boundaries but left restricted and open beside closed (mutation pin)", () => {
    const b = failures("light", resolveTheme("light", B_LIGHT));
    expect(b).toContain(de("light: restricted ~8f5c00 vs closed ~940018 as tokens 9.0 < 25.5 (deutan (vienot))"));
    expect(b).toContain(de("light: open ~1D7741 vs restricted ~8f5c00 as tokens 8.9 < 25.5 (protan (machado))"));
  });
});
