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
 * So this measures ALL FIVE as they are drawn, in all three themes, as the WORST of normal vision and protan / deutan /
 * tritan under Viénot 1999 AND Machado 2009:
 *   the owner's pair  closed / own as tokens, thin lines, lake fills and over satellite, held ABOVE candidate B (light /
 *                     cvd) and above the palette before T47 (dark) — the pair he complained about must be the pair that
 *                     moved most, not the one that gave ground for the others (review of the first D);
 *   water             closed / base and own / base, the same four ways;
 *   permit            restricted against closed / own / base: the hatch is thin strokes laid over the water, so tokens,
 *                     thin lines and satellite;
 *   temperature       open against restricted and closed: these meet ONLY as gauge dots, 5 px filled circles in a 2 px
 *                     paper stroke (runtime-style.ts `gauge-dot`), so the dot's pixels are the token — measured as tokens,
 *                     never as a line antialiased into a ground that is not under them;
 *   boundary          the region and unit boundaries against every status as tokens, and against every status the map
 *                     draws as a line (all but open) as lines; and held neutral, so a boundary never reads as a closure;
 *   ink               every status against body ink — a sixth meaning on the card — at the one-meaning floor in normal
 *                     vision and the chrome floor in every vision, so a release word or a restricted pill never reads as
 *                     plain text (the first D's near-black restricted was 18.3 / 10.9 from ink, 1.08:1).
 * Palette D (handoff/PLAN/app-data/status-colours/T47-colours.md, part 2) was searched to maximise exactly these minima;
 * the floors below are what D reaches, rounded down to 0.5, so the palette may move only to something at least as
 * distinct. Where a floor sits under the ticket's 20 it is because no palette inside the house rules reaches 20 there
 * (search-d.mjs proves it; T47-colours.md part 2 says which pair binds).
 */
import { describe, expect, it } from "vitest";
import { resolveTheme } from "@app/map";
import { deltaE, MODELS, parseHex, worstSeparation } from "./cvd";

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
/** The statuses the map draws as LINES (water, and the permit hatch). Open is a dot and a card word, never a line. */
const LINES: readonly Status[] = ["closed", "own", "base", "restricted"];
/** The water pairs besides the owner's (closed / own has its own floors, above). */
const WATER: readonly [Status, Status][] = [["closed", "base"], ["own", "base"]];
const PERMIT: readonly [Status, Status][] = [["restricted", "closed"], ["restricted", "own"], ["restricted", "base"]];
/** Own and base are never beside open (the temperature view draws no status water): held only to one meaning per colour. */
const TEMPERATURE: readonly [Status, Status][] = [["open", "restricted"], ["open", "closed"]];

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
 * THE FLOORS, per theme: what palette D reaches, rounded down to 0.5.
 *
 * `owner*` is closed / own. Light and the colour-blind palette sit above candidate B on all four (B: light 28.4 / 20.3 /
 * 23.6 / 16.9, cvd 27.3 / 18.5 / 27.2 / 16.3) and dark above the palette before T47 (20.8 / 19.6 / 20.6 / 17.6). The thin
 * line cannot go much further in light: own must stay 3:1 on the white card, which caps how light it gets, and the
 * search's best closed/own line with every other pair let go is 21.4.
 *
 * Dark closed / base as a thin line is the one water pair under where it was before T47 (19.1; before 22.6, B 20.7):
 * the dark base had to go from sky blue to deep blue to stand off the yellow own and the rose closed, and holding closed /
 * base at 20 as a line while closed / own clears the before-T47 numbers finds no palette (search-d.mjs T47_TERM_MIN).
 *
 * Under the ticket's 20: thin lines and satellite for most pairs (no palette reaches 20 as a 0.6-coverage line for every
 * pair in any theme); the permit and temperature pairs, because restricted, closed and open must all be pill text
 * (4.5:1) on the card AND stand 20 / 15 off body ink, which leaves restricted no room to go darker than closed and open
 * (the first D bought 25 there with a near-black restricted the card could not tell from ink); and the dark boundary,
 * which no neutral grey lifts past 15 against the dark card's light statuses (search-d.mjs with T47_BND_TOKEN_CAP=20
 * finds no dark palette, ink rules on or off). Light and cvd boundaries hold the ticket's 20 as tokens.
 */
const FLOOR: Record<Theme, Record<string, number>> = {
  light: { ownerToken: 29, ownerLine: 21, ownerLake: 25, ownerSat: 17.5,
           waterToken: 45.5, waterLine: 35, waterLake: 35, waterSat: 35,
           permitToken: 22, permitLine: 16, permitSat: 15,
           temperature: 15.5,
           boundaryToken: 20, boundaryLine: 13 },
  dark:  { ownerToken: 21.5, ownerLine: 21, ownerLake: 22, ownerSat: 18.5,
           waterToken: 29.5, waterLine: 19, waterLake: 23.5, waterSat: 18,
           permitToken: 15, permitLine: 13.5, permitSat: 10.5,
           temperature: 17.5,
           boundaryToken: 15, boundaryLine: 15.5 },
  cvd:   { ownerToken: 29, ownerLine: 21, ownerLake: 29.5, ownerSat: 17.5,
           waterToken: 49, waterLine: 34, waterLake: 47, waterSat: 33.5,
           permitToken: 23, permitLine: 17.5, permitSat: 16,
           temperature: 20,
           boundaryToken: 20, boundaryLine: 12.5 },
};
/** One meaning per colour: every two statuses, adjacent or not, and every status against body ink, in normal vision. */
const MEANING_FLOOR = 20;
/** A status word is never body text to any reader: every status against body ink, worst vision. */
const INK_FLOOR = 15;
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
  const name = (a: Status, b: Status) => `${a} ${colour(a)} vs ${b} ${colour(b)}`;
  /** Tokens, thin lines over the theme's grounds, thin lines over satellite, and (water) lake fills. */
  const asDrawn = (a: Status, b: Status, k: string, lake: boolean) => {
    hold(`${name(a, b)} as tokens`, worst(colour(a), colour(b)), f[`${k}Token`]!);
    hold(`${name(a, b)} as thin lines`, drawn(colour(a), colour(b), GROUND[th], LINE, LINE), f[`${k}Line`]!);
    hold(`${name(a, b)} over satellite`, drawn(colour(a), colour(b), SATELLITE, LINE, LINE), f[`${k}Sat`]!);
    if (lake) hold(`${name(a, b)} as lake fills`, drawn(colour(a), colour(b), [LAKE_GROUND[th]], lakeA, lakeA), f[`${k}Lake`]!);
  };
  asDrawn("closed", "own", "owner", true);
  for (const [a, b] of WATER) asDrawn(a, b, "water", true);
  for (const [a, b] of PERMIT) asDrawn(a, b, "permit", false);
  for (const [a, b] of TEMPERATURE) hold(`${name(a, b)} as gauge dots`, worst(colour(a), colour(b)), f.temperature!);
  for (const edge of ["color.region.edge", "color.mu.edge"])
    for (const s of STATUSES) {
      const [e, c] = [t[edge] as string, colour(s)];
      hold(`${edge} ${e} vs ${s} ${c} as tokens`, worst(e, c), f.boundaryToken!);
      if (LINES.includes(s)) hold(`${edge} vs ${s} as lines`, drawn(e, c, GROUND[th], EDGE, LINE), f.boundaryLine!);
    }
  for (let i = 0; i < STATUSES.length; i++)
    for (let j = i + 1; j < STATUSES.length; j++) {
      const [a, b] = [STATUSES[i]!, STATUSES[j]!];
      const d = deltaE(colour(a), colour(b));
      if (d < MEANING_FLOOR) out.push(`${th}: ${a} ${colour(a)} and ${b} ${colour(b)} are one colour to anybody: ${d.toFixed(1)} < ${MEANING_FLOOR}`);
    }
  const ink = t["color.ui.ink"] as string;
  for (const s of STATUSES) {
    const d = deltaE(colour(s), ink);
    if (d < MEANING_FLOOR) out.push(`${th}: ${s} ${colour(s)} reads as body ink ${ink}: ${d.toFixed(1)} < ${MEANING_FLOOR}`);
    hold(`${s} ${colour(s)} vs body ink ${ink}`, worst(colour(s), ink), INK_FLOOR);
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
/**
 * The first palette D (2026-10-22, superseded the same day by review): it maximised the minimum of every pair, so the
 * owner's pair gave ground to the others (light closed/own as a line 19.5, under B's 20.3; dark no better than before
 * T47), its restricted was a near-black the card could not tell from ink, and the cvd boundary fell under 20.
 */
const FIRST_D: Record<Theme, Record<string, string>> = de({
  light: { "color.status.closed": "~8F0901", "color.status.own": "~C68900", "color.status.base": "~6E87E9",
           "color.status.restricted": "~1D002B", "color.status.open": "~1B6A91" },
  dark:  { "color.status.closed": "~F4505E", "color.status.own": "~FFD208", "color.status.base": "~0364E2",
           "color.status.restricted": "~B37CA8", "color.status.open": "~94E1FF" },
  cvd:   { "color.status.closed": "~8F0901", "color.status.own": "~C68900", "color.status.base": "~6E87E9",
           "color.status.restricted": "~1D002B", "color.status.open": "~1154C3" },
});

describe("status colours, as drawn (T47)", () => {
  for (const th of THEMES)
    it(`keeps every status apart from its neighbours, off body ink, and every boundary off every status — ${th}`, () => {
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
    expect(light).toContain(de("light: closed ~940018 vs own ~B37B00 as thin lines 18.9 < 21 (deutan (vienot) on ~d9cecc)"));
    expect(light).toContain(de("light: closed ~940018 vs own ~B37B00 as lake fills 17.0 < 25 (tritan (vienot) on ~82cdd9)"));
    expect(light).toContain(de("light: color.region.edge ~8E2F52 is a hue, not a neutral (RGB spread 95)"));
    expect(failures("cvd", resolveTheme("cvd", TODAY.cvd)))
      .toContain(de("cvd: closed ~C80D00 vs own ~B37B00 as tokens 8.6 < 29 (deutan (vienot))"));
    const dark = failures("dark", resolveTheme("dark", TODAY.dark));
    expect(dark).toContain(de("dark: closed ~FC4164 vs own ~F0E442 as thin lines 19.6 < 21 (deutan (vienot) on ~221e1e)"));
    expect(dark).toContain(de("dark: open ~48b07a vs closed ~FC4164 as gauge dots 7.8 < 17.5 (deutan (machado))"));
  });

  it("fails candidate B, which fixed the boundaries but left restricted and open beside closed (mutation pin)", () => {
    const b = failures("light", resolveTheme("light", B_LIGHT));
    // B's owner's pair is what D has to beat, and does
    expect(b).toContain(de("light: closed ~940018 vs own ~D68100 as thin lines 20.3 < 21 (tritan (vienot) on ~e9e7e1)"));
    expect(b).toContain(de("light: closed ~940018 vs own ~D68100 as lake fills 23.6 < 25 (tritan (vienot) on ~82cdd9)"));
    expect(b).toContain(de("light: restricted ~8f5c00 vs closed ~940018 as tokens 9.0 < 22 (deutan (vienot))"));
    expect(b).toContain(de("light: open ~1D7741 vs restricted ~8f5c00 as gauge dots 8.9 < 15.5 (protan (machado))"));
  });

  it("fails the first palette D: owner's pair under B, restricted reads as ink, cvd boundary under 20 (mutation pin)", () => {
    const light = failures("light", resolveTheme("light", FIRST_D.light));
    expect(light).toContain(de("light: closed ~8F0901 vs own ~C68900 as thin lines 19.5 < 21 (deutan (vienot) on ~e9e7e1)"));
    expect(light).toContain(de("light: restricted ~1D002B reads as body ink ~15181C: 18.3 < 20"));
    expect(light).toContain(de("light: restricted ~1D002B vs body ink ~15181C 10.9 < 15 (deutan (machado))"));
    const dark = failures("dark", resolveTheme("dark", FIRST_D.dark));
    expect(dark).toContain(de("dark: closed ~F4505E vs own ~FFD208 as thin lines 19.7 < 21 (deutan (vienot) on ~221e1e)"));
    expect(dark).toContain(de("dark: open ~94E1FF reads as body ink ~F0F2F0: 19.5 < 20"));
    expect(failures("cvd", resolveTheme("cvd", FIRST_D.cvd)))
      .toContain(de("cvd: color.region.edge ~8B857F vs own ~C68900 as tokens 18.9 < 20 (tritan (vienot))"));
  });
});
