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
 *   temperature       the three bands' dots against each other (cool = open, warm = `color.temp.warm`, critical =
 *                     closed; @app/core `TEMP_BAND_TOKEN`): these meet ONLY as gauge dots, 5 px filled circles in a 2 px
 *                     paper stroke (gaugeDots.ts `gauge-dot`), so the dot's pixels are the token — measured as tokens,
 *                     never as a line antialiased into a ground that is not under them. Warm was `restricted` until
 *                     T83; it is its own hue now, and held off every status (one meaning) and off body ink like one;
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
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { TEMP_BAND_TOKEN, type TempBand } from "@app/core";
import { resolveTheme } from "@app/map";
import { deltaE, lch, MODELS, parseHex, worstSeparation } from "./cvd";

/**
 * Colours below are written ENCODED, `~` for the hash (the house trick from check-colours.test.ts), so this file holds no
 * colour literal and the one-palette gate needs no exception for it. They are measurement inputs and old palettes, never
 * paint: the grounds were sampled from screenshots, the mutants are the palettes this gate exists to refuse.
 */
const de = <T>(x: T): T => JSON.parse(JSON.stringify(x).replace(/~/g, "#")) as T;

type Theme = "light" | "dark" | "cvd";
const THEMES: Theme[] = ["light", "dark", "cvd"];
const STATUSES = ["closed", "own", "base", "restricted", "open", "no_access"] as const;
type Status = (typeof STATUSES)[number];
/** The statuses the map draws as LINES (water, and the permit hatch). Open is a dot and a card word, never a line. */
const LINES: readonly Status[] = ["closed", "own", "base", "restricted", "no_access"];
/** The water pairs besides the owner's (closed / own has its own floors, above). */
const WATER: readonly [Status, Status][] = [["closed", "base"], ["own", "base"]];
const PERMIT: readonly [Status, Status][] = [["restricted", "closed"], ["restricted", "own"], ["restricted", "base"]];
/**
 * (T54, T165) NO PUBLIC ACCESS, the fourth map colour — a dotted grey since T165: a section mostly inside no-access land, drawn as a line or a lake fill
 * beside the three water statuses (and over the closed crimson of the no-access area's own hatch), so it is held to them
 * as tokens, thin lines, lake fills and over satellite; and against the permit hatch as tokens, lines and satellite.
 */
const ACCESS: readonly [Status, Status][] = [["no_access", "closed"], ["no_access", "own"], ["no_access", "base"]];
/**
 * The temperature dots, every pair of bands. Own, base and restricted are never beside them (the temperature view draws no
 * status water and no permit hatch): warm is held off those only to one meaning per colour.
 */
const TEMPERATURE: readonly [TempBand, TempBand][] = [["cool", "warm"], ["warm", "critical"], ["cool", "critical"]];

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
 * TEMPERATURE (T83): the warm dot has its own hue, so the dots are cool / warm / critical (open / `color.temp.warm` /
 * closed). Light and cvd LIFTED past 20 (light 15.9 -> 27.9, cvd 20 -> 25.3: a coral warm stands far off the teal open
 * and the dark red closed for every reader). Dark went the other way, 17.7 -> 15.4, and is a recorded exception below:
 * all three dark pairs sit under 20, and cool / critical (17.7, open vs closed) already did before T83.
 *
 * Every floor under the ticket's 20 is listed, with its reason, in UNDER_20 — a new one fails until it is recorded.
 */
const FLOOR: Record<Theme, Record<string, number>> = {
  light: { ownerToken: 29, ownerLine: 21, ownerLake: 25, ownerSat: 17.5,
           waterToken: 45.5, waterLine: 35, waterLake: 35, waterSat: 35,
           permitToken: 22, permitLine: 16, permitSat: 15,
           temperature: 27.5,
           boundaryToken: 20, boundaryLine: 13,
           accessToken: 25, accessLine: 14, accessLake: 14, accessSat: 13.5,
           accessPermitToken: 17.5, accessPermitLine: 12.5, accessPermitSat: 11.5, accessBoundaryToken: 13.5, accessBoundaryLine: 1 },
  dark:  { ownerToken: 21.5, ownerLine: 21, ownerLake: 22, ownerSat: 18.5,
           waterToken: 29.5, waterLine: 19, waterLake: 23.5, waterSat: 18,
           permitToken: 15, permitLine: 13.5, permitSat: 10.5,
           temperature: 15,
           boundaryToken: 15, boundaryLine: 15.5,
           accessToken: 12.5, accessLine: 8.5, accessLake: 12, accessSat: 8,
           accessPermitToken: 12.5, accessPermitLine: 8, accessPermitSat: 8, accessBoundaryToken: 8.5, accessBoundaryLine: 15.5 },
  cvd:   { ownerToken: 29, ownerLine: 21, ownerLake: 29.5, ownerSat: 17.5,
           waterToken: 49, waterLine: 34, waterLake: 47, waterSat: 33.5,
           permitToken: 23, permitLine: 17.5, permitSat: 16,
           temperature: 25,
           boundaryToken: 20, boundaryLine: 12.5,
           accessToken: 25, accessLine: 14.5, accessLake: 23.5, accessSat: 13.5,
           accessPermitToken: 14, accessPermitLine: 10, accessPermitSat: 8.5, accessBoundaryToken: 17.5, accessBoundaryLine: 2.5 },
};

/** The reasons, once each (T47 part 2, T83 Q16: lift where cheap, else a recorded exception). */
const WHY = {
  satellite: "a 0.6-coverage line over conifer / shadow imagery: the near-black ground pulls every pair together, and no " +
             "palette inside the house rules reaches 20 there for every pair (search-d.mjs)",
  darkBase: "dark closed / base as a line, 19.1 (before T47 22.6): the dark base went from sky blue to deep blue to stand " +
            "off the yellow own and the rose closed; 20 here while closed / own clears before-T47 finds no palette " +
            "(search-d.mjs T47_TERM_MIN)",
  permit: "restricted, closed and open are all pill text (4.5:1 on the card) and must stand 20 / 15 off body ink, which " +
          "leaves restricted no room to go darker; the first D bought 25 with a near-black restricted the card read as ink",
  darkWarm: "every dark dot pair: warm / critical 15.4 and cool / warm 17.8 (T83's own warm hue, owner default Q15: no " +
            "warm hue, LCh h 25-95, light enough to read on the dark map, L >= 55, stands more than 15.4 off the dark rose " +
            "closed for a deuteranope, sRGB cube step 4), and cool / critical 17.7 (open vs closed, untouched by T83 and " +
            "the pair that bound this floor before it; the plum warm stood 36.0 / 22.7 off them but said 'permit area')",
  darkBoundary: "no neutral grey lifts past 15 against the dark card's light statuses (search-d.mjs T47_BND_TOKEN_CAP=20, " +
                "ink rules on or off); the boundary is told apart by dash and width, and is drawn under the water (T83)",
  boundaryLine: "a 3 px grey edge and a 0.6-coverage status line both wash toward a pale ground; a boundary is told apart " +
                "by dash and width, and since T83 it is drawn UNDER the water lines, so where they cross the status wins",
  greyAccess: "no public access is a GREY drawn DOTTED (T165, owner 2026-10-23: 'dotted grey … like under lakes but a " +
              "little more visible'), its own layer (`stream-noaccess`); the stream line under it is hidden. A neutral " +
              "shares no hue with any status, so it parts from them by LIGHTNESS alone, which a thin line over a pale or " +
              "dark ground washes toward; the dot pattern is the main cue and the grey the second (searched over every " +
              "neutral: handoff/PLAN/app-integration/T165-shots/grey.mjs, inside the style build's ΔE76 12). Dark binds " +
              "worst: the rose closed sits at the grey's own lightness for a protanope (8.5 as lines, 12.8 as tokens) — " +
              "every lighter grey meets the boundary and missing greys, every darker one the ground",
  greyPermit: "no public access's grey against the permit hatch's plum / lilac (T165): the hatch is thin strokes laid over " +
              "permit AREAS, never over the no-access land a no-access stream runs through, and it is hatched where the " +
              "stream is dotted",
  greyBoundary: "no public access's grey against the grey boundary (T165): two neutrals, told apart by pattern and weight — " +
                "the boundary is a wide line (3 px region, dashed unit) drawn UNDER the water, the no-access water a " +
                "dotted 1.2-2.8 px line over it — and by lightness in normal vision (light 25+, dark the boundary the lighter)",
} as const;
/** Every floor under 20, by theme and key, and why. Whole: a floor newly under 20, or one lifted past it, fails. */
const UNDER_20: Record<string, string> = {
  "light ownerSat": WHY.satellite, "light permitLine": WHY.permit, "light permitSat": WHY.permit,
  "light boundaryLine": WHY.boundaryLine,
  "light accessLine": WHY.greyAccess, "light accessLake": WHY.greyAccess, "light accessSat": WHY.satellite,
  "light accessPermitToken": WHY.greyPermit, "light accessPermitLine": WHY.greyPermit, "light accessPermitSat": WHY.greyPermit,
  "light accessBoundaryToken": WHY.greyBoundary, "light accessBoundaryLine": WHY.greyBoundary,
  "dark ownerSat": WHY.satellite, "dark waterLine": WHY.darkBase, "dark waterSat": WHY.satellite,
  "dark permitToken": WHY.permit, "dark permitLine": WHY.permit, "dark permitSat": WHY.permit,
  "dark temperature": WHY.darkWarm, "dark boundaryToken": WHY.darkBoundary, "dark boundaryLine": WHY.darkBoundary,
  "dark accessToken": WHY.greyAccess, "dark accessLine": WHY.greyAccess, "dark accessLake": WHY.greyAccess,
  "dark accessSat": WHY.satellite, "dark accessPermitToken": WHY.greyPermit,
  "dark accessPermitLine": WHY.greyPermit, "dark accessPermitSat": WHY.greyPermit,
  "dark accessBoundaryToken": WHY.greyBoundary, "dark accessBoundaryLine": WHY.greyBoundary,
  "cvd ownerSat": WHY.satellite, "cvd permitLine": WHY.permit, "cvd permitSat": WHY.permit,
  "cvd boundaryLine": WHY.boundaryLine,
  "cvd accessLine": WHY.greyAccess, "cvd accessSat": WHY.satellite, "cvd accessPermitToken": WHY.greyPermit,
  "cvd accessPermitLine": WHY.greyPermit, "cvd accessPermitSat": WHY.greyPermit,
  "cvd accessBoundaryToken": WHY.greyBoundary, "cvd accessBoundaryLine": WHY.greyBoundary,
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
  for (const [a, b] of ACCESS) asDrawn(a, b, "access", true);
  asDrawn("no_access", "restricted", "accessPermit", false);
  for (const [a, b] of WATER) asDrawn(a, b, "water", true);
  for (const [a, b] of PERMIT) asDrawn(a, b, "permit", false);
  const dot = (b: TempBand) => t[TEMP_BAND_TOKEN[b]] as string;
  for (const [a, b] of TEMPERATURE)
    hold(`${a} ${dot(a)} vs ${b} ${dot(b)} as gauge dots`, worst(dot(a), dot(b)), f.temperature!);
  // warm is not a status: one meaning per colour, in normal vision, against all five
  for (const s of STATUSES) {
    const d = deltaE(dot("warm"), colour(s));
    if (d < MEANING_FLOOR)
      out.push(`${th}: the warm dot ${dot("warm")} and ${s} ${colour(s)} are one colour to anybody: ${d.toFixed(1)} < ${MEANING_FLOOR}`);
  }
  for (const edge of ["color.region.edge", "color.mu.edge"])
    for (const s of STATUSES) {
      const [e, c] = [t[edge] as string, colour(s)];
      const [bt, bl] = s === "no_access" ? [f.accessBoundaryToken!, f.accessBoundaryLine!] : [f.boundaryToken!, f.boundaryLine!];
      hold(`${edge} ${e} vs ${s} ${c} as tokens`, worst(e, c), bt);
      if (LINES.includes(s)) hold(`${edge} vs ${s} as lines`, drawn(e, c, GROUND[th], EDGE, LINE), bl);
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
  const warmInk = deltaE(dot("warm"), ink);
  if (warmInk < MEANING_FLOOR) out.push(`${th}: the warm dot ${dot("warm")} reads as body ink ${ink}: ${warmInk.toFixed(1)} < ${MEANING_FLOOR}`);
  hold(`the warm dot ${dot("warm")} vs body ink ${ink}`, worst(dot("warm"), ink), INK_FLOOR);
  for (const edge of ["color.region.edge", "color.mu.edge"]) {
    const v = parseHex(t[edge] as string);
    const spread = Math.max(...v) - Math.min(...v);
    if (spread > NEUTRAL_SPREAD) out.push(`${th}: ${edge} ${t[edge]} is a hue, not a neutral (RGB spread ${spread})`);
  }
  return out;
}

/** Before T83 the warm dot was painted with `restricted`: every older palette is measured that way. */
const warmWasRestricted = <T extends Record<string, unknown>>(p: T): T =>
  ({ ...p, "color.temp.warm": p["color.status.restricted"] }) as T;

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
    const light = failures("light", resolveTheme("light", warmWasRestricted(TODAY.light)));
    expect(light).toContain(de("light: closed ~940018 vs own ~B37B00 as thin lines 18.9 < 21 (deutan (vienot) on ~d9cecc)"));
    expect(light).toContain(de("light: closed ~940018 vs own ~B37B00 as lake fills 17.0 < 25 (tritan (vienot) on ~82cdd9)"));
    expect(light).toContain(de("light: color.region.edge ~8E2F52 is a hue, not a neutral (RGB spread 95)"));
    expect(failures("cvd", resolveTheme("cvd", warmWasRestricted(TODAY.cvd))))
      .toContain(de("cvd: closed ~C80D00 vs own ~B37B00 as tokens 8.6 < 29 (deutan (vienot))"));
    const dark = failures("dark", resolveTheme("dark", warmWasRestricted(TODAY.dark)));
    expect(dark).toContain(de("dark: closed ~FC4164 vs own ~F0E442 as thin lines 19.6 < 21 (deutan (vienot) on ~221e1e)"));
    expect(dark).toContain(de("dark: cool ~48b07a vs critical ~FC4164 as gauge dots 7.8 < 15 (deutan (machado))"));
  });

  it("fails candidate B, which fixed the boundaries but left restricted and open beside closed (mutation pin)", () => {
    const b = failures("light", resolveTheme("light", warmWasRestricted(B_LIGHT)));
    // B's owner's pair is what D has to beat, and does
    expect(b).toContain(de("light: closed ~940018 vs own ~D68100 as thin lines 20.3 < 21 (tritan (vienot) on ~e9e7e1)"));
    expect(b).toContain(de("light: closed ~940018 vs own ~D68100 as lake fills 23.6 < 25 (tritan (vienot) on ~82cdd9)"));
    expect(b).toContain(de("light: restricted ~8f5c00 vs closed ~940018 as tokens 9.0 < 22 (deutan (vienot))"));
    expect(b).toContain(de("light: cool ~1D7741 vs warm ~8f5c00 as gauge dots 8.9 < 27.5 (protan (machado))"));
  });

  it("fails the first palette D: owner's pair under B, restricted reads as ink, cvd boundary under 20 (mutation pin)", () => {
    const light = failures("light", resolveTheme("light", warmWasRestricted(FIRST_D.light)));
    expect(light).toContain(de("light: closed ~8F0901 vs own ~C68900 as thin lines 19.5 < 21 (deutan (vienot) on ~e9e7e1)"));
    expect(light).toContain(de("light: restricted ~1D002B reads as body ink ~15181C: 18.3 < 20"));
    expect(light).toContain(de("light: restricted ~1D002B vs body ink ~15181C 10.9 < 15 (deutan (machado))"));
    const dark = failures("dark", resolveTheme("dark", warmWasRestricted(FIRST_D.dark)));
    expect(dark).toContain(de("dark: closed ~F4505E vs own ~FFD208 as thin lines 19.7 < 21 (deutan (vienot) on ~221e1e)"));
    expect(dark).toContain(de("dark: open ~94E1FF reads as body ink ~F0F2F0: 19.5 < 20"));
    expect(failures("cvd", resolveTheme("cvd", warmWasRestricted(FIRST_D.cvd))))
      .toContain(de("cvd: color.region.edge ~8B857F vs own ~C68900 as tokens 18.9 < 20 (tritan (vienot))"));
  });

  it("fails palette D with the warm dot still painted restricted, the plum of the permit hatch (T83 mutation pin)", () => {
    for (const th of THEMES) {
      const t = resolveTheme(th) as Record<string, string>;
      const r = t["color.status.restricted"]!;
      const f = failures(th, resolveTheme(th, { "color.temp.warm": r }));
      expect([th, f.filter((x) => x.includes("warm dot") && x.includes("restricted"))])
        .toEqual([th, [`${th}: the warm dot ${r} and restricted ${r} are one colour to anybody: 0.0 < 20`]]);
    }
    expect(failures("light", resolveTheme("light", { "color.temp.warm": resolveTheme("light")["color.status.restricted"] as string })))
      .toEqual(de([
        "light: cool ~016890 vs warm ~421652 as gauge dots 15.9 < 27.5 (deutan (vienot))",
        "light: warm ~421652 vs critical ~8F0006 as gauge dots 22.1 < 27.5 (tritan (vienot))",
        "light: the warm dot ~421652 and restricted ~421652 are one colour to anybody: 0.0 < 20"]));
  });

  it("records every floor under the ticket's 20, each with its reason (T83)", () => {
    const under = THEMES.flatMap((th) => Object.entries(FLOOR[th]).filter(([, v]) => v < 20).map(([k]) => `${th} ${k}`));
    expect(under).toEqual(Object.keys(UNDER_20));
    expect(Object.values(UNDER_20).filter((why) => why.length < 40)).toEqual([]);
  });

  /*
   * THE DARK TEMPERATURE EXCEPTION, MEASURED (T83 review). Held to the ticket's 20, EVERY dark dot pair fails: warm /
   * critical 15.4 and cool / warm 17.8 (T83's orange), and cool / critical 17.7 — open against closed, which T83 did not
   * touch and which alone bound the dark floor at 17.7 before it (the plum it replaced stood 36.0 / 22.7 off them). The
   * recorded reason must name all three, or the exception hides pairs nobody chose.
   */
  it("every dark temperature dot pair is under 20, and the recorded reason names all three", () => {
    const was = FLOOR.dark.temperature!;
    FLOOR.dark.temperature = 20;
    try {
      expect(failures("dark", resolveTheme("dark")).filter((x) => x.includes("gauge dots"))).toEqual(de([
        "dark: cool ~A7FBB2 vs warm ~FC9C28 as gauge dots 17.8 < 20 (deutan (machado))",
        "dark: warm ~FC9C28 vs critical ~FF3B6D as gauge dots 15.4 < 20 (deutan (vienot))",
        "dark: cool ~A7FBB2 vs critical ~FF3B6D as gauge dots 17.7 < 20 (deutan (vienot))"]));
    } finally { FLOOR.dark.temperature = was; }
    const why = UNDER_20["dark temperature"]!;
    expect(["warm / critical 15.4", "cool / warm 17.8", "cool / critical 17.7"].map((p) => [p, why.includes(p)]))
      .toEqual([["warm / critical 15.4", true], ["cool / warm 17.8", true], ["cool / critical 17.7", true]]);
  });
});

/*
 * NO PUBLIC ACCESS IS A DOTTED GREY (T165, owner 2026-10-23: "maybe the stream color through no access areas should be
 * dotted grey? like under lakes but a little more visible?"). It was a hue of its own — T54's indigo (read as base),
 * T146's spruce / ice cyan (read as "too benign") — and is now a NEUTRAL drawn on its own dotted layer
 * (`stream-noaccess`, paint.ts `noaccessPaint`), the stream line hidden under it. The warning moved to the
 * Regulations page's block (`color.ui.warning`). A grey has no hue to share, so the T146 hue gate (45 degrees off base)
 * became this: the colour is a NEUTRAL (RGB spread within NEUTRAL_SPREAD, like the boundaries), and in normal vision it
 * stands off what it could be read as:
 *   base              the blue every other water wears — as a token, a thin line over every ground, a lake fill;
 *   water.ungauged    the Conditions map's "nothing speaks here" grey (one meaning per colour, MEANING_FLOOR);
 *   water.unmapped    the status mode's own `missing` grey, a SOLID line: told apart by the dots first, lightness second;
 *   the under-lake route  the dotted thread through a lake (`under_lake`: `water.route` at `opacity.route`), which the
 *                     dots are modelled on — told apart by weight and strength (1.2-2.8 px at full strength against
 *                     0.8-2.4 px at 0.35) and, here, by colour as drawn on every ground.
 * The floors are what the built greys reach, rounded down to 0.5 (searched: T165-shots/grey.mjs). The dichromat pairs are
 * held as drawn above (`WHY.greyAccess`). The one low number is dark against the solid missing grey (8.3: the dark
 * boundary, missing and closed colours fill the mid lightnesses, and a darker grey is lost on the dark ground); the
 * missing grey is only tidal water and water outside B.C., and it is a solid line where no access is dotted.
 */
const GREY_NORMAL: Record<Theme, { baseToken: number; baseLine: number; baseLake: number; unmapped: number; route: number }> = {
  light: { baseToken: 31.5, baseLine: 17.5, baseLake: 17.5, unmapped: 22, route: 21.5 },
  dark:  { baseToken: 33.5, baseLine: 24, baseLake: 30.5, unmapped: 8, route: 26 },
  cvd:   { baseToken: 34.5, baseLine: 16, baseLake: 29, unmapped: 22, route: 26.5 },
};
function greyFailures(th: Theme, t: Record<string, unknown>): string[] {
  const f = GREY_NORMAL[th];
  const x = t["color.status.no_access"] as string;
  const out: string[] = [];
  const v = parseHex(x);
  const spread = Math.max(...v) - Math.min(...v);
  if (spread > NEUTRAL_SPREAD) out.push(`${th}: no_access ${x} is a hue, not a grey (RGB spread ${spread})`);
  const b = t["color.status.base"] as string;
  const lakeA = t["opacity.lake"] as number;
  const hold = (what: string, d: number, floor: number) => {
    if (d < floor) out.push(`${th}: no_access ${x} vs ${what}, normal vision ${d.toFixed(1)} < ${floor}`);
  };
  hold(`base ${b} as a token`, deltaE(x, b), f.baseToken);
  hold(`base ${b} as a thin line`, Math.min(...GROUND[th].map((g) => deltaE(over(x, g, LINE), over(b, g, LINE)))), f.baseLine);
  hold(`base ${b} as a lake fill`, deltaE(over(x, LAKE_GROUND[th], lakeA), over(b, LAKE_GROUND[th], lakeA)), f.baseLake);
  hold(`water.ungauged ${t["color.water.ungauged"]}`, deltaE(x, t["color.water.ungauged"] as string), MEANING_FLOOR);
  hold(`water.unmapped ${t["color.water.unmapped"]}`, deltaE(x, t["color.water.unmapped"] as string), f.unmapped);
  const route = t["color.water.route"] as string, ro = t["opacity.route"] as number;
  hold(`the under-lake route ${route} as drawn`,
       Math.min(...GROUND[th].map((g) => deltaE(over(x, g, EDGE), over(route, g, ro)))), f.route);
  return out;
}

/*
 * NO PUBLIC ACCESS IS NOT THE SELECTION (T146 adversarial review). The first T146 build, a magenta #F01C84, sat 10.4
 * (normal vision) from `color.highlight` — the light theme's selection outline, a magenta too (LCh hue 351 against 359):
 * a selected no-access stretch was magenta inside magenta and every unselected no-access tributary read as selected. So no
 * access stays at least ACCESS_HUE degrees of LCh hue off the selection (a grey, chroma under GREY_CHROMA, has no hue to
 * share and skips that line), at least MEANING_FLOOR as tokens, and a thin no-access line at least SELECTION_LINE from the
 * casing beside it over every ground — normal vision, light and dark.
 */
const ACCESS_HUE = 45;
const GREY_CHROMA = 10;
const SELECTION_LINE = 20;
function apartFromSelection(th: "light" | "dark", t: Record<string, unknown>): string[] {
  const [x, s] = [t["color.status.no_access"] as string, t["color.highlight"] as string];
  const out: string[] = [];
  const d = Math.abs(lch(x).h - lch(s).h) % 360;
  const hue = d > 180 ? 360 - d : d;
  if (lch(x).C >= GREY_CHROMA && hue < ACCESS_HUE)
    out.push(`${th}: no_access ${x} is the selection's hue ${s}: ${hue.toFixed(1)} degrees < ${ACCESS_HUE}`);
  const token = deltaE(x, s);
  if (token < MEANING_FLOOR) out.push(`${th}: no_access ${x} vs the selection ${s} as tokens, normal vision ${token.toFixed(1)} < ${MEANING_FLOOR}`);
  const line = Math.min(...GROUND[th].map((g) => deltaE(over(x, g, LINE), over(s, g, EDGE))));
  if (line < SELECTION_LINE)
    out.push(`${th}: no_access ${x} as a thin line vs the selection's casing ${s}, normal vision ${line.toFixed(1)} < ${SELECTION_LINE}`);
  return out;
}

/*
 * OWN KEEPS ITS AMBER (T165: "own regulations" is not moved; the amber is also the page's warning, `color.ui.warning`, a
 * reference to it). Held as hue and strength: LCh hue inside OWN_HUE (Okabe & Ito's orange / yellow) and chroma at least
 * OWN_CHROMA, every theme; and the warning is that same colour.
 */
const OWN_HUE = [50, 100] as const;
const OWN_CHROMA = 60;
function ownIsAmber(th: Theme, t: Record<string, unknown>): string[] {
  const o = t["color.status.own"] as string;
  const { h, C } = lch(o);
  const out: string[] = [];
  if (h < OWN_HUE[0] || h > OWN_HUE[1] || C < OWN_CHROMA)
    out.push(`${th}: own ${o} is not the amber: LCh hue ${h.toFixed(0)}, chroma ${C.toFixed(0)}`);
  if (t["color.ui.warning"] !== o) out.push(`${th}: the warning ${t["color.ui.warning"]} is not own's amber ${o}`);
  return out;
}

describe("no public access is a dotted grey; own keeps its amber (T165)", () => {
  for (const th of THEMES)
    it(`no access is a neutral, apart from base, the ungauged and missing greys and the under-lake route — ${th}`, () => {
      expect(greyFailures(th, resolveTheme(th))).toEqual([]);
    });

  it("fails T54's indigo and T146's spruce and ice cyan: hues, not greys (mutation pin)", () => {
    const hue = (th: Theme, x: string) => greyFailures(th, resolveTheme(th, de({ "color.status.no_access": x })))
      .filter((f) => f.includes("is a hue"));
    expect([hue("light", "~2A00E0"), hue("light", "~00604C"), hue("dark", "~50E4FF")]).toEqual(de([
      ["light: no_access ~2A00E0 is a hue, not a grey (RGB spread 224)"],
      ["light: no_access ~00604C is a hue, not a grey (RGB spread 96)"],
      ["dark: no_access ~50E4FF is a hue, not a grey (RGB spread 175)"]]));
  });

  it("fails a no-access grey painted in the ungauged or the missing grey (mutation pin)", () => {
    for (const th of THEMES) {
      const t = resolveTheme(th) as Record<string, string>;
      for (const k of ["color.water.ungauged", "color.water.unmapped"]) {
        const f = greyFailures(th, resolveTheme(th, { "color.status.no_access": t[k]! }));
        expect([th, k, f.some((x) => x.includes(`vs ${k.slice(6)} `) && x.includes("normal vision 0.0"))]).toEqual([th, k, true]);
      }
    }
  });

  it("stands apart from the selection outline, in normal vision — light and dark (T146 review)", () => {
    expect((["light", "dark"] as const).flatMap((th) => apartFromSelection(th, resolveTheme(th)))).toEqual([]);
  });

  it("fails the first T146 magenta, which read as the selection on the water's map (mutation pin)", () => {
    expect(apartFromSelection("light", resolveTheme("light", de({ "color.status.no_access": "~F01C84" })))).toEqual(de([
      "light: no_access ~F01C84 is the selection's hue ~c2187a: 7.6 degrees < 45",
      "light: no_access ~F01C84 vs the selection ~c2187a as tokens, normal vision 10.4 < 20",
      "light: no_access ~F01C84 as a thin line vs the selection's casing ~c2187a, normal vision 10.3 < 20"]));
  });

  it("own keeps the amber in every theme, and the page's warning is that amber", () => {
    expect(THEMES.flatMap((th) => ownIsAmber(th, resolveTheme(th)))).toEqual([]);
  });

  it("fails own moved off the amber, and a warning with a hex of its own (mutation pin)", () => {
    expect(ownIsAmber("light", resolveTheme("light", de({ "color.status.own": "~207050" })))).toEqual(de([
      "light: own ~207050 is not the amber: LCh hue 161, chroma 34",
      "light: the warning ~D08404 is not own's amber ~207050"]));
  });
});

/*
 * DEPTH CONTOURS STAND OFF EVERY LAKE THEY ARE DRAWN ON (T158). The contours (`contour`, group `depth`: the "Depth" water
 * colouring and the "Depth contours" switch) are a 0.8 px line laid OVER the lake fill. T146 moved the dark no public
 * access to an ice cyan, 4.7 from the dark contour cyan as tokens and 1.0 as drawn: the contours vanished on a no-access
 * lake (Coquitlam Lake with Depth on). Measuring every fill the same way found the Depth colouring's own fill had the
 * same fault in every theme (normal vision dark 5.7, light 4.8, cvd 9.2: a contour on a digitised lake was a cyan on a
 * teal). So the contour is held, as a thin line over each fill (the fill at `opacity.lake` over the basemap's water), off
 * every fill of the lake colourings it is drawn over: Regulations (`status` / `profile`), Depth (`surveyed`) and the
 * plain lake, read from the lake layer itself, so a category added there is held the day it is added.
 *
 * NOT HELD: Stocked. The colour-blind theme's stocking ramp runs from a deep navy to a pale yellow, and no one line colour
 * stands off both ends (the searched best is 17.5 normal / 8.3 worst); light and dark clear it with the contour they have.
 * Conditions (`standing`) never draws contours (the switch is offered on the map and water views only).
 */
const CONTOUR_NORMAL = 15;
const CONTOUR_WORST = 12;
const CONTOUR_OVER = ["plain", "surveyed", "status", "profile"] as const;

/** Every token the lake layer paints in the colourings contours are drawn over (categories and `missing`). */
function contourGrounds(): string[] {
  const src = JSON.parse(readFileSync(new URL("../packages/map/style/layers.source.json", import.meta.url), "utf8")) as
    { layers: { id: string; colorModes?: Record<string, unknown> }[] };
  const lake = src.layers.find((l) => l.id === "lake")!;
  const tokens = new Set<string>();
  for (const mode of CONTOUR_OVER)
    JSON.stringify(lake.colorModes![mode], (k, v) => { if (k === "token") tokens.add(v as string); return v; });
  return [...tokens];
}

/** Every lake fill a theme's contours are lost on, one whole sentence each. Empty = they are seen on every one. */
function contourFailures(th: Theme, t: Record<string, unknown>): string[] {
  const c = t["color.contour.line"] as string;
  const a = t["opacity.lake"] as number;
  const out: string[] = [];
  for (const token of contourGrounds()) {
    const fill = over(t[token] as string, LAKE_GROUND[th], a);
    const line = over(c, fill, LINE);
    const normal = deltaE(line, fill);
    const w = worst(line, fill);
    if (normal < CONTOUR_NORMAL)
      out.push(`${th}: depth contours ${c} on a ${token} ${t[token]} lake, normal vision ${normal.toFixed(1)} < ${CONTOUR_NORMAL}`);
    if (w.dE < CONTOUR_WORST)
      out.push(`${th}: depth contours ${c} on a ${token} ${t[token]} lake ${w.dE.toFixed(1)} < ${CONTOUR_WORST} (${w.under})`);
  }
  return out;
}

describe("depth contours are seen on every lake (T158)", () => {
  it("holds them over the Regulations, Depth and plain lake fills, read from the lake layer", () => {
    expect(contourGrounds()).toEqual(["color.lake.fill", "color.survey.digitised", "color.survey.sheet",
      "color.status.closed", "color.status.own", "color.status.base", "color.status.no_access", "color.water.unmapped"]);
  });

  for (const th of THEMES)
    it(`stand off every lake fill they are drawn on — ${th}`, () => {
      expect(contourFailures(th, resolveTheme(th))).toEqual([]);
    });

  it("fails the dark contour cyan that T146's ice-cyan no public access swallowed (mutation pin)", () => {
    // (T165: no public access is a grey now; the pin keeps the ice cyan it was written against)
    expect(contourFailures("dark", resolveTheme("dark", de({ "color.contour.line": "~4FD0E8", "color.status.no_access": "~50E4FF" }))))
      .toEqual(de([
      "dark: depth contours ~4FD0E8 on a color.survey.digitised ~30D0D4 lake, normal vision 5.7 < 15",
      "dark: depth contours ~4FD0E8 on a color.survey.digitised ~30D0D4 lake 4.0 < 12 (tritan (vienot))",
      "dark: depth contours ~4FD0E8 on a color.status.no_access ~50E4FF lake, normal vision 1.0 < 15",
      "dark: depth contours ~4FD0E8 on a color.status.no_access ~50E4FF lake 1.0 < 12 (tritan (machado))",
      "dark: depth contours ~4FD0E8 on a color.water.unmapped ~93A0AB lake 10.7 < 12 (deutan (machado))"]));
  });

  it("fails the light contour cyan on the Depth colouring's own digitised fill (mutation pin)", () => {
    expect(contourFailures("light", resolveTheme("light", de({ "color.contour.line": "~2FA9C9" })))
      .filter((x) => x.includes("digitised") && x.includes("normal"))).toEqual(de([
      "light: depth contours ~2FA9C9 on a color.survey.digitised ~0E7C86 lake, normal vision 4.8 < 15"]));
  });

  it("fails a contour painted in the lake fill it lies on (mutation pin)", () => {
    // a contour drawn in no public access's own colour is lost on exactly those lakes, in every theme, for every reader
    // (not 0.0: the fill is drawn at `opacity.lake` over the basemap's water, the line at full strength)
    for (const th of THEMES) {
      const x = (resolveTheme(th) as Record<string, string>)["color.status.no_access"]!;
      const lost = contourFailures(th, resolveTheme(th, { "color.contour.line": x }))
        .filter((f) => f.startsWith(`${th}: depth contours ${x} on a color.status.no_access ${x} lake`));
      expect([th, lost.map((f) => (f.includes(", normal vision ") ? "normal vision" : "every reader"))])
        .toEqual([th, ["normal vision", "every reader"]]);
    }
  });
});
