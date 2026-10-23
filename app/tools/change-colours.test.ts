/**
 * THE EXPECTED-CHANGE RAMP, AS A READER SEES IT (T52).
 *
 * The Change view colours every river by the largest move its forecast expects over the next ten days, on a DIVERGING
 * ramp — a big fall (a quarter of today's flow), a fall (half), steady, a rise (twice), a big rise (four times):
 * `color.change.fall2 … rise2`. It must be:
 *   not a status colour   every stop at least MEANING_FLOOR (ΔE2000, normal vision) from all five statuses — one meaning
 *                         per colour (status-colours-distinct.test.ts' floor). Dark sits under it: see UNDER.
 *   readable step to step  adjacent stops apart for every reader (worst of normal / protan / deutan / tritan, Viénot and
 *                         Machado);
 *   unmistakable at the ends  a big fall and a big rise far apart, and the two inner stops too;
 *   a reading when steady  steady is a colour a forecast gave, never the ungauged grey that means nothing speaks here;
 *   diverging             each end further from steady than its inner stop.
 * The values were searched to these floors (T52, inside the style build's own ΔE76 ≥ 12 against everything drawn with
 * them); the floors are what they reach, rounded down to 0.5, so the ramp may move only to something as distinct.
 */
import { describe, expect, it } from "vitest";
import { resolveTheme } from "@app/map";
import { deltaE, MODELS, worstSeparation } from "./cvd";

type Theme = "light" | "dark" | "cvd";
const THEMES: Theme[] = ["light", "dark", "cvd"];
const STOPS = ["fall2", "fall1", "steady", "rise1", "rise2"] as const;
const STATUSES = ["open", "restricted", "closed", "own", "base"] as const;
const MEANING_FLOOR = 20;

const FLOOR: Record<Theme, { status: number; step: number; ends: number; inner: number; ungauged: number }> = {
  light: { status: 21.5, step: 11.5, ends: 31, inner: 33, ungauged: 21 },
  dark:  { status: 17, step: 11.5, ends: 30.5, inner: 25.5, ungauged: 18 },
  cvd:   { status: 20.5, step: 12, ends: 31, inner: 28, ungauged: 26.5 },
};
/** Every floor under MEANING_FLOOR, with its reason; whole, so a new one fails until it is recorded. */
const UNDER: Record<string, string> = {
  "dark status": "the dark statuses hold mint (open), rose (closed), lavender (restricted), yellow (own) and deep blue " +
                 "(base), and the flow ramp and the map's own blues and greys crowd what is left; olive-to-teal is the " +
                 "best pair the search found that also clears the style build's ΔE76 12 against everything drawn with " +
                 "it (17.2: fall2 olive against own yellow). The dark temperature and permit floors are 15 for the same " +
                 "reason (status-colours-distinct.test.ts).",
};

const worst = (a: string, b: string) => worstSeparation(a, b, MODELS).dE;

function failures(th: Theme, t: Record<string, unknown>): string[] {
  const out: string[] = [];
  const f = FLOOR[th];
  const c = (s: string) => t[`color.change.${s}`] as string;
  const hold = (what: string, d: number, floor: number) => {
    if (!(d >= floor)) out.push(`${th}: ${what} ${d.toFixed(1)} < ${floor}`);
  };
  for (const s of STOPS)
    for (const st of STATUSES)
      hold(`${s} ${c(s)} vs status ${st} ${t[`color.status.${st}`]}`, deltaE(c(s), t[`color.status.${st}`] as string),
           f.status);
  for (let i = 1; i < STOPS.length; i++)
    hold(`${STOPS[i - 1]} / ${STOPS[i]} step`, worst(c(STOPS[i - 1]!), c(STOPS[i]!)), f.step);
  hold("fall2 / rise2 (the ends)", worst(c("fall2"), c("rise2")), f.ends);
  hold("fall1 / rise1 (the inner stops)", worst(c("fall1"), c("rise1")), f.inner);
  hold("steady vs ungauged", worst(c("steady"), t["color.water.ungauged"] as string), f.ungauged);
  if (!(deltaE(c("fall2"), c("steady")) > deltaE(c("fall1"), c("steady"))
        && deltaE(c("rise2"), c("steady")) > deltaE(c("rise1"), c("steady"))))
    out.push(`${th}: not diverging — an end is no further from steady than its inner stop`);
  return out;
}

describe("the expected-change ramp (T52)", () => {
  for (const th of THEMES)
    it(`holds every floor in ${th}`, () => {
      expect(failures(th, resolveTheme(th) as Record<string, unknown>)).toEqual([]);
    });

  it("records every floor under one meaning, and only those", () => {
    const under = THEMES.filter((th) => FLOOR[th].status < MEANING_FLOOR).map((th) => `${th} status`);
    expect(under).toEqual(Object.keys(UNDER));
  });

  it("refuses a status colour, a flat step and a grey steady (mutation pins)", () => {
    const light = resolveTheme("light") as Record<string, unknown>;
    // the big rise painted as `closed`
    expect(failures("light", { ...light, "color.change.rise2": light["color.status.closed"] }).length).toBeGreaterThan(0);
    // two stops one colour
    expect(failures("light", { ...light, "color.change.rise1": light["color.change.rise2"] }).length).toBeGreaterThan(0);
    // steady drawn as nothing-speaks-here
    expect(failures("light", { ...light, "color.change.steady": light["color.water.ungauged"] }).length)
      .toBeGreaterThan(0);
  });
});
