/**
 * ONE SPEAKER OF A STANDING (T583; AGENTS 23, 33, 40).
 *
 * A gauge's standing was worded five ways on one sheet and the public page ("Low", "Below normal · 20th percentile",
 * "Reads 2 in 10", "p20th", and a rounded figure beside a different phrase). The words are now `packages/core/src/standing.ts`
 * and nothing else. This pins, over every app module and the feeds worker (tests excluded):
 *   1. no module but standing.ts defines or calls the old speakers (`standingWord`, `plainStanding`, `percentileLabel`,
 *      `readingStandingLine`, `chartFigure`), and none spells the old `p20th` format;
 *   2. no module composes a percentile phrase of its own: a number, an ordinal or an interpolation followed by
 *      "percentile" (`${…} percentile`, `th percentile`) or "N days in 10";
 *   3. no module defines a `figure` / `chartFigure` formatter of its own (the three-figure rounding is core's).
 * Each pattern is proven able to fail on the spelling it replaced (`SPELLINGS`).
 *
 * Mutations: re-adding `const word = standingWord(...)` anywhere but standing.ts fails 1; `${percentileOrdinal(p)} percentile`
 * in change.ts fails 2; `function figure(v: number)` in the worker card fails 3.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps", "packages", "workers/feeds/src", "tools/seo"];
const MODULE = "packages/core/src/standing.ts";

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (/apps\/mobile\/(ios|android)$/.test(p)) continue;
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx|mjs)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
/** Source files other than the module that say it. Comments are skipped: a doc may name the old function. */
const strip = (s: string) => s.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
const hits = (re: RegExp) => files().filter((p) => p !== MODULE && re.test(strip(readFileSync(join(APP, p), "utf8"))));

const OLD_SPEAKERS = /\b(?:standingWord|plainStanding|percentileLabel|readingStandingLine|chartFigure)\b/;
const OLD_FORMAT = /`p\$\{(?:percentile|ordinal)|\bp\d+(?:st|nd|rd|th)\b/;
const PHRASE = /\}\s*percentile\b|\d(?:st|nd|rd|th)\s+percentile\b|\bdays in 10\b/;
const OWN_FIGURE = /\bfunction\s+figure\b|\b(?:const|let)\s+figure\s*=\s*\(/;

const SPELLINGS: [RegExp, string][] = [
  [OLD_SPEAKERS, "  const word = standingWord(band ?? \"no-record\").toLowerCase();"],
  [OLD_SPEAKERS, "export const f = (v: number) => chartFigure(v);"],
  [OLD_FORMAT, "export const label = (p: number) => `p${percentileOrdinal(p)}`;"],
  [PHRASE, "  return `${percentileOrdinal(e.percentile)} percentile, ${w}`;"],
  [PHRASE, "  const side = `lower than ${n} days in 10`;"],
  [OWN_FIGURE, "export function figure(v: number): string {"],
];

describe("one speaker of a standing (T583)", () => {
  it("only standing.ts speaks the old forms", () => expect(hits(OLD_SPEAKERS)).toEqual([]));
  it("only standing.ts spells a percentile as p20th", () => expect(hits(OLD_FORMAT)).toEqual([]));
  it("only standing.ts composes a percentile phrase", () => expect(hits(PHRASE)).toEqual([]));
  it("only core owns a figure formatter (the shipped page's copy is written from it)", () =>
    expect(hits(OWN_FIGURE).filter((p) => p !== "tools/seo/gauges.ts")).toEqual([]));
  it.each(SPELLINGS)("the pattern %s fails on the spelling it replaced", (re, spelling) => {
    expect(re.test(spelling)).toBe(true);
  });
  it("the module exists and says the forms", () => {
    const src = readFileSync(join(APP, MODULE), "utf8");
    for (const name of ["standingShort", "standingLong", "standingHeadline", "standingReads", "percentileSaid", "figure"])
      expect(src).toMatch(new RegExp(`export (?:function|const) ${name}\\b`));
  });
});
