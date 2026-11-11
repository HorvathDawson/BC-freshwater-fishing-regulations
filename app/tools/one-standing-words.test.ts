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
 *   4. (T598) no string or template in any module says "percentile" to a reader: a literal with a space in it that holds the word
 *      is refused (a bare identifier such as the kind tag "percentile" or a testID is not text). The reader sees "lower than
 *      8 days in 10", and "8 in 10 days higher" where that does not fit.
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
/** The Help line that explains the plain form may show it ("lower than 8 days in 10"); nothing else may. */
const HELP = "packages/core/src/help.ts";
const hits = (re: RegExp) => files().filter((p) => p !== MODULE && !(re === PHRASE && p === HELP) && re.test(strip(readFileSync(join(APP, p), "utf8"))));

const OLD_SPEAKERS = /\b(?:standingWord|plainStanding|percentileLabel|readingStandingLine|chartFigure)\b/;
const OLD_FORMAT = /`p\$\{(?:percentile|ordinal)|\bp\d+(?:st|nd|rd|th)\b/;
const PHRASE = /\}\s*percentile\b|\d(?:st|nd|rd|th)\s+percentile\b|\bdays in 10\b/;
const OWN_FIGURE = /\bfunction\s+figure\b|\b(?:const|let)\s+figure\s*=\s*\(/;

/**
 * The static text of every string and template literal in a source (the `${…}` expressions are code, not text, and are
 * scanned for literals of their own). A quote that meets a newline is not a string (an apostrophe in JSX text).
 */
function literals(src: string): string[] {
  const out: string[] = [];
  let i = 0;
  const code = (stopAtBrace: boolean): void => {
    let depth = 0;
    while (i < src.length) {
      const c = src[i]!;
      if (c === "'" || c === '"') {
        let j = i + 1, text = "";
        while (j < src.length && src[j] !== c && src[j] !== "\n") { if (src[j] === "\\") j++; text += src[j] ?? ""; j++; }
        if (src[j] === c) { out.push(text); i = j + 1; } else i++;
      } else if (c === "`") {
        i++;
        let text = "";
        while (i < src.length && src[i] !== "`") {
          if (src[i] === "\\") { text += src[i + 1] ?? ""; i += 2; }
          else if (src[i] === "$" && src[i + 1] === "{") { i += 2; code(true); }
          else text += src[i++];
        }
        out.push(text);
        i++;
      } else if (c === "{") { depth++; i++; }
      else if (c === "}") { if (stopAtBrace && depth === 0) { i++; return; } depth--; i++; }
      else i++;
    }
  };
  code(false);
  return out;
}
/** Prose that says the word: a literal with a space in it holding "percentile(s)" (an identifier or a test id is not prose). */
const sayings = (src: string): string[] => literals(strip(src)).filter((s) => /\s/.test(s) && /\bpercentiles?\b/i.test(s));
const saysPercentile = () => files().filter((p) => sayings(readFileSync(join(APP, p), "utf8")).length > 0);

const SPELLINGS: [RegExp, string][] = [
  [OLD_SPEAKERS, "  const word = standingWord(band ?? \"no-record\").toLowerCase();"],
  [OLD_SPEAKERS, "export const f = (v: number) => chartFigure(v);"],
  [OLD_FORMAT, "export const label = (p: number) => `p${percentileOrdinal(p)}`;"],
  [PHRASE, "  return `${percentileOrdinal(e.percentile)} percentile, ${w}`;"],
  [PHRASE, "  const side = `lower than ${n} days in 10`;"],
  [OWN_FIGURE, "export function figure(v: number): string {"],
];

describe("one speaker of a standing (T583)", () => {
  it("no module says 'percentile' to a reader (T598)", () => expect(saysPercentile()).toEqual([]));
  it.each([
    "return `${n}th percentile for the date`;",
    "const w = `${percentileOrdinal(p)} percentile`;",
    'const w = "Percentile, 10 to 90";',
    "x = 'a percentile for the day';",
  ])("the percentile pattern fails on %s", (s) => expect(sayings(s).length).toBe(1));
  it.each(['kind: "percentile",', 'testID="alert-form-line-percentile"', "const pctile = `x`;"])(
    "the percentile pattern lets an identifier through: %s", (s) => expect(sayings(s)).toEqual([]));
  it("only standing.ts speaks the old forms", () => expect(hits(OLD_SPEAKERS)).toEqual([]));
  it("only standing.ts spells a percentile as p20th", () => expect(hits(OLD_FORMAT)).toEqual([]));
  it("only standing.ts composes a percentile phrase", () => expect(hits(PHRASE)).toEqual([]));
  it("only core owns a figure formatter (the shipped page's copy is written from it, `clientScript`)", () =>
    expect(hits(OWN_FIGURE)).toEqual([]));
  it.each(SPELLINGS)("the pattern %s fails on the spelling it replaced", (re, spelling) => {
    expect(re.test(spelling)).toBe(true);
  });
  it("the module exists and says the forms", () => {
    const src = readFileSync(join(APP, MODULE), "utf8");
    for (const name of ["standingShort", "standingLong", "standingHeadline", "standingReads", "daysInTen", "daysShort", "figure"])
      expect(src).toMatch(new RegExp(`export (?:function|const) ${name}\\b`));
  });
});
