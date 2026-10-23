/**
 * ONE WATER NOUN (T116; owner 2026-10-22: one decision = one function). "What noun names this kind of water?" is
 * decided by `waterNoun(kind, form)` in `packages/core/src/regs/words.ts`, and nowhere else. Before it, three modules
 * each wrote `kind === "lake" ? lake : river` — so the 54 wetland items (marshes, swamps, oxbows) read "this river's
 * rule" and "Whole river" — and search kept a fourth answer in its own kind-to-word table.
 *
 * This pins that no app module grows a second copy back: no noun ternary, no kind-to-word table, no "Whole <noun>"
 * literal, and the noun constants are read only by `waterNoun` itself. Comments are stripped first, so a doc comment
 * may still say what the button reads.
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const APP = new URL("..", import.meta.url).pathname;
const ROOTS = ["packages", "apps"];
const HOME = "packages/core/src/regs/words.ts";

function sources(dir: string, out: string[] = []): string[] {
  for (const n of readdirSync(dir)) {
    if (n === "node_modules" || n.startsWith(".") || n === "dist" || n === "build") continue;
    const p = join(dir, n);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(n) && !/\.test\.tsx?$/.test(n) && !n.endsWith(".d.ts")) out.push(p);
  }
  return out;
}

/** Code with its comments taken out (a crude strip: good enough to find a pattern, never to parse). */
const code = (src: string): string => src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/[^\n]*/g, "$1");

/** The ways a second copy of the noun decision has been, or would be, written. */
const SECOND_COPY: readonly (readonly [string, RegExp])[] = [
  ["a ternary choosing a water noun", /\?\s*(?:WORDS\.WATER_\w+|["'`](?:lake|river|wetland|stream)["'`])\s*:/i],
  ["a kind-to-word table", /\b(?:stream|lake|wetland)\s*:\s*["'`](?:stream|lake|wetland|river|creek|marsh)["'`]/i],
  ["a 'Whole <noun>' literal", /["'`]Whole (?:river|lake|wetland|stream)\b/i],
  ["the noun constants read outside waterNoun", /WORDS\.(?:WATER|ROW)_(?:RIVER|LAKE|WETLAND|STREAM)\b/],
  // (review) a page's own kind is pageWaterNoun's: item, else the unnamed section's key — never item-else-"stream"
  ["a page kind that skips the section's key", /\bitem\?\.kind\s*\?\?\s*["'`]/],
];

function secondCopies(files: readonly { path: string; src: string }[]): string[] {
  const hits: string[] = [];
  for (const { path, src } of files) {
    // waterNoun's and pageWaterNoun's own bodies are the one place allowed to choose a noun; the rest of their file is
    // held to the same rule
    const c = path === HOME ? code(src).replace(/export function (?:waterNoun|pageWaterNoun)\([\s\S]*?\n\}\n/g, "") : code(src);
    for (const [what, re] of SECOND_COPY) {
      const m = c.match(re);
      if (m) hits.push(`${path}: ${what}: ${m[0]}`);
    }
  }
  return hits;
}

const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => ({ path: relative(APP, p), src: readFileSync(p, "utf8") }));

describe("one water noun (T116)", () => {
  it("no app module decides a water's noun but waterNoun", () => {
    expect(secondCopies(files())).toEqual([]);
  });

  it("the four places that name a kind call waterNoun (a regs page through pageWaterNoun)", () => {
    const callers: readonly (readonly [string, RegExp])[] = [
      ["packages/core/src/regs/decided.ts", /\bpageWaterNoun\(inp\)/], ["packages/core/src/regs/caveats.ts", /\bpageWaterNoun\(inp\)/],
      ["packages/core/src/waterNav.ts", /\bwaterNoun\(/], ["packages/core/src/search.ts", /\bwaterNoun\(/]];
    expect(callers.filter(([p, re]) => !re.test(code(readFileSync(join(APP, p), "utf8")))).map(([p]) => p)).toEqual([]);
  });

  it("each pattern catches the copy it names (the gate cannot pass vacuously)", () => {
    expect(secondCopies([
      { path: "packages/core/src/regs/decided.ts", src: `WORDS.BY_WATER(inp.item?.kind === "lake" ? WORDS.WATER_LAKE : WORDS.WATER_RIVER)` },
      { path: "packages/core/src/waterNav.ts", src: `return g.parts[i]?.kind === "lake" ? "Whole lake" : "Whole river";` },
      { path: "packages/core/src/search.ts", src: `export const KIND_WORD = { stream: "Stream", lake: "Lake", wetland: "Wetland" };` },
      // the as-built T116 caller: an unnamed lake (no item) fell back to "river"
      { path: "packages/core/src/regs/caveats.ts", src: `WORDS.CALM_BOAT(waterNoun(inp.item?.kind ?? "stream", "page"))` },
      // in waterNoun's own body (and in a comment) a noun choice is allowed; anywhere else in its file it is not
      { path: HOME, src: `/** k === "lake" ? "lake" : "river" */\nexport function waterNoun(kind: string, form: NounForm): string {\n` +
        `  return form === "page" ? WORDS.WATER_LAKE : WORDS.ROW_LAKE;\n}\nconst x = wet ? WORDS.WATER_WETLAND : "";\n` },
    ])).toEqual([
      "packages/core/src/regs/decided.ts: a ternary choosing a water noun: ? WORDS.WATER_LAKE :",
      "packages/core/src/regs/decided.ts: the noun constants read outside waterNoun: WORDS.WATER_LAKE",
      `packages/core/src/waterNav.ts: a 'Whole <noun>' literal: "Whole lake`,
      `packages/core/src/search.ts: a kind-to-word table: stream: "Stream"`,
      `packages/core/src/regs/caveats.ts: a page kind that skips the section's key: item?.kind ?? "`,
      `${HOME}: a ternary choosing a water noun: ? WORDS.WATER_WETLAND :`,
      `${HOME}: the noun constants read outside waterNoun: WORDS.WATER_WETLAND`,
    ]);
  });
});
