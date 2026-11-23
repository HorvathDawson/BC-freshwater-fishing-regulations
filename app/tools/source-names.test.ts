/**
 * (T1031 item 3, owner 2026-11-22: "clean up our inline sources. Right now the way they display is inconsistent") ONE
 * TABLE OF SOURCE NAMES: no app source spells an authority's short name, the inline "Rule book" word, or a retired source
 * spelling outside `packages/core/src/sourceNames.ts`, so every surface names a source the same way (T991: the synopsis
 * alone had 8 names, the Province 5, DFO 5).
 *
 * Walked: app/packages/{core,ui,ui-native}/src, .ts/.tsx, string literals only (comments are stripped first). Excepted,
 * exactly: test files, `sourceNames.ts` (the owner), and `packages/core/src/menu/` — More → Sources and licences, the one
 * page of the publishers' full names and their legally exact credits, and the legal words (T991 "Names, fixed").
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { RULE_BOOK, SOURCE_AUTHORITIES } from "../packages/core/src/sourceNames";

const APP = fileURLToPath(new URL("..", import.meta.url));
const OWNER = "packages/core/src/sourceNames.ts";
const EXCEPT_DIRS = ["packages/core/src/menu/"];
const ROOTS = ["packages/core/src", "packages/ui/src", "packages/ui-native/src"];

/** The names only the table may type: each authority's short name and the inline word. */
const NAMES = [...Object.values(SOURCE_AUTHORITIES).map((a) => a.name), RULE_BOOK];
/** The spellings T991 counted and T1031 retired: a surface that brings one back is drawing a source its own way. */
const RETIRED = ["source ›", "Sources ›", "Source ›", "Fishing regulations synopsis", "Province in-season changes", "DFO salmon notices"];

/** A file's code with its comments taken out (a comment may name a source; only the strings it draws count). */
const stripComments = (code: string): string =>
  code.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`\\])\/\/.*$/gm, "$1");

/** Each forbidden spelling the code's string literals hold, in order of appearance (none: []). */
function offences(code: string): string[] {
  const lits = stripComments(code).match(/"(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)*'|`(?:[^`\\]|\\.)*`/g) ?? [];
  return lits.flatMap((l) => [...NAMES, ...RETIRED].filter((n) => l.includes(n)));
}

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "fixtures" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.tsx?$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}

describe("one table of source names (T1031 item 3)", () => {
  it("no app source outside sourceNames.ts (and the Sources and licences page) spells a source name", () => {
    const hits = ROOTS.flatMap((r) => sources(join(APP, r)))
      .map((p) => [relative(APP, p), p] as const)
      .filter(([r]) => r !== OWNER && !EXCEPT_DIRS.some((d) => r.startsWith(d)))
      .flatMap(([r, p]) => offences(readFileSync(p, "utf8")).map((n) => `${r}: "${n}"`));
    expect(hits).toEqual([]);
  });

  it("the owner does spell them (the walk reads the right tree)", () => {
    const code = readFileSync(join(APP, OWNER), "utf8");
    expect(NAMES.every((n) => offences(code).includes(n))).toBe(true);
  });

  it("catches a name typed in a string, in any quote, and not one in a comment", () => {
    expect(offences(`const a = "Open the B.C. Synopsis p.8 ↗";`)).toEqual(["B.C. Synopsis"]);
    expect(offences("const b = `${n} DFO notices ↗`;")).toEqual(["DFO notices"]);
    expect(offences(`const c = 'Rule book';`)).toEqual(["Rule book"]);
    expect(offences(`const d = { source: "source ›" };`)).toEqual(["source ›"]);
    expect(offences(`// "Rule book p.8 ↗" is the inline link\n/** "DFO notices ↗" */ const e = RULE_BOOK;`)).toEqual([]);
    expect(offences(`const f = "https://example.com/x"; // "B.C. Synopsis"`)).toEqual([]);
  });
});
