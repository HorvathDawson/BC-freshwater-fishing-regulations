/**
 * (T1031 item 3, owner 2026-11-22: "clean up our inline sources. Right now the way they display is inconsistent") ONE
 * TABLE OF SOURCE NAMES: no app source spells an authority's short name, the inline "Rule book" word, or a retired source
 * spelling outside `packages/core/src/sourceNames.ts`, so every surface names a source the same way (T991: the synopsis
 * alone had 8 names, the Province 5, DFO 5).
 *
 * Walked: app/packages/{core,ui,ui-native}/src and app/apps/mobile/src, .ts/.tsx, string literals only (comments are
 * stripped first), case-insensitively ("the rule book" in a sentence is the retired name too). Excepted, exactly: test
 * files, `sourceNames.ts` (the owner) and the files in `EXCEPT` below, each for its reason.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { RULE_BOOK, SOURCE_AUTHORITIES } from "../packages/core/src/sourceNames";

const APP = fileURLToPath(new URL("..", import.meta.url));
const OWNER = "packages/core/src/sourceNames.ts";
const EXCEPT: Readonly<Record<string, string>> = {
  "packages/core/src/menu/licences.ts": "More → Sources and licences: each publisher's full name and its legally exact required wording",
  "packages/core/src/menu/sources.ts": "the map's and the forecasts' credit lines, verbatim as their licences require",
  "packages/core/src/menu/legal.ts": "the first-run notice and the legal foot: the authorities named in a sentence, the owner's words",
  "packages/core/src/changelog.ts": "release notes as they shipped (history is not reworded)",
};
const ROOTS = ["packages/core/src", "packages/ui/src", "packages/ui-native/src", "apps/mobile/src"];

/** The names only the table may type: each authority's short name and the inline word. */
const NAMES = [...new Set([...Object.values(SOURCE_AUTHORITIES).map((a) => a.name), RULE_BOOK])];
/** The spellings T991 counted and T1031 retired: a surface that brings one back is drawing a source its own way. "Rule
 *  book" (owner 2026-11-23: "'B.C. Synopsis' everywhere") is refused in any string, a spoken name ("Rule book page 8") too. */
const RETIRED = ["Rule book", "Fishing regulations synopsis", "Province in-season changes", "DFO salmon notices"];
/** The retired link spellings, matched exactly ("All sources ›", the every-source sheet's link, is not one of them). */
const RETIRED_LINKS = ["source ›", "Sources ›", "Source ›"];

/** A file's code with its comments taken out (a comment may name a source; only the strings it draws count). */
const stripComments = (code: string): string =>
  code.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`\\])\/\/.*$/gm, "$1");

/** Each forbidden spelling the code's string literals hold, in order of appearance (none: []). */
function offences(code: string): string[] {
  const lits = stripComments(code).match(/"(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)*'|`(?:[^`\\]|\\.)*`/g) ?? [];
  return lits.flatMap((l) => [...[...NAMES, ...RETIRED].filter((n) => l.toLowerCase().includes(n.toLowerCase())),
                              ...RETIRED_LINKS.filter((n) => l.includes(n))]);
}

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    // `testing/`: test helpers holding the export's own data words, not words a screen draws
    if (["node_modules", "fixtures", "testing", "android", "ios"].includes(f) || f.startsWith(".")) continue;
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
      .filter(([r]) => r !== OWNER && !(r in EXCEPT))
      .flatMap(([r, p]) => offences(readFileSync(p, "utf8")).map((n) => `${r}: "${n}"`));
    expect(hits).toEqual([]);
  });

  it("every excepted file exists and still needs its exception (a stale one is removed, not kept)", () => {
    expect(Object.keys(EXCEPT).filter((r) => offences(readFileSync(join(APP, r), "utf8")).length === 0)).toEqual([]);
  });

  it("the owner does spell them (the walk reads the right tree)", () => {
    const code = readFileSync(join(APP, OWNER), "utf8");
    expect(NAMES.every((n) => offences(code).includes(n))).toBe(true);
  });

  it("catches a name typed in a string, in any quote, and not one in a comment", () => {
    expect(offences(`const a = "Open the B.C. Synopsis p.8 ↗";`)).toEqual(["B.C. Synopsis"]);
    expect(offences("const b = `${n} DFO notices ↗`;")).toEqual(["DFO notices"]);
    expect(offences(`const c = 'Rule book';`)).toEqual(["Rule book"]);
    expect(offences(`const g = "Couldn’t show the rule book here.";`)).toEqual(["Rule book"]);
    expect(offences(`const h = "Sources (b.c. synopsis and DFO)";`)).toEqual(["B.C. Synopsis"]);
    expect(offences("const a11y = (n) => `Rule book page ${n}, opens the PDF`;")).toEqual(["Rule book"]);
    expect(offences(`const d = { source: "source ›" };`)).toEqual(["source ›"]);
    expect(offences(`const i = "Sources ›"; const j = "All sources ›";`)).toEqual(["Sources ›"]);
    expect(offences(`// "Rule book p.8 ↗" is the inline link\n/** "DFO notices ↗" */ const e = RULE_BOOK;`)).toEqual([]);
    expect(offences(`const f = "https://example.com/x"; // "B.C. Synopsis"`)).toEqual([]);
  });
});
