/**
 * THE VINTAGE-PIN GUARD (T63, 2026-10-22: three rebuilds in a row broke the live-release suites). A test that reads the
 * LIVE release — or the regs fixture release, which is re-cut from it — may not assert a bare integer literal (a rule
 * index, a frame id, a section handle, a count) unless a comment on that line or the line above says why the number is
 * fixed. A value that moves with the release is derived from a stated source instead: taps.json / the goldens, the
 * manifest, the store, a rule key.
 *
 * What counts: an integer over 366 outside a string and outside a comment, on a line that asserts (`expect(`, `.toBe(`,
 * `.toEqual(`, …) — an item ordinal, a section handle, a frame or rule id. Up to 366 are reading days, run lengths and
 * small counts, facts about the water the tests name in words; item ids are strings (`"gnis:8634"`) and durable
 * (AGENTS.md rule 5). Not a vintage either: a year (20xx), a time budget (`toBeLessThan`), and the frozen oracle's rule
 * numbers mapped to the store by key (`R(3091)`, `[…].map(R)` — `oracleRixOver`, T63's lane wf_64942248-585).
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const APP = new URL("..", import.meta.url).pathname;
const ROOTS = ["conformance/src", "packages", "tools"];

/** A test reads the live release (or the fixture cut from it) when it names one of these. */
export const LIVE = /\b(liveRelease|liveReleaseAt|provinceStoreOf|parityStore|paritySource|regsStore|releasePairing|REGS_STORE_REQUIRED|openRegsFixture)\b/;
const ASSERTS = /\bexpect\(|\.to(Be|Equal|StrictEqual|Contain|HaveLength|BeGreaterThan|BeLessThan)\(/;

function testFiles(dir: string, out: string[] = []): string[] {
  for (const n of readdirSync(dir)) {
    if (n === "node_modules" || n === "dist") continue;
    const p = join(dir, n);
    if (statSync(p).isDirectory()) testFiles(p, out);
    else if (/\.test\.tsx?$/.test(n)) out.push(p);
  }
  return out;
}

/** The line's code with strings, template literals' text and comments blanked. */
export function codeOf(line: string): string {
  return line
    .replace(/\/\/.*$/, "")
    .replace(/\/\*.*?\*\//g, "")
    .replace(/"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|`(?:[^`\\]|\\.)*`/g, '""');
}

/** The code with the numbers that are not a release's vintage blanked (see the header). */
function pinsIn(code: string): boolean {
  if (/\.toBeLessThan\(/.test(code)) return false;
  const rest = code
    .replace(/\bR\(\d+\)/g, "R()")
    .replace(/\[[\d,\s]+\]\.map\(R\)/g, "[]")
    .replace(/(?<![\w.$])20\d\d(?![\w.])/g, "0");
  return (rest.match(/(?<![\w.$])\d{3,}(?![\w.])/g) ?? []).some((n) => Number(n) > 366);
}

const commented = (line: string | undefined): boolean => !!line && /\/\/|\/\*|^\s*\*/.test(line);

/** `path:line` of every bare integer pin in a live-release test's assertion with no comment saying why. */
export function vintagePins(file: string, text: string): string[] {
  if (!LIVE.test(text) && !/\.release\.test\.tsx?$/.test(file)) return [];
  const lines = text.split("\n");
  const out: string[] = [];
  lines.forEach((l, i) => {
    const c = codeOf(l);
    if (!ASSERTS.test(c) || !pinsIn(c)) return;
    if (commented(l) || commented(lines[i - 1])) return;
    out.push(`${file}:${i + 1}: ${l.trim().slice(0, 140)}`);
  });
  return out;
}

describe("no live-release test pins a number the next release moves", () => {
  it("every bare integer in a live-release assertion says why it is fixed", () => {
    const found = ROOTS.flatMap((r) => testFiles(join(APP, r)))
      .flatMap((f) => vintagePins(relative(APP, f), readFileSync(f, "utf8")));
    expect(found).toEqual([]);
  });

  it("MUTATION PIN: a planted bare pin in a live-release test is found; a commented one, a string id and a fixture test are not", () => {
    const live = "const s = await regsStore();\n";
    expect(vintagePins("x.test.ts", `${live}expect(rix).toBe(615);`)).toEqual(["x.test.ts:2: expect(rix).toBe(615);"]);
    expect(vintagePins("x.test.ts", `${live}expect(rix).toBe(615); // Endako 7A r3: fixed by the oracle page`)).toEqual([]);
    expect(vintagePins("x.test.ts", `${live}// the oracle's own numbering, frozen with page v36\nexpect(rix).toBe(615);`)).toEqual([]);
    expect(vintagePins("x.test.ts", `${live}expect(item).toBe("gnis:8634");`)).toEqual([]);
    expect(vintagePins("x.test.ts", "expect(rix).toBe(615);")).toEqual([]);
    expect(vintagePins("a.release.test.ts", "expect(n).toBe(1234);")).toEqual(["a.release.test.ts:1: expect(n).toBe(1234);"]);
    expect(vintagePins("x.test.ts", `${live}expect([R(3091), y, d, ms]).toEqual([2026, 365]); expect(t).toBeLessThan(3000);`)).toEqual([]);
  });
});
