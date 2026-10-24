/**
 * ONE GAUGE SIDE (T168; owner 2026-10-23, "make a division between real gauges and donor gauges").
 *
 * "Is this station on this water, or a donor estimating from another?" is core's `gaugeSide` (gaugeSide.ts), and the
 * groups every surface draws — the count line, the donor rows, the gauge switch, the route map's key — are core's
 * `groupGauges` and `gaugeCountLine`. This pins that no other module:
 *   1. tests a station's water against the section's waters (`….waters.includes(…)` and its spellings);
 *   2. compares a side to "own" / "donor" itself (an own-first sort, a filter, a ternary on the side);
 *   3. spells the groups' words ("on this water", "from other waters", the headings) — those are core's, composed whole.
 * Comments are stripped first: the words may be quoted in a doc comment. Each pattern is proven able to fail on the
 * spelling it forbids (`SPELLINGS`), so a pass is not a regex that cannot match.
 *
 * Mutations (each run 2026-10-24, each failed the named test and was reverted):
 *   - the own-first sort T168 first wrote in ui `panel.ts` (`list.filter((r) => sideOf(r) === "own")`) fails test 2.
 *   - writing `"ON THIS WATER"` into ui-native `GaugeGroups.tsx` as a fallback heading fails test 3.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps", "packages"];
const HOME = "packages/core/src/gaugeSide.ts";

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx|mjs)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();

/** A module's code without its comments; "" for one gone since the walk (check-boundaries writes a probe module). */
function code(p: string): string {
  let t: string;
  try { t = readFileSync(join(APP, p), "utf8"); } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return "";
    throw e;
  }
  return t.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/.*$/gm, "$1");
}

const TESTS: Record<string, RegExp> = {
  waters: /\.waters\s*\.\s*(?:includes|indexOf|some|has|find|findIndex)\s*\(/g,
  side: /\bside\w*\b(?:\([^)]*\))?\s*!?==?=?\s*["'](?:own|donor)["']|["'](?:own|donor)["']\s*!?==?=?\s*\w*side/gi,
  words: /gauges? on this water\b(?! is close)|from other waters\b|from another water\b|ON THIS WATER|ESTIMATING FROM/g,
};
/** What each pattern must catch — the spellings it exists to forbid. */
const SPELLINGS: Record<string, string> = {
  waters: "return w.waters.includes(g.item) ? 1 : 0;",
  side: 'const own = rows.filter((r) => sideOf(r) === "own");',
  words: 'const h = "ON THIS WATER";',
};

describe("one gauge side", () => {
  it("each pattern can fail", () => {
    for (const [k, re] of Object.entries(TESTS)) expect(SPELLINGS[k]!.match(re), k).toBeTruthy();
  });

  for (const k of Object.keys(TESTS))
    it(`no module but core gaugeSide.ts decides it (${k})`, () => {
      const hits = files().filter((p) => p !== HOME)
        .flatMap((p) => (code(p).match(TESTS[k]!) ?? []).map((m) => `${p}: ${m}`));
      expect(hits).toEqual([]);
    });

  it("every surface groups through core: the rows, the switch and the key", () => {
    const panel = code("packages/ui/src/panel.ts");
    // the donor rows and the key (`donorView`) and the switch (`gaugeChoiceGroups`) each call `groupGauges`
    expect(panel.match(/\bgroupGauges\(/g)?.length).toBe(3);
    expect(panel).toMatch(/\bgaugeCountLine\(/);
    for (const f of ["DonorPanel.tsx", "ConditionsPanel.tsx", "DonorMap.tsx"])
      expect(code(`packages/ui-native/src/${f}`), f).toMatch(/<GaugeGroups\b/);
  });
});
