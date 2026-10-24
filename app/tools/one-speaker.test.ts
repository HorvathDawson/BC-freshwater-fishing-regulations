/**
 * ONE GAUGE SPEAKER (T105; owner 2026-10-22, "unify checks that decide the same thing").
 *
 * "Which gauge speaks for this section?" is `speakerFor` in `@app/data` `standing.ts`: its panel, else the station in
 * its lake, else nothing. The map (classes and viewport standings) and the conditions sheet (`useSpeaker`, `sheetLead`)
 * both ask it. The sheet used to ask a second question — panel rows, then the station `section_gauge` matched to the
 * reach, with no lake road — and quoted a station for 54,386 sections the map left uncoloured, while gauged lakes were
 * coloured on the map and read nothing on the sheet.
 *
 * This pins, over every app module (tests excluded):
 *   1. no module but `standing.ts` builds a speaker (`kind: "panel"` / `kind: "lake"`);
 *   2. no module falls back to a matched station (`matched?.station`, the old sheet's spelling);
 *   3. `gaugeForSection` (the `section_gauge` match record) is called once each by the two traces: the sheet's
 *      (`useGaugeTrace`, shown only for a station that `speaks`) and the spot's (`traceFor`, frozen only for a station
 *      that `speaks`). The spot's reading and lead-up freeze the sheet's station (`sheetLead`, T105 review);
 *   4. there is no `stationsFor` (a third speaker: `section_gauge` first, then the lake) and no `useConditions`.
 * Each pattern is proven able to fail on the spelling it replaced (`SPELLINGS`), so a pass is not a regex that cannot
 * match.
 *
 * Mutations: re-adding `?? matched?.station` to `useConditionsView` fails test 2 here (and the "no panel and no lake"
 * render test); building `{ kind: "lake", ... }` inline in `buildStandingClasses` fails test 1; calling
 * `source.gaugeForSection` from `panel.ts` fails test 3.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps", "packages"];

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
const hits = (re: RegExp) => files().filter((p) => re.test(readFileSync(join(APP, p), "utf8")));
/** How many times each hit file says it. */
const counts = (re: RegExp) => hits(re).map((p) =>
  [p, readFileSync(join(APP, p), "utf8").match(new RegExp(re.source, "g"))!.length] as const);

const BUILDS = /\bkind:\s*["'](?:panel|lake)["']\s*,/;
const MATCHED = /\bmatched\??\.station\b/;
const MATCH_READ = /\.gaugeForSection\(/;
const THIRD = /\bstationsFor\s*\(|\bfunction useConditions\b/;

/** The spellings T105 replaced, one per pattern. */
const SPELLINGS: [RegExp, string][] = [
  [BUILDS, `      classes.push({ kind: "lake", station });`],
  [MATCHED, `    ?? matched?.station`],
  [MATCH_READ, `      const link = await source.gaugeForSection(section);`],
  [THIRD, `    async stationsFor(sections): Promise<ReadonlyMap<SectionId, StationId>> {`],
  [THIRD, `export function useConditions(source: RegsSource, section: SectionId | null): Async<Conditions> {`],
];

describe("one gauge speaker: speakerFor decides, nothing else (T105)", () => {
  it("each pattern catches the spelling it replaced", () => {
    expect(SPELLINGS.map(([re, s]) => re.test(s))).toEqual(SPELLINGS.map(() => true));
  });

  it("only standing.ts builds a speaker", () => {
    expect(hits(BUILDS)).toEqual(["packages/data/src/standing.ts"]);
  });

  it("nothing falls back to the station matched to a reach", () => {
    expect(hits(MATCHED)).toEqual([]);
  });

  it("section_gauge is read only for the two traces: the sheet's and the spot's, once each", () => {
    // a second call in capture.ts is the spot freezing the matched station's reading again (the pre-review spelling)
    expect(counts(MATCH_READ)).toEqual([["packages/data/src/spots/capture.ts", 1], ["packages/ui/src/hooks/gauge.ts", 1]]);
  });

  it("no third speaker: no stationsFor, no useConditions", () => {
    expect(hits(THIRD)).toEqual([]);
  });
});
