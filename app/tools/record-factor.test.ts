/**
 * T110 adversarial review — ONE RECORD FACTOR. `recordFactor` in core/trust.ts is the only place a record length is
 * turned into a weight (`years / RECORD_FULL_YEARS`, capped, 0 under MIN_RECORD_YEARS). The basin field had its own copy
 * of the ratio, so the floor T110 added to the reach panels never reached it.
 *
 * Mutation pin: `basinStanding` computing `Math.min(1, Math.max(0, v.years / RECORD_FULL_YEARS))` again fails this test
 * (and "a basin vote under the floor weighs nothing" in data/src/recordYears.test.ts).
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const PACKAGES = new URL("../packages/", import.meta.url).pathname;

describe("one record factor", () => {
  it("nothing in the app's sources divides by RECORD_FULL_YEARS but recordFactor", () => {
    const hits: string[] = [];
    const walk = (dir: string) => {
      for (const name of readdirSync(dir)) {
        if (name === "node_modules" || name === "dist") continue;
        const p = join(dir, name);
        if (statSync(p).isDirectory()) walk(p);
        else if (/\.(ts|tsx)$/.test(name) && !/\.test\.tsx?$/.test(name)) {
          const code = readFileSync(p, "utf8").split("\n").filter((l) => !/^\s*(\*|\/\/|\/\*)/.test(l)).join("\n");
          for (const _ of code.matchAll(/\/\s*RECORD_FULL_YEARS/g)) hits.push(p.slice(PACKAGES.length));
        }
      }
    };
    walk(PACKAGES);
    expect(hits).toEqual(["core/src/trust.ts"]);
  });
});
