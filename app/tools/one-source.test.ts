// (T436) ONE SOURCE EACH — duplicates the audit found (2026-11-05, app #9 / #10), each now read from one place:
//   the report / crash / version-header platform line: ui-native `platformLine` (a .web / .native pair), not five copies
//     of `${Platform.OS} ${Platform.Version}`;
//   the gear marks the map draws: core's `GEAR_LEGEND`, not `["fly", "bait"]` typed again in three map files (a new class
//     would get a legend row and no layer).
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { GEAR_LEGEND } from "@app/core";
import { GEAR_LAYERS } from "../packages/map/src/paint";

const APP = fileURLToPath(new URL("..", import.meta.url));
const walk = (dir: string, out: string[] = []): string[] => {
  for (const n of readdirSync(dir)) {
    if (n === "node_modules" || n === "dist" || n.startsWith(".")) continue;
    const p = join(dir, n);
    if (statSync(p).isDirectory()) walk(p, out);
    else if (/\.(ts|tsx)$/.test(n) && !/\.test\.tsx?$/.test(n)) out.push(p);
  }
  return out;
};
const sources = (...roots: string[]) => roots.flatMap((r) => walk(join(APP, r))).map((p) => relative(APP, p)).sort();
const read = (f: string) => readFileSync(join(APP, f), "utf8");

describe("one source each (T436)", () => {
  it("the platform line is built in platformLine.native.ts only", () => {
    const at = sources("apps/mobile/src", "packages").filter((f) => /\$\{Platform\.OS\}\s*\$\{Platform\.Version\}/.test(read(f)));
    expect(at).toEqual(["packages/ui-native/src/platformLine.native.ts"]);
  });

  it("the map draws core's gear marks: GEAR_LAYERS has a layer per GEAR_LEGEND mark, and no map file lists them again", () => {
    for (const lid of ["stream", "lake"] as const) expect(Object.keys(GEAR_LAYERS[lid]).sort()).toEqual([...GEAR_LEGEND].sort());
    const again = sources("packages/map/src").filter((f) => /\[\s*"fly"\s*,\s*"bait"\s*\]|"fly"\s*\|\s*"bait"/.test(read(f)));
    expect(again).toEqual([]);
  });
});
