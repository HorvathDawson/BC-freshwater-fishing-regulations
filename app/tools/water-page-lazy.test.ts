/**
 * (T910) THE REGULATIONS PAGE IS IN THE LATER CHUNK (ui-native shell/WaterModeHost.tsx `part`, later.ts): only the later
 * chunk imports it (Metro does not tree-shake), and the page's part draws its loading face (WaterPageLoading) while its
 * code comes. Source checks, here because the boundaries gate keeps node:fs out of the packages; the render tests are
 * packages/ui-native/src/WaterPageLoading.test.tsx.
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const SRC = fileURLToPath(new URL("../packages/ui-native/src/", import.meta.url));
const read = (p: string) => readFileSync(join(SRC, p), "utf8");

describe("(T910) the Regulations page loads later on the web", () => {
  it("is a part with its loading face", () => {
    expect(read("shell/WaterModeHost.tsx")).toMatch(/const WaterScreen = part<[^\n]+>\(\(\) => import\("\.\.\/later"\)\.then\(\(m\) => m\.WaterScreen\), undefined,\s*\(p\) => <WaterPageLoading palette=\{p\.palette\} \/>\)/);
  });

  it("keeps the page out of the first script: only the later chunk imports it (Metro does not tree-shake)", () => {
    expect(read("later.ts")).toMatch(/export \{ WaterScreen \} from "\.\/WaterScreen";/);
    expect(read("index.ts")).not.toMatch(/WaterScreen"|regs\/SheetHost"|regs\/YearStrip"/);
    const files: string[] = [];
    const walk = (d: string) => { for (const f of readdirSync(d)) { const p = join(d, f); if (statSync(p).isDirectory()) walk(p); else if (/\.tsx?$/.test(f) && !/\.test\.tsx?$/.test(f)) files.push(p); } };
    walk(SRC);
    const statics = files.filter((p) => /^\s*import\s+(?!type\b)[^;]*from\s+"(\.\.?\/)+WaterScreen"/m.test(readFileSync(p, "utf8")))
      .map((p) => relative(SRC, p));
    expect(statics).toEqual([]);
  });
});
