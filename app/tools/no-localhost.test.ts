/** P10: no `localhost` literal outside the one endpoints file (apps/mobile/src/config.ts). */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ALLOWED = new Set(["apps/mobile/src/config.ts"]);

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f)) out.push(p);
  }
  return out;
}

describe("endpoints", () => {
  it("no source names localhost but config.ts", () => {
    const hits = [...sources(join(APP, "apps")), ...sources(join(APP, "packages"))]
      .map((p) => relative(APP, p))
      .filter((p) => !ALLOWED.has(p) && /localhost|127\.0\.0\.1/.test(readFileSync(join(APP, p), "utf8")));
    expect(hits).toEqual([]);
  });
});
