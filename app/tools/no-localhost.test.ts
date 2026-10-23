/** P10: no `localhost` literal outside the one endpoints file (apps/mobile/src/config.ts). */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ALLOWED = new Set(["apps/mobile/src/config.ts"]);

interface Fs {
  readdirSync(p: string): string[];
  statSync(p: string): { isDirectory(): boolean };
  readFileSync(p: string, enc: "utf8"): string;
}
const NODE: Fs = { readdirSync, statSync, readFileSync: (p, enc) => readFileSync(p, enc) };

/**
 * A file that is gone by the time it is looked at is not a source (T80 7). check-boundaries.test.ts and
 * check-platform.test.ts write and delete their `_bnd*` / `_plat*` fixtures under packages/ while this walk runs in
 * another worker: listed, then gone, and the walk threw ENOENT under load. Only ENOENT is forgiven. (T156: those
 * fixtures are now written to a scratch tree outside the app; this stays forgiving of any other writer.)
 */
const gone = (e: unknown): boolean => (e as { code?: string } | null)?.code === "ENOENT";

function sources(dir: string, fs: Fs = NODE, out: string[] = []): string[] {
  for (const f of fs.readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    let isDir: boolean;
    try { isDir = fs.statSync(p).isDirectory(); } catch (e) { if (gone(e)) continue; throw e; }
    if (isDir) sources(p, fs, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f)) out.push(p);
  }
  return out;
}

function namesLocalhost(p: string, fs: Fs = NODE): boolean {
  try { return /localhost|127\.0\.0\.1/.test(fs.readFileSync(p, "utf8")); } catch (e) { if (gone(e)) return false; throw e; }
}

describe("endpoints", () => {
  it("no source names localhost but config.ts", () => {
    const hits = [...sources(join(APP, "apps")), ...sources(join(APP, "packages"))]
      .map((p) => relative(APP, p))
      .filter((p) => !ALLOWED.has(p) && namesLocalhost(join(APP, p)));
    expect(hits).toEqual([]);
  });

  it("a file deleted mid-walk (another worker's temp fixture) is skipped, any other error still throws (T80 7)", () => {
    const enoent = Object.assign(new Error("ENOENT"), { code: "ENOENT" });
    const tree: Record<string, string[]> = { "/r": ["a.ts", "_bnd.ts", "sub"], "/r/sub": ["b.ts", "_plat.ts"] };
    const fs = (bad: Error): Fs => ({
      readdirSync: (p) => tree[p] ?? [],
      statSync: (p) => {
        if (p.endsWith("_bnd.ts")) throw bad;
        return { isDirectory: () => p in tree };
      },
      readFileSync: (p) => { if (p.endsWith("_plat.ts")) throw bad; return "localhost"; },
    });
    const walked = sources("/r", fs(enoent));
    expect({ walked, hits: walked.filter((p) => namesLocalhost(p, fs(enoent))) })
      .toEqual({ walked: ["/r/a.ts", "/r/sub/b.ts", "/r/sub/_plat.ts"], hits: ["/r/a.ts", "/r/sub/b.ts"] });
    const eacces = Object.assign(new Error("EACCES"), { code: "EACCES" });
    expect(() => sources("/r", fs(eacces))).toThrow("EACCES");
    expect(() => namesLocalhost("/r/sub/_plat.ts", fs(eacces))).toThrow("EACCES");
  });
});
