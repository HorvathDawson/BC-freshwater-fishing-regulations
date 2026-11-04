/**
 * (T369, owner 2026-11-03) LINKS ARE WEB-ONLY: the native app's import graph holds no place / lines module. A phone has
 * no address bar and never holds the `lines` file, so making or resolving a place (`@app/ui/place`, `@app/data/regs/place`
 * and the format it reads, `regs/line.ts`) is reached only through the app's `.web.ts` modules.
 *
 * Walked as Metro resolves for a phone: from `apps/mobile/index.js`, `.native.*` before the shared file, a workspace
 * package through its `exports` (else `main`), type-only imports skipped (erased), every other package external. The
 * same walk with `.web.*` first must reach them — so a walker that stopped finding modules cannot pass for a clean graph.
 */
import { existsSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const ROOT = resolve(__dirname, "..");
const ENTRY = join(ROOT, "apps/mobile/index.js");
const LINK_ONLY = ["packages/ui/src/place.ts", "packages/data/src/regs/place.ts", "packages/data/src/regs/line.ts"];

// value imports / re-exports / dynamic imports / require; `import type` and `export type` are erased and not followed
const SPEC = /(?:^|[\n;])\s*(?:import|export)\s+(?!type\s)(?:[^;()]*?\sfrom\s*)?["']([^"']+)["']|\bimport\s*\(\s*["']([^"']+)["']\s*\)|\brequire\s*\(\s*["']([^"']+)["']\s*\)/g;
const strip = (s: string) => s.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/.*$/gm, "$1");

function file(base: string, platform: "native" | "web"): string | null {
  const exts = [`.${platform}.tsx`, `.${platform}.ts`, ".tsx", ".ts", `.${platform}.js`, ".js"];
  if (existsSync(base) && statSync(base).isFile()) return base;
  for (const e of exts) if (existsSync(base + e)) return base + e;
  for (const e of exts) if (existsSync(join(base, "index" + e))) return join(base, "index" + e);
  return null;
}

function resolveSpec(spec: string, from: string, platform: "native" | "web"): string | null {
  if (spec.startsWith(".")) return file(resolve(dirname(from), spec), platform);
  const m = /^@app\/([^/]+)(\/.*)?$/.exec(spec);
  if (!m) return null;                                            // a third-party package: not ours to walk
  const dir = join(ROOT, "packages", m[1]!);
  const pkg = JSON.parse(readFileSync(join(dir, "package.json"), "utf8")) as { main?: string; exports?: Record<string, string> };
  const sub = m[2] ? `.${m[2]}` : ".";
  const target = pkg.exports ? pkg.exports[sub] : sub === "." ? pkg.main : sub;
  if (!target) throw new Error(`${spec}: no such export of @app/${m[1]} (imported by ${relative(ROOT, from)})`);
  return file(join(dir, target), platform);
}

function graph(platform: "native" | "web"): Set<string> {
  const seen = new Set<string>(), todo = [ENTRY];
  while (todo.length) {
    const f = todo.pop()!;
    if (seen.has(f)) continue;
    seen.add(f);
    if (!/\.(tsx?|jsx?|mjs)$/.test(f)) continue;
    const src = strip(readFileSync(f, "utf8"));
    for (const x of src.matchAll(SPEC)) {
      const spec = x[1] ?? x[2] ?? x[3]!;
      const r = resolveSpec(spec, f, platform);
      if (r && !r.includes("node_modules")) todo.push(r);
    }
  }
  return new Set([...seen].map((f) => relative(ROOT, f)));
}

describe("links are web-only (T369)", () => {
  it("the native app's import graph holds no place / lines module", () => {
    const g = graph("native");
    expect(g.has("apps/mobile/src/App.tsx") && g.has("apps/mobile/src/startLink.native.ts")
           && g.has("packages/ui/src/startLink.ts")).toBe(true);
    expect(LINK_ONLY.filter((f) => g.has(f))).toEqual([]);
  });

  it("the web's graph reaches them (the walk finds what it is looking for)", () => {
    const g = graph("web");
    expect(LINK_ONLY.filter((f) => !g.has(f))).toEqual([]);
  });
});
