/**
 * `pnpm clean` and a typecheck that emits no JavaScript (T70, audit wave 1 item 1.2).
 *
 * Every package resolves `main`/`types` to `./src/index.ts` and vitest aliases source, so nothing reads a `dist/`.
 * `tsc -b` used to write JS there anyway, and JS left behind by deleted sources (34 orphan files, ~10 MB) polluted
 * every grep. The typecheck now writes declarations only, and `pnpm clean` deletes every `dist/` outright.
 */
import { describe, expect, it } from "vitest";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { readdirSync, statSync } from "node:fs";
import { clean } from "./clean.mjs";

const APP = join(dirname(new URL(import.meta.url).pathname), "..");

function files(root: string): string[] {
  const out: string[] = [];
  const walk = (dir: string) => {
    for (const name of readdirSync(dir)) {
      const p = join(dir, name);
      if (statSync(p).isDirectory()) walk(p);
      else out.push(relative(root, p));
    }
  };
  walk(root);
  return out.sort();
}

describe("pnpm clean", () => {
  it("is wired as the `clean` script", () => {
    const pkg = JSON.parse(readFileSync(join(APP, "package.json"), "utf8"));
    expect(pkg.scripts.clean).toBe("node tools/clean.mjs");
  });

  it("removes every workspace dist/ and nothing else", () => {
    const root = mkdtempSync(join(tmpdir(), "clean-"));
    try {
      const put = (p: string) => {
        mkdirSync(dirname(join(root, p)), { recursive: true });
        writeFileSync(join(root, p), "x");
      };
      for (const p of [
        "packages/core/src/index.ts", "packages/core/dist/index.js", "packages/core/dist/tsconfig.tsbuildinfo",
        "packages/ui-native/dist/regs/old.js", "conformance/src/suite.ts", "conformance/dist/suite.d.ts",
        "apps/mobile/dist/App.js", "apps/mobile/src/App.tsx", "tools/dist-like.mjs",
      ]) put(p);
      const removed = clean(root);
      expect(removed).toEqual(["apps/mobile/dist", "conformance/dist", "packages/core/dist", "packages/ui-native/dist"]);
      expect(files(root)).toEqual([
        "apps/mobile/src/App.tsx", "conformance/src/suite.ts", "packages/core/src/index.ts", "tools/dist-like.mjs",
      ]);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("typecheck emits no JavaScript", () => {
  it("the base tsconfig writes declarations only (composite projects still need them)", () => {
    const base = JSON.parse(readFileSync(join(APP, "tsconfig.base.json"), "utf8"));
    expect(base.compilerOptions.emitDeclarationOnly).toBe(true);
  });
});
