#!/usr/bin/env node
/**
 * `pnpm clean`: delete every workspace `dist/` (packages/*, apps/*, conformance).
 *
 * Nothing reads a `dist/` — every package's `main`/`types` is `./src/index.ts` and vitest aliases source — so these
 * hold only `tsc -b` declarations and build info, plus whatever an older typecheck left behind from deleted sources.
 * `tsc -b --clean` would miss those orphans (it removes only the outputs it still knows about), hence a plain delete.
 * The next `pnpm typecheck` rebuilds the declarations from scratch.
 */
import { existsSync, readdirSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

/** Remove the workspace `dist/` directories under `root`; returns the removed paths, relative and sorted. */
export function clean(root) {
  const removed = [];
  const consider = (rel) => {
    const abs = join(root, rel);
    if (existsSync(abs) && statSync(abs).isDirectory()) {
      rmSync(abs, { recursive: true, force: true });
      removed.push(rel);
    }
  };
  for (const group of ["packages", "apps"]) {
    const dir = join(root, group);
    if (!existsSync(dir)) continue;
    for (const name of readdirSync(dir)) consider(`${group}/${name}/dist`);
  }
  consider("conformance/dist");
  return removed.sort();
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const root = fileURLToPath(new URL("..", import.meta.url));
  const removed = clean(root);
  console.log(removed.length ? `removed ${removed.join(", ")}` : "nothing to clean");
}
