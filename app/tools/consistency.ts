/**
 * T60 — THE CONSISTENCY AUDITORS' SHARED SCANNER (audit N16, owner 2026-10-22: "strict consistency auditors that check
 * data and UI agree and that every route into a screen reaches the SAME screen").
 *
 * Six checks share this: one route per place (`one-route-per-place.test.ts`), one surface per action and back everywhere
 * (`apps/mobile/src/surfaces.test.tsx`, a render test), data-vs-UI (`data-vs-ui.test.ts`), one icon set
 * (`one-icon-set.test.ts`), one colouring pathway (`one-colour-pathway.test.ts`). Every static check walks the same
 * files, strips comments the same way, and is MUTATION-PINNED: each pattern is proven against a planted copy of the
 * defect it refuses, so a regex that quietly stopped matching cannot pass forever (`check-colours.test.ts` set the
 * shape).
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

export const APP = fileURLToPath(new URL("..", import.meta.url));
/** The app's UI: what draws, and the hooks that feed it. `packages/core` is the words' home and is scanned by name. */
export const UI_ROOTS = ["apps/mobile/src", "packages/ui/src", "packages/ui-native/src"] as const;

export function sources(dir: string, out: string[] = []): string[] {
  let names: string[];
  try { names = readdirSync(dir); } catch { return out; }
  for (const f of names) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".") || f.startsWith("_")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}

/** Comments out (a doc comment may name what the code may not use); strings kept. */
export const code = (src: string): string => src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\{\/\*[\s\S]*?\*\/\}/g, "")
  .replace(/(^|[^:"'`\\])\/\/.*$/gm, "$1");

/** Every UI source, app-relative, sorted. `roots` narrows the walk. */
export const files = (roots: readonly string[] = UI_ROOTS): string[] =>
  roots.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
export const read = (f: string): string => code(readFileSync(join(APP, f), "utf8"));
