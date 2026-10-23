/**
 * ONE SHOWN QUANTITY (T106; owner 2026-10-22, "unify checks that decide the same thing").
 *
 * "Which quantity does a station's reading show, discharge or level?" is decided by the feed publisher
 * (`pipeline/gauges/feed/publish.py`, `parameter`) and read in the app by ONE function, core's `shownParameter`, with
 * the value in it from `readingIn`. Four places used to infer it from which value was present
 * (`c.discharge != null ? "discharge" : "level"`), and 08ME030 — both quantities, declared level — printed its
 * 5.46 m³/s beside its LEVEL percentile.
 *
 * This pins that no module outside `packages/core` picks a quantity by testing a value for null, falls back from one
 * quantity's value to the other's, or re-states the undeclared default. Each pattern is proven able to fail on the
 * spelling it replaced (the `SPELLINGS` cases below), so a pass is not a regex that cannot match.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { readingIn, shownParameter } from "@app/core";

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
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p))
  .filter((p) => !p.startsWith("packages/core/")).sort();

/** The ways a module can decide the quantity itself. */
const INFERS: Record<string, RegExp> = {
  // `c.discharge != null ? "discharge" : "level"`
  ternary: /\b(?:discharge|level)\s*(?:!==?|===?)\s*(?:null|undefined)[^;{}]{0,160}?\?\s*["'](?:discharge|level)["']/,
  // `if (days.some((d) => d.discharge !== null)) return "discharge";`
  early: /\b(?:discharge|level)\s*(?:!==?|===?)\s*(?:null|undefined)\)*\s*return\s+["'](?:discharge|level)["']/,
  // `(c.discharge ?? c.level)` — one quantity's value standing in for the other's
  fallback: /\.discharge\s*\?\?\s*[\w?.]*\.level\b|\.level\s*\?\?\s*[\w?.]*\.discharge\b/,
  // `row.parameter ?? "discharge"` — a second copy of the undeclared default
  defaults: /\.parameter\s*\?\?\s*["'](?:discharge|level)["']/,
};

/** The spellings T106 replaced, one per pattern: each must be caught, or the pin proves nothing. */
const SPELLINGS: Record<string, string> = {
  ternary: `const shown: Parameter = c.discharge != null ? "discharge" : "level";`,
  early: `  if (days.some((d) => d.discharge !== null)) return "discharge";`,
  fallback: `      figure: (c.discharge ?? c.level)!,`,
  defaults: `    const param = row.parameter ?? "discharge";`,
};

describe("one shown quantity: core's shownParameter decides, nothing else (T106)", () => {
  it("each pattern catches the spelling it replaced", () => {
    expect(Object.fromEntries(Object.entries(INFERS).map(([k, re]) => [k, re.test(SPELLINGS[k]!)])))
      .toEqual({ ternary: true, early: true, fallback: true, defaults: true });
  });

  it("no module outside core infers the quantity, falls back across quantities, or re-states the default", () => {
    const hits = files().flatMap((p) => {
      const text = readFileSync(join(APP, p), "utf8");
      return Object.entries(INFERS).filter(([, re]) => re.test(text)).map(([k]) => `${p}: ${k}`);
    });
    expect(hits).toEqual([]);
  });

  it("the decision itself: the declaration, else discharge (the publisher's default) — never the value present", () => {
    // 08ME030's `now` as the feed published it 2026-10-22
    const both = { discharge: 5.46, level: 4.162 };
    expect([
      shownParameter({ ...both, parameter: "level" }),
      shownParameter({ ...both, parameter: "discharge" }),
      shownParameter({ discharge: null, level: 1.4 }),               // undeclared, level only: still discharge
      shownParameter({ discharge: null, level: 1.4, parameter: null }),
      readingIn({ ...both, parameter: "level" }, "level"),
      readingIn(both, "discharge"),
      readingIn({ discharge: null, level: 1.4 }, "discharge"),       // never the other quantity's number
      readingIn({}, "level"),
    ]).toEqual(["level", "discharge", "discharge", "discharge", 4.162, 5.46, null, null]);
  });
});
