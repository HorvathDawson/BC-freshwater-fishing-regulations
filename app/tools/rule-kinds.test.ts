/**
 * One vocabulary of restriction types, held across the two languages that use it.
 *
 * `RestrictionType` is an enum in `pipeline/regs/parsing/entry_models.py`; `RuleKind` is a
 * union hand-typed in `packages/core/src/status.ts`. Two lists of the same six strings,
 * maintained by hand, in different languages.
 *
 * WHAT MAKES IT DANGEROUS RATHER THAN UNTIDY is the fallback. `toRule` maps a kind it does
 * not recognise to `"note"` — a sane default for an unknown, and the worst possible one for
 * a MISSED closure: a note does not close anything, so a seventh restriction type would
 * reach a reader as open water with a remark attached. Silently, on the screen where being
 * wrong costs a fine.
 *
 * So the enum is read out of Python and compared. The same shape as
 * `tools/one-formula.test.ts`, which executes the real weighting function in both languages
 * rather than trusting that two implementations still agree.
 */
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const ROOT = new URL("../../", import.meta.url).pathname;
const PY = `${ROOT}.venv/bin/python`;

/** The pipeline's own enum, asked of the pipeline. */
function fromPipeline(): string[] {
  const script = `
import json, sys
sys.path.insert(0, ${JSON.stringify(ROOT)})
from pipeline.regs.parsing.entry_models import RestrictionType
print(json.dumps([m.value for m in RestrictionType]))
`;
  return JSON.parse(execFileSync(PY, ["-c", script], { encoding: "utf8" }));
}

/**
 * The app's union, read from its SOURCE rather than imported.
 *
 * A TypeScript union is erased at runtime — importing it would give nothing to compare. The
 * declaration is the artifact that has to stay in step, so the declaration is what is read.
 */
function fromCore(): string[] {
  const src = readFileSync(`${ROOT}app/packages/core/src/status.ts`, "utf8");
  const m = src.match(/export type RuleKind =([\s\S]*?);/);
  if (!m) throw new Error("RuleKind not found in status.ts");
  return [...m[1]!.matchAll(/"([a-z_]+)"/g)].map((x) => x[1]!);
}

/** The runtime guard list, which is a THIRD copy and the one that decides the fallback. */
function fromSource(): string[] {
  const src = readFileSync(`${ROOT}app/packages/data/src/bundle/source.ts`, "utf8");
  const m = src.match(/const KINDS = new Set<RuleKind>\(\[([\s\S]*?)\]\)/);
  if (!m) throw new Error("KINDS not found in source.ts");
  return [...m[1]!.matchAll(/"([a-z_]+)"/g)].map((x) => x[1]!);
}

const has = existsSync(PY);

describe("restriction types", () => {
  it("are the same six strings in the app's type and its runtime guard", () => {
    // These two are both in TypeScript and still managed to be separate lists.
    expect([...fromSource()].sort()).toEqual([...fromCore()].sort());
  });

  it.skipIf(!has)("match the pipeline's enum exactly", () => {
    /*
     * Exactly, not "the app knows at least as many". A kind the app knows and the pipeline
     * never emits is dead code that reads as coverage; a kind the pipeline emits and the
     * app does not know becomes "note", which is the failure this file exists for.
     */
    expect([...fromCore()].sort()).toEqual([...fromPipeline()].sort());
  });
});
