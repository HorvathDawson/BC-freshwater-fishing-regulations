/**
 * One vocabulary of rule types, and one type-to-family mapping, held across the two
 * languages that use them.
 *
 * `RuleType` is an enum in `pipeline/regs/parsing/catalogue.py`; `RuleType` is a union
 * hand-typed in `packages/core/src/status.ts`, `RULE_TYPES` is its runtime copy, and
 * `TYPES` in `bundle/source.ts` is a third. Four lists of the same fifteen strings.
 *
 * WHAT MAKES IT DANGEROUS RATHER THAN UNTIDY is the fallback. `toRule` maps a type it does
 * not recognise to `"advisory"` — a sane default for an unknown, and the worst possible one
 * for a MISSED restriction: an advisory restricts nobody, so a sixteenth type would reach a
 * reader as open water with a remark attached. Silently, on the screen where being wrong
 * costs a fine.
 *
 * The FAMILY mapping is checked for the same reason: the bundle ships `family` per rule, but
 * `FAMILY_OF` exists for constructing rules, and a family the app files under the wrong
 * heading puts a closure in the "information" section, which `severityOf` reads as open.
 */
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const ROOT = new URL("../../", import.meta.url).pathname;
const PY = `${ROOT}.venv/bin/python`;

function python<T>(body: string): T {
  const script = `
import json, sys
sys.path.insert(0, ${JSON.stringify(ROOT)})
${body}
`;
  return JSON.parse(execFileSync(PY, ["-c", script], { encoding: "utf8" }));
}

/** The pipeline's own enum, asked of the pipeline. */
const fromPipeline = () => python<string[]>(
  "from pipeline.regs.parsing.catalogue import RuleType\n" +
  "print(json.dumps([m.value for m in RuleType]))");

/** The pipeline's type -> family map, asked of the pipeline. */
const familiesFromPipeline = () => python<Record<string, string>>(
  "from pipeline.regs.parsing.catalogue import _FAMILY\n" +
  "print(json.dumps({k.value: v for k, v in _FAMILY.items()}))");

/**
 * The app's union, read from its SOURCE rather than imported.
 *
 * A TypeScript union is erased at runtime — importing it would give nothing to compare. The
 * declaration is the artifact that has to stay in step, so the declaration is what is read.
 */
function fromCore(): string[] {
  const src = readFileSync(`${ROOT}app/packages/core/src/status.ts`, "utf8");
  const m = src.match(/export type RuleType =([\s\S]*?);/);
  if (!m) throw new Error("RuleType not found in status.ts");
  return [...m[1]!.matchAll(/"([a-z_]+)"/g)].map((x) => x[1]!);
}

/** The runtime list core exports — a second copy, in the same file as the first. */
function fromCoreRuntime(): string[] {
  const src = readFileSync(`${ROOT}app/packages/core/src/status.ts`, "utf8");
  const m = src.match(/export const RULE_TYPES[\s\S]*?\[([\s\S]*?)\] as const;/);
  if (!m) throw new Error("RULE_TYPES not found in status.ts");
  return [...m[1]!.matchAll(/"([a-z_]+)"/g)].map((x) => x[1]!);
}

/** The app's family map, read from source for the same reason. */
function familiesFromCore(): Record<string, string> {
  const src = readFileSync(`${ROOT}app/packages/core/src/status.ts`, "utf8");
  const m = src.match(/export const FAMILY_OF: Record<RuleType, RuleFamily> = \{([\s\S]*?)\};/);
  if (!m) throw new Error("FAMILY_OF not found in status.ts");
  return Object.fromEntries(
    [...m[1]!.matchAll(/([a-z_]+):\s*"([a-z_]+)"/g)].map((x) => [x[1]!, x[2]!]));
}

const has = existsSync(PY);

describe("rule types", () => {
  it("are the same fifteen strings in the app's type and its runtime list", () => {
    // Both in TypeScript, in one file, and still two lists.
    expect([...fromCoreRuntime()].sort()).toEqual([...fromCore()].sort());
  });

  it.skipIf(!has)("match the pipeline's enum exactly", () => {
    /*
     * Exactly, not "the app knows at least as many". A type the app knows and the pipeline
     * never emits is dead code that reads as coverage; a type the pipeline emits and the
     * app does not know becomes "advisory", which is the failure this file exists for.
     */
    expect([...fromCore()].sort()).toEqual([...fromPipeline()].sort());
  });

  it.skipIf(!has)("file every type under the same family the pipeline does", () => {
    expect(familiesFromCore()).toEqual(familiesFromPipeline());
  });
});
