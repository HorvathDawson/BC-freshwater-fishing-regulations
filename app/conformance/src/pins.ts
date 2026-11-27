/**
 * (T1135) THE RELEASE PINS, the app half (the pipeline half: `pipeline/tests/pins.py`, same contract).
 *
 * A value a test copies from a release (a page, a count) lives in ONE file, app/fixtures/golden/release-pins.json,
 * re-cut with the fixtures by `node app/tools/regs-recut/sync.mjs` (its last step, `pins.mjs`). A test asks
 * `pin("reads.glossary", { v, n })`:
 *  - compare (the default): the value must equal the file's; a name the file does not hold FAILS (never a pass);
 *  - record (PIN_RECORD=<dir>, set by the re-cut alone): the value is written to <dir>/app/<name>.json and nothing is
 *    asserted. The re-cut writes the file from the records and drops (and names) a pin no test asked for.
 * Each pin is { answers, value }: `answers: true` is words or pages an angler reads (a moved one stops the release for a
 * look, like a golden page); `false` is a count or an id (committed with the release). Budgets and limits are never
 * pins: they stay typed in their tests.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { expect } from "vitest";

const REPO = fileURLToPath(new URL("../../../", import.meta.url));
export const PINS_FILE = fileURLToPath(new URL("../../fixtures/golden/release-pins.json", import.meta.url));
export const RECUT = "node app/tools/regs-recut/sync.mjs";
const NAME = /^[A-Za-z0-9][A-Za-z0-9_.-]*$/;
const rel = (f: string) => relative(REPO, f) || f;
/** Keys sorted at every depth: one spelling for one value (the record a second call compares with). */
const sorted = (v: unknown): unknown => Array.isArray(v) ? v.map(sorted)
  : v && typeof v === "object" ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, sorted((v as Record<string, unknown>)[k])])) : v;

export interface Pin { answers: boolean; value: unknown }

/** The pins of a pins file; throws naming the file when it is not one. */
export function readPins(file: string): Record<string, Pin> {
  const doc = JSON.parse(readFileSync(file, "utf8")) as { pins?: unknown };
  if (!doc || typeof doc.pins !== "object" || doc.pins === null) throw new Error(`${rel(file)}: no "pins" object`);
  return doc.pins as Record<string, Pin>;
}

/** A `pin(name, actual)` over `file`, recording into `record` when it is set (the env PIN_RECORD). */
export function makePin(file: string, record: string | undefined): (name: string, actual: unknown) => void {
  let pins: Record<string, Pin> | null = null;
  return (name, actual) => {
    if (!NAME.test(name)) throw new Error(`pin ${JSON.stringify(name)}: a pin name is letters, digits, '.', '_' or '-'`);
    if (actual === undefined) throw new Error(`pin ${name}: no value (undefined)`);
    const value = sorted(JSON.parse(JSON.stringify(actual)));          // what the file can hold, in one spelling
    if (record) {
      const dir = join(record, "app"), out = join(dir, `${name}.json`);
      const body = `${JSON.stringify({ name, value })}\n`;
      mkdirSync(dir, { recursive: true });
      if (existsSync(out) && readFileSync(out, "utf8") !== body) throw new Error(`pin ${name}: recorded twice with different values (${out})`);
      writeFileSync(out, body);
      process.stderr.write(`pin ${name}: RECORDED, not compared (PIN_RECORD=${record})\n`);   // record mode is never silent
      return;
    }
    pins ??= readPins(file);
    if (!Object.hasOwn(pins, name)) {
      throw new Error(`pin ${name}: not in ${rel(file)} — add its row by hand with its answers flag `
        + `({"answers": true|false, "value": ...}); the re-cut keeps it: ${RECUT}`);
    }
    expect(value, `pin ${name} (${rel(file)}): the release says otherwise. If the release is right, re-cut (${RECUT}); `
      + "a release re-cuts it itself").toEqual(pins[name]!.value);
  };
}

export const pin = makePin(PINS_FILE, process.env.PIN_RECORD || undefined);
