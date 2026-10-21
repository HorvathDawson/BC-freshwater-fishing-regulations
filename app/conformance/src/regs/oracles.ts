/**
 * The golden oracle FILES (app/design/regs-v11/port/golden/), each read once per test file. Kept apart from golden.ts so
 * exclusions.ts (which golden.ts's `cases` reads) can compute PINS_ERROR_IDS at load without an import cycle; golden.ts
 * re-exports these.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { GoldenFile, TapsFile } from "@app/core/regs/testing";

export const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
export const GOLDEN_DIR = `${REPO}app/design/regs-v11/port/golden/`;

let G: GoldenFile | undefined;
/** golden.json (8.6 MB), parsed once per test file. */
export function golden(): GoldenFile {
  return (G ??= JSON.parse(readFileSync(`${GOLDEN_DIR}golden.json`, "utf8")) as GoldenFile);
}

/** taps.json, parsed fresh (callers that hold it keep their own copy; `tapsOnce` is the shared one). */
export function taps(): TapsFile {
  return JSON.parse(readFileSync(`${GOLDEN_DIR}taps.json`, "utf8")) as TapsFile;
}
let TP: TapsFile | undefined;
export const tapsOnce = (): TapsFile => (TP ??= taps());

let H: unknown;
/** hydrate.json (T06/T07/T08's oracle), parsed once per test file. */
export function hydrate(): unknown {
  return (H ??= JSON.parse(readFileSync(`${GOLDEN_DIR}hydrate.json`, "utf8")));
}
