/**
 * THE ORACLE FILES (app/design/regs-v11/port/golden/) — the one module every regs parity suite reads them through:
 * golden.json (`golden()`), taps.json (`taps()`), hydrate.json (`hydrate()`), their paths and REPO. Kept apart from
 * golden.ts so exclusions.ts (which golden.ts's `cases` reads) can compute its id lists at load without an import cycle;
 * golden.ts re-exports these.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { GoldenFile, TapsFile } from "@app/core/regs/testing";

export const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
export const GOLDEN_DIR = `${REPO}app/design/regs-v11/port/golden/`;
export const TAPS = `${GOLDEN_DIR}taps.json`;
export const HYDRATE = `${GOLDEN_DIR}hydrate.json`;

/** hydrate.json (port/hydrate.mjs: page v36's decoded rules and describe() per rule, and each state's via / ranks). */
export interface Hydrate {
  readonly meta: { readonly generator: string; readonly ref: string; readonly sha: string; readonly golden: string; readonly version: string };
  readonly describe: Readonly<Record<string, string>>;
  readonly rules: Readonly<Record<string, Readonly<Record<string, unknown>>>>;
  readonly states: Readonly<Record<string, { readonly via: Readonly<Record<string, "trib">>; readonly ranks: Readonly<Record<string, number>> }>>;
}

let G: GoldenFile | undefined;
/** golden.json (8.6 MB), parsed once per test file. */
export function golden(): GoldenFile {
  return (G ??= JSON.parse(readFileSync(`${GOLDEN_DIR}golden.json`, "utf8")) as GoldenFile);
}

/** taps.json, parsed fresh (callers that hold it keep their own copy; `tapsOnce` is the shared one). */
export function taps(): TapsFile {
  return JSON.parse(readFileSync(TAPS, "utf8")) as TapsFile;
}
let TP: TapsFile | undefined;
export const tapsOnce = (): TapsFile => (TP ??= taps());

/** pins.json (port/pins.mjs, T28): the regs v38 pinUpdate inputs golden.json's `pins` block lacks — each tier's `end`
 *  (secEnd) and each drawn line's measured height `h` — recorded from the same prototype (same version and sha). */
export interface PinsOracle {
  readonly meta: { readonly generator: string; readonly ref: string; readonly sha: string; readonly version: string; readonly states: number };
  readonly states: Readonly<Record<string, {
    readonly error: unknown;
    readonly viewport: { readonly width: number; readonly height: number; readonly header: number; readonly LH: number; readonly ROW: number };
    readonly anchors: readonly {
      readonly row: string; readonly group: boolean; readonly tree: boolean; readonly name: string; readonly formA: boolean;
      readonly headlineN: number | null; readonly top: number; readonly height: number;
      readonly pin: { readonly top: number; readonly height: number } | null;
      readonly tiers: readonly { readonly tid: string; readonly tier: string; readonly n: number | null;
        readonly head: { readonly top: number; readonly height: number } | null; readonly end: number | null }[];
    }[];
    readonly samples: readonly { readonly y: number; readonly height: number;
      readonly lines: readonly { readonly cls: "g" | "g two" | "c"; readonly text: string; readonly sub: string | null; readonly h: number }[] }[];
  }>>;
}
let PO: PinsOracle | undefined;
export function pinsOracle(): PinsOracle {
  return (PO ??= JSON.parse(readFileSync(`${GOLDEN_DIR}pins.json`, "utf8")) as PinsOracle);
}

let H: Hydrate | undefined;
/** hydrate.json (T06/T07/T08's oracle), parsed once per test file. */
export function hydrate(): Hydrate {
  return (H ??= JSON.parse(readFileSync(HYDRATE, "utf8")) as Hydrate);
}
