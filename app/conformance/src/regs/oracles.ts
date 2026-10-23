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

/** `oracleRix` over any rule set keyed by rix (a store's `rules()`, a RuleSource's `stored`). */
export function oracleRixOver(rules: Iterable<readonly [number, { readonly export: { readonly entry_id: string; readonly rule_id: string } }]>):
  (oracle: number) => number {
  const byKey = new Map<string, number>();
  for (const [rix, r] of rules) byKey.set(`${r.export.entry_id}::${r.export.rule_id}`, rix);
  const rid = pageRid();
  return (oracle: number) => {
    const key = rid.get(oracle);
    if (key === undefined) throw new Error(`oracle rule ${oracle} is not in page_v36.html's #data.rid`);
    const now = byKey.get(key);
    if (now === undefined) throw new Error(`oracle rule ${oracle} (${key}) is not in the store: a content change — re-cut the page blocks and hydrate.json`);
    return now;
  };
}
let RID: Map<number, string> | undefined;
/** page_v36.html's `#data.rid` (oracle rule number → "entry::rule"), read once. */
function pageRid(): Map<number, string> {
  if (RID) return RID;
  const html = readFileSync(`${REPO}pipeline/deliver/answers/reference/page_v36.html`, "utf8");
  const m = html.match(/<script[^>]*id="data"[^>]*>([\s\S]*?)<\/script>/);
  if (!m) throw new Error("page_v36.html has no #data");
  return (RID = new Map(Object.entries((JSON.parse(m[1]!) as { rid: Record<string, string> }).rid).map(([k, v]) => [Number(k), v])));
}
