/**
 * THE ORACLE FILES (app/fixtures/golden/) — the one module every regs parity suite reads them through:
 * golden.json (`golden()`), taps.json (`taps()`), hydrate.json (`hydrate()`), their paths and REPO. Kept apart from
 * golden.ts so exclusions.ts (which golden.ts's `cases` reads) can compute its id lists at load without an import cycle;
 * golden.ts re-exports these.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { GoldenFile, TapsFile } from "@app/core/regs/testing";

export const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
export const GOLDEN_DIR = `${REPO}app/fixtures/golden/`;
export const TAPS = `${GOLDEN_DIR}taps.json`;
export const HYDRATE = `${GOLDEN_DIR}hydrate.json`;

/** hydrate.json (port/hydrate.mjs: page v36's decoded rules and describe() per rule, and each state's via / ranks). */
export interface Hydrate {
  readonly meta: { readonly generator: string; readonly ref: string; readonly sha: string; readonly golden: string; readonly version: string };
  readonly describe: Readonly<Record<string, string>>;
  readonly rules: Readonly<Record<string, Readonly<Record<string, unknown>>>>;
  readonly states: Readonly<Record<string, { readonly via: Readonly<Record<string, "trib">>; readonly ranks: Readonly<Record<string, number>> }>>;
}

/**
 * (T109 step 1) Page v36's levelTag read a zone row's id with `/^z(\d+)/`, so a `z7a:`/`z7b:` rule's level word was
 * "Region 7" while the pipeline's own words on the same line say "From the Region 7B table" (and Python's region words
 * "Region 7B"). The app reads the lettered zone (level.ts). The golden is held to that ruling by ONE rewrite applied at
 * load: in a state whose zone lines name exactly one zone (7A or 7B), the level word "Region 7" before " ›" or before
 * "From the Region 7X table" becomes "Region 7X". Nothing else in the golden moves; `ZONE7_REWRITES` counts what it did
 * (asserted in exclusions.test.ts), and a state with a "Region 7" level word and no single zone throws.
 */
const ZONE7_LEVEL = /\bRegion 7(?= ›| From the Region 7[AB] table)/g;
export const ZONE7_REWRITES: Record<string, number> = {};
function zone7Ruled(id: string, state: unknown): unknown {
  const text = JSON.stringify(state);
  if (!ZONE7_LEVEL.test(text)) return state;
  ZONE7_LEVEL.lastIndex = 0;
  const zones = new Set([...text.matchAll(/From the Region 7([AB]) table/g)].map((m) => m[1]!));
  if (zones.size !== 1) throw new Error(`golden ${id}: a "Region 7" level word with zones {${[...zones].join(",")}} — T109 cannot rule it`);
  const z = [...zones][0]!;
  let n = 0;
  const out = JSON.parse(text.replace(ZONE7_LEVEL, () => { n++; return `Region 7${z}`; }));
  ZONE7_REWRITES[id] = n;
  return out;
}

/**
 * (2026-10-26 owner, handoff/PLAN/curation-review/zone-entry-merges.md) The zone DAILY QUOTA merge: each region's
 * trout/char quota and "Other daily quotas" (7B: and its Arctic grayling block) are one entry, `zN:daily_quotas`, as the
 * book prints them ("Region N Daily Quotas"). Every rule_id was kept; only the entry id moved, and the binding diff showed
 * the same sections under the new keys. Page v36 predates it, so its keys are renamed ONCE at load — in golden.json,
 * hydrate.json and #data.rid — and nothing else in the oracle moves. `ENTRY_RENAMES` counts what it did per file
 * (asserted in exclusions.test.ts); an entry here the oracle no longer names throws, so a re-cut oracle retires it.
 */
export const ORACLE_ENTRY_RENAMED: Readonly<Record<string, string>> = {
  "z1:trout_quota": "z1:daily_quotas", "z1:species_quotas": "z1:daily_quotas",
  "z2:trout_char_quota": "z2:daily_quotas", "z2:species_quotas": "z2:daily_quotas",
  "z3:trout_char_quota": "z3:daily_quotas", "z3:species_quotas": "z3:daily_quotas",
  "z4:trout_char_quota": "z4:daily_quotas", "z4:species_quotas": "z4:daily_quotas",
  "z5:trout_char_quota": "z5:daily_quotas", "z5:species_quotas": "z5:daily_quotas",
  "z6:trout_char_quota": "z6:daily_quotas", "z6:species_quotas": "z6:daily_quotas",
  "z7a:trout_char_quota": "z7a:daily_quotas", "z7a:species_quotas": "z7a:daily_quotas",
  "z7b:trout_char_quota": "z7b:daily_quotas", "z7b:arctic_grayling": "z7b:daily_quotas", "z7b:species_quotas": "z7b:daily_quotas",
  "z8:trout_char_quota": "z8:daily_quotas", "z8:species_quotas": "z8:daily_quotas",
};
const RENAMED_RE = new RegExp(`\\b(${Object.keys(ORACLE_ENTRY_RENAMED).join("|")})(?![a-z_])`, "g");
export const ENTRY_RENAMES: Record<string, number> = {};
function entryRenamed(file: string, text: string): string {
  let n = 0;
  const out = text.replace(RENAMED_RE, (m) => { n++; return ORACLE_ENTRY_RENAMED[m]!; });
  if (n === 0) throw new Error(`${file}: names none of ORACLE_ENTRY_RENAMED — the oracle was re-cut; drop the map`);
  ENTRY_RENAMES[file] = n;
  return out;
}

let G: GoldenFile | undefined;
/** golden.json (8.6 MB), parsed once per test file, with the zone daily-quota entry renames (`entryRenamed`) and the T109
 *  zone-level ruling applied (`zone7Ruled`). */
export function golden(): GoldenFile {
  if (G) return G;
  const raw = JSON.parse(entryRenamed("golden.json", readFileSync(`${GOLDEN_DIR}golden.json`, "utf8"))) as GoldenFile & { states: Record<string, unknown> };
  for (const id of Object.keys(raw.states)) raw.states[id] = zone7Ruled(id, raw.states[id]) as never;
  return (G = raw);
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
  return (H ??= JSON.parse(entryRenamed("hydrate.json", readFileSync(HYDRATE, "utf8"))) as Hydrate);
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
  const html = entryRenamed("page_v36.html", readFileSync(`${REPO}app/design/regs-v11/reference/page_v36.html`, "utf8"));
  const m = html.match(/<script[^>]*id="data"[^>]*>([\s\S]*?)<\/script>/);
  if (!m) throw new Error("page_v36.html has no #data");
  return (RID = new Map(Object.entries((JSON.parse(m[1]!) as { rid: Record<string, string> }).rid).map(([k, v]) => [Number(k), v])));
}
