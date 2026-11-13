/**
 * THE ORACLE FILES (app/fixtures/golden/) — the one module every regs parity suite reads them through:
 * taps.json (`taps()`), page-oracle.json (`pageOracle()`), their paths and REPO; and the frozen page's inputs (`pageHtml`). (golden.json, pins.json, tokens.json and
 * inventory.json, the retired prototype extracts, are gone: T439.) golden.ts re-exports these.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { TapsFile } from "@app/core/regs/testing";

export const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
export const GOLDEN_DIR = `${REPO}app/fixtures/golden/`;
export const TAPS = `${GOLDEN_DIR}taps.json`;

/**
 * page_v36.html's FROZEN inputs are read with the 2026-10-26 zone daily-quota entry merge applied (owner,
 * handoff/PLAN/curation-review/zone-entry-merges.md: each region's trout/char quota and "Other daily quotas" became one
 * entry, zN:daily_quotas; every rule_id kept), because core's store-free fixtures (packages/core/src/regs/testing/
 * ruleRows.ts) were copied in the renamed form and app/tools/regs-fixtures.test.ts holds them to the page. FROZEN: it
 * never grows — a later ruling moves the release, which the parities read through page-oracle.json and pageDecode, never
 * the frozen page (T667 slice 5b retired the per-ruling table, v36-rulings.json). Throws if the page names none of them.
 */
const RENAMED: Readonly<Record<string, string>> = {
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
const RENAMED_RE = new RegExp(`\\b(${Object.keys(RENAMED).join("|")})(?![a-z_])`, "g");
function entryRenamed(file: string, text: string): string {
  let n = 0;
  const out = text.replace(RENAMED_RE, (m) => { n++; return RENAMED[m]!; });
  if (n === 0) throw new Error(`${file}: names none of the renamed zone entries — the page was re-cut; drop the rename`);
  return out;
}

/** taps.json, parsed fresh (callers that hold it keep their own copy; `tapsOnce` is the shared one). */
export function taps(): TapsFile {
  return JSON.parse(readFileSync(TAPS, "utf8")) as TapsFile;
}
let TP: TapsFile | undefined;
export const tapsOnce = (): TapsFile => (TP ??= taps());

/** A rule as the page oracle names it: its durable key, "entry::rule" (never a rix — the oracle does not renumber). */
type RuleKey = string;
type Res = { readonly status: string; readonly daily: number | string | null; readonly win: RuleKey; readonly narrow: RuleKey | null } | null;
/** One golden state in page-oracle.json (regs-recut/page-oracle.mjs: page v36's code over the release under test). */
export interface PageOracleState {
  readonly s: number;
  readonly via: Readonly<Record<RuleKey, "trib">>;
  readonly ranks: Readonly<Record<RuleKey, number>>;
  readonly settle: {
    readonly status: string | null; readonly broad: readonly RuleKey[]; readonly speaks: readonly RuleKey[];
    readonly beside: readonly RuleKey[]; readonly shown: readonly RuleKey[]; readonly inpart: readonly (readonly [RuleKey, boolean])[];
    readonly lifted: readonly (readonly [RuleKey, RuleKey | null])[]; readonly timed: readonly RuleKey[]; readonly side: readonly RuleKey[];
    readonly standing: readonly RuleKey[]; readonly anglerclosed: readonly RuleKey[];
  };
  readonly model: {
    readonly spp: readonly string[]; readonly line: string | null; readonly missingRows: boolean; readonly status: string | null;
    readonly broad: readonly RuleKey[];
    readonly rows: readonly { readonly kind: string; readonly pool: RuleKey | null; readonly win: RuleKey | null;
      readonly daily: number | string | null; readonly narrow: RuleKey | null; readonly members: readonly string[] }[];
    readonly R: Readonly<Record<string, { readonly H: Res; readonly W: Res }>>;
  };
  readonly dayStatus: readonly (readonly (readonly [string, number])[])[];
}
interface PageOracle {
  readonly meta: { readonly generator: string; readonly page: string; readonly golden: string; readonly version: string; readonly year: number };
  readonly states: Readonly<Record<string, PageOracleState>>;
}
const PAGE_ORACLE = `${GOLDEN_DIR}page-oracle.json`;
let PO: PageOracle | undefined;
/**
 * page-oracle.json (T667 slice 5b): what page v36's OWN code says of every tapped golden state when it reads the RELEASE
 * the goldens were cut from (regs-recut/page-oracle.mjs, through the Python reference decoder). Re-cut by sync.mjs with
 * taps.json; rules by durable key, so it changes only when an answer does (release_follows `page-oracle`: a stop).
 */
export function pageOracle(): PageOracle {
  return (PO ??= JSON.parse(readFileSync(PAGE_ORACLE, "utf8")) as PageOracle);
}

let HTML: string | undefined;
/** page_v36.html with the zone daily-quota renames applied (`entryRenamed`), read once: its #data.rid here, and
 *  ruleSources' page source (whose rules carry the entry ids). */
export function pageHtml(): string {
  return (HTML ??= entryRenamed("page_v36.html", readFileSync(`${REPO}app/fixtures/reference/page_v36.html`, "utf8")));
}
