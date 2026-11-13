/**
 * THE ORACLE FILES (app/fixtures/golden/) — the one module every regs parity suite reads them through:
 * taps.json (`taps()`), page-oracle.json (`pageOracle()`), their paths and REPO. (hydrate.json is read only by card.parity's
 * row-index check, until the Python year-strip test that needs it goes, T667 5b.) (golden.json, pins.json, tokens.json and
 * inventory.json, the retired prototype extracts, are gone: T439.) golden.ts re-exports these.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { TapsFile } from "@app/core/regs/testing";

export const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
export const GOLDEN_DIR = `${REPO}app/fixtures/golden/`;
export const TAPS = `${GOLDEN_DIR}taps.json`;
export const HYDRATE = `${GOLDEN_DIR}hydrate.json`;

/**
 * THE RULINGS PAGE v36 PREDATES (T667 slice 5): `app/fixtures/reference/v36-rulings.json`, one entry per ruling, read here
 * and by `pipeline/tests/test_answers_year_strip_live.py` — a new ruling is one entry there, never a table per test. Each
 * export below is that file's entry under the name the parities have always read; its `why` is the ruling and its source.
 */
const RULINGS_FILE = `${REPO}app/fixtures/reference/v36-rulings.json`;
interface Rulings {
  readonly zone_daily_quota_merge: { readonly renamed: Readonly<Record<string, string>> };
  readonly overlap_retired: {
    readonly lois_fold: { readonly id: string; readonly retired: string; readonly into: string; readonly row: number } };
  readonly peace_kokanee: { readonly item: string; readonly out: string; readonly base: string; readonly daily: number;
    readonly kept_rows: Readonly<Record<string, number>> };
  readonly kitimat_nov_jun: { readonly rows: Readonly<Record<string, number>> };
}
const RULINGS = JSON.parse(readFileSync(RULINGS_FILE, "utf8")) as Rulings;

/** (zone_daily_quota_merge) page v36's entry ids renamed ONCE at load; an entry the oracle no longer names throws. */
export const ORACLE_ENTRY_RENAMED: Readonly<Record<string, string>> = RULINGS.zone_daily_quota_merge.renamed;
const RENAMED_RE = new RegExp(`\\b(${Object.keys(ORACLE_ENTRY_RENAMED).join("|")})(?![a-z_])`, "g");
function entryRenamed(file: string, text: string): string {
  let n = 0;
  const out = text.replace(RENAMED_RE, (m) => { n++; return ORACLE_ENTRY_RENAMED[m]!; });
  if (n === 0) throw new Error(`${file}: names none of ORACLE_ENTRY_RENAMED — the oracle was re-cut; drop the map`);
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

/** page v36's rule number for a key (after the renames); throws when the page lacks it. */
export function oracleRixOf(key: string): number {
  for (const [o, k] of pageRid()) if (k === key) return o;
  throw new Error(`page_v36.html has no rule ${key}`);
}
/** (overlap_retired.lois_fold) The one golden state where a retired rule drew a ROW of its own (Lois Lake): its row folds
 *  into `into`'s; parities that compare the card's rows amend page v36's by this. `row` is the folded row's index. */
export const LOIS_FOLD = RULINGS.overlap_retired.lois_fold;

/**
 * (peace_kokanee) Zone B's kokanee stream release (`out`) binds no Peace River section; page v36 predates that. On a Peace
 * state the oracle is amended (settle.parity, card.parity), and each amendment throws once the oracle no longer names `out`
 * on that state (a re-cut): where a Peace row's own quota lifted it the `lifted` pair is gone; where nothing lifted it
 * (`kept_rows`) the table's "Kokanee: 10" (`base`) answers, keep `daily` a day all year.
 */
export const PEACE_KOKANEE = RULINGS.peace_kokanee;
/** (kitimat_nov_jun) the one row per Kitimat state whose Nov 1-June 30 'keep' days the ruling makes 'release'. */
export const KITIMAT_RULED = RULINGS.kitimat_nov_jun.rows;

let HTML: string | undefined;
/** page_v36.html with the zone daily-quota renames applied (`entryRenamed`), read once: its #data.rid here, and
 *  ruleSources' page source (whose rules carry the entry ids). */
export function pageHtml(): string {
  return (HTML ??= entryRenamed("page_v36.html", readFileSync(`${REPO}app/fixtures/reference/page_v36.html`, "utf8")));
}
let RID: Map<number, string> | undefined;
/** page_v36.html's `#data.rid` (oracle rule number → "entry::rule"), read once. */
function pageRid(): Map<number, string> {
  if (RID) return RID;
  const html = pageHtml();
  const m = html.match(/<script[^>]*id="data"[^>]*>([\s\S]*?)<\/script>/);
  if (!m) throw new Error("page_v36.html has no #data");
  return (RID = new Map(Object.entries((JSON.parse(m[1]!) as { rid: Record<string, string> }).rid).map(([k, v]) => [Number(k), v])));
}
