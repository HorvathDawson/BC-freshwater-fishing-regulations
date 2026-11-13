/**
 * THE ORACLE FILES (app/fixtures/golden/) — the one module every regs parity suite reads them through:
 * taps.json (`taps()`), hydrate.json (`hydrate()`), their paths and REPO. (golden.json, pins.json, tokens.json and
 * inventory.json, the retired prototype extracts, are gone: T439.) golden.ts re-exports these.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { TapsFile } from "@app/core/regs/testing";

export const REPO = fileURLToPath(new URL("../../../../", import.meta.url));
export const GOLDEN_DIR = `${REPO}app/fixtures/golden/`;
export const TAPS = `${GOLDEN_DIR}taps.json`;
export const HYDRATE = `${GOLDEN_DIR}hydrate.json`;

/** hydrate.json (regs-recut/hydrate.mjs: page v36's decoded rules and describe() per rule, and each state's via / ranks). */
export interface Hydrate {
  readonly meta: { readonly generator: string; readonly ref: string; readonly sha: string; readonly golden: string; readonly version: string };
  readonly describe: Readonly<Record<string, string>>;
  readonly rules: Readonly<Record<string, Readonly<Record<string, unknown>>>>;
  readonly states: Readonly<Record<string, { readonly via: Readonly<Record<string, "trib">>; readonly ranks: Readonly<Record<string, number>> }>>;
}

/**
 * THE RULINGS PAGE v36 PREDATES (T667 slice 5): `app/fixtures/reference/v36-rulings.json`, one entry per ruling, read here
 * and by `pipeline/tests/test_answers_year_strip_live.py` — a new ruling is one entry there, never a table per test. Each
 * export below is that file's entry under the name the parities have always read; its `why` is the ruling and its source.
 */
const RULINGS_FILE = `${REPO}app/fixtures/reference/v36-rulings.json`;
interface Rulings {
  readonly zone_daily_quota_merge: { readonly renamed: Readonly<Record<string, string>> };
  readonly overlap_retired: { readonly retired: readonly string[];
    readonly lois_fold: { readonly id: string; readonly retired: string; readonly into: string; readonly row: number } };
  readonly on_parts_dropped: { readonly rules: readonly string[] };
  readonly part_drawn: { readonly rules: readonly string[] };
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

let H: Hydrate | undefined;
/** hydrate.json (T06/T07/T08's oracle), parsed once per test file. */
export function hydrate(): Hydrate {
  return (H ??= JSON.parse(entryRenamed("hydrate.json", readFileSync(HYDRATE, "utf8"))) as Hydrate);
}

type Keyed = Iterable<readonly [number, { readonly export: { readonly entry_id: string; readonly rule_id: string } }]>;
const keyOf = (r: { readonly export: { readonly entry_id: string; readonly rule_id: string } }) => `${r.export.entry_id}::${r.export.rule_id}`;

/**
 * (overlap_retired) The rules the release RETIRED that page v36 still prints (keys after the renames). `oracleLive` is false
 * for them: a parity drops them from the oracle's lists instead of mapping them — `oracleRixOver` throws on one, so a list
 * that still names one cannot pass silently.
 */
export const ORACLE_RETIRED: readonly string[] = RULINGS.overlap_retired.retired;
/** Is page v36's rule `oracle` one the release still has (not ORACLE_RETIRED)? */
export const oracleLive = (oracle: number): boolean => !ORACLE_RETIRED.includes(pageRid().get(oracle) ?? "");
/** page v36's rule number for a key (after the renames); throws when the page lacks it. */
export function oracleRixOf(key: string): number {
  for (const [o, k] of pageRid()) if (k === key) return o;
  throw new Error(`page_v36.html has no rule ${key}`);
}
/** (overlap_retired.lois_fold) The one golden state where a retired rule drew a ROW of its own (Lois Lake): its row folds
 *  into `into`'s; parities that compare the card's rows amend page v36's by this. `row` is the folded row's index. */
export const LOIS_FOLD = RULINGS.overlap_retired.lois_fold;

/** (on_parts_dropped) "on parts" holds on the WHOLE water: page v36's `inpart` entry is the rule speaking on the day or
 *  not at all. `onPartsDropped` names them by oracle rule. */
const ORACLE_ON_PARTS_DROPPED: readonly string[] = RULINGS.on_parts_dropped.rules;
export const onPartsDropped = (oracle: number): boolean => ORACLE_ON_PARTS_DROPPED.includes(pageRid().get(oracle) ?? "");

/** (part_drawn) an undrawn part that is DRAWN now: page v36's `inpart` entry on the water's other parts is gone. */
const ORACLE_PART_DRAWN: readonly string[] = RULINGS.part_drawn.rules;
export const partDrawn = (oracle: number): boolean => ORACLE_PART_DRAWN.includes(pageRid().get(oracle) ?? "");

/**
 * (peace_kokanee) Zone B's kokanee stream release (`out`) binds no Peace River section; page v36 predates that. On a Peace
 * state the oracle is amended (settle.parity, card.parity), and each amendment throws once the oracle no longer names `out`
 * on that state (a re-cut): where a Peace row's own quota lifted it the `lifted` pair is gone; where nothing lifted it
 * (`kept_rows`) the table's "Kokanee: 10" (`base`) answers, keep `daily` a day all year.
 */
export const PEACE_KOKANEE = RULINGS.peace_kokanee;
/** (kitimat_nov_jun) the one row per Kitimat state whose Nov 1-June 30 'keep' days the ruling makes 'release'. */
export const KITIMAT_RULED = RULINGS.kitimat_nov_jun.rows;

/** `oracleRix` over any rule set keyed by rix (a store's `rules()`, a RuleSource's `stored`). */
export function oracleRixOver(rules: Keyed): (oracle: number) => number {
  const byKey = new Map<string, number>();
  for (const [rix, r] of rules) byKey.set(keyOf(r), rix);
  const rid = pageRid(), named = new Set(rid.values());
  for (const k of ORACLE_RETIRED) {
    if (byKey.has(k)) throw new Error(`ORACLE_RETIRED ${k} is in the store again: drop it from the list`);
    if (!named.has(k)) throw new Error(`ORACLE_RETIRED ${k} is not in page_v36.html: the oracle was re-cut; drop it`);
  }
  return (oracle: number) => {
    const key = rid.get(oracle);
    if (key === undefined) throw new Error(`oracle rule ${oracle} is not in page_v36.html's #data.rid`);
    const now = byKey.get(key);
    if (now === undefined) throw new Error(`oracle rule ${oracle} (${key}) is not in the store: ${ORACLE_RETIRED.includes(key)
      ? "ORACLE_RETIRED — drop it from the list (oracleLive) first" : "a content change — re-cut the page blocks and hydrate.json"}`);
    return now;
  };
}

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
