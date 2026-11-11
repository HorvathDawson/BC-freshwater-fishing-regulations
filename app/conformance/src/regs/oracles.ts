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
 * (2026-10-26 owner, handoff/PLAN/curation-review/zone-entry-merges.md) The zone DAILY QUOTA merge: each region's
 * trout/char quota and "Other daily quotas" (7B: and its Arctic grayling block) are one entry, `zN:daily_quotas`, as the
 * book prints them ("Region N Daily Quotas"). Every rule_id was kept; only the entry id moved, and the binding diff showed
 * the same sections under the new keys. Page v36 predates it, so its keys are renamed ONCE at load — in hydrate.json
 * and #data.rid — and nothing else in the oracle moves. An entry here the oracle no longer names throws, so a re-cut
 * oracle retires it.
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

let H: Hydrate | undefined;
/** hydrate.json (T06/T07/T08's oracle), parsed once per test file. */
export function hydrate(): Hydrate {
  return (H ??= JSON.parse(entryRenamed("hydrate.json", readFileSync(HYDRATE, "utf8"))) as Hydrate);
}

type Keyed = Iterable<readonly [number, { readonly export: { readonly entry_id: string; readonly rule_id: string } }]>;
const keyOf = (r: { readonly export: { readonly entry_id: string; readonly rule_id: string } }) => `${r.export.entry_id}::${r.export.rule_id}`;

/**
 * (2026-10-26 owner, overlapping-rules.md; T206) The rules the release RETIRED that page v36 still prints (keys after
 * ORACLE_ENTRY_RENAMED): `z1:trout_quota.r3`, `z2:trout_char_quota.r3` and `lois_lake.r3` (with `khartoum_lake.r3`, which
 * is not in page v36's slice) were dropped in r-3d6399afd7e2; the binding diff showed only those changed, card_check found
 * 0 problems. `oracleLive` is false for them: a parity drops them from the oracle's lists instead of mapping them —
 * `oracleRixOver` throws on one, so a list that still names one cannot pass silently.
 */
export const ORACLE_RETIRED: readonly string[] = [
  "z1:daily_quotas::trout_quota.r3", "z2:daily_quotas::trout_char_quota.r3", "r2:lois_lake@2-12::lois_lake.r3",
];
/** Is page v36's rule `oracle` one the release still has (not ORACLE_RETIRED)? */
export const oracleLive = (oracle: number): boolean => !ORACLE_RETIRED.includes(pageRid().get(oracle) ?? "");
/** page v36's rule number for a key (after the renames); throws when the page lacks it. */
export function oracleRixOf(key: string): number {
  for (const [o, k] of pageRid()) if (k === key) return o;
  throw new Error(`page_v36.html has no rule ${key}`);
}
/**
 * (T206, ORACLE_RETIRED) The one golden state where a retired rule drew a ROW of its own: Lois Lake (x25p_1010). Page v36
 * drew `lois_lake.r3` (hatchery rainbow trout, 6 a day) as a row; with it retired, hatchery rainbow trout answer to
 * `lois_lake.r4` (6 a day) and the card folds them into r4's steelhead row: one "Rainbow trout and steelhead" row. The
 * answer did not change (6 a day; card_check passed). Parities that compare the card's rows amend page v36's by this.
 */
export const LOIS_FOLD = { id: "x25p_1010", retired: "r2:lois_lake@2-12::lois_lake.r3", into: "r2:lois_lake@2-12::lois_lake.r4" } as const;

/**
 * (2026-10-26 owner, curation-review/on-parts.md; c16b1c5f `drop_on_parts`) "on parts" holds on the WHOLE water: these
 * rules lost their `part: "on parts"`, so the store has no undrawn part for them — page v36's `inpart` entry is the rule
 * speaking on the day (`now` true) or not at all. The rules in page v36's slice; `onPartsDropped` names them by oracle rule.
 */
const ORACLE_ON_PARTS_DROPPED: readonly string[] = [
  "r1:cowichan_lake_including_bear_lake@1-4::cowichan_lake.r4", "r1:cowichan_river_see_map_below@1-4::cowichan_river.r8",
  "r1:cowichan_river_see_map_below@1-4::cowichan_river.r9", "r2:coquitlam_river@2-8::coquitlam_river.r2",
];
export const onPartsDropped = (oracle: number): boolean => ORACLE_ON_PARTS_DROPPED.includes(pageRid().get(oracle) ?? "");

/**
 * (2026-10-27 owner, decisions.md "needs a split") an undrawn part that is DRAWN now: the rule binds its own stretch
 * (`columbia_river.r11` "from Mud Lake to Columbia Lake": between the Fairmont and Columbia Lake cuts), so it holds on that
 * part alone — page v36's `inpart` entry on the water's other parts is gone, and the rule's view has no part. The one golden
 * state on the drawn part (x18p3) is taps.json `gone`.
 */
const ORACLE_PART_DRAWN: readonly string[] = ["r4:columbia_river@4-15+4-26+4-34+4-38+4-8::columbia_river.r11"];
export const partDrawn = (oracle: number): boolean => ORACLE_PART_DRAWN.includes(pageRid().get(oracle) ?? "");

/**
 * (T623 F6, ea1c74d67; owner 2026-11-11 "Keep it"; RULINGS Z2, Z1's one named exception b5f1c5cf6) p.64 prints Region 7B's
 * kokanee as "Kokanee: 10 (none from streams, except Peace River)". The stream release (`out`) now carries
 * `outside_items: [gnis:14619]`, so it binds no Peace River section; page v36 predates that and still has it there.
 * On a Peace state the oracle is amended (settle.parity, card.parity), and each amendment throws once the oracle no longer
 * names `out` on that state (a re-cut):
 * - where a Peace row's own kokanee quota lifted it (x22p0/p1/p3), the `lifted` pair is gone: nothing is there to lift;
 * - where nothing lifted it (x22p6, the Peace above its rows: Dinosaur Lake up to the Williston outlet), the table's
 *   "Kokanee: 10" (`base`) answers in its place: keep `daily` a day, all year (T623's table: "Peace part 9: keep 10").
 */
export const PEACE_KOKANEE = { item: "gnis:14619", out: "z7b:daily_quotas::species_quotas.r5",
  base: "z7b:daily_quotas::species_quotas.r4", daily: 10 } as const;

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

/**
 * (T206) ORDER-ONLY amendment for the zone daily-quota rename (bcaa5c79, ORACLE_ENTRY_RENAMED). The store numbers its rules
 * in `entry::rule` key order, so renaming `zN:trout_char_quota` / `zN:species_quotas` (…) to `zN:daily_quotas` moved those
 * rules ahead of the zone siblings whose names sort between (`z4:invasive_species_notice`, `z2:hatchery_steelhead_stop`…).
 * Page v36 and the app both list rules — and what rules name: the card's fish, a sheet's lines — in rule order within a
 * tie, so where page v36 numbered a before b and the store numbers b before a, the two lists may hold that pair the other
 * way round. Nothing else may move.
 *
 * `flipsOver(rules)`: (store rix a, store rix b) → did the renumbering swap their order? (false for a rule page v36 lacks)
 */
export function flipsOver(rules: Keyed): (a: number, b: number) => boolean {
  const back = new Map<string, number>();
  for (const [o, k] of pageRid()) back.set(k, o);
  const old = new Map<number, number>();
  for (const [rix, r] of rules) { const o = back.get(keyOf(r)); if (o !== undefined) old.set(rix, o); }
  return (a, b) => {
    const oa = old.get(a), ob = old.get(b);
    return oa !== undefined && ob !== undefined && Math.sign(a - b) !== Math.sign(oa - ob);
  };
}
/**
 * `want` (the oracle's list, in store numbering) re-ordered as `got` when the two hold the same items and every pair whose
 * order differs is one `swapped(x, y)` allows (a renumbering flip, `flipsOver`); else `want` unchanged, so the caller's
 * toEqual shows the real difference.
 */
export function renamedOrder<T>(got: readonly T[], want: readonly T[], swapped: (x: T, y: T) => boolean): readonly T[] {
  if (got.length !== want.length) return want;
  const tagged = (l: readonly T[]) => {
    const seen = new Map<string, number>();
    return l.map((x) => { const k = JSON.stringify(x), n = seen.get(k) ?? 0; seen.set(k, n + 1); return `${k}#${n}`; });
  };
  const w = tagged(want), g = tagged(got);
  if (w.every((k, i) => k === g[i])) return want;
  const at = new Map(g.map((k, i) => [k, i]));
  if (w.some((k) => !at.has(k))) return want;
  for (let i = 0; i < w.length; i++) for (let j = i + 1; j < w.length; j++)
    if (at.get(w[i]!)! > at.get(w[j]!)! && !swapped(want[i]!, want[j]!)) return want;
  return got;
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
