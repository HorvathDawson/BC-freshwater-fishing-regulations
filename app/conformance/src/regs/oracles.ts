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

/** hydrate.json (regs-recut/hydrate.mjs: page v36's decoded rules and describe() per rule, and each state's via / ranks). */
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
  for (const id of Object.keys(raw.states)) raw.states[id] = subcapRuled(id, loisRuled(id, retiredRuled(id, zone7Ruled(id, raw.states[id])))) as never;
  return (G = raw);
}

/**
 * (T214 item 1, SUBCAP_NAMES) Page v36 prints a size sub-cap as "Of these, only 1 may be over 50 cm (20 in)"; the owner
 * (2026-10-27) has it name the total it belongs to: "Of your 2 trout and char, only 1 may be …" (core tree.ts
 * `subcapLead`). Rewritten ONCE at load, nothing else moved, from the golden's OWN facts and not the app's: the group's
 * title (lower-cased) and the number of the tier the cap sits in, read off the golden tree (that tier's first big number;
 * a folded share has none, so the head's leadN; a cap at the top of the tree sits in the head's root, the head's big).
 * Every "Of these, " the state prints is changed (the tier's line, the group's lines, the Keep tab's lines);
 * `SUBCAP_REWRITES` counts the strings changed (exclusions.test.ts pins it); a tier whose parent has no number throws.
 */
export let SUBCAP_REWRITES = 0;
function subcapRuled(id: string, state: unknown): unknown {
  const groups = (state as { keep?: { groups?: { title: string; head: { big: string | null; leadN: string | null }; tree: GTier[] }[] } }).keep?.groups ?? [];
  type GTier = { t: string; line?: string | null; folded?: boolean; big?: { n: string; second: boolean }[]; children?: GTier[] };
  let text = JSON.stringify(state), leads = 0;
  const lead = (g: (typeof groups)[number], parent: GTier | null): string => {
    const n = parent === null ? g.head.big : parent.folded ? g.head.leadN : parent.big?.find((b) => !b.second)?.n;
    if (!n) throw new Error(`golden ${id}: the cap's parent tier has no number — SUBCAP_NAMES is stale`);
    return `Of your ${n} ${/^[A-Z][a-z]/.test(g.title) ? g.title[0]!.toLowerCase() + g.title.slice(1) : g.title}, `;
  };
  const found: string[] = [];
  const walk = (g: (typeof groups)[number], ts: GTier[], parent: GTier | null): void => {
    for (const t of ts) {
      if (t.t !== "tier") continue;
      if ((t.line ?? "").startsWith("Of these, ")) found.push(lead(g, parent));
      walk(g, t.children ?? [], t);
    }
  };
  for (const g of groups) walk(g, g.tree, null);
  if (!found.length) return state;
  if (new Set(found).size !== 1) throw new Error(`golden ${id}: more than one cap lead — SUBCAP_NAMES needs a per-group rewrite`);
  const n = text.split("Of these, ").length - 1;
  text = text.split("Of these, ").join(found[0]!); leads += n;
  SUBCAP_REWRITES += leads;
  return JSON.parse(text);
}

/**
 * (T206, LOIS_FOLD) Lois Lake's golden Keep tab already draws hatchery rainbow trout inside the steelhead group (its tree's
 * "Up to 6 may be rainbow trout" kind); with r3 retired the group IS the row both fish answer to, so the app names it for
 * both and it is the card's row r1 (r3's own row, r1 in page v36, is gone). Rewritten ONCE at load, nothing else moved:
 * the group's row r2 → r1 (Keep group, pin anchor) and its name "Steelhead" → "Rainbow trout and steelhead" in the title,
 * the head, the pin bar's group line and "How it adds up" ("Rainbow trout or steelhead you kept …"). `LOIS_REWRITES`
 * counts the strings changed (exclusions.test.ts); none found throws.
 */
const LOIS_STRINGS: readonly (readonly [string, string])[] = [
  ["SteelheadSeparate from your 4 trout and char", "Rainbow trout and steelheadSeparate from your 4 trout and char"],
  ["This lake’s rule: 6 a day here. Steelhead you kept", "This lake’s rule: 6 a day here. Rainbow trout or steelhead you kept"],
  ["Steelhead · This lake’s rule: 6 a day", "Rainbow trout and steelhead · This lake’s rule: 6 a day"],
];
export let LOIS_REWRITES = 0;
function loisRuled(id: string, state: unknown): unknown {
  if (id !== LOIS_FOLD.id) return state;
  const s = state as { keep: { groups: { row: string; title: string }[] }; pins: { anchors: { row: string }[] } };
  const g = s.keep.groups.find((x) => x.row === "r2" && x.title === "Steelhead"), a = s.pins.anchors.find((x) => x.row === "r2");
  if (!g || !a) throw new Error(`golden ${id}: no Steelhead group on r2 — LOIS_FOLD is stale`);
  g.row = "r1"; g.title = "Rainbow trout and steelhead"; a.row = "r1";
  let text = JSON.stringify(state);
  for (const [from, to] of LOIS_STRINGS) {
    const n = text.split(from).length - 1;
    if (!n) throw new Error(`golden ${id}: no "${from}" — LOIS_FOLD is stale`);
    text = text.split(from).join(to); LOIS_REWRITES += n;
  }
  return JSON.parse(text);
}

/**
 * (T206, ORACLE_RETIRED) What the retired zone trout quota r3 ("2 hatchery steelhead over 50 cm allowed", Regions 1 and 2)
 * drew in page v36's strings, rewritten ONCE at load (the answers did not change: card_check passed):
 * - a trout source sheet where r3 DECIDED the over-50 cm cap: r2b's line decides it instead (r3's line becomes r2b's), and
 *   r2b's Overruled entry (its quote, scope and "Beaten by" lines, and its rule link in `nested`) is gone;
 * - a sheet where r3 was OVERRULED: its entry and rule link are gone;
 * - a "Rules that don't apply here" list that named r3: its line is gone.
 * Each takes one off the list's count. `RETIRED_REWRITES` counts the rewrites per state (exclusions.test.ts); a sheet
 * that names r3 any other way throws, so a re-cut oracle retires this.
 */
const R3_DECIDED = [
  { now: "At most 2 trout or char over 50 cm (20 in) a day 1 over 50 cm (2 hatchery steelhead over 50 cm allowed)",
    was: "At most 2 hatchery steelhead over 50 cm (20 in) a day 2 hatchery steelhead over 50 cm allowed",
    quote: "“1 over 50 cm (2 hatchery steelhead over 50 cm allowed)”", link: "rule:z2:daily_quotas::trout_char_quota.r2b" },
  { now: "At most 2 trout over 50 cm (20 in) a day 1 over 50 cm (2 hatchery steelhead over 50 cm allowed, see table for exceptions)",
    was: "At most 2 hatchery steelhead over 50 cm (20 in) a day 2 hatchery steelhead over 50 cm allowed, see table for exceptions",
    quote: "“1 over 50 cm (2 hatchery steelhead over 50 cm allowed”", link: "rule:z1:daily_quotas::trout_quota.r2b" },
] as const;
const R3_OVERRULED = { quote: "“2 hatchery steelhead over 50 cm allowed”", links: ["rule:z2:daily_quotas::trout_char_quota.r3", "rule:z1:daily_quotas::trout_quota.r3"] } as const;
const R3_WHERE_NOT = "Only 2 hatchery steelhead over 50 cm (20 in) a day. Replaced by this river’s rule Rule book p.21 ↗";
export const RETIRED_REWRITES: Record<string, number> = {};
const recount = (l: string[], re: RegExp, id: string) => {
  const at = l.findIndex((x) => re.test(x));
  if (at < 0) throw new Error(`golden ${id}: no count line ${re.source} to take the retired r3 off`);
  l[at] = l[at]!.replace(/\((\d+)\)$/, (_, n: string) => `(${+n - 1})`);
};
function retiredRuled(id: string, state: unknown): unknown {
  const s = state as { keep?: { lines?: string[]; plainRows?: string[][] } | null; sheets?: Record<string, unknown> };
  let n = 0;
  for (const l of [s.keep?.lines, ...(s.keep?.plainRows ?? [])]) {
    const at = l?.indexOf(R3_WHERE_NOT) ?? -1;
    if (at < 0) continue;
    l!.splice(at, 1); recount(l!, /^Rules that don’t apply here \(\d+\)$/, id); n++;
  }
  for (const [k, v] of Object.entries(s.sheets ?? {})) {
    // the retired rule's own sheet: the app has no such rule to open
    if (ORACLE_RETIRED.some((r) => k === `rule:${r}`)) { delete s.sheets![k]; n++; continue; }
    if (typeof v !== "object" || v === null) continue;
    const sh = v as { lines: string[]; folds: { summary: string }[]; nested: string[] };
    const unlink = (link: string) => {
      const at = sh.nested.indexOf(link);
      if (at < 0) throw new Error(`golden ${id} ${k}: no rule link ${link}`);
      sh.nested.splice(at, 1);
    };
    const d = R3_DECIDED.find((r) => sh.lines.includes(r.was));
    const q = sh.lines.indexOf(d ? d.quote : R3_OVERRULED.quote);
    if (!d && q < 0) {
      if (JSON.stringify(sh).includes("hatchery steelhead over 50 cm allowed”")) throw new Error(`golden ${id} ${k}: names r3 another way`);
      continue;
    }
    if (q < 0) throw new Error(`golden ${id} ${k}: r3 decides but r2b is not overruled`);
    if (d) { sh.lines[sh.lines.indexOf(d.was)] = d.now; unlink(d.link); }
    else unlink(R3_OVERRULED.links.find((x) => sh.nested.includes(x)) ?? R3_OVERRULED.links[0]);
    sh.lines.splice(q, 3);
    recount(sh.lines, /^Overruled \(\d+\)$/, id);
    const f = sh.folds.find((x) => /^Overruled \(\d+\)$/.test(x.summary));
    if (!f) throw new Error(`golden ${id} ${k}: no Overruled fold`);
    f.summary = f.summary.replace(/\((\d+)\)$/, (_, m: string) => `(${+m - 1})`);
    n++;
  }
  if (n) RETIRED_REWRITES[id] = n;
  return state;
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
  if (PO) return PO;
  const raw = JSON.parse(readFileSync(`${GOLDEN_DIR}pins.json`, "utf8")) as { states: Record<string, unknown> };
  // (T206, LOIS_FOLD) the same rewrite as golden.json's (loisRuled): the folded group is row r1, named for both fish
  const lois = raw.states[LOIS_FOLD.id] as { anchors: { row: string; name: string }[] } | undefined;
  const a = lois?.anchors.find((x) => x.row === "r2" && x.name === "Steelhead");
  if (!lois || !a) throw new Error("pins.json: no Lois Lake Steelhead anchor on r2 — LOIS_FOLD is stale");
  a.row = "r1"; a.name = "Rainbow trout and steelhead";
  let text = JSON.stringify(lois);
  for (const [from, to] of LOIS_STRINGS) text = text.split(from).join(to);
  raw.states[LOIS_FOLD.id] = JSON.parse(text);
  return (PO = raw as unknown as PinsOracle);
}

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
