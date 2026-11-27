/**
 * The keys of the sheets a Regulations page can open (contracts.md §1). The app's keys carry NO state id — a
 * RegsPage is one state; the golden's keys embed it, and `appKeyOf`/`goldenKeyOf` bridge the two (T36: the only bridge;
 * T37's deep links and the parity harness both use it).
 */
import type { RuleKey } from "./line";

export type SheetKey =
  | `rule:${RuleKey}` | `gl:${string}`            // gl: one id, or ids joined by "," (multi-term sheet)
  | `sp:${string}` | "hatch"
  /** (T1031) a fish's Sources tab where no table row's key names it (a salmon species, a fish of a card drawn on the page) */
  | `sp:${string}|src`
  | `srcs:${"bait" | "ways" | "always" | "boats" | "gear"}` | `srcs:gl|${"mode" | "lines" | "hook" | "weight"}`
  | `other:o${number}` | "pos:pos" | `src:r${number}`
  /** (T1031) an Other fish row's species sheet on its Identification tab (`other:oN` is its Sources tab) */
  | `other:o${number}|id`
  /** (T475) a card row's share ⓘ: how the headline's share stands to the regional total (`headline.info`) */
  | `share:r${number}`
  /** (T481) a card row's "Fishing more than one water today? ›": how the region's total holds across waters */
  | `multi:r${number}`
  /** (T810 P) a trout table row's fish sheet: `tf:ST`, `tf:ST/hatchery` (the row key, `model/trout.py` RowKey); (T1031) its
   *  Sources tab, and `tf:ST|id` its Identification tab */
  | `tf:${string}`
  /** (T810 P) the trout table's every rule (`TroutTable.src`): its "Sources ›" and the fish sheet's "All sources ›" */
  | "src:trout"
  /** (T990) the page's every source, from the button at the Regulations page's foot */
  | "src:all";

/**
 * (T900, T1077) A Source line FOLDED INTO ITS SECTION'S ONE LINE, so no screen draws it: a per-row gear line's (`srcs:gl|…`)
 * and the bait list's (`srcs:bait`). The Gear section's one line (`srcs:gear`, `mergedSource`) carries their rules and pages.
 * Every walk of what the screen draws (the every-source sheet's, the Source row's pages) skips them by this one test.
 */
export const foldedSource = (open: string): boolean => open.startsWith("srcs:gl|") || open === "srcs:bait";
/**
 * (T1036 review) THE KEYS A SCREEN LINE BUILDS TO OPEN A SHEET, composed in this ONE place: a component that opens a sheet
 * from a field of its view model (a trout row's key, a salmon row's code, a caveat / timed line's or a boat chip's rule, a
 * term run's ids, the hatchery link) takes its key from here, never a string of its own; a view model that carries its key
 * whole (`open`, an Other fish row's `id`) is opened as it is. The every-source sheet's walk of what is one tap away
 * (`reachableSheets`) reads the same fields through the same builders, and the probe (`WaterScreen.sources.test.tsx`)
 * presses every press target on the drawn screen to prove the two agree.
 */
export const OPEN = Object.freeze({
  /** a rule's own sheet: a caveat or timed line, a boat chip, a Sources row */
  rule: (rule: RuleKey): SheetKey => `rule:${rule}`,
  /** a term run's ⓘ: one id, or "What these words mean" over several */
  terms: (ids: readonly string[]): SheetKey => `gl:${ids.join(",")}`,
  /** a trout table row: its fish sheet on Sources; its picture: on Identification */
  troutRow: (key: string): SheetKey => `tf:${key}`,
  troutLooks: (key: string): SheetKey => `tf:${key}|id`,
  /** the trout table's hatchery link */
  hatch: "hatch" as SheetKey,
  /** a salmon table row: its species sheet on Sources; its picture: on Identification */
  salmonRow: (code: string): SheetKey => `sp:${code}|src`,
  salmonLooks: (code: string): SheetKey => `sp:${code}`,
});

/** (T810 P) a trout row key after `tf:` (trout.ts ROW_KEY): the "/" of a twin row is part of the key */
const TF = /^tf:(?:CT|DV|EB|GB|LT|RB|ST)(?:\/(?:wild|hatchery))?(?:\|id)?$/;
const SRCS = /^srcs:(?:bait|ways|always|boats|gear|gl\|(?:mode|lines|hook|weight))$/;

/** A sheet key the app knows, or null (an unknown shape is refused — the date sheet is D7's, not a SheetKey). */
export function parseSheetKey(k: string): SheetKey | null {
  // no "parts": the Parts sheet is removed (T698, owner 2026-11-12) — a start link `sheet=parts` is refused, said
  if (k === "hatch" || k === "pos:pos") return k;
  if (/^rule:\S+::\S+$/.test(k)) return k as SheetKey;   // entry ids carry ":" ("zp:conduct"); "::" joins entry and rule
  if (/^gl:[a-z0-9_]+(?:,[a-z0-9_]+)*$/.test(k)) return k as SheetKey;
  if (/^sp:[A-Za-z0-9_]+(?:\|src)?$/.test(k)) return k as SheetKey;
  if (SRCS.test(k)) return k as SheetKey;
  if (TF.test(k) || k === "src:trout" || k === "src:all") return k as SheetKey;
  if (/^other:o\d+(?:\|id)?$/.test(k) || /^src:r\d+$/.test(k) || /^share:r\d+$/.test(k) || /^multi:r\d+$/.test(k)) return k as SheetKey;
  return null;   // `licsrc:` (the licence sources sheet) is no key: removed, owner 2026-11-04
}

/** golden → app: "other:vedder_1010|o0" → "other:o0"; "srcs:gl|vedder_1010|hook" → "srcs:gl|hook"; "srcs:bait|vedder_1010" →
 *  "srcs:bait"; "pos:vedder_1010|pos" → "pos:pos". A key with no state id
 *  (rule:, gl:, sp:, tf:, hatch) is the same in both; "src:trout" is "src:<sid>|trout", as a row's "src:r<i>". Throws on a
 *  key that is not a sheet key once mapped. */
export function appKeyOf(goldenKey: string, sid: string): SheetKey {
  const out = /^(rule|gl|sp|tf):/.test(goldenKey) ? goldenKey
    : goldenKey.replace(`:${sid}|`, ":").replace(`|${sid}|`, "|").replace(new RegExp(`\\|${sid.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`), "");
  const k = parseSheetKey(out);
  if (!k) throw new Error(`appKeyOf: ${goldenKey} (state ${sid}) is not a sheet key`);
  return k;
}

/** app → golden (the inverse of appKeyOf for state `sid`). */
export function goldenKeyOf(k: SheetKey, sid: string): string {
  if (k.startsWith("srcs:gl|")) return `srcs:gl|${sid}|${k.slice(8)}`;
  if (k.startsWith("srcs:")) return `${k}|${sid}`;
  const m = /^(other|src|pos|share):(.+)$/.exec(k);
  return m ? `${m[1]}:${sid}|${m[2]}` : k;
}
