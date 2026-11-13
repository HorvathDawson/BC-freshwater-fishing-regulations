/**
 * The keys of the sheets a Regulations page can open (contracts.md §1). The app's keys carry NO state id — a
 * RegsPage is one state; the golden's keys embed it, and `appKeyOf`/`goldenKeyOf` bridge the two (T36: the only bridge;
 * T37's deep links and the parity harness both use it).
 */
import type { RuleKey } from "./line";

export type SheetKey =
  | `rule:${RuleKey}` | `gl:${string}`            // gl: one id, or ids joined by "," (multi-term sheet)
  | `sp:${string}` | "hatch"
  | `srcs:${"bait" | "ways" | "always" | "boats"}` | `srcs:gl|${"mode" | "lines" | "hook" | "weight"}`
  | `other:o${number}` | "pos:pos" | `src:r${number}`
  /** (T475) a card row's share ⓘ: how the headline's share stands to the regional total (`headline.info`) */
  | `share:r${number}`
  /** (T481) a card row's "Fishing more than one water today? ›": how the region's total holds across waters */
  | `multi:r${number}`;
const SRCS = /^srcs:(?:bait|ways|always|boats|gl\|(?:mode|lines|hook|weight))$/;

/** A sheet key the app knows, or null (an unknown shape is refused — the date sheet is D7's, not a SheetKey). */
export function parseSheetKey(k: string): SheetKey | null {
  // no "parts": the Parts sheet is removed (T698, owner 2026-11-12) — a start link `sheet=parts` is refused, said
  if (k === "hatch" || k === "pos:pos") return k;
  if (/^rule:\S+::\S+$/.test(k)) return k as SheetKey;   // entry ids carry ":" ("zp:conduct"); "::" joins entry and rule
  if (/^gl:[a-z0-9_]+(?:,[a-z0-9_]+)*$/.test(k)) return k as SheetKey;
  if (/^sp:[A-Za-z0-9_]+$/.test(k)) return k as SheetKey;
  if (SRCS.test(k)) return k as SheetKey;
  if (/^other:o\d+$/.test(k) || /^src:r\d+$/.test(k) || /^share:r\d+$/.test(k) || /^multi:r\d+$/.test(k)) return k as SheetKey;
  return null;   // `licsrc:` (the licence sources sheet) is no key: removed, owner 2026-11-04
}

/** golden → app: "other:vedder_1010|o0" → "other:o0"; "srcs:gl|vedder_1010|hook" → "srcs:gl|hook"; "srcs:bait|vedder_1010" →
 *  "srcs:bait"; "pos:vedder_1010|pos" → "pos:pos". A key with no state id
 *  (rule:, gl:, sp:, hatch) is the same in both. Throws on a key that is not a sheet key once mapped. */
export function appKeyOf(goldenKey: string, sid: string): SheetKey {
  const out = /^(rule|gl|sp):/.test(goldenKey) ? goldenKey
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
