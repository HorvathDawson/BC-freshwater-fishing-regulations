/**
 * The keys of the sheets a Regulations page can open (contracts.md §1). The app's keys carry NO state id — a
 * RegsPage is one state; the golden's keys embed it, and `appKeyOf`/`goldenKeyOf` bridge the two (T36: the only bridge;
 * T37's deep links and the parity harness both use it).
 */
import type { RuleKey } from "./line";
import type { Who } from "./model";

export type SheetKey =
  | `rule:${RuleKey}` | `gl:${string}`            // gl: one id, or ids joined by "," (multi-term sheet)
  | `sp:${string}` | "hatch" | "parts"
  | `srcs:${"bait" | "ways" | "always" | "boats"}` | `srcs:gl|${"mode" | "lines" | "hook" | "weight"}`
  | `licsrc:${ProfileKey}` | `other:o${number}` | "pos:pos" | `src:r${number}`;

export type ProfileKey = "adult" | "adult_non" | "adult_alien" | "u16_res" | "u16_non" | "u16_alien";

const PROFILES: readonly ProfileKey[] = ["adult", "adult_non", "adult_alien", "u16_res", "u16_non", "u16_alien"];
const SRCS = /^srcs:(?:bait|ways|always|boats|gl\|(?:mode|lines|hook|weight))$/;

/** A sheet key the app knows, or null (an unknown shape is refused — the date sheet is D7's, not a SheetKey). */
export function parseSheetKey(k: string): SheetKey | null {
  if (k === "hatch" || k === "parts" || k === "pos:pos") return k;
  if (/^rule:\S+::\S+$/.test(k)) return k as SheetKey;   // entry ids carry ":" ("zp:conduct"); "::" joins entry and rule
  if (/^gl:[a-z0-9_]+(?:,[a-z0-9_]+)*$/.test(k)) return k as SheetKey;
  if (/^sp:[A-Za-z0-9_]+$/.test(k)) return k as SheetKey;
  if (SRCS.test(k)) return k as SheetKey;
  if (/^other:o\d+$/.test(k) || /^src:r\d+$/.test(k)) return k as SheetKey;
  const lic = /^licsrc:(\w+)$/.exec(k);
  if (lic && (PROFILES as readonly string[]).includes(lic[1]!)) return k as SheetKey;
  return null;
}

/** golden → app: "other:vedder_1010|o0" → "other:o0"; "srcs:gl|vedder_1010|hook" → "srcs:gl|hook"; "srcs:bait|vedder_1010" →
 *  "srcs:bait"; "licsrc:lic|vedder_1010|u16_res" → "licsrc:u16_res"; "pos:vedder_1010|pos" → "pos:pos". A key with no state id
 *  (rule:, gl:, sp:, hatch, parts) is the same in both. Throws on a key that is not a sheet key once mapped. */
export function appKeyOf(goldenKey: string, sid: string): SheetKey {
  const lic = /^licsrc:lic\|([^|]+)\|(\w+)$/.exec(goldenKey);
  const out = lic ? (lic[1] === sid ? `licsrc:${lic[2]}` : goldenKey)
    : /^(rule|gl|sp):/.test(goldenKey) ? goldenKey
    : goldenKey.replace(`:${sid}|`, ":").replace(`|${sid}|`, "|").replace(new RegExp(`\\|${sid.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`), "");
  const k = parseSheetKey(out);
  if (!k) throw new Error(`appKeyOf: ${goldenKey} (state ${sid}) is not a sheet key`);
  return k;
}

/** app → golden (the inverse of appKeyOf for state `sid`). */
export function goldenKeyOf(k: SheetKey, sid: string): string {
  if (k.startsWith("licsrc:")) return `licsrc:lic|${sid}|${k.slice(7)}`;
  if (k.startsWith("srcs:gl|")) return `srcs:gl|${sid}|${k.slice(8)}`;
  if (k.startsWith("srcs:")) return `${k}|${sid}`;
  const m = /^(other|src|pos):(.+)$/.exec(k);
  return m ? `${m[1]}:${sid}|${m[2]}` : k;
}

/** The licence profile a reader is: {u16, non_resident} → "u16_non". */
export const profileKeyOf = (who: Who): ProfileKey =>
  who.age === "adult"
    ? (who.residency === "resident" ? "adult" : who.residency === "non_resident" ? "adult_non" : "adult_alien")
    : (who.residency === "resident" ? "u16_res" : who.residency === "non_resident" ? "u16_non" : "u16_alien");
