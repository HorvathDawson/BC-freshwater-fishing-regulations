/**
 * page v36 levelTag and openRule's sub — the one copy. T30's levelWord / downWater (gear.ts) and T36's rule sheet sub
 * (sheets.ts) are built on it, so the level words live once (WORDS.LEVEL_TAG_*). Words only: the rank is the store's.
 */
import type { RuleView } from "./inputs";
import { WORDS } from "./words";

/** page v36 levelTag over the part's view (`r.rank`, the rule's own entry id for the region number). A zone row's id names
 * its lettered zone (`z7a:` → "Region 7A rule", T109), as Python's region words do. */
export function levelTag(r: RuleView): string {
  const k = r.rank;
  if (k === 4) return WORDS.LEVEL_TAG_BC;
  if (k === 3) { const m = /^z(\d+[a-z]?):/.exec(r.export.entry_id || ""); return m ? WORDS.LEVEL_TAG_REGION(m[1]!.toUpperCase()) : WORDS.LEVEL_TAG_REGION_ANY; }
  if (k === 2) return WORDS.LEVEL_TAG_AREA;
  if (k === 1) return WORDS.LEVEL_TAG_DOWN;
  if (k === -1) return WORDS.LEVEL_TAG_FED;
  return WORDS.LEVEL_TAG_WATER;
}

/** A rule sheet's sub (openRule's `levelTag · entry_name`, then sheetHtml's "Region 2 rule · Region 2" → "Region 2"). */
export function ruleSub(r: RuleView): string {
  const name = r.export.prov?.entry_name;
  return `${levelTag(r)}${name ? WORDS.DOT + name : ""}`.replace(/^(.+?) rule · \1$/, "$1");
}
