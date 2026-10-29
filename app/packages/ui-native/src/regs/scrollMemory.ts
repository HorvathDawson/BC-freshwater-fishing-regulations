/**
 * T50 — WHERE THE READER WAS on a water's Regulations page, kept across the page's remounts: a view switch (Conditions →
 * Regulations), a tab switch away and back (Map ↔ More), back / forward between waters, a new day, and Android's activity
 * recreation (font size / display size: the JS lives on, so a module-level memory survives where component state does not).
 *
 * Keyed by the water as the reader opened it (`regsScrollKey`: the item, its part and the tapped reach) — in memory only,
 * never persisted, so no section id leaves the bundle (AGENTS 5). The newest `CAP` waters are kept; an older one opens
 * at the top again. A water opened again the same way (the same reach tapped) also returns to where it was left.
 */
export interface RegsScrollMemo { readonly y: number }

const CAP = 50;
const memo = new Map<string, RegsScrollMemo>();

/**
 * the memory's key for a water as opened — its item, part and the tapped reach (the reach picks the part when no part is
 * named, and is the whole key of an unnamed water); null when there is nothing to key on (nothing is remembered)
 */
export function regsScrollKey(item: string | null, part: number | null, tapSid: string | number | null): string | null {
  if (!item && tapSid == null) return null;
  return `${item ?? ""}|${part ?? ""}|${tapSid ?? ""}`;
}

export function rememberRegsScroll(key: string, y: number): void {
  memo.delete(key);                       // re-insert: the Map's order is the recency order
  memo.set(key, { y: Math.max(0, Math.round(y)) });
  while (memo.size > CAP) memo.delete(memo.keys().next().value!);
}

export function recallRegsScroll(key: string): RegsScrollMemo | null {
  return memo.get(key) ?? null;
}

/** tests only */
export function forgetRegsScroll(): void { memo.clear(); }
