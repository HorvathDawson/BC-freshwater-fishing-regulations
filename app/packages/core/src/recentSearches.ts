/**
 * (T247) RECENT SEARCHES: what the reader looked for and then picked, kept on this device only (never sent anywhere,
 * cleared by one tap). The list is whole text under one name (`@app/data` `openRecentSearchStore`: localStorage on the
 * web, a file on a phone), most recent first. Pure: the hook in `@app/ui` reads and writes it through these.
 */
import { searchKey } from "./searchKey";
import type { AreaAsk } from "./searchIndex";

/** How many recents are kept and shown. */
export const RECENT_MAX = 6;

/** The words search adds to the sheet beside its results: recents, the area heading, the typo note, what else it takes. */
export const SEARCH_FIND = {
  /** under the start line: what else the field understands */
  hint: "You can also type a management unit (2-8), a region (region 2) or a town (near Hope). Small slips in spelling are forgiven.",
  recent: "Recent searches",
  clear: "Clear",
  clearLabel: "Clear recent searches",
  /** a recent's action aloud */
  again: (q: string) => `Search again for ${q}`,
  /** the waters group's heading when the query named an area */
  inArea: (a: AreaAsk, n: number) => `${n} ${n === 1 ? "water" : "waters"} in ${a.kind === "mu" ? "MU" : "Region"} ${a.code}`,
  /** … and when it named every unit of a region ("4-") */
  inUnits: (prefix: string, n: number) => `${n} ${n === 1 ? "water" : "waters"} in MUs ${prefix}…`,
  /** the waters group's heading when some matched only after a slip in spelling was forgiven */
  closeSpellings: (n: number) => `${n} named ${n === 1 ? "water" : "waters"}, some by close spelling`,
} as const;

/** The stored text as a list: a JSON array of strings, anything else (a damaged file, another version) as none. */
export function parseRecents(text: string | null | undefined): string[] {
  if (!text) return [];
  try {
    const v: unknown = JSON.parse(text);
    if (!Array.isArray(v)) return [];
    const out: string[] = [];
    for (const x of v) {
      if (typeof x !== "string") continue;
      const q = x.trim();
      if (q.length >= 2 && q.length <= 80 && !out.some((o) => searchKey(o) === searchKey(q))) out.push(q);
    }
    return out.slice(0, RECENT_MAX);
  } catch { return []; }
}

/** `q` put at the front (a repeat moves up, never doubles); under two letters nothing is added. */
export function pushRecent(list: readonly string[], q: string): string[] {
  const t = q.trim().replace(/\s+/g, " ");
  if (searchKey(t).length < 2 || t.length > 80) return [...list];
  return [t, ...list.filter((o) => searchKey(o) !== searchKey(t))].slice(0, RECENT_MAX);
}

export const serializeRecents = (list: readonly string[]): string => JSON.stringify(list.slice(0, RECENT_MAX));
