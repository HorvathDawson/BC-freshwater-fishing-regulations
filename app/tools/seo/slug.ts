/**
 * T192 — THE SLUG MAP. A water's URL is `/water/<slug>`, assigned ONCE and kept in a committed map (`app/seo/slug-map.json`)
 * so a URL never changes between releases: a renamed water keeps its old slug (the map is keyed by the durable item id),
 * a removed one is reported so the host can 301 it to its region page (never a soft 404).
 *
 * Slug = the name folded to ASCII, lowercase, words joined by "-". Two waters that fold to the same slug BOTH take the
 * region suffix (`mill-creek-r3`), so neither URL depends on which was seen first; a slug already held by the committed map
 * is never taken from its owner. A third collision (same name, same region) takes `-2`, `-3` in item-id order.
 */

export interface SlugEntry { readonly slug: string; readonly region: number }
export interface SlugMap { readonly version: 1; readonly waters: Readonly<Record<string, SlugEntry>> }
export interface WaterRef { readonly id: string; readonly name: string; readonly region: number }
export interface Assigned { readonly map: SlugMap; readonly removed: readonly (SlugEntry & { readonly id: string })[] }

export const EMPTY_SLUG_MAP: SlugMap = { version: 1, waters: {} };

/** "Chilliwack/Vedder River" -> "chilliwack-vedder-river"; "Écho Lake" -> "echo-lake". */
export function slugOf(name: string): string {
  return name.normalize("NFKD").replace(/\p{M}+/gu, "").toLowerCase().replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

/** The map for `waters`: every committed slug kept, new waters assigned, waters gone reported in `removed` (and kept in the map). */
export function assignSlugs(waters: readonly WaterRef[], committed: SlugMap = EMPTY_SLUG_MAP): Assigned {
  const out: Record<string, SlugEntry> = { ...committed.waters };
  const taken = new Set(Object.values(out).map((e) => e.slug));
  const fresh = waters.filter((w) => !(w.id in out)).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const groups = new Map<string, WaterRef[]>();
  for (const w of fresh) {
    const base = slugOf(w.name) || "water";
    groups.set(base, [...(groups.get(base) ?? []), w]);
  }
  const claim = (want: string): string => {
    let s = want, n = 2;
    while (taken.has(s)) s = `${want}-${n++}`;
    taken.add(s);
    return s;
  };
  for (const [base, group] of [...groups].sort((a, b) => (a[0] < b[0] ? -1 : 1))) {
    const alone = group.length === 1 && !taken.has(base);
    for (const w of group) out[w.id] = { slug: alone ? claim(base) : claim(`${base}-r${w.region}`), region: w.region };
  }
  const here = new Set(waters.map((w) => w.id));
  // a water that is still here but moved region keeps its slug; its entry's region follows it
  for (const w of waters) { const e = out[w.id]!; if (e.region !== w.region) out[w.id] = { slug: e.slug, region: w.region }; }
  const removed = Object.entries(out).filter(([id]) => !here.has(id)).map(([id, e]) => ({ id, ...e }))
    .sort((a, b) => (a.slug < b.slug ? -1 : 1));
  const sorted = Object.fromEntries(Object.entries(out).sort((a, b) => (a[0] < b[0] ? -1 : 1)));
  return { map: { version: 1, waters: sorted }, removed };
}
