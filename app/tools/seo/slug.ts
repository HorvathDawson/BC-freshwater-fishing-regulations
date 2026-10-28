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
/** `town`: the nearest settlement's name (the bundle's item_near), the human qualifier for a repeated name */
export interface WaterRef { readonly id: string; readonly name: string; readonly region: number; readonly town?: string | null }
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
    if (group.length === 1 && !taken.has(base)) { out[group[0]!.id] = { slug: claim(base), region: group[0]!.region }; continue; }
    // a repeated name is told apart by where it is: "mud-lake-near-quesnel", else "mud-lake-r1"; what still collides adds the
    // region, and only then a counter (in item-id order). Waters that end up unique take their slug together, so none depends on order.
    const want1 = (w: WaterRef) => { const t = w.town ? slugOf(w.town) : ""; return t ? `${base}-near-${t}` : `${base}-r${w.region}`; };
    const want2 = (w: WaterRef) => { const w1 = want1(w); return w1.endsWith(`-r${w.region}`) ? w1 : `${w1}-r${w.region}`; };
    let left = group;
    for (const want of [want1, want2]) {
      const n = new Map<string, number>();
      for (const w of left) n.set(want(w), (n.get(want(w)) ?? 0) + 1);
      const rest: WaterRef[] = [];
      for (const w of left) {
        const s1 = want(w);
        if (n.get(s1) === 1 && !taken.has(s1)) out[w.id] = { slug: claim(s1), region: w.region }; else rest.push(w);
      }
      left = rest;
    }
    for (const w of left) out[w.id] = { slug: claim(want2(w)), region: w.region };
  }
  const here = new Set(waters.map((w) => w.id));
  // a water that is still here but moved region keeps its slug; its entry's region follows it
  for (const w of waters) { const e = out[w.id]!; if (e.region !== w.region) out[w.id] = { slug: e.slug, region: w.region }; }
  const removed = Object.entries(out).filter(([id]) => !here.has(id)).map(([id, e]) => ({ id, ...e }))
    .sort((a, b) => (a.slug < b.slug ? -1 : 1));
  const sorted = Object.fromEntries(Object.entries(out).sort((a, b) => (a[0] < b[0] ? -1 : 1)));
  return { map: { version: 1, waters: sorted }, removed };
}
