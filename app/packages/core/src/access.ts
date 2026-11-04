/**
 * ACCESS (T54) — land a section runs through that the public may not enter, or may enter only with leave. NOT A
 * REGULATION: a section's closed / own / base answer never changes because of it (owner 2026-10-22: "regs are ok but
 * access is not"). Two tiers, decided here and nowhere else:
 *
 *   no access   (tier `no_access`: the `no_access` tile layer's closed land — the Coquitlam, Seymour and Capilano
 *               watersheds, the Greater Victoria Water Supply Area, military ranges). A section MOSTLY inside is drawn
 *               in a fourth map colour (`noPublicAccess` → the status `no_access`: the MAP is per section); a part any
 *               section of which is inside opens its Regulations page with a banner.
 *   restricted  (private land, an Indian reserve, permit-only land — owner Q1 default: permit-only is this gentle tier,
 *               worded "a permit is needed"). No colour; one small note.
 *
 * (T387, owner 2026-11-04) THE PAGE IS THE PART'S: every tap on one part of a water gives the same notices — the part's
 * areas (`partAccess`, each section weighed by its length along the water), each said with how much of the part it covers
 * (`pageAccessNotices`). Only the map colour stays the tapped section's own.
 *
 * THE FACT IS THE PIPELINE'S: `section_access` in the bundle (pipeline/deliver/bundle/access.py), measured against the
 * tiles' own polygons and section geometry, shipped in paint/3. It carries each area's SHARE of the section; what
 * "mostly" means is decided here, once (`MOSTLY_INSIDE`).
 */
import type { SectionKey } from "./section";

export type AccessTier = "no_access" | "permit" | "reserve" | "private";
/** In paint's code order (0..3) — the order the gentle note names its causes in, the strongest first. */
export const ACCESS_TIERS: readonly AccessTier[] = ["no_access", "permit", "reserve", "private"];

/** One polygon a section touches. `name` is "" for a nameless polygon (the private land is always nameless). */
export interface AccessArea { readonly tier: AccessTier; readonly name: string }
/** One area over one section, and how much of the section lies inside it (0 < share <= 1). */
export interface AccessRow { readonly area: AccessArea; readonly share: number }

export interface SectionAccess {
  /** The section's rows, in the pipeline's area order; [] for a section touching no access polygon. */
  of(section: SectionKey): readonly AccessRow[];
}

/**
 * MOSTLY INSIDE: more than half of the section's length (a lake: its area) lies in no-access land, its no-access areas
 * summed. Measured 2026-10-22 against the live tiles (T54 ticket, "threshold"): restricted land is cut first entry /
 * last exit, so nearly every section inside is wholly inside and the straddlers are the cut's few; half is "most".
 */
export const MOSTLY_INSIDE = 0.5;
/**
 * A TOUCH, NOT A CROSSING: an area over less than 1% of a section is a boundary drawn along the bank or a positional
 * graze (the live tiles: 41 of 4,098 no-access rows sit under 10%, most of them at 1-6 permille), and is not noted.
 */
export const TOUCH_ONLY = 0.01;

const EMPTY: readonly AccessRow[] = [];

/**
 * The table from its columns (paint/3's access half, decoded): `sids` ascending, each row's area index and its share in
 * permille. Built once; a section's rows are a binary search.
 */
export function accessOf(areas: readonly AccessArea[], sids: ArrayLike<number>, area: ArrayLike<number>,
                         permille: ArrayLike<number>): SectionAccess {
  const n = sids.length;
  if (area.length !== n || permille.length !== n)
    throw new Error(`section access: ${n} sids, ${area.length} areas, ${permille.length} shares`);
  for (let i = 0; i < n; i++) {
    if (i && sids[i]! < sids[i - 1]!) throw new Error(`section access: sid ${sids[i]} after ${sids[i - 1]}`);
    if (area[i]! >= areas.length) throw new Error(`section access: area ${area[i]} of ${areas.length}`);
  }
  return {
    of(section) {
      const sid = typeof section === "number" ? section : Number(section);
      if (!Number.isInteger(sid)) return EMPTY;
      let lo = 0, hi = n;
      while (lo < hi) { const mid = (lo + hi) >> 1; if (sids[mid]! < sid) lo = mid + 1; else hi = mid; }
      if (lo >= n || sids[lo] !== sid) return EMPTY;
      const out: AccessRow[] = [];
      for (let i = lo; i < n && sids[i] === sid; i++) out.push({ area: areas[area[i]!]!, share: permille[i]! / 1000 });
      return out;
    },
  };
}

const noAccessShare = (rows: readonly AccessRow[]): number =>
  rows.reduce((s, r) => s + (r.area.tier === "no_access" ? r.share : 0), 0);

/** Is the section drawn in the fourth colour? Only no-access land, and only when MOST of it is inside. */
export function noPublicAccess(access: SectionAccess | null | undefined, section: SectionKey): boolean {
  return !!access && noAccessShare(access.of(section)) > MOSTLY_INSIDE;
}

/** The words, once (AGENTS 23/33: core composes the sentence, a test pins it whole). */
export const ACCESS_WORDS = {
  legend: "No public access",
  restricted: "Access may be restricted",
} as const;

/**
 * The notice that opens a Regulations page: the banner (`no_access`) or the gentle note (`restricted`). (T147) The banner
 * LEADS the page: its `title` is the fact in two words, drawn large in the warning block (T165: the amber), and `text` says what it
 * means in plain words; the gentle note's title is "Access may be restricted" and its text names the causes (T54 Q1:
 * permit-only land is this tier). (T367, owner 2026-11-03) No map link on either: a link that does not highlight the
 * area only moved the reader; it returns when the map can highlight it.
 *
 * (T367, owner 2026-11-03: "it should live in the stickied header") Each is drawn as one slim line pinned under the page's
 * header — its `title` and `brief` (what the land is, by name) — and a tap opens `text`, the whole sentence, under it.
 */
export interface AccessNotice {
  readonly tier: "no_access" | "restricted";
  readonly title: string;
  /** the strip's one line after the title: the areas by name ("Capilano Watershed"; "Indian reserve (Capilano 5) and private land") */
  readonly brief: string;
  readonly text: string;
}

const and = (xs: readonly string[]): string =>
  xs.length <= 1 ? (xs[0] ?? "") : `${xs.slice(0, -1).join(", ")} and ${xs[xs.length - 1]}`;

/**
 * What a page is about (T387: the PAGE, never the tap): a water's part ("this part of the water"), a one-part water ("this
 * water"), or an unnamed reach, whose page is its section's ("this section").
 */
export type AccessSubject = "section" | "part" | "water";
const SUBJECT: Record<AccessSubject, string> = { section: "this section", part: "this part of the water", water: "this water" };

/**
 * (T387) One area over a page's whole subject, and how much of it lies inside (0 < share <= 1); `share` null: not known —
 * the sections differ and the release ships no lengths to weigh them by (one cut before `part_section.m`).
 */
export interface PartAccessRow { readonly area: AccessArea; readonly share: number | null }

/** An area's identity on a page: one name per tier is one area (the same polygon may be more than one row). */
const areaKey = (a: AccessArea): string => `${a.tier}\u0000${a.name}`;

/** A section's rows as they count: grazes under TOUCH_ONLY dropped, one row per area (summed). */
function counted(rows: readonly AccessRow[]): Map<string, AccessRow> {
  const out = new Map<string, AccessRow>();
  for (const r of rows) {
    if (r.share < TOUCH_ONLY) continue;
    const k = areaKey(r.area), had = out.get(k);
    out.set(k, { area: r.area, share: Math.min(1, (had?.share ?? 0) + r.share) });
  }
  return out;
}

/**
 * (T387) WHETHER THE PART'S SHARES NEED ITS SECTIONS' LENGTHS: not when no section counts an area, nor when every section
 * counts the same areas at the same shares (any weights then give that share) — so most pages read no length at all.
 */
export function accessNeedsWeights(sections: readonly (readonly AccessRow[])[]): boolean {
  const sig = (rows: readonly AccessRow[]) =>
    [...counted(rows)].map(([k, r]) => `${k}\u0001${r.share}`).sort().join("\u0002");
  const sigs = new Set(sections.map(sig));
  return sigs.size > 1;
}

/**
 * (T387, owner 2026-11-04: "in the one section not all clicks are equal") THE PART'S AREAS, the same for every tap on it:
 * each area once, with the share of the WHOLE part it covers — Σ(share × length) / Σ length over every section of the
 * part, those touching nothing included (a section's grazes under TOUCH_ONLY are dropped first, as a section's page
 * always dropped them). `lengths`: each section's length along the water, in `sections`' order (null: not shipped, or a
 * section not measured); needed only where the sections differ (`accessNeedsWeights`) — without them each share is null.
 * In tier order (ACCESS_TIERS), then the larger share first, then by name.
 */
export function partAccess(sections: readonly (readonly AccessRow[])[],
                           lengths: readonly (number | null)[] | null): PartAccessRow[] {
  const known = lengths !== null && lengths.length === sections.length && lengths.every((x) => x !== null && x >= 0)
    && lengths.some((x) => x! > 0);
  const weighed = !accessNeedsWeights(sections) || known;
  const total = known ? lengths.reduce((s: number, x) => s + x!, 0) : 0;
  // alike sections: any one of them is the part (every weighting gives its shares)
  const w = (i: number) => (!accessNeedsWeights(sections) ? (i === 0 ? 1 : 0) : known ? lengths[i]! / total : 0);
  const sum = new Map<string, { area: AccessArea; share: number }>();
  sections.forEach((rows, i) => {
    for (const [k, r] of counted(rows)) {
      const had = sum.get(k) ?? { area: r.area, share: 0 };
      had.share += r.share * w(i);
      sum.set(k, had);
    }
  });
  const byName = (a: PartAccessRow, b: PartAccessRow) => (a.area.name < b.area.name ? -1 : a.area.name > b.area.name ? 1 : 0);
  return [...sum.values()].map((r): PartAccessRow => ({ area: r.area, share: weighed ? Math.min(1, r.share) : null }))
    .sort((a, b) => ACCESS_TIERS.indexOf(a.area.tier) - ACCESS_TIERS.indexOf(b.area.tier)
      || (b.share ?? 0) - (a.share ?? 0) || byName(a, b));
}

/** How much of the subject a share is, in words: "all", "under 1%", "about 40%". */
function cover(share: number): string {
  if (share >= 0.995) return "all";
  if (share < 0.01) return "under 1%";
  return `about ${Math.round(share * 100)}%`;
}

/** A cause in the note's words, with its share when known ("an Indian reserve (Skumalasph 16, about 6% of it)"). */
const cause = (a: AccessArea, share: number | null): string => {
  const say = [a.tier === "private" ? "" : a.name, share === null ? "" : `${cover(share)} of it`].filter(Boolean).join(", ");
  const par = say ? ` (${say})` : "";
  return a.tier === "private" ? `private land${par}`
    : a.tier === "reserve" ? `an Indian reserve${par}`
    : `permit-only land${par}, where a permit is needed`;
};

/** The strip's short name for a cause: no article, no clause ("Indian reserve (Capilano 5)", "permit-only land (UBC …)"). */
const short = (a: AccessArea): string =>
  a.tier === "private" ? "private land"
  : a.tier === "reserve" ? (a.name ? `Indian reserve (${a.name})` : "Indian reserve")
  : a.name ? `permit-only land (${a.name})` : "permit-only land";

const cap = (s: string): string => `${s[0]!.toUpperCase()}${s.slice(1)}`;

/**
 * (T387) WHAT A REGULATIONS PAGE SAYS ABOUT ACCESS — the same for every tap on its part: the banner when no-access land
 * covers any of it (T147/Q140: "unless a section is fully out of the no-access area it should have a banner" — now the
 * PART's), its share said ("About 40% of this part of the water is in Coquitlam Watershed"; not known: "Some of …"); then
 * (T367) the gentle note when gentler land does, each cause with its share ("This part of the water crosses an Indian
 * reserve (Skumalasph 16, about 20% of it) and private land (about 5% of it)."). `part` is `partAccess`'s. Banner first;
 * empty: nothing to say.
 */
export function pageAccessNotices(part: readonly PartAccessRow[], subject: AccessSubject): AccessNotice[] {
  const it = SUBJECT[subject];
  const strong = part.filter((r) => r.area.tier === "no_access");
  const gentle = part.filter((r) => r.area.tier !== "no_access");
  const out: AccessNotice[] = [];
  if (strong.length) {
    const known = strong.every((r) => r.share !== null);
    const total = Math.min(1, strong.reduce((s, r) => s + (r.share ?? 0), 0));
    const name = (r: PartAccessRow) => r.area.name || "land with no public access";
    const names = strong.length === 1 || !known ? and(strong.map(name)) : and(strong.map((r) => `${name(r)} (${cover(r.share!)})`));
    out.push({
      tier: "no_access",
      title: ACCESS_WORDS.legend,
      brief: and(strong.map(name)),
      text: `${known ? cap(cover(total)) : "Some"} of ${it} is in ${names}, closed to the public. The regulations below `
        + "don't give you access.",
    });
  }
  if (gentle.length) out.push({
    tier: "restricted",
    title: ACCESS_WORDS.restricted,
    brief: and(gentle.map((r) => short(r.area))),
    text: `${cap(it)} crosses ${and(gentle.map((r) => cause(r.area, r.share)))}.`,
  });
  return out;
}
