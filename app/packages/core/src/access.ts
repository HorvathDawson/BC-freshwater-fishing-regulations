/**
 * ACCESS (T54) — land a section runs through that the public may not enter, or may enter only with leave. NOT A
 * REGULATION: a section's closed / own / base answer never changes because of it (owner 2026-10-22: "regs are ok but
 * access is not"). Two tiers, decided here and nowhere else:
 *
 *   no access   (tier `no_access`: the `no_access` tile layer's closed land — the Coquitlam, Seymour and Capilano
 *               watersheds, the Greater Victoria Water Supply Area, military ranges). A section MOSTLY inside is drawn
 *               in a fourth map colour (`noPublicAccess` → the status `no_access`); any section inside at all opens its
 *               Regulations tab with a banner.
 *   restricted  (private land, an Indian reserve, permit-only land — owner Q1 default: permit-only is this gentle tier,
 *               worded "a permit is needed"). No colour; one small note.
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
  showOnMap: "Show area on map",
  seeMap: "See the map",
} as const;

/** The notice that opens a section's Regulations tab: the banner (`no_access`) or the gentle note (`restricted`). */
export interface AccessNotice {
  readonly tier: "no_access" | "restricted";
  readonly text: string;
  /** The link under it, which shows the area on the water's map. */
  readonly link: string;
}

const and = (xs: readonly string[]): string =>
  xs.length <= 1 ? (xs[0] ?? "") : `${xs.slice(0, -1).join(", ")} and ${xs[xs.length - 1]}`;

/** Each area once, in the order given, by name (the same polygon may be more than one row: never twice). */
const uniq = (rows: readonly AccessRow[]): AccessArea[] => {
  const seen = new Set<string>(), out: AccessArea[] = [];
  for (const r of rows) {
    const k = `${r.area.tier}\u0000${r.area.name}`;
    if (!seen.has(k)) { seen.add(k); out.push(r.area); }
  }
  return out;
};

const cause = (a: AccessArea): string =>
  a.tier === "private" ? "private land"
  : a.tier === "reserve" ? (a.name ? `an Indian reserve (${a.name})` : "an Indian reserve")
  : a.name ? `permit-only land (${a.name}), where a permit is needed` : "permit-only land, where a permit is needed";

/**
 * WHAT THE PAGE SAYS about a section's access, or null. No-access land in any share is the banner (mostly inside: "is
 * inside"; otherwise "is partly inside"); else any gentler restriction is the note, naming its causes from the data —
 * permit-only land, then reserves, then private land.
 */
export function accessNotice(all: readonly AccessRow[]): AccessNotice | null {
  const rows = all.filter((r) => r.share >= TOUCH_ONLY);
  const strong = uniq(rows.filter((r) => r.area.tier === "no_access"));
  if (strong.length) {
    const mostly = noAccessShare(rows) > MOSTLY_INSIDE;
    const names = and(strong.map((a) => a.name || "land with no public access"));
    return {
      tier: "no_access",
      text: `No public access: this section is ${mostly ? "inside" : "partly inside"} ${names}. The regulations below `
          + `are the Province's, but you may not enter ${mostly ? "this" : "that"} area to fish.`,
      link: ACCESS_WORDS.showOnMap,
    };
  }
  const gentle = uniq(rows).sort((a, b) => ACCESS_TIERS.indexOf(a.tier) - ACCESS_TIERS.indexOf(b.tier));
  if (!gentle.length) return null;
  return {
    tier: "restricted",
    text: `Access may be restricted here: this section crosses ${and(gentle.map(cause))}. See the map.`,
    link: ACCESS_WORDS.seeMap,
  };
}
