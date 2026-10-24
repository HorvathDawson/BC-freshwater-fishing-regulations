/**
 * SEARCH — which water a person meant, and where on the ground it is.
 *
 * Pure: the bundle hands over candidates and facts, this decides the order and the camera.
 * It lives in core because the answer to "which Elk River did they mean" must be the same
 * on the phone, on mobile web and on the desktop, and a ranking written into a component
 * is a ranking the other two surfaces re-invent.
 *
 * TWO QUESTIONS, KEPT APART.
 *
 *  1. ORDER. A match's QUALITY comes first — exact, then prefix, then a word inside the
 *     name, then anywhere — and only among equally good matches does the bigger water win.
 *     "elk" must offer the Elk River that drains 10,000 km² before the one that drains
 *     1,000, but it must never put "Welkin Creek" (a contains match on a big water) above
 *     "Elk Lake" (a prefix match on a small one): nobody typing "elk" meant Welkin.
 *
 *  2. PLACE (`searchGeometry.ts`). The bundle carries no geometry, so where a water IS has to be worked out from
 *     what it does carry: gauges and stocking sites that sit ON the water (exact points),
 *     and the precomputed distance from every town within 25 km (a ring each). `fixOf`
 *     turns those into a box the camera can open on; the map then refines it against the
 *     real tile geometry (`refineFit`), because an estimate is a starting point, not an
 *     outline.
 */
import { sizeSignal } from "./near";
import { searchKey } from "./searchKey";
import { waterNoun } from "./regs/words";
import type { LatLon } from "./searchGeometry";

/** What search compares — the one key the SQL filter and this ranking share (T114, `./searchKey`). */
export { searchKey } from "./searchKey";

/**
 * HOW GOOD A MATCH IS, lower is better. The ladder is the ranking's first key.
 *
 *   0 the name, exactly            "chilliwack river"
 *   2 the name starts with it      "chilliwack riv"
 *   2 an alias, exactly            "vedder river" -> Chilliwack River
 *   3 an alias starts with it
 *   4 a word in the name does      "chilliwack" -> "Upper Chilliwack River"
 *   5 a word in an alias does
 *   6 anywhere in the name         "illiwa"
 *   7 anywhere in an alias
 *   null — no match at all
 *
 * An exact ALIAS ties with a name prefix rather than beating it, and that was measured:
 * two small Elk Lakes carry the alias "Elk", so "elk" put two ponds above the Elk River
 * that drains the Rockies. Tied, size decides — which is what a reader typing "elk" meant.
 */
export function matchTier(query: string, name: string, alias: string | null = null):
    number | null {
  const q = searchKey(query);
  if (!q) return null;
  const tierOf = (text: string, isAlias: boolean): number | null => {
    const t = searchKey(text);
    const a = isAlias ? 1 : 0;
    if (t === q) return isAlias ? 2 : 0;
    if (t.startsWith(q)) return 2 + a;
    if (t.split(/[\s\-(/]+/).some((w) => w.startsWith(q))) return 4 + a;
    if (t.includes(q)) return 6 + a;
    return null;
  };
  const byName = tierOf(name, false);
  const byAlias = alias ? tierOf(alias, true) : null;
  if (byName === null) return byAlias;
  if (byAlias === null) return byName;
  return Math.min(byName, byAlias);
}

/** What the ranking needs to know about one candidate. */
export interface Candidate {
  name: string;
  /** stream | lake | wetland — which size figure speaks for it (`sizeSignal`). */
  kind: string;
  /** The alias that matched, if the query found it by another name. */
  matchedAs: string | null;
  /**
   * How notable a STREAM is — the FWA stream magnitude of its biggest reach. Null when the
   * bundle has no such figure (lakes carry none). Null sorts as small, never as big.
   */
  size: number | null;
  /** How big a LAKE is, in hectares (`item.area_ha`). Null for a stream. */
  areaHa: number | null;
  /** How many reaches it is drawn as. A tie-break after size: more water, more notable. */
  pieces: number;
}

/**
 * Order candidates for a query: match tier, then size (`sizeSignal` — a lake by its area, a
 * stream by its magnitude, on one log scale, the SAME signal the near-place list ranks by),
 * then pieces, then the shorter name, then alphabetically — so the order is total and a
 * re-render never reshuffles equals. Candidates that do not match at all are dropped rather
 * than sorted last.
 */
export function rankWaters<T extends Candidate>(query: string, hits: readonly T[]): T[] {
  const scored = hits
    .map((h) => ({ h, tier: matchTier(query, h.name, h.matchedAs),
                   size: sizeSignal(h.kind, h.size, h.areaHa) }))
    .filter((x): x is { h: T; tier: number; size: number } => x.tier !== null);
  scored.sort((a, b) =>
    a.tier - b.tier
    || b.size - a.size
    || b.h.pieces - a.h.pieces
    || a.h.name.length - b.h.name.length
    || a.h.name.localeCompare(b.h.name));
  return scored.map((x) => x.h);
}

/**
 * Which hits share a display name with another hit — the ones that need a "where".
 *
 * Two rows reading "Elk River" with nothing else on them are one choice presented twice;
 * the reader cannot pick. So a duplicate always carries its location line, and a unique
 * name does not have to.
 */
export function duplicateNames(hits: readonly { name: string }[]): Set<string> {
  const seen = new Map<string, number>();
  for (const h of hits) seen.set(searchKey(h.name), (seen.get(searchKey(h.name)) ?? 0) + 1);
  return new Set([...seen].filter(([, n]) => n > 1).map(([k]) => k));
}

/** One town near a water, and how far the water comes to it. */
export interface NearPlace extends LatLon {
  name: string;
  kind: string;
  km: number;
}

/**
 * The town to describe a water by — "near Fernie".
 *
 * Not simply the nearest named place: that is often a locality nobody has heard of — the
 * Elk River's nearest name is "Mosquito Flats", 100 m off, while it runs through Fernie
 * 440 m further on. So each kind of place carries a handicap in km, and the smallest
 * handicapped distance wins: a city a few km off beats a hamlet on the bank, but a
 * locality ON the water still beats a city 20 km away.
 */
const HANDICAP_KM: Record<string, number> = {
  city: 0, town: 0.5, village: 3, suburb: 3, neighbourhood: 4, quarter: 4,
  hamlet: 5, locality: 6,
};
export function describeBy(near: readonly NearPlace[]): NearPlace | null {
  if (!near.length) return null;
  const cost = (p: NearPlace) => p.km + (HANDICAP_KM[p.kind] ?? 6);
  return [...near].sort((a, b) => cost(a) - cost(b) || a.name.localeCompare(b.name))[0]!;
}

/** "near Fernie · 3 km", "at Hope", or "near Fernie · under 1 km". */
export function wherePhrase(p: { name: string; km: number } | null): string | null {
  if (!p) return null;
  if (p.km < 0.05) return `at ${p.name}`;
  return `near ${p.name} · ${p.km < 1 ? "under 1 km" : `${Math.round(p.km)} km`}`;
}

/*
 * THE SEARCH ROWS' DISTANCE WORDS (P12). These were composed inline in `SearchScreen.tsx`, with the 25 km ring typed a
 * second time as "no town within 25 km" beside `NEAR_KM`. One place now, pinned whole in search.test.ts.
 */

/** How far "water near a town" reaches: the bundle's precomputed `place_water` ring. */
export const NEAR_KM = 25;

/** What the atlas calls a water, said in a row: `waterNoun`'s row form (T116). An unknown kind is said as spelled. */
const kindWord = (kind: string): string => waterNoun(kind, "row");

/** A distance in a near-list row: "under 1 km", or one decimal ("7.9 km"). */
export function kmWords(km: number): string {
  return km < 1 ? "under 1 km" : `${km.toFixed(1)} km`;
}

/** The line under a water in "Water near <town>": "Stream · 1.5 km". */
export function nearRowSub(kind: string, km: number): string {
  return `${kindWord(kind)} · ${kmWords(km)}`;
}

/**
 * The line under a searched water: "Stream · near Fernie · 3 km". A name shared by several waters with no town to
 * tell it apart says so ("Stream · no town within 25 km"); a unique name needs no where.
 */
export function waterRowSub(kind: string, near: { name: string; km: number } | null, dup: boolean): string {
  const where = wherePhrase(near) ?? (dup ? `no town within ${NEAR_KM} km` : null);
  return where ? `${kindWord(kind)} · ${where}` : kindWord(kind);
}

/** A searched water's name with its where, as spoken (the "·" is noise read aloud): "Elk River, near Fernie, 3 km". */
export function spokenName(name: string, near: { name: string; km: number } | null): string {
  const where = wherePhrase(near);
  return where ? `${name}, ${where.replace(" · ", ", ")}` : name;
}

/** The near-list header's count: "12 within 25 km". */
export function nearCount(n: number): string {
  return `${n} within ${NEAR_KM} km`;
}

/**
 * A GROUP NAME THAT ONLY POINTS (owner 2026-10-22): a book row whose only words are "See …" — "TEE PEE LAKES  See
 * Friday, Saturday, and Sunday Lakes". It states no regulation and is drawn on no water; what it gives a reader who
 * looks the group up is the way to the waters it names, each under its own name. The rows are the pipeline's
 * (`see_pointer`); this only says them.
 */
export interface Pointer {
  name: string;
  see: readonly { name: string }[];
}

/** A pointer in words: "Tee Pee Lakes: see Friday Lake, Saturday Lake, Sunday Lake". */
export function seeLine(p: Pointer): string {
  return `${p.name}: see ${p.see.map((s) => s.name).join(", ")}`;
}

/**
 * Which pointers a query finds, in order: a group name that IS the query or STARTS with it (`matchTier` 0 and 2) — a
 * word somewhere inside one is no reason to show it ("lake" would list every group). Exact first, then the shorter
 * name, then alphabetically, so a re-render never reshuffles equals.
 */
export function rankPointers<T extends { name: string }>(query: string, hits: readonly T[]): T[] {
  return hits
    .map((h) => ({ h, tier: matchTier(query, h.name) }))
    .filter((x): x is { h: T; tier: number } => x.tier !== null && x.tier <= 2)
    .sort((a, b) => a.tier - b.tier || a.h.name.length - b.h.name.length || a.h.name.localeCompare(b.h.name))
    .map((x) => x.h);
}

/** What is near a town, said whole: "2 waters within 25 km of Smithers", or that there is none. */
export function watersNear(n: number, town: string): string {
  return n ? `${n} ${n === 1 ? "water" : "waters"} within ${NEAR_KM} km of ${town}`
           : `No named water within ${NEAR_KM} km of ${town}`;
}

/**
 * (T48) THE TOP BAR'S SEARCH — the always-open field over the browse maps and the results sheet it raises over the SAME
 * map (no preview map of its own any more: the main map frames what the reader looks at). One copy of the words.
 *
 * THE PLACEHOLDER is one line per opening of the app, picked by `placeholderFor` and never cycled while it is on screen.
 * Each still says what the field finds; none promises fish. The first six are the owner-approved lines (2026-10-22), the
 * last three name towns (owner: "placeholders could also mention towns").
 */
export const SEARCH_BAR = {
  /** What the field is called aloud, whatever its placeholder says. */
  label: "Search waters and towns",
  /**
   * What closes search and restores the map: on a phone the ‹ that replaces the magnifier while search is open; on a
   * wide screen the results panel's ✕ at its top-right (owner 2026-10-22 late — the close sits by the results).
   */
  close: "Close search",
  /** The ✕ inside the field: empties what was typed and NEVER closes search (owner 2026-10-22 late). */
  clear: "Clear search text",
  /** A selected water row's action: the water screen, through `openWater` (the one opener). */
  open: (name: string) => `Open ${name}`,
  /** The heading over the group names that only point (`seeLine`): names the regulations print, not waters. */
  seeHead: "In the regulations",
  /** The sheet before two letters are typed. */
  start: "Type two letters — a river, a lake or a town. Aliases count: “Vedder” finds the Chilliwack.",
  none: "Nothing by that name in this bundle.",
  failed: "Search could not read the bundle.",
  /** (T180) The sheet's head: what the count counts, and the spinner's name aloud while search reads. */
  namedWaters: "named waters",
  searching: "Searching",
  /** (T180) A town picked: the way back to every result, its heading, and a town it could not read. */
  allResults: "All results",
  backToAll: "Back to all results",
  near: (town: string) => `Water near ${town}`,
  nearFailed: "Could not read what is near this town.",
  /** (T180) A place row's line under its name (`kind`: the place's kind, capitalised), and any row's action aloud. */
  placeSub: (kind: string) => `${kind} · every named water around it`,
  show: (name: string) => `Show ${name} on the map`,
  placeholders: [
    "Cast your search — any lake or river",
    "Drop a line: river, lake or creek…",
    "Hook a water by name",
    "Reel in a river…",
    "Where to next? Search waters",
    "Find your next water",
    "Search a lake, river or town",
    "Name a town to find water near it",
    "Fishing near a town? Type its name",
  ],
} as const;

/**
 * THE LONGEST PLACEHOLDER, in characters, that fits the field at 390 px: the field's text box there is about 309 px
 * (390 − 2 × 14 inset − the 1.5 edges − the magnifier's 38 − 12 padding) and Archivo at 16 px averages 7 px a character
 * ("Cast your search — any lake or river", 36 characters, measured 252 px in the mocks). 36 keeps a margin for the widest
 * glyphs; the web check measures every line in Chrome (handoff/PLAN/app-integration/T48-shots).
 */
export const PLACEHOLDER_MAX = 36;

/** The placeholder for one opening: `seed` (any number; the app passes a random one, a test a fixed one) picks the line. */
export function placeholderFor(seed: number): string {
  const lines = SEARCH_BAR.placeholders;
  const i = Number.isFinite(seed) ? Math.abs(Math.floor(seed)) % lines.length : 0;
  return lines[i]!;
}

/** The caption under the preview for a water: its name, or — only where the source holds no geometry — that it cannot. */
export function waterCaption(name: string, placed: boolean): string {
  return placed ? name : `${name} — the map cannot place it yet`;
}
