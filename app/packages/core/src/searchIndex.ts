/**
 * (T247, owner 2026-10-28, proposal P13) THE SEARCH INDEX — built once, asked on every keystroke, in memory.
 *
 * WHY IT EXISTS. Search was a contains-LIKE over the release's `core` file (`SEARCH` in @app/data), one SQL round trip per
 * keystroke and nothing but substrings: "chillwak" found nothing, "2-8" found nothing, and two waters called Elk River
 * differed only by a town. The whole searchable set is small — ~20,000 waters, ~3,000 aliases, ~2,800 towns — so it is read
 * ONCE (`WaterSource.searchIndex`) and held; a keystroke is then pure code, the same on the web, a phone and a test.
 *
 * WHAT IT ANSWERS (`queryIndex`), in the order it tries:
 *
 *  1. AN AREA IN THE QUERY. "2-8" or "mu 2-8" is a management unit; "region 2" / "r2" / "zone 7a" a region. Either narrows
 *     the waters to those in it; what is left of the query (if anything) is a name inside the area ("elk 2-8"), and with
 *     nothing left the area's biggest waters are listed.
 *  2. A NAME. The ladder is `matchTier`'s, unchanged (exact · prefix · a word · anywhere; an alias a step behind), pinned
 *     equal to it by `searchIndex.test.ts`. Quality first, then the bigger water (`sizeSignal`).
 *  3. A TYPO. Only when few waters match as typed: every query word must be within an edit of a word of the name or an
 *     alias (Damerau–Levenshtein, bounded: none up to 3 letters, one up to 6, two beyond), the last word also as the
 *     start of one (a word half-typed with a slip in it). A fuzzy match ranks below every exact one.
 *  4. A TOWN. "near Chilliwack", or a plain town name that matched no water: the waters whose "near X" town it is
 *     (`item_near`), below every name match.
 *
 * The towns themselves (`places`) are matched the same way, a typo included. Pure, no I/O: the data layer hands in rows.
 */
import { sizeSignal } from "./near";
import { searchKey } from "./searchKey";
import type { NearPlace } from "./search";

/** One searchable water, as the data layer reads it from core. */
export interface IndexWater {
  item: string;
  name: string;
  kind: string;
  /** stream magnitude, or null (every lake) */
  size: number | null;
  areaHa: number | null;
  pieces: number;
  /** the regions it lies in ("2", "7A"), in the book's order; empty: not known (a core cut before `item_area`) */
  regions: readonly string[];
  /** the management units it touches ("2-8"), numerically; empty: not known */
  mus: readonly string[];
  /** the town it is best described by (`describeBy` over `item_near`), or null */
  near: NearPlace | null;
  /** (T224) where the shown name came from, when it is not the official one */
  nameSource?: string | null;
}

export interface IndexAlias { item: string; alias: string; source: string | null }

export interface IndexPlace { place: string; name: string; kind: string; lat: number; lon: number; pop: number | null }

/** Everything the index is built from. */
export interface SearchIndexData<W extends IndexWater = IndexWater, P extends IndexPlace = IndexPlace> {
  waters: readonly W[];
  aliases: readonly IndexAlias[];
  places: readonly P[];
}

/** A water as answered: its facts, and the alias it was found by (null: by its name). */
export type WaterMatch<W extends IndexWater> = W & { matchedAs: string | null; matchedSource: string | null };

/** What the query asked of an area, said once for the sheet's heading. */
export interface AreaAsk { kind: "mu" | "region"; code: string }

export interface IndexAnswer<W extends IndexWater, P extends IndexPlace> {
  waters: WaterMatch<W>[];
  places: P[];
  /** the area the query named, if any */
  area: AreaAsk | null;
  /** the waters in the answer were found by a typo-tolerant match (none matched as typed) */
  fuzzy: boolean;
}

// ── the key tiers: `matchTier`, over precomputed keys ────────────────────────────────────────────────────────────────

/** The separators `matchTier` splits a name into words by. */
const SEP = /[\s\-(/]+/;
const hasSep = (q: string) => SEP.test(q);

/** `matchTier`'s ladder for one key against a query key; `wordKey` is the key with separators as spaces, led by one. */
function tierOf(q: string, qWord: boolean, key: string, wordKey: string, isAlias: boolean): number | null {
  const a = isAlias ? 1 : 0;
  if (key === q) return isAlias ? 2 : 0;
  if (key.startsWith(q)) return 2 + a;
  if (qWord && wordKey.includes(` ${q}`)) return 4 + a;
  if (key.includes(q)) return 6 + a;
  return null;
}

/** The tier the fuzzy matches start at; one edit is 8 + 2, a name beating an alias by 1. */
const FUZZY_BASE = 8;
/** A water found only because it is the "near X" town's: below every name, fuzzy or not. */
const TOWN_TIER = 30;

/** Fuzzy matching runs only when fewer than this many waters matched as typed. */
const FUZZY_BELOW = 8;
/** At most this many waters are added for a town matched by a plain name. */
const TOWN_ADD = 15;

// ── edit distance ─────────────────────────────────────────────────────────────────────────────────────────────────────

/** How many edits a word of this length may be from the one typed: none up to 3 letters, one for 4–6, two beyond. */
export const editsAllowed = (len: number): number => (len < 4 ? 0 : len <= 6 ? 1 : 2);

/**
 * Optimal-string-alignment (Damerau–Levenshtein with adjacent swaps) distance between `a` and `b`, or `max + 1` as soon as
 * it must exceed `max`. Rows of two prior generations are kept, so it allocates three short arrays and exits early.
 */
export function boundedEdits(a: string, b: string, max: number): number {
  const n = a.length, m = b.length;
  if (Math.abs(n - m) > max) return max + 1;
  if (n === 0) return m <= max ? m : max + 1;
  if (m === 0) return n <= max ? n : max + 1;
  let prev2 = new Array<number>(m + 1).fill(0);
  let prev = Array.from({ length: m + 1 }, (_, j) => j);
  let cur = new Array<number>(m + 1).fill(0);
  for (let i = 1; i <= n; i++) {
    cur[0] = i;
    let rowMin = cur[0]!;
    const ai = a.charCodeAt(i - 1);
    for (let j = 1; j <= m; j++) {
      const bj = b.charCodeAt(j - 1);
      let v = Math.min(prev[j]! + 1, cur[j - 1]! + 1, prev[j - 1]! + (ai === bj ? 0 : 1));
      if (i > 1 && j > 1 && ai === b.charCodeAt(j - 2) && a.charCodeAt(i - 2) === bj) v = Math.min(v, prev2[j - 2]! + 1);
      cur[j] = v;
      if (v < rowMin) rowMin = v;
    }
    if (rowMin > max) return max + 1;
    [prev2, prev, cur] = [prev, cur, prev2];
  }
  return prev[m]! <= max ? prev[m]! : max + 1;
}

// ── the query's area ──────────────────────────────────────────────────────────────────────────────────────────────────

const MU_RE = /(?:^|[\s,(])(?:wmu|mu|unit)?\s*(\d{1,2})\s*[-–]\s*(\d{1,2})(?=$|[\s,)])/;
const MU_PREFIX_RE = /^(?:wmu|mu|unit)?\s*(\d{1,2})\s*[-–]$/;
const REGION_RE = /(?:^|[\s,(])(?:region|reg|zone|r)\s*(\d)\s*([ab])?(?=$|[\s,)])/;
/** A leading "near" — the words that say the rest is a town. */
const NEAR_RE = /^(?:near|by|around|close to)\s+(.+)$/;

interface Parsed {
  area: AreaAsk | null;
  /** MU prefix ("2-"): every unit of a region */
  muPrefix: string | null;
  /** the name words left once the area is taken out, as a search key */
  rest: string;
  /** "near X": the town words, else null */
  town: string | null;
}

function parseQuery(key: string): Parsed {
  const near = NEAR_RE.exec(key);
  if (near) return { area: null, muPrefix: null, rest: "", town: near[1]!.trim() };
  const pre = MU_PREFIX_RE.exec(key);
  if (pre) return { area: null, muPrefix: `${Number(pre[1])}-`, rest: "", town: null };
  let rest = key, area: AreaAsk | null = null;
  const mu = MU_RE.exec(rest);
  if (mu) {
    area = { kind: "mu", code: `${Number(mu[1])}-${Number(mu[2])}` };
    rest = rest.replace(mu[0], " ");
  } else {
    const rg = REGION_RE.exec(rest);
    if (rg) {
      area = { kind: "region", code: `${rg[1]}${(rg[2] ?? "").toUpperCase()}` };
      rest = rest.replace(rg[0], " ");
    }
  }
  return { area, muPrefix: null, rest: rest.replace(/\s+/g, " ").trim(), town: null };
}

// ── the index ─────────────────────────────────────────────────────────────────────────────────────────────────────────

interface Entry { water: number; key: string; wordKey: string; alias: string | null; source: string | null }

export interface SearchIndex<W extends IndexWater = IndexWater, P extends IndexPlace = IndexPlace> {
  readonly waters: readonly W[];
  readonly places: readonly P[];
  /** how many names and aliases are searchable (a sheet's head, a test) */
  readonly entries: number;
  /** @internal names first (entry i is water i), then aliases */ readonly _e: readonly Entry[];
  /** @internal each water's size signal, once */ readonly _size: Float64Array;
  /** @internal each water's town key ("" none) */ readonly _town: readonly string[];
  /** @internal distinct words → the entries holding them */ readonly _vocab: ReadonlyMap<string, readonly number[]>;
  /** @internal place keys */ readonly _pk: readonly string[];
  /** @internal place words → places */ readonly _pvocab: ReadonlyMap<string, readonly number[]>;
}

const wordsOf = (key: string): string[] => key.split(SEP).filter(Boolean);
const wordKeyOf = (key: string) => ` ${key.replace(/[\-(/]+/g, " ")}`;

/** Build the index from the rows the data layer read. Once per source; the cost is a few tens of ms on 20,000 waters. */
export function buildSearchIndex<W extends IndexWater, P extends IndexPlace>(
    data: SearchIndexData<W, P>): SearchIndex<W, P> {
  const byItem = new Map<string, number>();
  const entries: Entry[] = [];
  const size = new Float64Array(data.waters.length);
  const town: string[] = [];
  data.waters.forEach((w, i) => {
    byItem.set(w.item, i);
    const key = searchKey(w.name);
    entries.push({ water: i, key, wordKey: wordKeyOf(key), alias: null, source: null });
    size[i] = sizeSignal(w.kind, w.size, w.areaHa);
    town.push(w.near ? searchKey(w.near.name) : "");
  });
  for (const a of data.aliases) {
    const i = byItem.get(a.item);
    if (i === undefined) continue;
    const key = searchKey(a.alias);
    if (!key) continue;
    entries.push({ water: i, key, wordKey: wordKeyOf(key), alias: a.alias, source: a.source });
  }
  const vocab = new Map<string, number[]>();
  entries.forEach((e, ei) => {
    for (const t of new Set(wordsOf(e.key))) (vocab.get(t) ?? vocab.set(t, []).get(t)!).push(ei);
  });
  const pk = data.places.map((p) => searchKey(p.name));
  const pvocab = new Map<string, number[]>();
  pk.forEach((k, pi) => { for (const t of new Set(wordsOf(k))) (pvocab.get(t) ?? pvocab.set(t, []).get(t)!).push(pi); });
  return { waters: data.waters, places: data.places, entries: entries.length, _e: entries, _size: size, _town: town,
           _vocab: vocab, _pk: pk, _pvocab: pvocab };
}

/**
 * Which of `vocab`'s words each query word may stand for, with the edits it costs; the last word may also be the START of
 * a word (typed so far). Returns, per query word, word → edits.
 */
function fuzzyWords(qWords: readonly string[], vocab: ReadonlyMap<string, readonly number[]>): Map<string, number>[] {
  return qWords.map((qw, qi) => {
    const k = editsAllowed(qw.length);
    const out = new Map<string, number>();
    if (qw.length < 4 && qi !== qWords.length - 1) {            // a short whole word is exact or nothing
      if (vocab.has(qw)) out.set(qw, 0);
      return out;
    }
    const last = qi === qWords.length - 1;
    for (const w of vocab.keys()) {
      if (w === qw) { out.set(w, 0); continue; }
      if (!last && (w.length - qw.length > k || qw.length - w.length > k)) continue;   // too far in length to be near
      if (k === 0) { if (last && w.startsWith(qw)) out.set(w, 0); continue; }
      let d = boundedEdits(qw, w, k);
      if (last && d > k && w.length > qw.length) {
        const p = boundedEdits(qw, w.slice(0, qw.length), k);   // half typed, with a slip in what is typed
        if (p <= k) d = p;
      }
      if (d <= k) out.set(w, d);
    }
    return out;
  });
}

/** Entries whose every query word is some word of theirs within an edit; value: the edits it took (≥ 1 — exact is done). */
function fuzzyEntries(qWords: readonly string[], vocab: ReadonlyMap<string, readonly number[]>): Map<number, number> {
  if (!qWords.length) return new Map();
  const per = fuzzyWords(qWords, vocab);
  let acc: Map<number, number> | null = null;
  for (const words of per) {
    const here = new Map<number, number>();
    for (const [w, d] of words) for (const ei of vocab.get(w) ?? []) {
      const had = here.get(ei);
      if (had === undefined || d < had) here.set(ei, d);
    }
    if (acc === null) acc = here;
    else {
      const next = new Map<number, number>();
      for (const [ei, d] of here) { const a = acc.get(ei); if (a !== undefined) next.set(ei, a + d); }
      acc = next;
    }
    if (!acc.size) return acc;
  }
  return acc ?? new Map();
}

/** Group waters of one name together, at the place the best of them ranks: two Elk Rivers are one choice, side by side. */
export function clusterByName<T extends { name: string }>(ranked: readonly T[]): T[] {
  const slots = new Map<string, T[]>();
  const order: string[] = [];
  for (const h of ranked) {
    const k = searchKey(h.name);
    const s = slots.get(k);
    if (s) s.push(h); else { slots.set(k, [h]); order.push(k); }
  }
  return order.flatMap((k) => slots.get(k)!);
}

const BY_SIZE = (size: Float64Array) => (a: { w: number; tier: number }, b: { w: number; tier: number }) =>
  a.tier - b.tier || size[b.w]! - size[a.w]!;

/** Answer one query: the waters (best first, same-named waters together), the towns, and the area asked for, if any. */
export function queryIndex<W extends IndexWater, P extends IndexPlace>(
    ix: SearchIndex<W, P>, query: string, limit = 40, placeLimit = 6): IndexAnswer<W, P> {
  const key = searchKey(query);
  if (key.length < 2) return { waters: [], places: [], area: null, fuzzy: false };
  const parsed = parseQuery(key);
  const waters = ix.waters;
  const inArea = (w: W): boolean =>
    parsed.muPrefix !== null ? w.mus.some((m) => m.startsWith(parsed.muPrefix!))
    : parsed.area === null ? true
    : parsed.area.kind === "mu" ? w.mus.includes(parsed.area.code) : w.regions.includes(parsed.area.code);
  const areaOnly = parsed.muPrefix !== null || (parsed.area !== null && parsed.rest === "");
  const areaAsk: AreaAsk | null = parsed.area ?? (parsed.muPrefix !== null ? { kind: "mu", code: parsed.muPrefix } : null);
  const sizeOf = ix._size;
  const finish = (best: Map<number, { w: number; tier: number; e: Entry }>, fuzzy: boolean, places: P[]):
      IndexAnswer<W, P> => {
    const ranked = [...best.values()].sort((a, b) =>
      a.tier - b.tier || sizeOf[b.w]! - sizeOf[a.w]! || waters[b.w]!.pieces - waters[a.w]!.pieces
      || waters[a.w]!.name.length - waters[b.w]!.name.length || waters[a.w]!.name.localeCompare(waters[b.w]!.name));
    const hits = ranked.map((r): WaterMatch<W> => ({ ...waters[r.w]!, matchedAs: r.e.alias, matchedSource: r.e.source }));
    return { waters: clusterByName(hits).slice(0, limit), places, area: areaAsk, fuzzy };
  };

  // AN AREA ALONE: its biggest waters.
  if (areaOnly) {
    const best = new Map<number, { w: number; tier: number; e: Entry }>();
    waters.forEach((w, i) => { if (inArea(w)) best.set(i, { w: i, tier: 0, e: ix._e[i]! }); });
    return finish(best, false, []);
  }

  const places = queryPlaces(ix, parsed.town ?? key, placeLimit);
  // A TOWN named: "near Chilliwack".
  if (parsed.town !== null) {
    const best = new Map<number, { w: number; tier: number; e: Entry }>();
    const take = (q: string) => ix._town.forEach((t, i) => {
      if (!t) return;
      const tier = t === q ? 0 : t.startsWith(q) ? 1 : null;
      if (tier !== null) best.set(i, { w: i, tier: TOWN_TIER + tier, e: ix._e[i]! });
    });
    take(parsed.town);
    // a slip in the town's name: the town the places index means
    if (!best.size && places.length) take(searchKey(places[0]!.name));
    return finish(best, false, places);
  }

  // A NAME (inside the area, if one was given).
  const q = parsed.rest || key;
  const qWord = !hasSep(q);
  const best = new Map<number, { w: number; tier: number; e: Entry }>();
  const offer = (e: Entry, tier: number) => {
    const had = best.get(e.water);
    if (!had || tier < had.tier) best.set(e.water, { w: e.water, tier, e });
  };
  const scoped = parsed.area !== null;
  for (const e of ix._e) {
    if (scoped && !inArea(waters[e.water]!)) continue;
    const t = tierOf(q, qWord, e.key, e.wordKey, e.alias !== null);
    if (t !== null) offer(e, t);
  }
  let fuzzy = false;
  if (best.size < FUZZY_BELOW && q.length >= 3) {
    const typed = new Set(best.keys());
    for (const [ei, d] of fuzzyEntries(wordsOf(q), ix._vocab)) {
      const e = ix._e[ei]!;
      if (typed.has(e.water) || (scoped && !inArea(waters[e.water]!))) continue;
      offer(e, FUZZY_BASE + 2 * d + (e.alias !== null ? 1 : 0));
      fuzzy = true;
    }
  }
  // A TOWN's name where no water matched as typed: the waters that town describes.
  if (!scoped && q.length >= 3 && best.size < FUZZY_BELOW) {
    let added = 0;
    for (let i = 0; i < ix._town.length && added < TOWN_ADD * 4; i++) {
      const t = ix._town[i]!;
      if (t && !best.has(i) && (t === q || t.startsWith(q))) { best.set(i, { w: i, tier: TOWN_TIER, e: ix._e[i]! }); added++; }
    }
    // keep the biggest few: the rest of the town's waters are one tap away on the town's own row
    if (added > TOWN_ADD) {
      const extra = [...best.values()].filter((r) => r.tier === TOWN_TIER).sort(BY_SIZE(sizeOf)).slice(TOWN_ADD);
      for (const r of extra) best.delete(r.w);
    }
  }
  return finish(best, fuzzy, places);
}

/** The towns a query means: the name as typed (exact · prefix · word · anywhere), else within an edit. Biggest first. */
export function queryPlaces<W extends IndexWater, P extends IndexPlace>(
    ix: SearchIndex<W, P>, query: string, limit = 6): P[] {
  const q = searchKey(query);
  if (q.length < 2 || limit < 1) return [];
  const qWord = !hasSep(q);
  const found: { p: number; tier: number }[] = [];
  ix._pk.forEach((k, p) => {
    const tier = k === q ? 0 : k.startsWith(q) ? 1 : qWord && wordKeyOf(k).includes(` ${q}`) ? 2 : k.includes(q) ? 3 : null;
    if (tier !== null) found.push({ p, tier });
  });
  if (!found.length && q.length >= 4) {
    const per = fuzzyEntries(wordsOf(q), ix._pvocab);
    for (const [p, d] of per) found.push({ p, tier: 3 + d });
  }
  return found
    .sort((a, b) => a.tier - b.tier || (ix.places[b.p]!.pop ?? 0) - (ix.places[a.p]!.pop ?? 0)
      || ix._pk[a.p]!.length - ix._pk[b.p]!.length)
    .slice(0, limit).map((f) => ix.places[f.p]!);
}

/**
 * The words a row says for where a water is: "Region 2 · MU 2-8", "Regions 2 and 5 · MUs 2-8, 2-9", or the first units and
 * a count when it spans many ("MUs 2-8, 2-9 +12"). Empty when the release states no area.
 */
export function areaPhrase(regions: readonly string[], mus: readonly string[], showMus = 2): string {
  const parts: string[] = [];
  if (regions.length)
    parts.push(regions.length === 1 ? `Region ${regions[0]}`
      : `Regions ${regions.slice(0, -1).join(", ")} and ${regions[regions.length - 1]}`);
  if (mus.length) {
    const head = mus.slice(0, showMus).join(", ");
    parts.push(`${mus.length === 1 ? "MU" : "MUs"} ${head}${mus.length > showMus ? ` +${mus.length - showMus}` : ""}`);
  }
  return parts.join(" · ");
}
