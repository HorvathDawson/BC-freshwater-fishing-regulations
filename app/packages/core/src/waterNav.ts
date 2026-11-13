/**
 * THE WATER MAP'S NAVIGATION (P15): a water's parts in mouth-first order, the boundary marks between them, and a step
 * ▼ downstream / ▲ upstream over that STORED graph — the release's `part_geo` (box), `part_mark` (a boundary: its
 * place, its end token, the part below it and the parts above it) and `water_exit` (where the water's mouth goes), written
 * by the pipeline (P09). Nothing here is a regulation: it is which part is next to which, and the words for it.
 *
 * THE GROUP. A water is one item; a lake divided into closure areas (Kootenay Lake: Main Body, Upper and Lower West
 * Arm, each its own item `part_of` the lake, P09's KNOWN GAP) is the group of its sibling items, walked as one water
 * the way the prototype's map does (`regs-v11.html` `klwa`). Siblings are ordered by their exits (the one that leaves
 * the group is the lowest; each that flows into one already placed comes next), and an exit from one sibling into
 * another is a boundary mark with no place (the release draws none between items) and no end token: it says no words.
 *
 * ONE PART, ONE CONNECTED RUN (T517). The release cuts a water into parts, each one connected stretch (`part_ix` is
 * river order, mouth first); neighbouring parts of one run the DFO salmon chain cuts are joined on the map where the live
 * salmon reads them alike (`mergeNav`). A joined entry knows every part it holds (`pieces`).
 *
 * THE STEP. ▲ takes the parts above the current one by the marks (a fork: a mark with two upper parts); ▼ the parts
 * below it (a fork too: two parts whose marks both say they sit below this one — the Zymoetz's source part has the main
 * stem and "A lake → McDonell Lake outlet" below it, and that lake part is reached ONLY that way). Where the marks say
 * nothing the order (`part_ix`) does, so every part stays reachable. At a fork the MAIN stem goes first: a side channel is
 * a branch from which the other branch is reachable going on in the same direction (it rejoins), and the main stem is
 * not — the Zymoetz's side channel rejoins above the canyon (`up_parts [0, 5]`).
 *
 * ACROSS WATERS (owner 2026-10-21: "navigate up if one stream comes directly from another, like vedder from
 * chilliwack"). The release's `water_continue` says which stream goes on as another — the same watercourse under a new
 * name (pipeline `continuation.py`; never a tributary). On the part where this water meets the one it goes on from
 * (`prev`), ▲ with nothing above it in this water steps into that water's part; on the part where it goes on as the
 * next (`next`), ▼ with nothing below steps into the next one's part. The step is the caller's to take: it opens the
 * other water (`crossUp` / `crossDown` say which, and where).
 */
import { endWords, spanName, type EndsTable } from "./ends";
import { waterNoun, WORDS } from "./regs/words";
import type { Bbox } from "./searchGeometry";

/** One part of the group as the release stores it, its section ids included (`part_section`). */
export interface NavPart {
  /** The item it belongs to (its durable id) and that item's name and kind. */
  readonly item: string;
  readonly itemName: string;
  readonly kind: string;
  /** Its index in its item's `part` table — what the regulations page and the route name a part by, and its river
   *  order (mouth first). An entry `mergeNav` joined is named by its lowest piece. */
  readonly partIx: number;
  /** (T517) Every release part this entry holds, ascending: `[partIx]`, or the pieces of one run the live salmon joins.
   *  What a frame's part is found by (`partAt`). */
  readonly pieces: readonly number[];
  readonly label: string;
  /** (T517, owner 2026-11-09) What its name is composed from (`@app/core` `spanName`, the one name function): its two
   *  END TOKENS (a joined entry: its first piece's `lo`, its last piece's `hi`); both null — a lake, a whole water, an
   *  unkeyed part — its water's name. */
  readonly lo: string | null;
  readonly hi: string | null;
  /** (T460) A side channel (the release's part with no km): a fork's branch, never a skipped part of the main stem. */
  readonly side?: boolean;
  /** (T517) Its rule-set group and connected run: parts sharing both are one run the DFO salmon chain cuts, which the
   *  map joins where the live salmon reads them alike (`mergeNav`). */
  readonly grp: number;
  readonly run: number;
  /** The box of its geometry `[w, s, e, n]` in degrees, or null where the builder wrote none. */
  readonly box: Bbox | null;
  /** A point on it (`part_geo.mid_lon / mid_lat`), in degrees. */
  readonly mid: readonly [number, number] | null;
  readonly sids: readonly number[];
}

/** (T207) How sure the curator is of where a cut stands — pipeline `models.splits.CONFIDENCE`; absent is high. */
export type Confidence = "high" | "medium" | "low";

export function isConfidence(v: unknown): v is Confidence {
  return v === "high" || v === "medium" || v === "low";
}

/** A boundary between parts: `down` and `up` are indexes into the group's `parts`. No place: not drawn. */
export interface NavMark {
  readonly lon: number | null;
  readonly lat: number | null;
  /** (T517, water/3) the end token the two parts share here (`part_mark.token`, a key of `display.ends`), said by
   *  `markLabel`; null: no named place (a side channel meets, a split lake's areas) — drawn with no words */
  readonly token: string | null;
  readonly down: number;
  readonly up: readonly number[];
  /** T207: the named cut's confidence where it is not high (`part_mark.confidence`). */
  readonly confidence?: Confidence;
}

/** Where the group's mouth flows: the receiving water and the part of it the mouth meets (null: not stored). `tidal`
 *  (T345, owner 2026-10-31: "we aren't doing tidal"): that part is tidal water (the release's tidal cut piece, which no
 *  water map lists) — the line is said, never a way into the receiving water. */
export interface NavExit { readonly item: string; readonly name: string; readonly part: number | null; readonly tidal?: boolean }

/** The same watercourse going on as another water (`water_continue`): that water, its part there, and where — `at`,
 *  the index in this group's `parts` — this water meets it. */
export interface NavCross { readonly item: string; readonly name: string; readonly part: number; readonly at: number }

export interface WaterGroup {
  /** Mouth-first. */
  readonly parts: readonly NavPart[];
  /** (T517) The release's `display.ends`: the words every part's name is composed from (`partName`). */
  readonly ends: EndsTable;
  readonly marks: readonly NavMark[];
  readonly exit: NavExit | null;
  /** The water this one goes on as, below its mouth (the Chilliwack's: the Vedder), or null. */
  readonly next: NavCross | null;
  /** The water this one goes on from, above its top (the Vedder's: the Chilliwack), or null. */
  readonly prev: NavCross | null;
}

/** One `water_continue` row as an item's rows carry it: the other water (its ord, id and name, from core), `part` this
 *  item's part at the junction and `otherPart` the other water's. */
export interface ContinueRow {
  readonly ord: number; readonly part: number; readonly otherPart: number;
  readonly item: string; readonly name: string;
}

/** One item's rows from the release, as the data layer reads them (degrees, not microdegrees). */
export interface ItemNavRows {
  readonly ord: number;
  readonly item: string;
  readonly name: string;
  readonly kind: string;
  /** `side` (T460): a side channel, the release's part with no km (`part.km` NULL), never a step on the main stem. */
  readonly parts: readonly { readonly partIx: number; readonly label: string;
                              readonly lo: string | null; readonly hi: string | null;
                              readonly side?: boolean; readonly grp: number; readonly run: number }[];
  /** (T278) the item's TIDAL pieces (no key, never listed): left out of `parts`, and a mark reaching one goes with it */
  readonly tidal: readonly number[];
  readonly geo: readonly { readonly partIx: number; readonly box: Bbox | null;
                           readonly mid: readonly [number, number] | null }[];
  readonly marks: readonly { readonly lon: number; readonly lat: number; readonly token: string | null;
                             readonly down: number; readonly up: readonly number[];
                             readonly confidence?: Confidence }[];
  readonly sections: readonly { readonly sid: number; readonly partIx: number }[];
  /** `water_exit`: the receiving item's ord, the part it is met at, and — when the release names it — its id and name. */
  readonly exit: { readonly ord: number; readonly part: number | null; readonly item: string;
                   readonly name: string;
                   /** (T345) the receiving part is tidal (its `part` row: no key, `province_except` "tidal") */
                   readonly tidal?: boolean } | null;
  /** `water_continue`: the water this one goes on as (`continuesAs`) and the one it goes on from (`continuesFrom`). */
  readonly continuesAs?: ContinueRow | null;
  readonly continuesFrom?: ContinueRow | null;
}

/** A release defect in a water's map rows: thrown (the map then says it is unavailable, and the read reports it). */
const defect = (i: ItemNavRows, what: string) => new Error(`water map: ${i.item} (ord ${i.ord}) ${what}`);

/**
 * The group, from its items' rows. `all[0]` is the water opened; the others are its siblings (a split lake's other
 * parts) — in any order: they are ordered here by their exits. An item with no parts is left out. `ends`: the release's
 * `display.ends` (the parts' names are composed from it).
 */
export function waterGroup(all: readonly ItemNavRows[], ends: EndsTable): WaterGroup {
  // an item with no part (a split lake itself: its closure areas own the parts) is not walked, and is no mouth
  const items = all.filter((i) => i.parts.length > 0);
  const byOrd = new Map(items.map((i) => [i.ord, i]));
  // the lowest: an item whose exit leaves the group (or that has none); then each that flows into one placed
  const placed: ItemNavRows[] = [];
  const left = new Set(items.map((i) => i.ord));
  const outside = items.filter((i) => !i.exit || !byOrd.has(i.exit.ord));
  if (items.length && !outside.length) throw defect(items[0]!, "and its siblings flow into each other in a cycle: no mouth");
  for (const i of outside) { placed.push(i); left.delete(i.ord); }
  for (let k = 0; k < placed.length; k += 1) {
    for (const i of items)
      if (left.has(i.ord) && i.exit?.ord === placed[k]!.ord) { placed.push(i); left.delete(i.ord); }
  }
  const stray = items.find((i) => left.has(i.ord));
  if (stray) throw defect(stray, "is reached by no exit from the group's mouth (a cycle among its siblings)");

  const parts: NavPart[] = [];
  const at = new Map<string, number>();                            // `${ord}:${partIx}` → index in `parts`
  for (const i of placed) {
    const geo = new Map(i.geo.map((g) => [g.partIx, g]));
    const sids = new Map<number, number[]>();
    for (const s of i.sections) {
      let l = sids.get(s.partIx);
      if (!l) sids.set(s.partIx, l = []);
      l.push(s.sid);
    }
    // `part_ix` is river order, mouth first (T517)
    for (const p of [...i.parts].sort((a, b) => a.partIx - b.partIx)) {
      const g = geo.get(p.partIx), own = sids.get(p.partIx);
      if (!g) throw defect(i, `part ${p.partIx} has no part_geo row`);
      if (!own) throw defect(i, `part ${p.partIx} has no part_section row`);
      at.set(`${i.ord}:${p.partIx}`, parts.length);
      parts.push({ item: i.item, itemName: i.name, kind: i.kind, partIx: p.partIx, pieces: [p.partIx], label: p.label,
                   lo: p.lo, hi: p.hi, grp: p.grp, run: p.run, ...(p.side ? { side: true } : {}),
                   box: g.box, mid: g.mid, sids: [...own].sort((a, b) => a - b) });
    }
  }
  const ix = (i: ItemNavRows, partIx: number, what: string) => {
    const k = at.get(`${i.ord}:${partIx}`);
    if (k === undefined) throw defect(i, `${what} names part ${partIx}, which it does not have`);
    return k;
  };
  const marks: NavMark[] = [];
  for (const i of placed) {
    const tidal = new Set(i.tidal);
    for (const m of i.marks) {
      // (T278) a boundary with a tidal piece goes with the piece (never listed); any other part it names must be here
      if (tidal.has(m.down) || m.up.some((u) => tidal.has(u))) continue;
      marks.push({ lon: m.lon, lat: m.lat, token: m.token, down: ix(i, m.down, "a part_mark"),
                   up: m.up.map((u) => ix(i, u, "a part_mark")), ...(m.confidence ? { confidence: m.confidence } : {}) });
    }
    // a sibling flowing into a sibling: a boundary with no place, from the receiving part to this item's lowest part
    if (i.exit && byOrd.has(i.exit.ord) && i !== placed[0]) {
      const into = byOrd.get(i.exit.ord)!;
      if (i.exit.part === null) throw defect(i, `flows into its sibling ${into.item} at no stored part (water_exit.down_part)`);
      marks.push({ lon: null, lat: null, token: null, down: ix(into, i.exit.part, `${i.item}'s water_exit`),
                   up: [ix(i, lowest(i), "its lowest part")] });
    }
  }
  const low = placed[0];
  const exit = low?.exit && !byOrd.has(low.exit.ord)
    ? { item: low.exit.item, name: low.exit.name, part: low.exit.part, ...(low.exit.tidal ? { tidal: true } : {}) } : null;
  // across waters: only into a water outside the group (a sibling is the same water), from a part this group holds
  const cross = (i: ItemNavRows | undefined, c: ContinueRow | null | undefined): NavCross | null => {
    if (!i || !c || byOrd.has(c.ord)) return null;
    return { item: c.item, name: c.name, part: c.otherPart, at: ix(i, c.part, "its water_continue row") };
  };
  const top = placed.find((i) => i.continuesFrom);
  return { parts, ends, marks, exit, next: cross(low, low?.continuesAs), prev: cross(top, top?.continuesFrom) };
}

/**
 * (T228, owner 2026-10-28; T517, owner 2026-11-08: "I only want a single mode and stretches cover both types of regulation
 * changes while adapting to new DFO rules") WHICH NEIGHBOURING PARTS ARE ONE ON THE MAP — the ONE join rule, every screen:
 * two parts that share a mark join where they are of one run (same item, rule-set group `grp` and connected run `run`:
 * the pieces the DFO salmon chain cuts, pipeline `topology.by_chain`) AND the LIVE salmon reads them alike (`salmonKey`,
 * per section: the release's chains with the feed's rules over them, so a feed that opens salmon on one of two pieces
 * parts them, and one that closes it again joins them, with no new release). With no reading (`salmonKey` null: not read
 * yet, or a release with no salmon) nothing joins: a cut is never hidden on a guess. A joined entry holds every piece
 * (`pieces`), its frame is their union, and its name runs from its first piece's lower end to its last piece's upper end
 * (`lo` / `hi`, said by `spanName` like any part's). Marks inside a joined entry go; the others, `next` / `prev`, follow
 * the new indexes.
 */
export function mergeNav(g: WaterGroup, salmonKey: ((sid: number) => string) | null): WaterGroup {
  if (!salmonKey) return g;
  const n = g.parts.length;
  const root = Array.from({ length: n }, (_, i) => i);
  const find = (i: number): number => { while (root[i] !== i) i = root[i] = root[root[i]!]!; return i; };
  const keys = g.parts.map((p) => [...new Set(p.sids.map(salmonKey))].sort().join("|"));
  const one = (a: number, b: number) => {
    const p = g.parts[a]!, q = g.parts[b]!;
    return p.item === q.item && keys[a] === keys[b] && p.grp === q.grp && p.run === q.run;
  };
  for (const m of g.marks) for (const u of m.up) if (u !== m.down && one(m.down, u)) {
    const a = find(m.down), b = find(u);
    if (a !== b) root[Math.max(a, b)] = Math.min(a, b);
  }
  const newIx = new Map<number, number>();
  const groups: number[][] = [];
  for (let i = 0; i < n; i += 1) {
    const r = find(i);
    if (r === i) { newIx.set(i, groups.length); groups.push([i]); } else groups[newIx.get(r)!]!.push(i);
  }
  for (let i = 0; i < n; i += 1) newIx.set(i, newIx.get(find(i))!);
  const parts: NavPart[] = groups.map((gr) => {
    const p = g.parts[gr[0]!]!;
    if (gr.length === 1) return p;
    let box: Bbox | null = null;
    for (const i of gr) {
      const b = g.parts[i]!.box;
      if (b) box = box ? [Math.min(box[0], b[0]), Math.min(box[1], b[1]), Math.max(box[2], b[2]), Math.max(box[3], b[3])] : b;
    }
    const sids = [...new Set(gr.flatMap((i) => g.parts[i]!.sids))].sort((a, b) => a - b);
    const pieces = [...new Set(gr.flatMap((i) => g.parts[i]!.pieces))].sort((a, b) => a - b);
    // the span's name: the first piece's lower end to the last piece's upper end (`spanName`)
    return { ...p, box, sids, pieces, hi: g.parts[gr[gr.length - 1]!]!.hi };
  });
  const marks: NavMark[] = [];
  const had = new Set<string>();
  for (const m of g.marks) {
    const down = newIx.get(m.down)!;
    const up = [...new Set(m.up.map((u) => newIx.get(u)!))].filter((u) => u !== down);
    const k = `${m.lon}:${m.lat}:${down}:${up.join(",")}`;
    if (!up.length || had.has(k)) continue;
    had.add(k);
    marks.push({ ...m, down, up });
  }
  const at = (c: NavCross | null) => (c ? { ...c, at: newIx.get(c.at)! } : null);
  return { parts, ends: g.ends, marks, exit: g.exit, next: at(g.next), prev: at(g.prev) };
}

/** An item's lowest part: its first in river order (`part_ix`, mouth first). */
function lowest(i: ItemNavRows): number {
  return Math.min(...i.parts.map((p) => p.partIx));
}

/** The group index of the entry holding an item's part (any of its `pieces`: a joined entry holds several), or -1. */
export function partAt(g: WaterGroup, item: string, partIx: number): number {
  return g.parts.findIndex((p) => p.item === item && p.pieces.includes(partIx));
}

/** (T517) THE NAME OF EVERY PART OF `item`, as the map names the entry holding it (`partName` over the group — the same
 *  `mergeNav(group, live salmon key)` the map draws): what the Regulations header and the Parts sheet say, so no screen
 *  names a part another way. */
export function entryNames(g: WaterGroup, item: string): ReadonlyMap<number, string> {
  const out = new Map<number, string>();
  g.parts.forEach((p, i) => { if (p.item === item) for (const k of p.pieces) out.set(k, partName(g, i)); });
  return out;
}

/** (T517) Every part of `item` → the pieces of the map entry holding it (a live-joined entry holds several): what a page
 *  on one part reads its in-season coverage over, so the banner covers what the map lights. */
export function entryPieces(g: WaterGroup, item: string): ReadonlyMap<number, readonly number[]> {
  const out = new Map<number, readonly number[]>();
  for (const p of g.parts) if (p.item === item) for (const k of p.pieces) out.set(k, p.pieces);
  return out;
}

/** The group index of the part holding section `sid`, or -1. */
export function partOfSection(g: WaterGroup, sid: number): number {
  return g.parts.findIndex((p) => p.sids.includes(sid));
}

type Dir = "up" | "down";
/** The marks that lead from `at` one step in `dir`, each with the part it leads to. */
function waysOut(g: WaterGroup, at: number, dir: Dir): { to: number; m: NavMark }[] {
  return dir === "up"
    ? g.marks.filter((m) => m.down === at).flatMap((m) => m.up.map((to) => ({ to, m })))
    : g.marks.filter((m) => m.up.includes(at)).map((m) => ({ to: m.down, m }));
}
/** The parts one step away by the marks alone. */
function byMarks(g: WaterGroup, at: number, dir: Dir): number[] {
  return [...new Set(waysOut(g, at, dir).map((w) => w.to))].filter((i) => i !== at);
}
const placed = (m: NavMark): boolean => m.lon !== null && m.lat !== null;
const samePlace = (a: NavMark, b: NavMark): boolean => a.lon === b.lon && a.lat === b.lat;

/** Whether, going on in `dir` from `from` by the marks (never through `avoid`), `to` is reached. */
function reaches(g: WaterGroup, from: number, to: number, dir: Dir, avoid = -1): boolean {
  const seen = new Set([from, avoid]);
  const queue = [from];
  while (queue.length) {
    for (const n of byMarks(g, queue.shift()!, dir)) {
      if (n === to) return true;
      if (!seen.has(n)) { seen.add(n); queue.push(n); }
    }
  }
  return false;
}

/**
 * (T460, owner 2026-11-06: "it gives an option to basically skip one of the sections") Whether the way from `at` to `c`
 * SKIPS a part: `c` is also reached by going on from another way out of `at`, and the two ways leave at different
 * places. The Fraser's "Williams Lake River confluence → Region 7A boundary" holds a side channel that touches the part
 * two boundaries up, so ▲ offered both "Region 7A boundary → Region 5 boundary" and, past it, "Region 5 boundary →
 * Source". A true fork leaves at ONE place (the Zymoetz's canyon upper entrance: main stem and side channel) and stays.
 * A side channel is never a skip, nor a step from one (the Skeena's Reserve: ▲ offers the main stem OR the channel beside
 * it, a true fork; the channel's ▼ offers both places it rejoins). The
 * shapes are the same in the marks, so the release's km says which: a side channel has none. A mark with no place (a
 * split lake's boundary) is never judged a skip, and the way on never comes back through `at`
 * (the Fraser's channels round Lillooet leave part 3 and come back to it: no skip); two ways that each reach the other
 * are a loop, not a skip, and both stay.
 */
function skips(g: WaterGroup, at: number, c: number, dir: Dir): boolean {
  if (g.parts[c]?.side || g.parts[at]?.side) return false;
  const ways = waysOut(g, at, dir);
  const mine = ways.filter((w) => w.to === c).map((w) => w.m);
  if (!mine.every(placed)) return false;
  return ways.some((o) => o.to !== c && o.to !== at && placed(o.m) && !mine.some((m) => samePlace(m, o.m))
    && reaches(g, o.to, c, dir, at) && !reaches(g, c, o.to, dir, at));
}

/**
 * The parts one step away in `dir`, the main stem first: by the marks, else (where they say nothing) the next part in
 * the order. Empty at the end.
 */
export function stepsFrom(g: WaterGroup, at: number, dir: Dir): readonly number[] {
  const marked = byMarks(g, at, dir).filter((c) => !skips(g, at, c, dir));
  if (!marked.length) {
    const n = dir === "up" ? at + 1 : at - 1;
    return n >= 0 && n < g.parts.length ? [n] : [];
  }
  const side = (c: number) => marked.some((o) => o !== c && reaches(g, c, o, dir));
  // (T204) a way that goes no further by the marks (a side channel's top: the release marks a channel with no km as
  // the UPPER part of every boundary it has) is never the main stem while another way goes on
  const end = (c: number) => byMarks(g, c, dir).length === 0;
  return [...marked].sort((a, b) => Number(end(a)) - Number(end(b)) || Number(side(a)) - Number(side(b)) || a - b);
}

/** ▼: the part below, main stem first, or null at the lowest part (`stepsDown`: every way down at a fork). */
export function stepDown(g: WaterGroup, at: number): number | null {
  return stepsFrom(g, at, "down")[0] ?? null;
}

/** ▼: the parts below, main stem first (two or more: a fork, offered as a choice); empty at the lowest part. */
export function stepsDown(g: WaterGroup, at: number): readonly number[] {
  return stepsFrom(g, at, "down");
}

/** ▼ off the end of this water into the one it goes on as: only on the part that meets it, with nothing below. */
export function crossDown(g: WaterGroup, at: number): NavCross | null {
  return g.next && g.next.at === at && !stepsDown(g, at).length ? g.next : null;
}

/**
 * (T221, owner 2026-10-28) A water of ONE part (one entry of the map: pieces the live salmon joins are one) that goes on as / from no
 * other water (`next` / `prev`, the Vedder ↔ Chilliwack): its map has nothing to choose, so a search result opens its
 * Regulations and the water screen offers no Map view. A split lake's areas are parts of its group, so it is never solo.
 */
export function soloWater(g: WaterGroup): boolean {
  return g.parts.length === 1 && g.next === null && g.prev === null;
}

/** ▲ off the top of this water into the one it goes on from: only on the part that meets it, with nothing above. */
export function crossUp(g: WaterGroup, at: number): NavCross | null {
  return g.prev && g.prev.at === at && !stepsUp(g, at).length ? g.prev : null;
}

/** ▲: the parts above, main stem first (two or more: a fork, offered as a choice); empty at the highest part. */
export function stepsUp(g: WaterGroup, at: number): readonly number[] {
  return stepsFrom(g, at, "up");
}

/** Whether more than one part of `item` is in the group (then a part is named by its short name). */
const manyOf = (g: WaterGroup, i: number) => g.parts.filter((q) => q.item === g.parts[i]!.item).length > 1;

/** A part's name on the map: its own name where its water has several parts, else its water's name. (T517, owner
 *  2026-11-09) The name is composed from the entry's two end tokens by the ONE name function (`spanName`, `ends.ts`) —
 *  a part's, or a span the live salmon joins (its first piece's lower end to its last piece's upper end). */
export function partName(g: WaterGroup, i: number): string {
  return manyOf(g, i) ? entryName(g, i) : entry(g, i).itemName;
}

/** Entry `i`'s own name, composed from its ends (`spanName`): what `partName` says where its water has several parts. */
function entryName(g: WaterGroup, i: number): string {
  const p = entry(g, i);
  // a joined entry is a span (`last`: it must have both ends); one part, its own
  return spanName(g.ends, p.itemName, p, p.pieces.length > 1 ? p : undefined);
}

/** Entry `i` of the group; an index it does not have throws (a caller's defect, never a guessed part). */
function entry(g: WaterGroup, i: number): NavPart {
  const p = g.parts[i];
  if (!p) throw new Error(`water map: no part ${i} in a group of ${g.parts.length}`);
  return p;
}

/** The water a part belongs to, said beside it where the water has several parts (else nothing). */
export function itemNote(g: WaterGroup, i: number): string {
  return manyOf(g, i) ? g.parts[i]!.itemName : "";
}

/** (T517, owner 2026-11-09) A boundary's words: the end token its two parts share, said by the release's `display.ends`
 *  (`endWords`, the one end resolver — the words already carry an approximate cut's confidence, T207); null: no named
 *  place (no token) — the mark is drawn with no words. */
export function markLabel(g: WaterGroup, m: NavMark): string | null {
  return m.token === null ? null : endWords(g.ends, m.token, "hi");
}

/** The section ids of the current part (`selected`) and of the group's other parts (`water`). */
export function navSections(g: WaterGroup, at: number): { selected: readonly number[]; water: readonly number[] } {
  return {
    selected: entry(g, at).sids,
    water: g.parts.flatMap((p, i) => (i === at ? [] : p.sids)),
  };
}

/** A box around a point, about `km` across — for a part the builder wrote no box for. */
function around([lon, lat]: readonly [number, number], km = 1): Bbox {
  const dLat = km / 111.32 / 2, dLon = dLat / Math.max(0.1, Math.cos((lat * Math.PI) / 180));
  return [lon - dLon, lat - dLat, lon + dLon, lat + dLat];
}

/** Where the camera frames part `i`: its box, else a kilometre around its midpoint; null when neither is stored. */
export function partBounds(g: WaterGroup, i: number): Bbox | null {
  const p = entry(g, i);
  return p.box ?? (p.mid ? around(p.mid) : null);
}

/**
 * (T688) THE FRAME OF A WHOLLY TIDAL WATER: the union of its tidal pieces' boxes (a piece with no box: a kilometre around
 * its midpoint). A wholly tidal water lists no part, so no `WaterGroup` holds a frame for it; its map is framed from the
 * release's own `part_geo` rows, the same ones a part's frame is read from. Null: neither is stored.
 */
export function geoBounds(geo: readonly { readonly box: Bbox | null; readonly mid: readonly [number, number] | null }[]): Bbox | null {
  let out: Bbox | null = null;
  for (const g of geo) {
    const b = g.box ?? (g.mid ? around(g.mid) : null);
    if (!b) continue;
    out = out ? [Math.min(out[0], b[0]), Math.min(out[1], b[1]), Math.max(out[2], b[2]), Math.max(out[3], b[3])] : b;
  }
  return out;
}

/** The whole group's frame: the union of its parts' frames. */
export function groupBounds(g: WaterGroup): Bbox | null {
  let out: Bbox | null = null;
  for (let i = 0; i < g.parts.length; i += 1) {
    const b = partBounds(g, i);
    if (!b) continue;
    out = out ? [Math.min(out[0], b[0]), Math.min(out[1], b[1]), Math.max(out[2], b[2]), Math.max(out[3], b[3])] : b;
  }
  return out;
}

/**
 * The water map's words, whole (composed here so a test pins the finished string, AGENTS rule 33). The step buttons'
 * chevrons are the icon set's (`collapse` / `expand`), drawn by the button beside these words (T186, Q162): no glyph is
 * typed into a word.
 */
export const WATER_MAP_WORDS = {
  down: "Downstream",
  up: "Upstream",
  /** Down / up into ANOTHER water (`water_continue`): the button says where it goes (owner 2026-10-21 late). */
  downInto: "Downstream into",
  upInto: "Upstream into",
  lowest: "Lowest part",
  highest: "Highest part",
  regs: "Show regulations for this part",
  /** (T688) A wholly tidal water has no parts: its bar names it as tidal and opens its page (the tidal card). */
  tidal: "Tidal water",
  tidalRegs: "Show regulations for this water",
  thisPart: "This part",
  /** The ⤢ button framing the whole water; the noun is `waterNoun`'s (T116): "Whole river", "Whole wetland". */
  whole: (noun: string) => `Whole ${noun}`,
  upstreamSide: "Upstream",
  downstreamSide: "Downstream",
  /** A boundary mark, to a screen reader ("Boundary: <its end's words>"; one with no named place: "Boundary"). */
  boundary: "Boundary",
  /** The close button's label; its glyph is the icon set's `close` (N1). */
  close: "Close",
  chooser: "Which way upstream?",
  chooserDown: "Which way downstream?",
  /** (T150) The list of the water's parts under the map — the header's Parts sheet, moved here. */
  parts: WORDS.PARTS_TITLE,
  /** (T150) The list's toggle, closed / open, with the count: "Show all 15 parts" / "Hide the list of parts". */
  partsShow: (n: number) => `Show all ${n} parts`,
  partsHide: "Hide the list of parts",
  /** (T150) A row's mark: the part the map (and the Regulations view) is on. */
  partsHere: "✓",
  /** The mouth's link into the water it flows into: "Flows into Vedder River ›". */
  exit: (name: string) => `Flows into ${name} ›`,
  /** (T345) The mouth meets TIDAL water: said, not linked (owner 2026-10-31: "we aren't doing tidal"). */
  exitTidal: (name: string) => `Flows into the tidal ${name}`,
  loading: "Loading the parts of this water",
  /** No release to read the parts from, or it could not be read: the map is drawn, unlit. */
  unavailable: "The parts of this water are not available",
} as const;

/** "Part 2 of 5 · Chilliwack River": where the current part sits in the group. */
export function partWhere(g: WaterGroup, i: number): string {
  const note = itemNote(g, i);
  return `Part ${i + 1} of ${g.parts.length}${note ? ` · ${note}` : ""}`;
}

/** (T150) A row of the parts list as a screen reader says it: "Vedder Crossing Bridge → Tamihi Rapids Bridge, part 2
 *  of 4 · Chilliwack River" — its name, then where it sits (`partWhere`, lower-cased "part"). Whether it is the part
 *  shown is the row's checked state, not words. */
export function partRowAria(g: WaterGroup, i: number): string {
  return `${partName(g, i)}, ${partWhere(g, i).replace(/^Part/, "part")}`;
}

/** The ⤢ button: frame the whole river (or lake), or — while it is framed — back to this part. */
export function wholeLabel(g: WaterGroup, i: number, whole: boolean): string {
  return whole ? wholeAria(g, i, whole) : `⤢ ${wholeAria(g, i, whole)}`;
}

/** The ⤢ button as a screen reader says it (no glyph): "Whole river", "Whole lake", "This part". */
export function wholeAria(g: WaterGroup, i: number, whole: boolean): string {
  if (whole) return WATER_MAP_WORDS.thisPart;
  return WATER_MAP_WORDS.whole(waterNoun(entry(g, i).kind, "page"));
}

/** Up / down into another water, on the button and to a screen reader alike: "Upstream into Chilliwack River". */
export function crossLabel(dir: "up" | "down", c: NavCross): string {
  return `${dir === "up" ? WATER_MAP_WORDS.upInto : WATER_MAP_WORDS.downInto} ${c.name}`;
}

/** The link at the lowest part to the water its mouth flows into — or, where the mouth meets tidal water (T345), the
 *  plain words "Flows into the tidal Fraser River": no link, so no chevron. */
export function exitLabel(e: NavExit): string {
  return e.tidal ? WATER_MAP_WORDS.exitTidal(e.name) : WATER_MAP_WORDS.exit(e.name);
}

/** What a boundary's mark says, for a screen reader. */
export function markA11y(g: WaterGroup, m: NavMark): string {
  const w = markLabel(g, m);
  return w === null ? WATER_MAP_WORDS.boundary : `${WATER_MAP_WORDS.boundary}: ${w}`;
}
