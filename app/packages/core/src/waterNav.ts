/**
 * THE WATER MAP'S NAVIGATION (P15): a water's parts in mouth-first order, the boundary marks between them, and a step
 * ▼ downstream / ▲ upstream over that STORED graph — the release's `part_geo` (rank, box), `part_mark` (a boundary: its
 * place, its label, the part below it and the parts above it) and `water_exit` (where the water's mouth goes), written
 * by the pipeline (P09). Nothing here is a regulation: it is which part is next to which, and the words for it.
 *
 * THE GROUP. A water is one item; a lake divided into closure areas (Kootenay Lake: Main Body, Upper and Lower West
 * Arm, each its own item `part_of` the lake, P09's KNOWN GAP) is the group of its sibling items, walked as one water
 * the way the prototype's map does (`regs-v11.html` `klwa`). Siblings are ordered by their exits (the one that leaves
 * the group is the lowest; each that flows into one already placed comes next), and an exit from one sibling into
 * another is a boundary mark with no place (the release draws none between items) and no label: its words are the two
 * parts' names without what they share (`markLabel`, the prototype's).
 *
 * STRETCHES (T204, owner 2026-10-25: "non-adjacent stretches of one part are SEPARATE stretches on the map, own label, own
 * selection"). A part the release splits into stretches (`part_stretch`: Kanaka Creek's part below the 112 Avenue bridge
 * AND above Cliff Park Falls) is walked as one entry PER STRETCH, in the release's stretch order, each lit, framed and
 * listed on its own; its marks say which stretch meets there (`down_stretch` / `up_stretches`). The regulations stay the
 * part's: every stretch of a part opens the same part.
 *
 * THE STEP. ▲ takes the parts above the current one by the marks (a fork: a mark with two upper parts); ▼ the parts
 * below it (a fork too: two parts whose marks both say they sit below this one — the Zymoetz's source part has the main
 * stem and "A lake → McDonell Lake outlet" below it, and that lake part is reached ONLY that way). Where the marks say
 * nothing the order (`rank`) does, so every part stays reachable. At a fork the MAIN stem goes first: a side channel is
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
import { waterNoun, WORDS } from "./regs/words";
import type { Bbox } from "./searchGeometry";

/** One part of the group as the release stores it, its section ids included (`part_section`). */
export interface NavPart {
  /** The item it belongs to (its durable id) and that item's name and kind. */
  readonly item: string;
  readonly itemName: string;
  readonly kind: string;
  /** Its index in its item's `part` table — what the regulations page and the route name a part by. */
  readonly partIx: number;
  /** Mouth-first order within its item (`part_geo.rank`). */
  readonly rank: number;
  readonly label: string;
  readonly short: string;
  /** (T460) A side channel (the release's part with no km): a fork's branch, never a skipped part of the main stem. */
  readonly side?: boolean;
  /** (T517) Its rule-set group and connected run: parts sharing both are one run the DFO salmon chain cuts, which the
   *  regular map joins (`mergeNav`). `lo` / `hi`: such a piece's own two ends; `runName`: the whole run's name. */
  readonly grp?: number;
  readonly run?: number;
  readonly lo?: string | null;
  readonly hi?: string | null;
  readonly runName?: string;
  /** The box of its geometry `[w, s, e, n]` in degrees, or null where the builder wrote none. */
  readonly box: Bbox | null;
  /** A point on it (`part_geo.mid_lon / mid_lat`), in degrees. */
  readonly mid: readonly [number, number] | null;
  readonly sids: readonly number[];
  /** (T204) Which stretch of its part this is (0 first, mouth up), and how many its part has; absent: one. */
  readonly stretch?: number;
  readonly stretches?: number;
  /** (T228) For a stretch: its whole part's frame (`part_geo`), which a piece joined back into the whole part takes. */
  readonly partBox?: Bbox | null;
  readonly partMid?: readonly [number, number] | null;
}

/** (T207) How sure the curator is of where a cut stands — pipeline `models.splits.CONFIDENCE`; absent is high. */
export type Confidence = "high" | "medium" | "low";

export function isConfidence(v: unknown): v is Confidence {
  return v === "high" || v === "medium" || v === "low";
}

/** THE ONE SENTENCE for a cut's confidence (pipeline `confidence_words`, the same words): "" for high or absent, else
 *  "approximate location (low confidence)". */
export function confidenceWords(c: Confidence | null | undefined): string {
  return !c || c === "high" ? "" : `approximate location (${c} confidence)`;
}

/** A boundary between parts: `down` and `up` are indexes into the group's `parts`. No place: not drawn. */
export interface NavMark {
  readonly lon: number | null;
  readonly lat: number | null;
  readonly label: string | null;
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
  readonly marks: readonly NavMark[];
  readonly exit: NavExit | null;
  /** The water this one goes on as, below its mouth (the Chilliwack's: the Vedder), or null. */
  readonly next: NavCross | null;
  /** The water this one goes on from, above its top (the Vedder's: the Chilliwack), or null. */
  readonly prev: NavCross | null;
}

/** One `water_continue` row as an item's rows carry it: the other water (its ord, id and name — null where the
 *  release's core does not hold it), `part` this item's part at the junction and `otherPart` the other water's. */
export interface ContinueRow {
  readonly ord: number; readonly part: number; readonly otherPart: number;
  readonly item: string | null; readonly name: string | null;
}

/** One item's rows from the release, as the data layer reads them (degrees, not microdegrees). */
export interface ItemNavRows {
  readonly ord: number;
  readonly item: string;
  readonly name: string;
  readonly kind: string;
  /** `side` (T460): a side channel, the release's part with no km (`part.km` NULL), never a step on the main stem. */
  readonly parts: readonly { readonly partIx: number; readonly label: string; readonly short: string; readonly side?: boolean;
                              readonly grp?: number; readonly run?: number; readonly lo?: string | null;
                              readonly hi?: string | null; readonly runName?: string }[];
  readonly geo: readonly { readonly partIx: number; readonly rank: number; readonly box: Bbox | null;
                           readonly mid: readonly [number, number] | null }[];
  readonly marks: readonly { readonly lon: number; readonly lat: number; readonly label: string | null;
                             readonly down: number; readonly up: readonly number[];
                             readonly confidence?: Confidence;
                             /** (T204) the stretch of `down`, and of each of `up` (same order); absent: stretch 0 */
                             readonly downStretch?: number; readonly upStretches?: readonly number[] }[];
  readonly sections: readonly { readonly sid: number; readonly partIx: number }[];
  /** (T204) `part_stretch`: every stretch of every part, for an item with a part of two or more (else absent or empty):
   *  its part, its index in the part, its rank among the item's stretches (mouth first), its frame and its sids. */
  readonly stretches?: readonly { readonly partIx: number; readonly stretch: number; readonly rank: number;
                                  readonly box: Bbox | null; readonly mid: readonly [number, number] | null;
                                  readonly sids: readonly number[] }[];
  /** `water_exit`: the receiving item's ord, the part it is met at, and — when the release names it — its id and name. */
  readonly exit: { readonly ord: number; readonly part: number | null; readonly item: string | null;
                   readonly name: string | null;
                   /** (T345) the receiving part is tidal (its `part` row: no key, `province_except` "tidal") */
                   readonly tidal?: boolean } | null;
  /** `water_continue`: the water this one goes on as (`continuesAs`) and the one it goes on from (`continuesFrom`). */
  readonly continuesAs?: ContinueRow | null;
  readonly continuesFrom?: ContinueRow | null;
}

/** (T517) The run facts a part carries into the group. */
const runOf = (p: ItemNavRows["parts"][number]) =>
  (p.runName ? { grp: p.grp, run: p.run, lo: p.lo ?? null, hi: p.hi ?? null, runName: p.runName } : {});

/**
 * The group, from its items' rows. `all[0]` is the water opened; the others are its siblings (a split lake's other
 * parts) — in any order: they are ordered here by their exits. An item with no parts is left out.
 */
export function waterGroup(all: readonly ItemNavRows[]): WaterGroup {
  // an item with no part (a split lake itself: its closure areas own the parts) is not walked, and is no mouth
  const items = all.filter((i) => i.parts.length > 0);
  const byOrd = new Map(items.map((i) => [i.ord, i]));
  // the lowest: an item whose exit leaves the group (or that has none); then each that flows into one placed
  const placed: ItemNavRows[] = [];
  const left = new Set(items.map((i) => i.ord));
  const outside = items.filter((i) => !i.exit || !byOrd.has(i.exit.ord));
  for (const i of outside.length ? outside : items.slice(0, 1)) { placed.push(i); left.delete(i.ord); }
  for (let k = 0; k < placed.length; k += 1) {
    for (const i of items)
      if (left.has(i.ord) && i.exit?.ord === placed[k]!.ord) { placed.push(i); left.delete(i.ord); }
  }
  for (const i of items) if (left.has(i.ord)) placed.push(i);      // a cycle or a stray: after the rest, as given

  const parts: NavPart[] = [];
  const at = new Map<string, number>();                            // `${ord}:${partIx}:${stretch}` → index in `parts`
  for (const i of placed) {
    const geo = new Map(i.geo.map((g) => [g.partIx, g]));
    const facts = new Map(i.parts.map((p) => [p.partIx, p]));
    const sids = new Map<number, number[]>();
    for (const s of i.sections) {
      let l = sids.get(s.partIx);
      if (!l) sids.set(s.partIx, l = []);
      l.push(s.sid);
    }
    const st = (i.stretches ?? []).filter((s) => facts.has(s.partIx));
    if (st.length) {
      // (T204) one entry per stretch, in the release's stretch order
      const count = new Map<number, number>();
      for (const s of st) count.set(s.partIx, (count.get(s.partIx) ?? 0) + 1);
      for (const s of [...st].sort((a, b) => a.rank - b.rank || a.partIx - b.partIx || a.stretch - b.stretch)) {
        const p = facts.get(s.partIx)!, whole = geo.get(s.partIx);
        at.set(`${i.ord}:${s.partIx}:${s.stretch}`, parts.length);
        parts.push({ item: i.item, itemName: i.name, kind: i.kind, partIx: s.partIx, rank: s.rank,
                     label: p.label, short: p.short, ...(p.side ? { side: true } : {}), ...runOf(p), box: s.box, mid: s.mid,
                     sids: [...s.sids].sort((a, b) => a - b), stretch: s.stretch, stretches: count.get(s.partIx)!,
                     ...(whole ? { partBox: whole.box, partMid: whole.mid } : {}) });
      }
      // a part the stretch rows miss (none should): after them, whole
      for (const p of i.parts) if (!count.has(p.partIx)) {
        const g = geo.get(p.partIx);
        at.set(`${i.ord}:${p.partIx}:0`, parts.length);
        parts.push({ item: i.item, itemName: i.name, kind: i.kind, partIx: p.partIx, rank: g?.rank ?? parts.length,
                     label: p.label, short: p.short, ...(p.side ? { side: true } : {}), ...runOf(p), box: g?.box ?? null, mid: g?.mid ?? null,
                     sids: [...(sids.get(p.partIx) ?? [])].sort((a, b) => a - b) });
      }
      continue;
    }
    const ordered = [...i.parts].sort((a, b) =>
      (geo.get(a.partIx)?.rank ?? Infinity) - (geo.get(b.partIx)?.rank ?? Infinity) || a.partIx - b.partIx);
    for (const p of ordered) {
      const g = geo.get(p.partIx);
      at.set(`${i.ord}:${p.partIx}:0`, parts.length);
      parts.push({ item: i.item, itemName: i.name, kind: i.kind, partIx: p.partIx, rank: g?.rank ?? parts.length,
                   label: p.label, short: p.short, ...(p.side ? { side: true } : {}), ...runOf(p), box: g?.box ?? null, mid: g?.mid ?? null,
                   sids: [...(sids.get(p.partIx) ?? [])].sort((a, b) => a - b) });
    }
  }
  const ix = (ord: number, partIx: number, stretch = 0) => at.get(`${ord}:${partIx}:${stretch}`);
  const marks: NavMark[] = [];
  for (const i of placed) {
    for (const m of i.marks) {
      const down = ix(i.ord, m.down, m.downStretch ?? 0);
      const up = m.up.map((u, k) => ix(i.ord, u, m.upStretches?.[k] ?? 0)).filter((u): u is number => u !== undefined);
      if (down !== undefined && up.length)
        marks.push({ lon: m.lon, lat: m.lat, label: m.label, down, up, ...(m.confidence ? { confidence: m.confidence } : {}) });
    }
    // a sibling flowing into a sibling: a boundary with no place, from the receiving part to this item's lowest part
    if (i.exit && byOrd.has(i.exit.ord) && i !== placed[0]) {
      const down = ix(i.exit.ord, i.exit.part ?? 0) ?? ix(i.exit.ord, lowest(byOrd.get(i.exit.ord)!));
      const up = ix(i.ord, lowest(i));
      if (down !== undefined && up !== undefined) marks.push({ lon: null, lat: null, label: null, down, up: [up] });
    }
  }
  const low = placed[0];
  const exit = low?.exit && !byOrd.has(low.exit.ord) && low.exit.item !== null && low.exit.name !== null
    ? { item: low.exit.item, name: low.exit.name, part: low.exit.part, ...(low.exit.tidal ? { tidal: true } : {}) } : null;
  // across waters: only into a water outside the group that the release names, from a part this group holds
  const cross = (i: ItemNavRows | undefined, c: ContinueRow | null | undefined): NavCross | null => {
    if (!i || !c || byOrd.has(c.ord) || c.item === null || c.name === null) return null;
    const at = ix(i.ord, c.part);
    return at === undefined ? null : { item: c.item, name: c.name, part: c.otherPart, at };
  };
  const top = placed.find((i) => i.continuesFrom);
  return { parts, marks, exit, next: cross(low, low?.continuesAs), prev: cross(top, top?.continuesFrom) };
}

/**
 * (T228, owner 2026-10-28) WHICH NEIGHBOURING PIECES ARE ONE ON THE MAP: "adjacent sections are merged only when BOTH
 * the standard regulations AND the salmon regulations match" — and the no-access land never merges (T228 item 1: the
 * bundle's parts already break there). The release cuts a part's stretches wherever the DFO salmon chain changes
 * (pipeline `topology.by_chain`) and marks where two such stretches of ONE part meet; this joins every two stretches of
 * one part that share such a mark AND read the same salmon today (`salmonKey`, per section — the live DFO feed laid over
 * the release, so a feed that opens salmon on one of two pieces parts them, and one that closes it again joins them,
 * with no new release). A piece's key is its sections' keys; with no `salmonKey` (salmon not read) every such pair
 * joins, which is the part's stretches as before the cut. Stretches of one part that no mark joins (T204: another part
 * lies between) stay apart. Marks inside a joined piece go; the others, `next` / `prev`, follow the new indexes.
 */
export function mergeNav(g: WaterGroup, salmonKey: ((sid: number) => string) | null): WaterGroup {
  const n = g.parts.length;
  const root = Array.from({ length: n }, (_, i) => i);
  const find = (i: number): number => { while (root[i] !== i) i = root[i] = root[root[i]!]!; return i; };
  const keyOf = (i: number) => salmonKey ? [...new Set(g.parts[i]!.sids.map(salmonKey))].sort().join("|") : "";
  const keys = g.parts.map((_, i) => keyOf(i));
  // (T517) one run the salmon chain cut: same item, grp and run (a T204 stretch of one part: same part)
  const one = (a: number, b: number) => {
    const p = g.parts[a]!, q = g.parts[b]!;
    return p.item === q.item && keys[a] === keys[b]
      && (p.partIx === q.partIx || (p.runName !== undefined && p.grp === q.grp && p.run === q.run));
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
  const count = new Map<string, number>(), seen = new Map<string, number>();
  for (const gr of groups) { const p = g.parts[gr[0]!]!; const k = `${p.item}:${p.partIx}`; count.set(k, (count.get(k) ?? 0) + 1); }
  const parts: NavPart[] = groups.map((gr) => {
    const p = g.parts[gr[0]!]!;
    const k = `${p.item}:${p.partIx}`, c = count.get(k)!, at = seen.get(k) ?? 0;
    seen.set(k, at + 1);
    let box: Bbox | null = null;
    for (const i of gr) {
      const b = g.parts[i]!.box;
      if (b) box = box ? [Math.min(box[0], b[0]), Math.min(box[1], b[1]), Math.max(box[2], b[2]), Math.max(box[3], b[3])] : b;
    }
    const { stretch: _s, stretches: _n, partBox, partMid, ...base } = p;
    const sids = [...new Set(gr.flatMap((i) => g.parts[i]!.sids))].sort((a, b) => a - b);
    // the whole part again: its own frame (`part_geo`), as before the cut
    if (c === 1 && gr.length > 1 && partBox !== undefined)
      return { ...base, box: partBox ?? box ?? p.box, mid: partMid ?? p.mid, sids };
    return { ...base, box: box ?? p.box, sids, ...(c > 1 ? { stretch: at, stretches: c } : {}),
             ...(partBox !== undefined ? { partBox, partMid } : {}) };
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
  // (T517) pieces of one salmon-cut run joined: the whole run says the run's name (the pipeline's); a few of its pieces
  // joined (salmon mode, alike today) say the lowest one's lower end and the highest one's upper end
  const runSize = new Map<string, number>();
  for (const p of g.parts) if (p.runName !== undefined) {
    const k = `${p.item}:${p.grp}:${p.run}`;
    runSize.set(k, (runSize.get(k) ?? 0) + 1);
  }
  const named = parts.map((p, i) => {
    const gr = groups[i]!;
    if (p.runName === undefined || gr.length < 2) return p;
    const whole = gr.length === runSize.get(`${p.item}:${p.grp}:${p.run}`);
    const lo = g.parts[gr[0]!]!.lo, hi = g.parts[gr[gr.length - 1]!]!.hi;
    return { ...p, short: whole || !lo || !hi ? p.runName : `${lo} → ${hi}` };
  });
  const at = (c: NavCross | null) => (c ? { ...c, at: newIx.get(c.at)! } : null);
  return { parts: named, marks, exit: g.exit, next: at(g.next), prev: at(g.prev) };
}

/** An item's lowest part (`rank` 0, else its first). */
function lowest(i: ItemNavRows): number {
  const g = [...i.geo].sort((a, b) => a.rank - b.rank)[0];
  return g?.partIx ?? i.parts[0]?.partIx ?? 0;
}

/** The group index of an item's part, or -1. */
export function partAt(g: WaterGroup, item: string, partIx: number): number {
  return g.parts.findIndex((p) => p.item === item && p.partIx === partIx);
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
 * (T464) The part next in the order in `dir`, when it is another stretch of `at`'s own part and its own step back,
 * having no mark that way, falls back to `at`: then `at` steps to it too, so ▲ and ▼ agree (any other part next in the
 * order is not, a side channel's neighbour least of all). The upper Bulkley's "Morice River confluence → Source" is 7 stretches; the 6
 * upstream ones (a section each) have no marks, so each stepped ▼ to the one before by the order. The main stretch's ▲
 * had a mark of its own (to the side channel), so the order was never asked, and 6 of the Bulkley's 12 parts could
 * not be stepped to.
 */
function fallsBackTo(g: WaterGroup, at: number, dir: Dir): number | null {
  const n = dir === "up" ? at + 1 : at - 1;
  if (n < 0 || n >= g.parts.length) return null;
  const p = g.parts[at]!, q = g.parts[n]!;
  if (p.stretch === undefined || q.item !== p.item || q.partIx !== p.partIx) return null;
  return byMarks(g, n, dir === "up" ? "down" : "up").length === 0 ? n : null;
}

/**
 * The parts one step away in `dir`, the main stem first: by the marks, else (where they say nothing) the next part in
 * the order. Empty at the end.
 */
export function stepsFrom(g: WaterGroup, at: number, dir: Dir): readonly number[] {
  const marked = byMarks(g, at, dir).filter((c) => !skips(g, at, c, dir));
  const back = fallsBackTo(g, at, dir);
  if (marked.length && back !== null && !marked.includes(back)) marked.push(back);
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
 * (T221, owner 2026-10-28) A water of ONE part (one stretch: `parts` counts each stretch) that goes on as / from no
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

/** A part's name on the map: its short name where its water has several parts, else its water's name. (T517) A part
 *  is one connected run, named by its own two ends in the pipeline (`answers.display.run_ends`); nothing here composes
 *  a name but `mergeNav`'s "lo → hi" for salmon-cut pieces joined in salmon mode. A stretch of a release cut before
 *  T517 keeps its count. */
export function partName(g: WaterGroup, i: number): string {
  const p = g.parts[i]!;
  if (!p.stretches || p.stretches <= 1) return manyOf(g, i) ? p.short : p.itemName;
  return `${p.short} · ${WATER_MAP_WORDS.stretchOf((p.stretch ?? 0) + 1, p.stretches)}`;
}

/** The water a part belongs to, said beside it where the water has several parts (else nothing). */
export function itemNote(g: WaterGroup, i: number): string {
  return manyOf(g, i) ? g.parts[i]!.itemName : "";
}

/** A boundary's words: its own label, else the two parts' names without what they share (the prototype's rule). A cut
 *  placed approximately says so (T207): "salmon viewing platform, approximate location (low confidence)". */
export function markLabel(g: WaterGroup, m: NavMark): string {
  if (m.label) {
    const cw = confidenceWords(m.confidence);
    return cw ? `${m.label}, ${cw}` : m.label;
  }
  const a = g.parts[m.down]?.short ?? "", b = g.parts[m.up[0]!]?.short ?? "";
  let k = 0;
  while (k < a.length && a[k] === b[k]) k += 1;
  k = a.lastIndexOf(" ", k) + 1;
  return `${a.slice(k)} / ${b.slice(k)}`;
}

/** The section ids of the current part (`selected`) and of the group's other parts (`water`). */
export function navSections(g: WaterGroup, at: number): { selected: readonly number[]; water: readonly number[] } {
  return {
    selected: g.parts[at]?.sids ?? [],
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
  const p = g.parts[i];
  if (!p) return null;
  return p.box ?? (p.mid ? around(p.mid) : null);
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
  thisPart: "This part",
  /** The ⤢ button framing the whole water; the noun is `waterNoun`'s (T116): "Whole river", "Whole wetland". */
  whole: (noun: string) => `Whole ${noun}`,
  upstreamSide: "Upstream",
  downstreamSide: "Downstream",
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
  /** (T204) One of a part's separate stretches, in its name: "stretch 1 of 2". */
  stretchOf: (k: number, n: number) => `stretch ${k} of ${n}`,
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
  return WATER_MAP_WORDS.whole(waterNoun(g.parts[i]?.kind ?? "stream", "page"));
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
  return `Boundary: ${markLabel(g, m)}`;
}
