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

/** Where the group's mouth flows: the receiving water and the part of it the mouth meets (null: not stored). */
export interface NavExit { readonly item: string; readonly name: string; readonly part: number | null }

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
  readonly parts: readonly { readonly partIx: number; readonly label: string; readonly short: string }[];
  readonly geo: readonly { readonly partIx: number; readonly rank: number; readonly box: Bbox | null;
                           readonly mid: readonly [number, number] | null }[];
  readonly marks: readonly { readonly lon: number; readonly lat: number; readonly label: string | null;
                             readonly down: number; readonly up: readonly number[];
                             readonly confidence?: Confidence }[];
  readonly sections: readonly { readonly sid: number; readonly partIx: number }[];
  /** `water_exit`: the receiving item's ord, the part it is met at, and — when the release names it — its id and name. */
  readonly exit: { readonly ord: number; readonly part: number | null; readonly item: string | null;
                   readonly name: string | null } | null;
  /** `water_continue`: the water this one goes on as (`continuesAs`) and the one it goes on from (`continuesFrom`). */
  readonly continuesAs?: ContinueRow | null;
  readonly continuesFrom?: ContinueRow | null;
}

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
  const at = new Map<string, number>();                            // `${ord}:${partIx}` → index in `parts`
  for (const i of placed) {
    const geo = new Map(i.geo.map((g) => [g.partIx, g]));
    const sids = new Map<number, number[]>();
    for (const s of i.sections) {
      let l = sids.get(s.partIx);
      if (!l) sids.set(s.partIx, l = []);
      l.push(s.sid);
    }
    const ordered = [...i.parts].sort((a, b) =>
      (geo.get(a.partIx)?.rank ?? Infinity) - (geo.get(b.partIx)?.rank ?? Infinity) || a.partIx - b.partIx);
    for (const p of ordered) {
      const g = geo.get(p.partIx);
      at.set(`${i.ord}:${p.partIx}`, parts.length);
      parts.push({ item: i.item, itemName: i.name, kind: i.kind, partIx: p.partIx, rank: g?.rank ?? parts.length,
                   label: p.label, short: p.short, box: g?.box ?? null, mid: g?.mid ?? null,
                   sids: [...(sids.get(p.partIx) ?? [])].sort((a, b) => a - b) });
    }
  }
  const ix = (ord: number, partIx: number) => at.get(`${ord}:${partIx}`);
  const marks: NavMark[] = [];
  for (const i of placed) {
    for (const m of i.marks) {
      const down = ix(i.ord, m.down), up = m.up.map((u) => ix(i.ord, u)).filter((u): u is number => u !== undefined);
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
    ? { item: low.exit.item, name: low.exit.name, part: low.exit.part } : null;
  // across waters: only into a water outside the group that the release names, from a part this group holds
  const cross = (i: ItemNavRows | undefined, c: ContinueRow | null | undefined): NavCross | null => {
    if (!i || !c || byOrd.has(c.ord) || c.item === null || c.name === null) return null;
    const at = ix(i.ord, c.part);
    return at === undefined ? null : { item: c.item, name: c.name, part: c.otherPart, at };
  };
  const top = placed.find((i) => i.continuesFrom);
  return { parts, marks, exit, next: cross(low, low?.continuesAs), prev: cross(top, top?.continuesFrom) };
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
/** The parts one step away by the marks alone. */
function byMarks(g: WaterGroup, at: number, dir: Dir): number[] {
  const out = dir === "up"
    ? g.marks.filter((m) => m.down === at).flatMap((m) => m.up)
    : g.marks.filter((m) => m.up.includes(at)).map((m) => m.down);
  return [...new Set(out)].filter((i) => i !== at);
}

/** Whether, going on in `dir` from `from` by the marks, `to` is reached. */
function reaches(g: WaterGroup, from: number, to: number, dir: Dir): boolean {
  const seen = new Set([from]);
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
 * The parts one step away in `dir`, the main stem first: by the marks, else (where they say nothing) the next part in
 * the order. Empty at the end.
 */
export function stepsFrom(g: WaterGroup, at: number, dir: Dir): readonly number[] {
  const marked = byMarks(g, at, dir);
  if (!marked.length) {
    const n = dir === "up" ? at + 1 : at - 1;
    return n >= 0 && n < g.parts.length ? [n] : [];
  }
  const side = (c: number) => marked.some((o) => o !== c && reaches(g, c, o, dir));
  return [...marked].sort((a, b) => Number(side(a)) - Number(side(b)) || a - b);
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

/** A part's name on the map: its short name where its water has several parts, else its water's name. */
export function partName(g: WaterGroup, i: number): string {
  const p = g.parts[i]!;
  return manyOf(g, i) ? p.short : p.itemName;
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

/** The link at the lowest part to the water its mouth flows into. */
export function exitLabel(e: NavExit): string {
  return `Flows into ${e.name} ›`;
}

/** What a boundary's mark says, for a screen reader. */
export function markA11y(g: WaterGroup, m: NavMark): string {
  return `Boundary: ${markLabel(g, m)}`;
}
