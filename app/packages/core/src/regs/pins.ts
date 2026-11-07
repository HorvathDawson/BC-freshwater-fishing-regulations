/**
 * T28 — the pin bar under the header, as pure functions (no React; T29 measures and draws).
 *
 * Ports prototype regs v38 @ 7fb6809f (regs-v11.html): `pinUpdate` 1728–1751 (round UR6: a tier's line stays until its
 * section's LAST row has gone up under the stack, whose height is MEASURED, not guessed), `mini` 1752–1760, the tab press
 * 2000 and the round 20i scroll margins (872). The v33 rule the ticket text describes (`nextHead`, DROP 110) is gone from
 * the prototype; port/pins.mjs records exactly what v38 reads (`end` per tier, each drawn line's height) and
 * conformance/src/regs/pins.parity.test.ts (retired, T439) replays every golden state through `pinChain`.
 *
 * IMPLEMENT-HANDOFF C5 (UI-NOTES v35 integration 1): a ledger article whose row has no group pins NOTHING — the prototype no
 * longer throws there, so there is no "keep the previous lines" state to reproduce.
 *
 * Nothing here decides a regulation: every word on a line is the store's (shortOf over the stored tree, the stored headline);
 * this only chooses which lines stand in the bar at a scroll offset.
 */
import type { GroupTotal, TreeTier } from "../answers.generated";
import { shortOf } from "./grouphead";
import type { Tab } from "./model";
import { WORDS } from "./words";

/** pinUpdate LH: one pinned line's allowance (an unmeasured line, and the article test's margin). */
export const PIN_LH = 26;
/** pinUpdate PIN_ROW: a tier's line stays while more than this many px of its section's rows show below the stack. */
export const PIN_ROW = 12;
/** the round 20i scroll margin under the stack (#sc scroll-padding-top: header + pin height + 8px) */
export const PIN_MARGIN = 8;
/** mini(): a section counts as reached this many px below the bar */
export const TAB_REACH = 12;

export interface PinLine {
  readonly cls: "g" | "g two" | "c"; readonly text: string; readonly sub: string | null;
  /** (T29) a tier line's tier (prototype `data-tier`: the size line is drawn in the size section's rule colour) */
  readonly tier?: string;
  /** (T481) the group line only: the tiers it already says (the share, a water's own tier beside it) — they never pin */
  readonly said?: readonly string[];
  /** (T481) the group line only: the number the breadcrumb's first level counts from ("1 of them") */
  readonly n?: number | null;
}

const tierKids = (t: TreeTier): TreeTier[] => t.children.filter((c): c is TreeTier => c.node === "tier");

/**
 * (T481, owner 2026-11-07, "here-max + one breadcrumb") The bar's first line: the WATER's limit, never the region's total.
 * - a plain root: "Kokanee · 10 a day";
 * - a stream (lake) share and nothing beside it: "Trout and char · 2 a day from streams";
 * - a share and a water's own tier beside it, inside the total (`within` "total", the T479 exceptions): the stored
 *   `water_max` leads and the parts are inside it — "Trout and char · 4 a day — up to 2 of any kind, the rest hatchery
 *   rainbow" (owner: no double negatives; nothing in the line adds up to more than the true most). Those tiers never pin.
 * Every number is the store's (the tree, `water_max`); this only words them.
 */
export function groupLine(name: string, g: Pick<GroupTotal, "tree"> & { readonly water_max?: number | null }): PinLine {
  const root = g.tree[0]!;
  const line = (rest: string, said: string[], n: number | null): PinLine => ({ cls: "g", text: `${name}${WORDS.DOT_SEP}${rest}`, sub: null, said, n });
  if (root.n == null) return line(shortOf(root), [], null);
  const share = tierKids(root).find((t) => t.tier === "share") ?? null;
  if (!share || share.n == null) return line(WORDS.PIN_A_DAY(root.n), [], root.n);
  const beside = tierKids(root).filter((t) => t.tier === "own" && t.within === "total");
  const max = g.water_max ?? root.n;
  const fish = beside.map((t) => t.fish).filter((f): f is string => !!f);
  if (beside.length && max > share.n && fish.length === beside.length)
    return line(WORDS.PIN_EXCEPTION(max, share.n, [...new Set(fish)].join(WORDS.PIN_OR)), [share.id, ...beside.map((t) => t.id)], share.n);
  const from = /from (this stream|this lake|streams|lakes)/.exec(share.says)?.[1] || WORDS.SHORT_STREAMS;
  return line(WORDS.PIN_SHARE(share.n, from), [share.id], share.n);
}

/** One breadcrumb level: a tier the scroll is inside (its kind, number and stored short words, TierVM.short). */
export interface CrumbLevel { readonly tier: string; readonly n: number | null; readonly short: string }

/**
 * (T481) The bar's second line: the path below the root, "; of those, " between levels. A level whose number is at or over
 * its parent's caps nothing further and is folded. A size level counts only what is over its size, so it is no parent:
 * "1 of them may be over 50 cm (20 in)" where it counts from the first line, "of those, 1 may be over …" under a count
 * level (owner: "of them" must point at the right fish). "" where nothing is left.
 */
export function breadcrumb(levels: readonly CrumbLevel[], n0: number | null): string {
  const parts: string[] = [];
  let ref = n0, prev: "line" | "count" | "size" = "line";
  for (const L of levels) {
    if (L.n != null && ref != null && L.n >= ref) continue;               // caps nothing further
    if (L.tier === "size") {
      const k = L.n ? String(L.n) : WORDS.PIN_NONE;
      parts.push(prev === "count" ? `${WORDS.SHORT_OF_THOSE}${k} ${L.short}` : `${k}${WORDS.PIN_OF_THEM} ${L.short}`);
      prev = "size";
    } else {
      parts.push(prev === "count" ? `${WORDS.SHORT_OF_THOSE}${L.short}` : L.short);
      ref = L.n ?? ref; prev = "count";
    }
  }
  return parts.join(WORDS.PIN_LEVEL_SEP);
}

/** A tier's pinned line: "↳ " + the breadcrumb. */
export const tierLine = (crumb: string, tier?: string): PinLine => ({ cls: "c", text: `${WORDS.SHARE_ARROW}${crumb}`, sub: null, ...(tier ? { tier } : {}) });

/** pinUpdate hOf before a line has been measured: one LH, two with a sub. */
export const pinLineEstimate = (l: PinLine): number => (l.sub ? 2 * PIN_LH : PIN_LH);

export interface PinBox { readonly top: number; readonly height: number }
/** One tier section (`section.tnode`). Coordinates are CONTENT offsets of the scroll view at scroll 0. */
export interface PinTierAnchor {
  readonly tid: string; readonly tier: string; readonly n: number | null;
  readonly head: PinBox | null;          // its header (`:scope > .tier`); null: never pins
  readonly end: number | null;           // secEnd: the bottom of the section's last kind row or tier head; null: none
  readonly short: string | null;         // TierVM.short (shortOf of the stored tier), WITHOUT the arrow; null: no tier data
}
/** One Keep article (`article.row.ledger`) in document order. */
export interface PinAnchor {
  readonly row: string;
  readonly group: boolean; readonly tree: boolean;   // the row has a stored group / its tree has a root
  readonly formA: boolean; readonly headlineN: number | null;
  readonly top: number; readonly height: number;
  readonly groupLine: PinLine | null;                // GroupVM.pinLine
  readonly pin: PinBox | null;                       // the group's head block (.ghd.gpin)
  readonly tiers: readonly PinTierAnchor[];          // document order (outer before inner)
}
/** `header`: where the bar's top line sits in the anchors' coordinates (prototype: the header's height, 119; the app's
 *  ScrollView under a fixed header: 0). */
export interface PinViewport { readonly header: number; readonly LH: number; readonly ROW: number }

/**
 * pinUpdate as a function: the lines standing in the bar at `scrollY`. `heightOf` is each line's MEASURED height (the
 * prototype caches it by its words after a render and recomputes until the stack stands still — with the true heights
 * that loop's fixed point is this single pass); before a line has been drawn, pass `pinLineEstimate`.
 */
export function pinChain(anchors: readonly PinAnchor[], scrollY: number, vp: PinViewport, heightOf: (l: PinLine) => number): readonly PinLine[] {
  const y = vp.header, top = (e: { top: number }) => e.top - scrollY;
  const art = anchors.find((a) => top(a) < y && top(a) + a.height > y + vp.LH);
  if (!art || !art.group || !art.tree || !art.groupLine || !art.pin || !(top(art.pin) < y)) return [];
  const g = art.groupLine, said = new Set(g.said ?? []);
  const B = y + heightOf(g);
  // (T481) ONE bar of at most two lines: every tier the scroll is inside, but what the first line says, as one breadcrumb
  const under = (h: number) => art.tiers.filter((t) => t.short != null && t.head && !said.has(t.tid) && t.tier !== "share"
    && top(t.head) < B && t.end != null && t.end - scrollY > B + h + vp.ROW);
  const lineOf = (ts: readonly PinTierAnchor[]): PinLine | null => {
    const crumb = breadcrumb(ts.map((t) => ({ tier: t.tier, n: t.n, short: t.short! })), g.n ?? null);
    return crumb ? tierLine(crumb, ts.every((t) => t.tier === "size") ? "size" : undefined) : null;
  };
  let line = lineOf(under(vp.LH));
  if (line) {
    const h = heightOf(line);
    if (h !== vp.LH) line = lineOf(under(h));          // the measured height of the line it would draw
  }
  return line ? [g, line] : [g];
}

/** What the app draws: the chain itself (kept as the seam T29 calls; the v33 "skip" divergence is gone, C5). */
export const appPinLines = (lines: readonly PinLine[]): readonly PinLine[] => lines;

/** The bold number of a pin line: the first whole number of `s` (pinHtml bolds text's first and sub's first separately). */
export function boldFirst(s: string): { pre: string; n: string; post: string } | null {
  const m = /\b(\d+)\b/.exec(s);
  return m ? { pre: s.slice(0, m.index), n: m[1]!, post: s.slice(m.index + m[1]!.length) } : null;
}

/**
 * mini(): the tab whose section has reached the bar — the LAST of licence / methods / gear whose top is within TAB_REACH
 * of `header`, the licence (or, without one, the methods) also when the page is scrolled to its end. `tops` are content
 * offsets (null: the section is absent); `methodsHeight` 0 counts as absent (m.offsetHeight).
 */
export function tabInView(tops: { gear: number | null; methods: number | null; methodsHeight: number; licence: number | null },
                          scrollY: number, viewportH: number, contentH: number, header = 0): Tab {
  if (tops.gear == null) return "keep";
  const atEnd = scrollY + viewportH >= contentH - 2 && scrollY > 0;
  const reached = (t: number | null) => t != null && t - scrollY <= header + TAB_REACH;
  if (tops.licence != null && (reached(tops.licence) || atEnd)) return "licence";
  if (tops.methods != null && tops.methodsHeight > 0 && (reached(tops.methods) || (tops.licence == null && atEnd))) return "methods";
  return reached(tops.gear) ? "gear" : "keep";
}

/** round 20i scroll margins: an anchored scroll lands with the target just under the pin stack, not beneath it. */
export const anchoredScrollY = (targetTop: number, pinStackHeight: number): number => Math.max(0, targetTop - pinStackHeight - PIN_MARGIN);

/** The tab press (2000): keep → 0; a section → its top + 2 (the prototype's offsetTop - header + 2; app coordinates exclude
 *  the header); a section the page does not have → 0 (the prototype falls back to the Keep section). */
export function tabScrollY(tab: Tab, tops: { gear: number | null; methods: number | null; licence: number | null }): number {
  if (tab === "keep") return 0;
  const t = tops[tab];
  return t == null ? 0 : t + 2;
}
