/**
 * T28 — the pin bar under the header, as pure functions (no React; T29 measures and draws).
 *
 * Ports prototype regs v38 @ 7fb6809f (regs-v11.html): `pinUpdate` 1728–1751 (round UR6: a tier's line stays until its
 * section's LAST row has gone up under the stack, whose height is MEASURED, not guessed), `mini` 1752–1760, the tab press
 * 2000 and the round 20i scroll margins (872). The v33 rule the ticket text describes (`nextHead`, DROP 110) is gone from
 * the prototype; port/pins.mjs records exactly what v38 reads (`end` per tier, each drawn line's height) and
 * conformance/src/regs/pins.parity.test.ts replays every golden state through `pinChain`.
 *
 * IMPLEMENT-HANDOFF C5 (UI-NOTES v35 integration 1): a ledger article whose row has no group pins NOTHING — the prototype no
 * longer throws there, so there is no "keep the previous lines" state to reproduce.
 *
 * Nothing here decides a regulation: every word on a line is the store's (shortOf over the stored tree, the stored headline);
 * this only chooses which lines stand in the bar at a scroll offset.
 */
import { shortOf, type ShortTier } from "./grouphead";
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

export interface PinLine { readonly cls: "g" | "g two" | "c"; readonly text: string; readonly sub: string | null }

/** The group line (pinUpdate 1735): form a with a headline → the share leads, the total in brackets under it; else
 *  "<name> · <shortOf(root)>". */
export function groupLine(name: string, root: ShortTier, headline: { n: number | null; says: string } | null, formA: boolean): PinLine {
  if (formA && headline) {
    // the total's "N a day across X" read as "N across X" inside the brackets (the prototype's own rewrite of shortOf's words)
    return { cls: "g two", text: `${name}${WORDS.DOT_SEP}${shortOf({ tier: "share", n: headline.n, says: headline.says })}`,
      sub: WORDS.PIN_OF_TOTAL(shortOf(root).replace(/ a day /, " ")) };
  }
  return { cls: "g", text: `${name}${WORDS.DOT_SEP}${shortOf(root)}`, sub: null };
}

/** A tier's pinned line: "↳ " + its short line (TierVM.short). */
export const tierLine = (short: string): PinLine => ({ cls: "c", text: `${WORDS.SHARE_ARROW}${short}`, sub: null });

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
  const lines: PinLine[] = [art.groupLine];
  let B = y + heightOf(art.groupLine);
  for (const t of art.tiers) {
    if (t.short == null || !t.head) continue;
    if (art.formA && t.tier === "share" && t.n === art.headlineN) continue;   // form a: the group line already says the share
    const line = tierLine(t.short), h = heightOf(line);
    if (top(t.head) < B && t.end != null && t.end - scrollY > B + h + vp.ROW) { lines.push(line); B += h; }
  }
  return lines;
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
