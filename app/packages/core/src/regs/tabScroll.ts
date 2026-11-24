/**
 * The tab row's two pure functions: the tab in view at a scroll offset (prototype regs v38 `mini`) and where a tab press
 * scrolls (the tab press, 2000). (T810 cutover, owner 2026-11-17 23:29 "Remove it (Recommended)") The pin bar these sat
 * beside (pins.ts: pinChain, the group line and breadcrumb) is gone: no Keep tab draws a group card since the cutover.
 */
import type { Tab } from "./model";

/** mini(): a section counts as reached this many px below the top */
export const TAB_REACH = 12;

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

/** The tab press (2000): keep → 0; a section → its top + 2 (the prototype's offsetTop - header + 2; app coordinates exclude
 *  the header); a section the page does not have → 0 (the prototype falls back to the Keep section). */
export function tabScrollY(tab: Tab, tops: { gear: number | null; methods: number | null; licence: number | null }): number {
  if (tab === "keep") return 0;
  const t = tops[tab];
  return t == null ? 0 : t + 2;
}

/** (T1049 review) a scroll event within this many px of a y the start asked for is that jump's own (platform rounding) */
export const START_OWN_SLACK = 1;

/**
 * (T1049 review) Is a scroll event at `y` the start's own jump, rather than the reader's? By POSITION, never by time: on a
 * slow phone the event of a programmatic jump can arrive long after it was made. `asked` is every y the start has asked the
 * page for (a late event of an earlier jump still counts); `maxY` is the page's last scrollable y (content - view), null when
 * the event did not say. A jump asked beyond the page's end lands on `maxY`: that is its own too. No jump asked yet: only
 * the page settling at the top is not the reader.
 */
export function isStartOwnScroll(y: number, asked: readonly number[], maxY: number | null): boolean {
  if (asked.length === 0) return y <= 0;
  const near = (a: number, b: number) => Math.abs(a - b) <= START_OWN_SLACK;
  return asked.some((a) => near(y, a) || (maxY != null && a > maxY && near(y, maxY)));
}
