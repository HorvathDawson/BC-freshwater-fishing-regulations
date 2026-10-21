/**
 * The Regulations page's header (T16): the water's name, "Today"/weekday + the date, the date pill's words and status
 * dot, the part's two label candidates (the component shows the full one when it fits two lines — prototype fitLabel is
 * layout, T23), the Map / Parts action and the four tab labels.
 *
 * Words only: the names and labels are the store's (item, part facts, picker choices); the dot is the day's status as the
 * strip already read it (T13). Nothing here decides a regulation.
 *
 * Ports (inventory names @ 7fb6809f): prototype hdr, statusOf (the dot), fullPart, hereLabel, shortLabel, placeRow,
 * placeGo; page_v36 renderAll's subtitle (`[data-f="sub"]`), which hereLabel tests the long label against.
 */
import { cutWords } from "./describe";
import type { RegsInputs } from "./inputs";
import type { Day, HeaderVM, Tab } from "./model";
import { fmtDay } from "./strip";
import { WD3, WORDS } from "./words";
import { WEEKDAYS } from "../calendar.generated";

/** The data cut a label with "…" (a match key, never said). */
const CUT = "…";
const uncut = (s: string | null | undefined): string => (s && !s.includes(CUT) ? s : "");
const SUB_MAX = 150;
const nParts = (inp: RegsInputs) => inp.item?.parts.filter(Boolean).length ?? 0;
const choiceOf = (inp: RegsInputs) => inp.item?.picker?.choices.find((c) => c.parts.includes(inp.partIx)) ?? null;

/** page v36's subtitle: "<kind> · N section(s)", then " · closed all year" for a closed choice of stretches, else
 *  " · <place> · <runs or label>" (cut at 150) — only for a water of more than one part. */
export function subtitleOf(inp: RegsInputs): string {
  const it = inp.item;
  if (!it) return "";
  const many = nParts(inp) > 1, g = choiceOf(inp), dp = it.parts[inp.partIx] ?? null;
  const n = many && g ? g.sections : (inp.item?.partSections[inp.partIx] ?? it.sections);
  let tail = "";
  if (many) {
    if (g && g.closed && g.parts.length > 1) tail = WORDS.DOT + WORDS.SUB_CLOSED_ALL_YEAR;
    else if (dp) tail = WORDS.DOT + (dp.place ? dp.place + WORDS.DOT : "") + cutWords(dp.runs || dp.label || "", SUB_MAX);
  }
  return `${it.kind}${WORDS.DOT}${n} ${n > 1 ? WORDS.SECTIONS : WORDS.SECTION}${tail}`;
}

/**
 * prototype hereLabel (`full`) and shortLabel (`short`). With a part picker: the choice's text is the long label; the full
 * label is the long one when the subtitle says it, else the choice's first part's short label unless the data cut it;
 * the short label is that short one, uncut, else the long. Without a picker the label is the part's short label, uncut,
 * else its label (T16: in the app every named part has one — D1 — so the prototype's "no water map, no label" does not
 * carry over; `headerOf` empties the slot only when the label is the water's own name). Null for an unnamed section or
 * a water with no part facts.
 */
export function partLabels(inp: RegsInputs): { full: string; short: string } | null {
  const it = inp.item;
  if (!it) return null;
  const g = nParts(inp) > 1 ? choiceOf(inp) : null;
  if (g) {
    const long = g.text, short = uncut(it.parts[g.parts[0]!]?.short);
    return { full: subtitleOf(inp).includes(long) ? long : short || long, short: short || long };
  }
  const p = it.parts[inp.partIx];
  if (!p) return null;
  const s = uncut(p.short) || p.label;
  return { full: s, short: s };
}

const sameDay = (a: Day, b: Day) => a.month === b.month && a.day === b.day;

/** The header of the tapped part on `inp.day`. `status` is the day's as the strip read it (T13; null strip → tidal or
 *  outside); `canMap` whether the map can frame the part (D1). */
export function headerOf(inp: RegsInputs, o: { today: Day; status: "open" | "closed" | "tidal" | "outside" | "unknown"; canMap: boolean }): HeaderVM {
  const name = inp.item?.name ?? inp.place?.name ?? "";
  const date = fmtDay(inp.day);
  const lab = partLabels(inp);
  // prototype placeRow: no label slot when the short label is the water's own name; the action's words keep the full one
  const lbl = lab && lab.short !== name ? lab.short : "";
  const full = lab?.full || lbl;
  const many = (inp.item?.picker?.choices.length ?? 0) > 1;
  const action: HeaderVM["action"] = !inp.item ? null
    : o.canMap ? { kind: "map", label: WORDS.MAP, aria: WORDS.MAP_ARIA(full) }
    : many ? { kind: "parts", label: WORDS.PARTS, aria: WORDS.PARTS_ARIA(full) } : null;
  const tabs: readonly [Tab, string][] = [["keep", WORDS.TAB_KEEP], ["gear", WORDS.TAB_GEAR], ["methods", WORDS.TAB_METHODS], ["licence", WORDS.TAB_LICENCE]];
  return {
    name,
    dayWord: sameDay(inp.day, o.today) ? WORDS.TODAY : WD3[WEEKDAYS.indexOf(inp.weekday)]!,
    date, dateAria: WORDS.DATE_ARIA(date),
    dot: o.status === "closed" ? "closed" : o.status === "open" ? "open" : "faint",
    place: lbl ? { full: lab!.full || lbl, short: lbl } : null,
    action,
    tabs: tabs.map(([tab, label]) => ({ tab, label })),
  };
}
