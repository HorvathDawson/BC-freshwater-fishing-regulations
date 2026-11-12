/**
 * The Regulations page's header (T16): the water's name, "Today"/weekday + the date, the date pill's words and status
 * dot, the part's two label candidates (the component shows the full one when it fits two lines — prototype fitLabel is
 * layout, T23) and the four tab labels. No action: the header offers neither Map (T71) nor Parts (T150) — the water's
 * Map view is the one place to see and pick its parts.
 *
 * Words only: the names and labels are the store's (item, part facts, picker choices); the dot is the day's status as the
 * strip already read it (T13). Nothing here decides a regulation.
 *
 * Ports (inventory names @ 7fb6809f): prototype hdr, statusOf (the dot), fullPart, hereLabel, shortLabel, placeRow,
 * placeGo; page_v36 renderAll's subtitle (`[data-f="sub"]`), which hereLabel tests the long label against.
 */
import { cutWords } from "./describe";
import { spanName } from "../ends";
import type { RegsInputs } from "./inputs";
import type { Day, HeaderVM, Tab } from "./model";
import { fmtDay } from "./strip";
import { WD3, WORDS } from "./words";
import { WEEKDAYS } from "../calendar.generated";
import { sameDay } from "../day";

/** (T517) Each part's name as the water map names the entry holding it, by part index: `@app/core` `entryNames` over
 *  the SAME `mergeNav(group, live salmon key)` the map draws (`@app/ui` `useMapEntries`), so the header, the Parts sheet
 *  and the map say one name. */
export type PartNames = ReadonlyMap<number, string>;

/** (T517) THE NAME OF PART `partIx` on the page: the map's name for the entry holding it (`names`, which holds every
 *  listed part: one it lacks throws). With NO names — a deliberate rule, pinned in header.test.ts: the water's group not
 *  read yet, or the SEO pages, which read no group — the part's own, composed by the same one function (`spanName` over
 *  its end tokens and the release's `display.ends`), which is what the map names it before a salmon reading joins any
 *  part. A part with no facts (unkeyed: outside B.C.; a tidal piece, never on the map) is said as its water — the
 *  `spanName` rule for a part with no ends. A page with no named water has no part name: throws. */
export function partNameOf(inp: RegsInputs, partIx: number, names: PartNames | null | undefined): string {
  const it = inp.item;
  if (!it) throw new Error(`regs: a part name asked of a page with no named water (part ${partIx})`);
  const p = it.parts[partIx];
  if (p === undefined) throw new Error(`regs: ${it.id} has no part ${partIx}`);
  if (p === null) return it.name;
  if (names) {
    const own = names.get(partIx);
    if (own === undefined) throw new Error(`regs: the map names no part ${partIx} of ${it.id}`);
    return own;
  }
  return spanName(inp.statics.ends, it.name, p);
}
const SUB_MAX = 150;
const nParts = (inp: RegsInputs) => inp.item?.parts.filter(Boolean).length ?? 0;
const choiceOf = (inp: RegsInputs) => inp.item?.picker?.choices.find((c) => c.parts.includes(inp.partIx)) ?? null;

/** page v36's subtitle: "<kind> · N section(s)", then " · closed all year" for a closed choice of stretches, else
 *  " · <place> · <runs or label>" (cut at 150) — only for a water of more than one part. */
export function subtitleOf(inp: RegsInputs): string {
  const it = inp.item;
  if (!it) return "";
  const many = nParts(inp) > 1, g = choiceOf(inp), dp = it.parts[inp.partIx] ?? null;
  const own = it.partSections[inp.partIx];
  if (own === undefined) throw new Error(`regs header: ${it.id} has no part ${inp.partIx} (partSections)`);
  const n = many && g ? g.sections : own;
  let tail = "";
  if (many) {
    if (g && g.closed && g.parts.length > 1) tail = WORDS.DOT + WORDS.SUB_CLOSED_ALL_YEAR;
    else if (dp) tail = WORDS.DOT + (dp.place ? dp.place + WORDS.DOT : "") + cutWords(dp.runs || dp.label || "", SUB_MAX);
  }
  return `${it.kind}${WORDS.DOT}${n} ${n > 1 ? WORDS.SECTIONS : WORDS.SECTION}${tail}`;
}

/**
 * prototype hereLabel (`full`) and shortLabel (`short`). With a part picker: the choice's text is the long label; the full
 * label is the long one when the subtitle says it, else the name of the choice's first part; the short label is that
 * name. Without a picker both are the part's name (`partNameOf`, the one name — T517: no stored label stands in;
 * `headerOf` empties the slot when it is the water's own name). Null for an unnamed section.
 */
export function partLabels(inp: RegsInputs, names?: PartNames | null): { full: string; short: string } | null {
  const it = inp.item;
  if (!it) return null;
  const g = nParts(inp) > 1 ? choiceOf(inp) : null;
  if (g) {
    const long = g.text, short = partNameOf(inp, g.parts[0]!, names);
    return { full: subtitleOf(inp).includes(long) ? long : short, short };
  }
  const s = partNameOf(inp, inp.partIx, names);
  return { full: s, short: s };
}


/**
 * (T53) An unnamed section's label slot: the named water it flows into (`downstream`, the first named water down its flow
 * pointers — the app asks the bundle; no regulation rides on it), worded by the section's own kind: an unnamed stream is
 * that water's tributary, anything else flows into it. Null for a named water (its slot is its part's) or nothing known.
 */
export function downstreamLabel(inp: RegsInputs, downstream: string | null | undefined): string | null {
  if (inp.item || !downstream) return null;
  return inp.key?.kind === "stream" ? WORDS.TRIBUTARY_OF(downstream) : WORDS.FLOWS_INTO(downstream);
}

/** The header of the tapped part on `inp.day`. `status` is the day's as the strip read it (T13; null strip → tidal or
 *  outside); `downstream` (T53, an unnamed section only) the named water it flows into.
 *
 *  THE HEADER HAS NO ACTION (owner call 3: T71 dropped its Map button, 2026-10-22 — "Keep only one Map entry (the view
 *  strip; drop the header's Map button)"; T150 its Parts button, 2026-10-23 — "dont we want to remove 'parts' from reg
 *  page since map is now at the top tabs?"). The water's Map view, offered once by the view strip, lists the parts by
 *  name and picks one; the header keeps the part's NAME (`place`), so the reader always knows whose rules are shown. */
export function headerOf(inp: RegsInputs, o: { today: Day; status: "open" | "closed" | "tidal" | "outside" | "unknown";
                                               downstream?: string | null;
                                               /** (T517) the map's names of the parts (`PartNames`) */
                                               names?: PartNames | null }): HeaderVM {
  // a named water's name, or an unnamed reach's (its place); a handle OUTSIDE B.C. has neither (T09: no key, no place,
  // no item) and the header says no name — a documented rule, pinned by edge.test.ts
  const name = inp.item ? inp.item.name : inp.place ? inp.place.name : "";
  const date = fmtDay(inp.day);
  const down = downstreamLabel(inp, o.downstream);
  const lab = down ? { full: down, short: down } : partLabels(inp, o.names);
  // prototype placeRow: no label slot when the short label is the water's own name
  const lbl = lab && lab.short !== name ? lab.short : "";
  const tabs: readonly [Tab, string][] = [["keep", WORDS.TAB_KEEP], ["gear", WORDS.TAB_GEAR], ["methods", WORDS.TAB_METHODS], ["licence", WORDS.TAB_LICENCE]];
  const dayWord = sameDay(inp.day, o.today) ? WORDS.TODAY : WD3[WEEKDAYS.indexOf(inp.weekday)]!;
  return {
    name,
    dayWord,
    date, dateAria: WORDS.DATE_ARIA(dayWord, date),
    dot: o.status === "closed" ? "closed" : o.status === "open" ? "open" : "faint",
    place: lbl ? { full: lab!.full || lbl, short: lbl } : null,
    // (T42) outside B.C. the page has no Gear, Methods or Licence section (no key): only the Keep tab is listed — as on
    // a key-less tidal piece (T672: `inp.tidalPart`, no frames either);
    // (owner 2026-11-04) on a closed day no Licence section (`buildLicence`), so no Licence tab
    tabs: (o.status === "outside" || inp.tidalPart ? tabs.slice(0, 1) : o.status === "closed" ? tabs.filter(([t]) => t !== "licence") : tabs)
      .map(([tab, label]) => ({ tab, label })),
  };
}
