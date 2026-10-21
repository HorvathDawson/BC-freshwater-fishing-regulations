/**
 * A rule's Rule book pages as Line runs (prototype `bookLinks` / `citeBooks`: one "Rule book p.N ↗" link per page the data
 * names, one space between them). The pages are the store's (`RuleFacts.book` [[printed, pdf]]); nothing is computed.
 *
 * Ports (regs-v11.html @ 7fb6809f, inventory names): bookLinks.
 */
import type { RuleView } from "./inputs";
import { text, type Line, type Run } from "./line";

/** The book runs of a page list, a " " text run between two links. No pages: []. */
export function bookLine(pages: readonly (readonly [number, number])[]): Line {
  const out: Run[] = [];
  pages.forEach(([printed, pdf], i) => {
    if (i) out.push(text(" "));
    out.push({ t: "book", printed, pdf });
  });
  return out;
}

/** A rule's book runs (`facts.book`); a rule with no facts (the noRule stand-in) has none. */
export const ruleBook = (r: RuleView): Line => bookLine(r.facts?.book ?? []);

/** A rule's book runs, or none for no rule (the banner's and group head's "Rule book p.N ↗"). */
export const ruleBookLine = (r: RuleView | null): Line => bookLine(r?.facts?.book ?? []);

/** `[...line, " ", ...book]` — a sentence and its book links as separate elements (contracts.md §1: one space); no book: the line. */
export function withBook(line: Line, book: Line): Line {
  return book.length ? [...line, text(" "), ...book] : line;
}

/** The B.C. Freshwater Fishing Regulations Synopsis the "Rule book p.N ↗" links open (prototype SYNOPSIS_PDF). */
export const SYNOPSIS_PDF = "https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf";
/**
 * A book run's link (decisions D6 a): on web the synopsis at its PDF page (`#page=`); on native (`os` = Platform.OS, any
 * value but "web") the bare PDF URL — a native viewer ignores the fragment and the link's label names the printed page.
 */
export const bookUrl = (pdf: number, os: string = "web"): string => (os === "web" ? `${SYNOPSIS_PDF}#page=${pdf}` : SYNOPSIS_PDF);
