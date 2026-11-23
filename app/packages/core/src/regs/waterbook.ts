/**
 * (T474, owner 2026-11-07: "we should always have a link somewhere that will open the regulation page for the specific
 * waterbody if there is one for it") THE WATER'S OWN RULE-BOOK PAGES. A rule's
 * `facts.book` is its ENTRY's printed pages (`display.decided.book_pages`: the entry's `pages`, with the PDF page); the
 * entries the book prints for THIS water are those whose rules sit at rank 0 (`baseRank`: the water's own level, not the
 * region's general pages such as p.23). Nothing is computed or guessed: the pages are the store's; a water with no entry of
 * its own has no such rule and so no page.
 * (T916, owner 2026-11-19 on Sweltzer River: "Has page 7 link at top (which is wrong)") rank 0 is the ladder's place of ANY
 * rule bound to a water, a B.C. or zone table's line too: the White Sturgeon Conservation Licence (p.7, the licences page)
 * binds the whole Fraser watershed at rank 0. Only a WATER TABLE'S ROW is the water's entry: the display fact `row`
 * (Python `add_sources`, the export's entry kind), read as stored, never re-derived from an id. A release without the fact
 * has no row, and so no page.
 */
import type { BookBox } from "../answers.generated";
import type { RegsInputs } from "./inputs";
import { bookLine } from "./book";
import type { Line } from "./line";

/**
 * (T991 round 3, owner 2026-11-22: "one for each page referenced in the card") The water's own rule-book pages as book
 * runs, each with its rows' highlight boxes (`book_box`, T215), in page order. They are no longer a line of their own
 * ("This water in the rule book p.N ↗", T474/T916): the Regulations tab's Source row (core `regsSourcesOf`) lists them
 * with every other page the page cites, once each — so the water's page is still always linked (T474) and never twice (T60).
 */
export function waterPagesOf(inp: RegsInputs): Line {
  const pages = new Map<number, number>();
  const boxes: BookBox[] = [];
  for (const ix of inp.ruleset?.reach ?? []) {
    const r = inp.rules.get(ix);
    if (r.missing || r.baseRank !== 0 || r.facts?.row !== true) continue;
    for (const [printed, pdf] of r.facts?.book ?? []) pages.set(printed, pdf);
    boxes.push(...(r.facts?.book_box ?? []));
  }
  return bookLine([...pages].sort((a, b) => a[0] - b[0]), boxes).filter((x) => x.t === "book");
}
