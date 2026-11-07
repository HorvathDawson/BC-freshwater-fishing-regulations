/**
 * (T474, owner 2026-11-07: "we should always have a link somewhere that will open the regulation page for the specific
 * waterbody if there is one for it") THE WATER'S OWN RULE-BOOK PAGE: "This water in the rule book p.N ↗". A rule's
 * `facts.book` is its ENTRY's printed pages (`display.decided.book_pages`: the entry's `pages`, with the PDF page); the
 * entries the book prints for THIS water are those whose rules sit at rank 0 (`baseRank`: the water's own level, not the
 * region's general pages such as p.23). Nothing is computed or guessed: the pages are the store's; a water with no entry of
 * its own has no such rule and so no line.
 */
import type { RegsInputs } from "./inputs";
import { text, type Line, type Run } from "./line";
import { WORDS } from "./words";

/** Every printed page a `{t:"book"}` run links anywhere in `v` (a RegsPage, its sheets, any nesting of lines). */
function linkedPages(v: unknown, out: Set<number>, seen = new Set<unknown>()): Set<number> {
  if (!v || typeof v !== "object" || seen.has(v)) return out;
  seen.add(v);
  if (Array.isArray(v)) { for (const x of v) linkedPages(x, out, seen); return out; }
  const o = v as Record<string, unknown>;
  if (o.t === "book" && typeof o.printed === "number") out.add(o.printed);
  for (const k in o) linkedPages(o[k], out, seen);
  return out;
}

/**
 * `page`: the Regulations page built from the same inputs. T60 (an action is offered once on a screen): a page the page's own
 * lines already link ("Rule book p.22 ↗" on a rule) is NOT repeated here. The water's own page is still linked on the screen,
 * only by that rule's link instead of by this line; when every page is already linked elsewhere this line is empty.
 */
export function waterBookOf(inp: RegsInputs, page?: unknown): Line {
  const pages = new Map<number, number>();
  for (const ix of inp.ruleset?.reach ?? []) {
    const r = inp.rules.get(ix);
    if (r.missing || r.baseRank !== 0) continue;
    for (const [printed, pdf] of r.facts?.book ?? []) pages.set(printed, pdf);
  }
  if (page) for (const printed of linkedPages(page, new Set())) pages.delete(printed);
  if (!pages.size) return [];
  const runs: Run[] = [text(WORDS.WATER_BOOK)];
  [...pages].sort((a, b) => a[0] - b[0]).forEach(([printed, pdf]) => runs.push(text(" "), { t: "book", printed, pdf, bare: true }));
  return runs;
}
