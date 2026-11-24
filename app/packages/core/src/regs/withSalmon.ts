/**
 * (T1052, owner 2026-11-24 ~12:33: "One block, both calendars (Recommended)") A WATER CLOSED TO ALL FISHING, SALMON
 * INCLUDED, SAYS IT ONCE. Nelson Creek (book p.25, "No Fishing") drew the same closure twice: the Salmon row of the
 * species list ("Closed to all fishing by the B.C. Synopsis (p.25): No Fishing (all year)", the salmon card's `book`) and
 * the game-fish "Closed all year" banner ("No fishing for any game fish. This water"). The two calendars stay; the two
 * statements become the banner alone: "Closed to all fishing all year, salmon included", the book row's line and its
 * source. The Salmon row is not drawn (`BannerVM.salmon`).
 *
 * No rule is read here: both halves are the pipeline's answers, and they are joined only where THE DATA says they are
 * the same closure — every salmon species on every chain of the page answers today by the rule book's closures alone
 * (`SalmonCardVM.todayBy` "book": the book places the pipeline put first in the chains, T796 A5), the rule keys
 * (`entry::rule`) of those closures (`SalmonCardVM.book`) are exactly the ones the banner's chain cites, each closing the
 * salmon every day of its own year (`bookAllYear`, the release's `static` year), the game fish closed every day too (the
 * banner has no reopening), and none is a closure another authority may lift (`facts.unless_opened`: a National Park's
 * "unless opened under the National Parks Fishing Regulations" is not "closed to all fishing all year"). Anything else
 * keeps today's two statements: a closure for part of the year, salmon closed by DFO (or by another rule), a DFO notice,
 * a DFO finfish closure or an undrawn DFO place on the card, a merged "closed all year on N stretches" banner, a page
 * with no salmon card.
 */
import type { SalmonCardVM } from "../salmon";
import type { RuleIndex, RuleView } from "./inputs";
import type { RegsPage } from "./model";
import { clean } from "./names";
import { WORDS } from "./words";

/** A closure the banner cites that the part's rules do not hold: the page and its rules disagree (a programming error). */
export class OneClosureError extends Error {
  override readonly name = "OneClosureError";
}

/**
 * The page as THE ONE all-closed statement leaves it (see the file head), or the page as it is. `card`: the page's
 * salmon card (null: none); `rules`: the part's rules (`RegsInputs.rules`), whose book row and line the statement names.
 */
export function closedWithSalmon(page: RegsPage, card: SalmonCardVM | null, rules: RuleIndex): RegsPage {
  const b = page.keep.banner;
  if (!card || page.status !== "closed" || !b || b.kind !== "closed" || b.reopen !== null || b.merged !== null || !b.chain.length) return page;
  if (card.todayBy !== "book" || !card.closedOnly || card.issues.length || card.finfish.length || card.caveat || card.side) return page;
  const salmon = new Set(card.book.map((x) => x.rule));
  const game = new Set(b.chain.map((c) => c.rule as string));
  const allYear = new Set(card.bookAllYear ?? []);
  if (!salmon.size || salmon.size !== game.size || [...game].some((k) => !salmon.has(k) || !allYear.has(k))) return page;
  const views = b.chain.map((c): RuleView => {
    const r = rules.byKey?.(c.rule) ?? null;
    if (!r) throw new OneClosureError(`closedWithSalmon: the banner's closure ${c.rule} is not one of the part's rules`);
    return r;
  });
  if (views.some((r) => r.facts?.unless_opened)) return page;
  const chain = b.chain.map((c, i) => {
    const r = views[i]!, row = r.export.prov.entry_name, line = clean(r.export.verbatim);   // as `describe` reads it
    if (!row || !line) throw new OneClosureError(`closedWithSalmon: closure ${c.rule} has no book row name or line`);
    return { ...c, text: r.via === "trib" ? WORDS.BOOK_ROW_TRIBS(row, line) : WORDS.BOOK_ROW_READS(row, line), scope: "" };
  });
  return { ...page, keep: { ...page.keep, banner: { ...b, big: [WORDS.CLOSED_ALL_FISHING_YEAR], paras: [], chain, salmon: true } } };
}
