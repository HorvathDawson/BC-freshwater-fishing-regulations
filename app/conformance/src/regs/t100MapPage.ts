/**
 * (T100) THE MAP'S PAGE, held against the golden by ONE rewrite.
 *
 * T100 (tickets/T100-lake-internal-closure-areas.md, release r-8b761969206f): a rule bound to the whole water that holds
 * only in an undrawn part the book draws on a map cites that map's page too — pipeline display.py `book_pages` appends
 * `not_yet_mapped.map.page` to the entry's printed pages, and the rule's `applies` sentence ends "… see map B on p.28 of
 * the regulations". The app draws one "Rule book p.N ↗" link per stored page (book.ts `bookLine`), the house format the
 * golden already prints ("Rule book p.8 ↗ Rule book p.22 ↗"). Page v36 (the golden) predates T100, so Shuswap Lake's
 * map-cited rows end "Rule book p.32 ↗" where the app now draws "Rule book p.32 ↗ Rule book p.28 ↗".
 *
 * `mapPages` decides which of a rule's stored pages are T100's from the STORE alone: a page after the first whose printed
 * number the rule's own `applies` sentence cites as a map ("map X on p.N of the regulations"). A rule with none is not
 * touched. `t100Ruled` then holds the golden to it, once per test file when the paired store is open (golden.ts
 * `parityGate`), for every map-cited rule of every compared state's rule set, where the golden prints that rule's book as
 * the store's pages LESS the map's (the golden's book vs the store's):
 *   - its rule sheet (`rule:<key>`, per state or the shared "@" one): `pages` gains the map page, the sheet's book line
 *     gains its link, and its why-it-applies line (the golden's `applies`) becomes the store's — the same sentence, ending
 *     in T100's area caveat ("… Not yet mapped: … see map A on p.28 of the regulations. …"), which cites the map;
 *   - each Keep tab line of the rule (`keep.lines`: its caveat row, found by the rule's own undrawn part as the caveat
 *     prints it, "Where: <part>", and ending in the golden's links) gains the map page's link.
 * Nothing else in the golden moves; `T100_REWRITES` counts what it did (pinned exactly in t100MapPage.test.ts — a store that
 * stops adding the page rewrites nothing and the pin fails), and a map-cited rule whose rule sheet prints any other book
 * throws.
 */
import type { RegsInputs, RuleView } from "@app/core";
import { excluded } from "./exclusions";
import { golden } from "./oracles";

type Book = readonly (readonly [number, number])[];
type GPage = { text: string; pdf: number | null };
type GSheet = { pages: GPage[]; lines: string[] };

/** The house spelling of one book link (line.ts `bookText`, the golden's). */
export const bookLink = (printed: number): string => `Rule book p.${printed} ↗`;
const links = (b: Book): string => b.map(([p]) => bookLink(p)).join(" ");

/** The pages of a stored book that T100 added: after the first, each cited by the rule's `applies` as a map's page — the
 *  area caveat's "see map B on p.28 of the regulations" (a lettered map), or "see the map(s) on p.28 …" (the entry's). */
export function mapPages(book: Book, applies: string): Book {
  return book.slice(1).filter(([p]) => new RegExp(`\\bsee (?:map [A-Z]\\w*|the maps?) on p\\.${p} of the regulations\\b`).test(applies));
}
/** T100's area caveat, which the store's `applies` ends with ("Not yet mapped: …"; display.py `applies_here`). */
const CAVEAT = " Not yet mapped: ";

/** The undrawn part as the in-part caveat prints it (caveats.ts `inPartOf` where: "(map X)" → "(map X in the regulations)"). */
const whereOf = (part: string): string =>
  `Where: ${part[0]!.toUpperCase()}${part.slice(1).replace(/\(map ([A-Z])\)/, "(map $1 in the regulations)")}`;

/** golden state id → what `t100Ruled` rewrote there: Keep tab lines, and rule sheets (a shared "@" sheet counts once, at
 *  the first state that opens it) */
export const T100_REWRITES: Record<string, { keep: number; sheets: number; applies: number }> = {};
/** every map-cited rule the compared states' rule sets reach (store key → its map pages, printed) */
export const T100_RULES: Record<string, number[]> = {};

let RULED: Promise<void> | undefined;
/** Apply the T100 map page to the golden of every state, once (see the header). */
export function t100Ruled(inputsOf: (id: string) => Promise<RegsInputs | null>): Promise<void> {
  return (RULED ??= (async () => {
    const G = golden(), shared = G.sheets as Record<string, GSheet>, done = new Set<string>();
    for (const id of Object.keys(G.states)) {
      const noKeep = !!excluded("keep", id), noSheets = !!excluded("sheets", id);
      if (noKeep && noSheets) continue;   // a whole-state exclusion (T115: a part gone) is never tapped
      const inp = await inputsOf(id);
      if (!inp?.ruleset) continue;
      const st = G.states[id]!, sheets = (noSheets ? {} : st.sheets) as Record<string, GSheet | "@">;
      const keep = noKeep ? null : (st.keep as { lines: string[] } | null);
      const n = { keep: 0, sheets: 0, applies: 0 };
      const rixes = [...new Set([...inp.ruleset.reach, ...inp.ruleset.trib])];
      for (const r of rixes.map((x) => inp.rules.get(x)) as RuleView[]) {
        const book = r.facts?.book ?? [], maps = mapPages(book, r.facts?.applies ?? "");
        if (!maps.length) continue;
        T100_RULES[r.key] = maps.map(([p]) => p);
        const old = book.filter((b) => !maps.includes(b)), was = links(old), now = links(book);
        const sk = `rule:${r.key}`, own = sheets[sk];
        if (own !== undefined && !(own === "@" && done.has(sk))) {
          const s = own === "@" ? shared[sk]! : own;
          const pages = s.pages.map((p) => p.text).join(" ");
          if (pages !== was) throw new Error(`T100: ${id} ${sk} prints "${pages}"; the store's book less its map is "${was}"`);
          const at = s.lines.lastIndexOf(was);
          if (at < 0) throw new Error(`T100: ${id} ${sk}: no "${was}" book line`);
          s.lines[at] = now;
          const applies = r.facts!.applies, cut = applies.indexOf(CAVEAT), ai = cut < 0 ? -1 : s.lines.indexOf(applies.slice(0, cut));
          if (ai >= 0) { s.lines[ai] = applies; n.applies++; }
          s.pages.push(...maps.map(([p, pdf]) => ({ text: bookLink(p), pdf })));
          if (own === "@") done.add(sk);
          n.sheets++;
        }
        if (keep && r.part) {
          const where = whereOf(r.part);
          keep.lines.forEach((l, i) => {
            if (l.includes(` ${where} `) && l.endsWith(` ${was}`)) { keep.lines[i] = `${l} ${links(maps)}`; n.keep++; }
          });
        }
      }
      if (n.keep || n.sheets || n.applies) T100_REWRITES[id] = n;
    }
  })());
}
