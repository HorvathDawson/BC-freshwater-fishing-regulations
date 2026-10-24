/** The glossary page: the release's own terms, searched. */
import type { GlossaryTerm } from "../regs/inputs";

export interface GlossaryRowVM { readonly id: string; readonly term: string; readonly says: string }

const fold = (s: string): string => s.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[’']/g, "'");

/**
 * The terms in alphabetical order, narrowed by `query`: a term whose name holds every word of the query comes first,
 * then one whose plain words do (a reader searching "barbless" wants the term, not every definition mentioning it).
 */
export function glossaryRows(terms: readonly GlossaryTerm[], query = ""): GlossaryRowVM[] {
  const byName = [...terms].sort((a, b) => fold(a.term).localeCompare(fold(b.term)) || a.id.localeCompare(b.id));
  const words = fold(query).split(/\s+/).filter(Boolean);
  const row = (t: GlossaryTerm): GlossaryRowVM => ({ id: t.id, term: t.term, says: t.says });
  if (!words.length) return byName.map(row);
  const has = (s: string) => words.every((w) => fold(s).includes(w));
  const named = byName.filter((t) => has(t.term));
  const said = byName.filter((t) => !has(t.term) && has(`${t.term} ${t.says}`));
  return [...named, ...said].map(row);
}
