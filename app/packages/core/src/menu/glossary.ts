/**
 * The glossary page: the release's own terms, A–Z in letter sections, and a typo-tolerant search (T288).
 *
 *   glossarySections  every term under its letter, and the A–Z rail (a letter with no term is dimmed, not dropped)
 *   glossaryRows      the terms narrowed by a query: every word of it must match the term's name or its words —
 *                     exactly (a substring, accents folded) or, failing that, within a typo or two of a word
 *                     ("barbles" → barbless, "steelhed" → steelhead) — name hits first, then the best matches; each
 *                     row carries where it matched (`termMarks`, `saysMarks`), so the page can mark it
 */
import type { GlossaryTerm } from "../regs/inputs";

/** [start, end) in the string it marks. */
export type GlossaryMark = readonly [number, number];

export interface GlossaryRowVM {
  readonly id: string; readonly term: string;
  /** the plain words, or, when a search matched them past the opening, the part around the first match ("… x …") */
  readonly says: string;
  /** the section letter: A–Z, "#" for a term that opens with neither */
  readonly letter: string;
  /** the book's printed pages ("p. 80", "pp. 13, 80"); "" when none */
  readonly cite: string;
  readonly termMarks: readonly GlossaryMark[]; readonly saysMarks: readonly GlossaryMark[];
}

export interface GlossarySection { readonly letter: string; readonly rows: readonly GlossaryRowVM[] }
export interface GlossaryRailLetter { readonly letter: string; readonly has: boolean }

export const GLOSSARY_LETTERS: readonly string[] = [..."ABCDEFGHIJKLMNOPQRSTUVWXYZ"];

/** One character folded (accents off, lower case, curly apostrophe straight) — per character, so a mark found in the
 *  folded text maps back onto the original. */
const foldChar = (c: string): string => c.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[’]/g, "'");
const fold = (s: string): string => [...s].map(foldChar).join("");

/** The folded text and, for each of its characters, the index in the original it came from. */
function folded(s: string): { f: string; at: number[] } {
  let f = ""; const at: number[] = [];
  let i = 0;
  for (const c of s) {
    const x = foldChar(c);
    for (let k = 0; k < x.length; k++) at.push(i);
    f += x; i += c.length;
  }
  at.push(i);
  return { f, at };
}

const letterOf = (term: string): string => {
  const c = fold(term).match(/[a-z0-9]/)?.[0] ?? "";
  return /[a-z]/.test(c) ? c.toUpperCase() : "#";
};

const citeOf = (pages: readonly number[]): string =>
  !pages.length ? "" : pages.length === 1 ? `p. ${pages[0]}` : `pp. ${pages.join(", ")}`;

const byName = (terms: readonly GlossaryTerm[]): GlossaryTerm[] =>
  [...terms].sort((a, b) => fold(a.term).localeCompare(fold(b.term)) || a.id.localeCompare(b.id));

const plainRow = (t: GlossaryTerm): GlossaryRowVM =>
  ({ id: t.id, term: t.term, says: t.says, letter: letterOf(t.term), cite: citeOf(t.pages), termMarks: [], saysMarks: [] });

/** Every term under its letter (A–Z order, "#" last), and the rail: all 26 letters, those with no term dimmed. */
export function glossarySections(terms: readonly GlossaryTerm[]): { sections: GlossarySection[]; rail: GlossaryRailLetter[] } {
  const by = new Map<string, GlossaryRowVM[]>();
  for (const t of byName(terms)) {
    const r = plainRow(t);
    by.set(r.letter, [...(by.get(r.letter) ?? []), r]);
  }
  const order = [...GLOSSARY_LETTERS, "#"];
  const sections = order.filter((l) => by.has(l)).map((letter) => ({ letter, rows: by.get(letter)! }));
  const rail = [...GLOSSARY_LETTERS, ...(by.has("#") ? ["#"] : [])].map((letter) => ({ letter, has: by.has(letter) }));
  return { sections, rail };
}

// ── typo-tolerant matching ──────────────────────────────────────────────────────────────────────────────────────────

/** Edits (insert, delete, substitute, swap two neighbours) between a and b, or `cap + 1` once it is over `cap`. */
export function editDistance(a: string, b: string, cap = 2): number {
  if (Math.abs(a.length - b.length) > cap) return cap + 1;
  const m = a.length, n = b.length;
  let prev2: number[] = [], prev = Array.from({ length: n + 1 }, (_, j) => j);
  for (let i = 1; i <= m; i++) {
    const cur = [i];
    let best = i;
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      let v = Math.min(prev[j]! + 1, cur[j - 1]! + 1, prev[j - 1]! + cost);
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) v = Math.min(v, prev2[j - 2]! + 1);
      cur.push(v);
      if (v < best) best = v;
    }
    if (best > cap) return cap + 1;
    prev2 = prev; prev = cur;
  }
  return prev[n]!;
}

/** The typos a query word of this length may carry in a term's NAME: none under 4 letters, one to 6, two from 7. In
 *  the plain words (`strict`) one at most, from 5 letters, and never in the first letter — a definition holds so many
 *  words that two edits anywhere find one ("barbles" is two from "tables"). */
const allowance = (len: number, strict: boolean): number =>
  strict ? (len < 5 ? 0 : 1) : (len < 4 ? 0 : len < 7 ? 1 : 2);

interface Hit { readonly score: number; readonly marks: GlossaryMark[] }

/** How well one folded query word matches a text: an exact substring (best at a word's start), else the nearest word
 *  — or the nearest word's opening, for a word still being typed — within the allowance. Null: no match. */
function matchWord(q: string, text: string, strict: boolean): Hit | null {
  const { f, at } = folded(text);
  const back = (a: number, b: number): GlossaryMark => [at[a]!, at[b]!];
  let first = -1;
  for (let i = f.indexOf(q); i >= 0; i = f.indexOf(q, i + 1)) {
    if (first < 0) first = i;
    if (i === 0 || !/[a-z0-9]/.test(f[i - 1]!)) {                     // at a word's start: the whole word is marked
      let e = i + q.length;
      while (e < f.length && /[a-z0-9]/.test(f[e]!)) e++;
      return { score: 4, marks: [back(i, e)] };
    }
  }
  if (first >= 0) return { score: 3, marks: [back(first, first + q.length)] };
  const cap = allowance(q.length, strict);
  if (!cap) return null;
  let best: { d: number; a: number; b: number } | null = null;
  for (const m of f.matchAll(/[a-z0-9]+/g)) {
    const w = m[0], a = m.index!;
    if (strict && w[0] !== q[0]) continue;
    for (const len of new Set([w.length, q.length - 1, q.length, q.length + 1])) {
      if (len < 3 || len > w.length) continue;
      const d = editDistance(q, w.slice(0, len), cap);
      if (d <= cap && (!best || d < best.d)) best = { d, a, b: a + w.length };
    }
  }
  return best ? { score: 2 - best.d * 0.5, marks: [back(best.a, best.b)] } : null;
}

/** The part of `says` a reader should see when the match is past its opening: a word or two before the first mark. */
function excerpt(says: string, marks: GlossaryMark[]): { says: string; marks: GlossaryMark[] } {
  const first = marks.length ? Math.min(...marks.map((m) => m[0])) : 0;
  if (first < 120) return { says, marks };
  let from = says.lastIndexOf(" ", first - 30);
  if (from < 0) from = 0; else from += 1;
  const shift = from - 2;                       // "… " takes two characters
  return { says: `… ${says.slice(from)}`, marks: marks.filter((m) => m[0] >= from).map((m) => [m[0] - shift, m[1] - shift]) };
}

const merge = (marks: GlossaryMark[]): GlossaryMark[] => {
  const s = [...marks].sort((a, b) => a[0] - b[0]);
  const out: [number, number][] = [];
  for (const [a, b] of s) {
    const last = out[out.length - 1];
    if (last && a <= last[1]) last[1] = Math.max(last[1], b); else out.push([a, b]);
  }
  return out;
};

/**
 * The terms narrowed by `query` (an empty query: every term, A–Z). Every word of the query must match the term's name
 * or its plain words; a term whose NAME holds every word comes first (a reader searching "barbless" wants the term, not
 * every definition mentioning it), then the closer matches, then A–Z. A typo is forgiven more readily in the name
 * than in the plain words (`allowance`).
 */
export function glossaryRows(terms: readonly GlossaryTerm[], query = ""): GlossaryRowVM[] {
  const sorted = byName(terms);
  const words = [...new Set(fold(query).split(/[^a-z0-9'/-]+/).filter(Boolean))];
  if (!words.length) return sorted.map(plainRow);
  const scored: { row: GlossaryRowVM; named: boolean; score: number; i: number }[] = [];
  sorted.forEach((t, i) => {
    let named = true, score = 0;
    const tm: GlossaryMark[] = [], sm: GlossaryMark[] = [];
    for (const w of words) {
      const inName = matchWord(w, t.term, false);
      const inSays = matchWord(w, t.says, true);
      if (!inName && !inSays) return;
      if (!inName) named = false;
      if (inName) { tm.push(...inName.marks); score += inName.score + 1; }
      if (inSays) { sm.push(...inSays.marks); if (!inName) score += inSays.score; }
    }
    // the plain words cut to the match only when the name did not match (a named hit opens with its own words)
    const ex = named ? { says: t.says, marks: merge(sm) } : excerpt(t.says, merge(sm));
    scored.push({ row: { ...plainRow(t), says: ex.says, termMarks: merge(tm), saysMarks: ex.marks }, named, score, i });
  });
  scored.sort((a, b) => Number(b.named) - Number(a.named) || b.score - a.score || a.i - b.i);
  return scored.map((s) => s.row);
}

/** A string cut at its marks: [text, marked?] runs, in order — what the page draws. */
export function markedRuns(s: string, marks: readonly GlossaryMark[]): [string, boolean][] {
  const out: [string, boolean][] = [];
  let at = 0;
  for (const [a, b] of marks) {
    if (a > at) out.push([s.slice(at, a), false]);
    if (b > a) out.push([s.slice(a, b), true]);
    at = Math.max(at, b);
  }
  if (at < s.length) out.push([s.slice(at), false]);
  return out;
}
