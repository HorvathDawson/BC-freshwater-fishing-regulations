/**
 * Rich text — `Line` (contracts.md §1). Every visible sentence of the Regulations page is a Line: runs of words, a
 * number set big or inline, a glossary ⓘ, a Rule book link, a link to a sheet / a day / a tab. Components draw the
 * runs; they never compose one.
 *
 * Ports (inventory.json names, regs v38 @ 7fb6809f): prototype `buildKeep.nbsp`, `buildKeep.boldN`; `plain`/`plainAll`
 * are the golden text form of port/golden.mjs `gap()`/`lines()` (§5).
 */
import type { SheetKey } from "./sheetKey";
import type { Day, Tab } from "./model";
import { WORDS } from "./words";

/** `${entry_id}::${rule_id}` — the export rule's own ids (store `rule.j`). */
export type RuleKey = string;

export type Run =
  | { t: "text"; s: string }             // data words or a words.ts constant; "50 cm" already carries U+00A0 (nbsp())
  | { t: "num"; s: string; size: "big" | "big2" | "inline" }  // "big": tier/head number (TYPE.answer); "big2": the size box's second number
  | { t: "term"; ids: readonly string[] }                    // ⓘ — one id: "What does <title> mean?"; 2+: "What these words mean"
  | { t: "book"; printed: number; pdf: number | null; bare?: true }   // "Rule book p.<printed> ↗"; bare: "p.<printed> ↗"; pdf null → "p.<printed>" unlinked
  | { t: "link"; s: string; open: SheetKey | { day: Day } | { tab: Tab } };  // "How to tell ›", "Source ›", "See the rules from then ›"; {tab}: "Steelhead stamp ›"

export type Line = readonly Run[];

export function text(s: string): Run {
  return { t: "text", s };
}

/** `s.replace(/\s+/g, " ").trim()` — note `\s` includes U+00A0, so a nbsp'd unit compares as a plain space. */
export function squash(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

/** The words one run reads as, as the golden records them. */
function runText(r: Run): string {
  switch (r.t) {
    case "text": case "num": case "link": return r.s;
    case "term": return r.ids.length >= 2 ? WORDS.TERMS_MANY : WORDS.TERM_ONE;
    case "book": return bookText(r);
  }
}

/**
 * (T43) A book run's label — THE one spelling (the golden's, RuleBookLink's, LineText's): "Rule book p.21 ↗"; bare
 * "p.21 ↗" (after a "Rule book" label); a page the PDF map lacks (pdf null) "p.21", unlinked. The printed page is the
 * data's; the PDF page is never computed from it (the repo synopsis is not byte-identical to the Province's).
 */
export function bookText(r: { readonly printed: number; readonly pdf: number | null; readonly bare?: boolean }): string {
  if (r.pdf === null) return `${WORDS.PAGE}${r.printed}`;
  const link = `${WORDS.PAGE}${r.printed} ${WORDS.LINK_OUT}`;
  return r.bare ? link : `${WORDS.BOOK} ${link}`;
}

/** The golden comparison form of one Line: its runs concatenated AS IS (no space is invented), then collapse + trim. */
export function plain(l: Line | null | undefined): string {
  return l ? squash(l.map(runText).join("")) : "";
}

/** Separate Lines / fields the page draws as separate elements: ONE space between them (golden `gap()`), then collapse. */
export function plainAll(ls: readonly (Line | null | undefined)[]): string {
  return squash(ls.map(plain).join(" "));
}

/** A number keeps its unit: `/(\d) (cm|in)\b/g` → "$1 $2" (prototype buildKeep.nbsp). */
export function nbsp(s: string): string {
  return s.replace(/(\d) (cm|in)\b/g, "$1 $2");
}

/**
 * The sentence with its first whole-word `n` as a num run (prototype buildKeep.boldN: nbsp first, then the first
 * `\b<n>\b`). Null n, or an n the sentence does not say, is one text run. No empty text run at either end.
 */
export function boldN(s: string, n: number | null, size: "big" | "inline" = "inline"): Line {
  const t = nbsp(s);
  const m = n === null ? null : new RegExp(`\\b${n}\\b`).exec(t);
  if (!m) return [text(t)];
  const out: Run[] = [];
  if (m.index > 0) out.push(text(t.slice(0, m.index)));
  out.push({ t: "num", s: m[0], size });
  const rest = t.slice(m.index + m[0].length);
  if (rest) out.push(text(rest));
  return out;
}
