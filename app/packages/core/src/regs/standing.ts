/**
 * Three small Keep blocks (T15): the "By another water’s rule" line under the strip, the "Where you can’t fish" list at
 * the foot, and the card-level Possession row.
 *
 * Owner rule 2026-10-21 (decisions.md): every answer here is the PIPELINE's. The inherited lifts are the display frame's
 * `inherited` (the pipeline decided the lift and wrote its sentence and caution); the no-fishing places are the day's
 * stored `shown` standing rules (T07 `settled.standing`) that carry the data's `where` heading and `plain` sentence; the
 * possession row is the rows frame's `page_possession` (T810 F0, rows 34: the lead sentence, its value words and rule,
 * and the lines kept under it, all chosen in Python). This file selects and words them; it never decides a lift, a
 * closure or a limit, and (T810 S) no longer picks which possession rule leads.
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): page_v36 inheritedHtml, whereHtml, possessionHtml, renderCard (when each is
 * drawn); prototype buildKeep (the .inh line, section.nofish from the first .caveats block, the .posrow from the .foot).
 */
import type { RegsInputs, RuleView } from "./inputs";
import { text } from "./line";
import type { InheritedVM, PossessionVM, WhereNotVM } from "./model";
import { clean, lc } from "./names";
import type { Settled } from "./settle";
import { ruleBook, withBook } from "./book";
import { WORDS } from "./words";

export type { InheritedVM, PossessionVM, WhereNotVM } from "./model";

/** The display frame's `inherited` lifts (page v36 inheritedHtml over the tap's segment), each sentence with the lifting
 *  rule's Rule book pages, and the nearer row's caution under it. A tidal frame has none. None → null. */
export function inheritedOf(inp: RegsInputs): InheritedVM | null {
  const d = inp.frames?.display;
  const xs = d && d.status !== "tidal" ? d.inherited ?? [] : [];
  if (!xs.length) return null;
  return {
    label: WORDS.INHERITED_LABEL,
    lines: xs.map((x) => ({ says: withBook([text(x.says)], ruleBook(inp.rules.get(x.by))), caution: x.caution ?? null })),
  };
}

/** page v36 whereHtml's item words: the data's sentence ("No fishing within …" → "No fishing: within …", no final stop);
 *  no sentence → "No fishing: " + the verbatim, lower-cased. */
export function whereWords(r: RuleView): string {
  const p = r.facts?.plain;
  return p ? p.replace(/^No fishing /, WORDS.NO_FISHING_COLON).replace(/\.$/, "")
    : WORDS.NO_FISHING_COLON + lc(clean(r.export.verbatim).replace(/\.$/, ""));
}

/**
 * The day's standing no-fishing places (page v36 whereHtml), grouped by the data's `where` heading. The prototype turns the
 * card's FIRST `.caveats` block into section.nofish, so only the first heading's group is this block (v38: every where
 * rule says "Where you can’t fish"). Drawn on every keyed OPEN day: on a closed day the whole water is closed, so the
 * places inside it are not said (owner 2026-11-04: "the where you cant fish is hidden on closed sections … just dont
 * need to confusing bloat"); a tidal day has none. None → null.
 */
export function whereNotOf(settled: Settled): WhereNotVM | null {
  const st = settled.status?.status;
  if (!st || st === "tidal" || st === "closed") return null;
  const groups = new Map<string, RuleView[]>();
  for (const r of settled.standing) {
    const w = r.facts?.where;
    if (!w) continue;
    const g = groups.get(w);
    if (g) g.push(r); else groups.set(w, [r]);
  }
  const first = groups.entries().next();
  if (first.done) return null;
  const [heading, rs] = first.value;
  return {
    heading: WORDS.WHERE_NOT,
    scope: /^where you can.t fish$/i.test(heading.trim()) ? null : heading,
    items: rs.map((r) => ({ text: withBook([text(whereWords(r))], ruleBook(r)), rule: r.key })),
  };
}

/**
 * The card-level Possession row (prototype .posrow from page v36's possession foot): label, the value words, the whole
 * sentence and the leading rule for its sheet. Only on a drawn card: a keyed, non-tidal day that is not closed.
 *
 * (T810 S) Every part is the rows frame's `page_possession` (F0, contract 2.11), the frame of the card's segment: the
 * lead (its sentence `says`, its value words `value`, its rule `src`) and `also`, the lines kept under it (the fish whose
 * possession differs, then the water's own possession limits; never a trout- or char-only rule: the table's rows say
 * those). null: the frame's `page_possession` is null (nothing left to say). A water whose only possession rules are its
 * own limits has no lead: `value` is "" and the lines say it. `says` is every sentence, lead first.
 */
export function possessionOf(inp: RegsInputs, settled: Settled): PossessionVM | null {
  const st = settled.status?.status;
  if (!st || st === "closed" || st === "tidal") return null;
  const p = settled.s >= 0 ? inp.year?.rowsBySeg[settled.s]?.page_possession ?? null : null;
  if (!p) return null;
  const also = p.also.map((a) => a.says);
  return { label: WORDS.POSSESSION, value: p.lead?.value ?? "", says: [...(p.lead ? [p.lead.says] : []), ...also].join(" "), also,
           rule: inp.rules.get(p.lead?.src ?? p.also[0]!.src).key, term: "possession_quota", open: "pos:pos" };
}
