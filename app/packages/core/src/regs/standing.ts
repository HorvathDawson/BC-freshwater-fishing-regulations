/**
 * Three small Keep blocks (T15): the "By another water’s rule" line under the strip, the "Where you can’t fish" list at
 * the foot, and the card-level Possession row.
 *
 * Owner rule 2026-10-21 (decisions.md): every answer here is the PIPELINE's. The inherited lifts are the display frame's
 * `inherited` (the pipeline decided the lift and wrote its sentence and caution); the no-fishing places are the day's
 * stored `shown` standing rules (T07 `settled.standing`) that carry the data's `where` heading and `plain` sentence; the
 * possession rules are the rules the stored rows answers give the role "possession" (`rows_decided.roles`). This file
 * selects them and words them; it never decides a lift, a closure or a limit. The one choice it makes is page v36's
 * wording choice for the possession foot: which possession rule's sentence leads (the one naming no fish, else the one
 * with the most days) and which others are said after it (those with a different number of days) — the set is the store's.
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): page_v36 inheritedHtml, whereHtml, possessionHtml, renderCard (when each is
 * drawn); prototype buildKeep (the .inh line, section.nofish from the first .caveats block, the .posrow from the .foot).
 */
import type { CardModel } from "./card";
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

/** Every rule the stored rows answers of the card's segment give `role` (page v36 possessionHtml's walk: the card's fish in
 *  frame order, hatchery then wild, each answer's roles in stored order; once per rule). */
function rulesOfRole(card: CardModel, inp: RegsInputs, role: string): RuleView[] {
  const f = card.ctx.s >= 0 ? inp.year?.rowsBySeg[card.ctx.s] ?? null : null;
  if (!f) return [];
  const seen = new Set<string>(), p: RuleView[] = [];
  for (const S of Object.keys(card.R)) {
    const a = f.fish[S];
    if (!a) continue;
    for (const d of [a.hatchery, a.wild]) for (const [rix, r0] of d?.roles ?? []) {
      if (r0 !== role) continue;
      const r = inp.rules.get(rix);
      if (!seen.has(r.key)) { seen.add(r.key); p.push(r); }
    }
  }
  return p;
}

/**
 * Every rule the stored rows answers of the card's segment give the role "possession" (page v36 possessionHtml: over the
 * card's fish in frame order, hatchery then wild, each answer's roles in stored order; once per rule), led by the one
 * naming no fish (else the most days), then the others whose number of days differs.
 */
export function possessionRules(card: CardModel, inp: RegsInputs): readonly RuleView[] {
  const p = rulesOfRole(card, inp, "possession");
  if (!p.length) return [];
  const perDaily = (r: RuleView) => r.f.per_daily as number | undefined;
  const species = (r: RuleView) => (r.f.species as readonly string[] | null | undefined) ?? [];
  const main = p.find((r) => !species(r).length) ?? [...p].sort((a, b) => (perDaily(b) ?? NaN) - (perDaily(a) ?? NaN))[0]!;
  return [main, ...p.filter((r) => r !== main && perDaily(r) !== perDaily(main))];
}

/**
 * (T670 K1) A WATER'S OWN POSSESSION LIMIT: every rule the stored rows answers give the role "possession_cap" (Charlie
 * Lake's "Walleye daily and possession quotas = 3", p.65: no more than 3 walleye in possession, where the region's rule
 * would allow 2 days' quota). The role is the store's; this only collects the rules, once each, in the stored order.
 */
export function possessionCaps(card: CardModel, inp: RegsInputs): readonly RuleView[] {
  return rulesOfRole(card, inp, "possession_cap");
}

/** The value words of the possession sentence: the prototype's two foot patterns, kept verbatim but over the data's
 *  sentence alone (no "ⓘ" before it, no "Source" after it). No match → null. */
export function possessionValue(says: string): string | null {
  const m = says.match(/^Possession\s*:\s*([^(.]+?)\s*(?:\(.*?\))?\.?\s*$/) ?? says.match(/^Possession\s*:\s*([^.]+?)\.\s*.*$/);
  return m ? m[1]! : null;
}

/**
 * The card-level Possession row (prototype .posrow from page v36's possession foot): label, the value words of the data's
 * sentence, the whole sentence and the leading rule for its sheet. Only on a drawn card: a keyed, non-tidal day that is
 * not closed. null when no rule has either role, when one has no sentence (page v36 prints "missing" there), or when the
 * sentence does not read as the prototype's patterns expect.
 *
 * (T670 K1) `also`: every sentence after the lead — the fish whose possession differs from the general rule, then the
 * water's own possession limits (`possessionCaps`) — drawn UNDER the row, so "2 days' quota" is never read alone for a
 * fish it does not hold for. `says` is all of them. A water whose only possession rules are its own limits has no
 * value word: the lines say it.
 */
export function possessionOf(card: CardModel, inp: RegsInputs, settled: Settled): PossessionVM | null {
  const st = settled.status?.status;
  if (!st || st === "closed" || st === "tidal") return null;
  const p = possessionRules(card, inp), caps = possessionCaps(card, inp);
  if (!p.length && !caps.length) return null;
  const said = [...p, ...caps].map((r) => r.facts?.plain ?? "");
  if (said.some((t) => !t)) return null;
  const says = said.join(" ");
  const value = p.length ? possessionValue(says) : "";
  if (value === null) return null;
  return { label: WORDS.POSSESSION, value, says, also: said.slice(p.length ? 1 : 0), rule: (p[0] ?? caps[0]!).key, term: "possession_quota", open: "pos:pos" };
}
