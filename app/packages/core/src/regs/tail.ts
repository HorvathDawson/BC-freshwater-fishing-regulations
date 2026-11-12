/**
 * T19 — a group's tail: each not-kept line (the store's words, whose rule and its Rule book page, ✕ for a closure, the
* fish pictured), "Examples of how to interpret regulations" (T479; "How it adds up" before: the store's elsewhere sentence and worked lines), the steelhead quiet line, and
 * "See every regulation line ›".
 *
 * Owner rule 2026-10-21: what is not kept, its status, the worked counts and the steelhead line are the store's
 * (`group.not_kept`, `group.worked`, `rows_row.elsewhere`, `rows_frame.steelhead_line`); this file words and orders them.
 *
 * A released fish's licence documents (prototype round UR7 stampsFor on a release line) are the store's (rows 31
 * `NotKept.stamps`); each is drawn as tree.ts `stampLine` words it.
 *
 * Ports (inventory names @ 7fb6809f): prototype buildKeep nk, els/worked, quiet, more; page v36 noRulesLine, STEEL_LINE,
 * summaryHtml's linkrow.
 */
import type { GroupTotal } from "../answers.generated";
import { mainRes, type CardModel, type HydRow } from "./card";
import { parentRow } from "./grouphead";
import type { RuleView } from "./inputs";
import { levelTag } from "./level";
import { ruleBookLine } from "./book";
import { text, type Line, type Run } from "./line";
import type { GroupVM, NotKeptVM } from "./model";
import type { SheetKey } from "./sheetKey";
import { fishNames, pictures, presenceLine, stampLine, type TreeEnv } from "./tree";
import { WORDS } from "./words";
import { cap, clean } from "./names";


/** prototype nk: one line per deciding rule of the fish nobody may keep here; then (T670 K3) the child rows' (`childNotKept`). */
export function notKeptOf(g: GroupTotal, env: TreeEnv): readonly NotKeptVM[] {
  const names = fishNames(env.inp, env.fish);
  const stored = (g.not_kept ?? []).map((x): NotKeptVM => {
    const r = env.inp.rules.get(x.by);
    const whose: Run[] = [text(cap(x.whose))];
    if (!r.missing) { whose.push(text(WORDS.DOT_SEP)); whose.push(...ruleBookLine(r)); }
    return { status: x.status, says: x.says, term: x.term ?? null, whose, mark: x.status === "closed" ? WORDS.CLOSED_MARK as "✕" : null,
      presence: presenceLine(x.presence), pictures: pictures(x.species, env.fish, names, env.inp.statics.exportSpecies.groups), stamps: (x.stamps ?? []).map(stampLine) };
  });
  return [...stored, ...childNotKept(g, env, new Set((g.not_kept ?? []).flatMap((x) => x.species)))];
}

/**
 * (T670 K3) THE FISH OF A CHILD ROW NOBODY MAY KEEP. A row the store parents to this card's row (`rows_row.parent`: its
 * fish are drawn inside this card's tree, and it gets no article of its own) can hold a fish the card keeps none of —
 * Shuswap Lake's own char row holds lake trout, which Region 3 releases Oct 15–Jan 31 (p.28). The stored `not_kept` reads
 * the card's own row only, so that fish was on no line of the tab. Each such fish (a child row's member, no kind of this
 * card, in no stored line: `said`) whose stored answer is release or closed gets a line: the deciding rule's stored
 * sentence, whose rule it is (the level words) and its Rule book page. One line per status and rule. Every answer read
 * here is the store's (`rows_decided.status` / `win` through the stored leading origin); nothing is decided.
 */
export function childNotKept(g: GroupTotal, env: TreeEnv, said: ReadonlySet<string>): readonly NotKeptVM[] {
  const { card, ctx, inp } = env, kinds = new Set<string>(g.kinds), names = fishNames(inp, env.fish);
  const lines = new Map<string, { status: "closed" | "release"; r: RuleView; species: string[] }>();
  for (const row of card.rows) {
    if (parentRow(card, row) !== ctx.row) continue;
    for (const S of row.allMembers) {
      if (kinds.has(S) || said.has(S)) continue;
      const m = mainRes(card.R[S]);
      if (!m || (m.status !== "release" && m.status !== "closed") || m.win.missing) continue;
      const k = `${m.status}|${m.win.key}`, x = lines.get(k);
      if (x) { if (!x.species.includes(S)) x.species.push(S); } else lines.set(k, { status: m.status, r: m.win, species: [S] });
    }
  }
  if (!lines.size) return [];
  const closedTerm = inp.statics.glossary.terms.some((t) => t.id === "no_fishing_for") ? "no_fishing_for" : null;
  return [...lines.values()].map(({ status, r, species }): NotKeptVM => ({
    status, says: r.facts?.plain ?? clean(r.export.label), term: status === "closed" ? closedTerm : null,
    whose: [text(levelTag(r)), text(WORDS.DOT_SEP), ...ruleBookLine(r)], mark: status === "closed" ? WORDS.CLOSED_MARK as "✕" : null,
    presence: null, pictures: pictures(species, env.fish, names, inp.statics.exportSpecies.groups), stamps: [],
  }));
}

/** prototype worked: "How it adds up" (T479: "Examples of how to interpret regulations", drawn folded) — the row's elsewhere sentence, then every worked line (a keep-here question with
 *  its colon; the data's stream / lake / elsewhere lines all read as kept-elsewhere questions). Null when neither. */
export function workedOf(row: HydRow, g: GroupTotal): GroupVM["worked"] {
  const els = row.raw.elsewhere?.says || "", wk = g.worked ?? [];
  if (!els && !wk.length) return null;
  return { label: WORDS.INTERPRET_EXAMPLES, says: els || null,
    lines: wk.map((w) => ({ at: w.at === "here" ? "here" as const : "elsewhere" as const, q: w.q + (w.at === "here" ? WORDS.COLON : ""), a: w.a })) };
}

/** page v36 STEEL_LINE: the store's steelhead line, in words. */
export const STEEL_LINE: Readonly<Record<"possible_with_rules" | "known_with_rules" | "known_no_rules", string>> = Object.freeze({
  possible_with_rules: WORDS.STEEL_POSSIBLE, known_with_rules: WORDS.STEEL_KNOWN, known_no_rules: WORDS.STEEL_NO_RULES,
});

/** page v36 noRulesLine: only "recorded here, but no steelhead rule applies" stays on a card, and only over rainbow trout. */
export function quietOf(row: HydRow, card: CardModel): Line | null {
  return card.line === "known_no_rules" && row.allMembers.includes("RB")
    ? [text(STEEL_LINE.known_no_rules), text(" "), { t: "term", ids: ["steelhead"] }] : null;
}

export const MORE: string = WORDS.SEE_EVERY_LINE;
/** page v36 summaryHtml's last line: every carded row opens its full regulation lines. */
export const moreOf = (rowIx: number): { text: string; open: SheetKey } => ({ text: MORE, open: `src:r${rowIx}` });
