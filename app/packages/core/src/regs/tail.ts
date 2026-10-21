/**
 * T19 — a group's tail: each not-kept line (the store's words, whose rule and its Rule book page, ✕ for a closure, the
 * fish pictured), "How it adds up" (the store's elsewhere sentence and worked lines), the steelhead quiet line, and
 * "See every regulation line ›".
 *
 * Owner rule 2026-10-21: what is not kept, its status, the worked counts and the steelhead line are the store's
 * (`group.not_kept`, `group.worked`, `rows_row.elsewhere`, `rows_frame.steelhead_line`); this file words and orders them.
 *
 * NOT ported: prototype `stampsFor` on a release line (the licence documents a released fish needs) — a regulation
 * answer the pipeline must ship (see tree.ts).
 *
 * Ports (inventory names @ 7fb6809f): prototype buildKeep nk, els/worked, quiet, more; page v36 noRulesLine, STEEL_LINE,
 * summaryHtml's linkrow.
 */
import type { GroupTotal } from "../answers.generated";
import type { CardModel, HydRow } from "./card";
import { ruleBookLine } from "./book";
import { text, type Line, type Run } from "./line";
import type { GroupVM, NotKeptVM } from "./model";
import type { SheetKey } from "./sheetKey";
import { fishNames, pictures, presenceLine, type TreeEnv } from "./tree";
import { WORDS } from "./words";

const cap1 = (t: string | null | undefined): string => (t ? t[0]!.toUpperCase() + t.slice(1) : "");

/** prototype nk: one line per deciding rule of the fish nobody may keep here. */
export function notKeptOf(g: GroupTotal, env: TreeEnv): readonly NotKeptVM[] {
  const names = fishNames(env.inp, env.fish);
  return (g.not_kept ?? []).map((x): NotKeptVM => {
    const r = env.inp.rules.get(x.by);
    const whose: Run[] = [text(cap1(x.whose))];
    if (!r.missing) { whose.push(text(WORDS.DOT_SEP)); whose.push(...ruleBookLine(r)); }
    return { status: x.status, says: x.says, whose, mark: x.status === "closed" ? WORDS.CLOSED_MARK as "✕" : null,
      presence: presenceLine(x.presence), pictures: pictures(x.species, env.fish, names) };
  });
}

/** prototype worked: "How it adds up" — the row's elsewhere sentence, then every worked line (a keep-here question with
 *  its colon; the data's stream / lake / elsewhere lines all read as kept-elsewhere questions). Null when neither. */
export function workedOf(row: HydRow, g: GroupTotal): GroupVM["worked"] {
  const els = row.raw.elsewhere?.says || "", wk = g.worked ?? [];
  if (!els && !wk.length) return null;
  return { label: WORDS.HOW_IT_ADDS_UP, says: els || null,
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
