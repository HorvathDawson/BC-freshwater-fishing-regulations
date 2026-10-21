/**
 * T36 part 2 — an Other fish row opened as its sheet (`other:oN`): the row's pictures, then its full article as the Keep
 * tab would draw it — a g28 group (its stored `group.headline`), else page v36's row (its value, its sentence through the
 * year or its weekday story, the presence line, the names, and "How this was decided" with each rule's place and pages).
 *
 * Owner rule 2026-10-21: every answer is the store's — the row's kind / daily / pool / members (`rows_row`), its group,
 * week split, restates and presence, its rules' roles (`rows_decided.roles`, decided.ts) and the year strip. This file
 * only SELECTS the row the list's N-th line is (others.ts splitRows, the same split that drew the list) and lays the
 * presenters' VMs out as the prototype's sheet does (its weekSheet / scopeLines / citeBooks rewrites of the article).
 *
 * Ports (regs-v11.html @ 7fb6809f): sheetHtml other: branch (pictures row, rtitle drop, details.decide → "How this was
 * decided" head), weekSheet (+ its per-step de-duplication, the weekday box, the day-parts and restates as the deciding
 * step's lines), scopeLines (a ladder line + where it applies + its pages); page_v36.js rowHtml (title + scope tag, valueHtml),
 * summaryHtml (the `!row.pool` sentence; the pool row's "Shared with all of … today." line), ladderHtml.
 */
import { bookLine } from "../book";
import type { HydRow } from "../card";
import { groupOf, plainRowSeams } from "../keep";
import { drawnRows, rowTitle, scopeBadge, scopeOf } from "../grouphead";
import type { RegsInputs } from "../inputs";
import { plain, text, type Line, type Run } from "../line";
import type { LadderStepVM, LadderVM, SheetBlock, SheetCtx, SheetVM } from "../model";
import { splitRows } from "../others";
import { plainRowOf, shortWindowsOf } from "../plainrow";
import type { SheetKey } from "../sheetKey";
import { fishNames, pictures } from "../tree";
import { WD3, WORDS } from "../words";
import { WEEKDAYS } from "../../calendar.generated";

type StepLine = LadderStepVM["lines"][number];
const para = (l: Line): SheetBlock => ({ k: "para", text: l });
const cap1 = (t: string): string => (t ? t[0]!.toUpperCase() + t.slice(1) : t);
const term = (inp: RegsInputs, id: string): Run[] => (inp.statics.glossary.terms.some((t) => t.id === id) ? [{ t: "term", ids: [id] }] : []);

/** page v36 rowHtml's value (valueHtml, big for a pooled keep row; "a day, shared" said "a day"; a real-daily row its sum). */
function valueLine(row: HydRow, inp: RegsInputs): Line {
  if (row.kind === "keep") {
    const n = row.real?.all ? row.real.sum : row.daily;
    return row.pool ? [{ t: "num", s: String(n), size: "big" }, text(WORDS.A_DAY.trim())] : [{ t: "num", s: String(row.daily), size: "inline" }, text(WORDS.A_DAY)];
  }
  if (row.kind === "nolimit") return [text(WORDS.PILL_NOLIMIT)];
  const closed = row.kind === "closed";
  const t = term(inp, closed ? "no_fishing_for" : "catch_and_release");
  return [text(closed ? WORDS.PILL_CLOSED : WORDS.PILL_RELEASE), ...(t.length ? [text(" "), ...t] : [])];
}

/** A ladder line as the sheet draws it (scopeLines): the line, where its rule applies; its pages are `book`. */
const withApplies = (inp: RegsInputs, l: StepLine): StepLine => {
  const r = l.rule ? inp.rules.byKey?.(l.rule) ?? null : null;
  return r ? { ...l, applies: [r.facts?.applies, r.facts?.applies_trib].filter((a): a is string => !!a) } : l;
};

/** prototype weekSheet: in each step, a rule line whose words repeat an earlier line of the same step is drawn once. */
export const dedupe = (lines: readonly StepLine[]): StepLine[] => {
  const seen = new Set<string>();
  return lines.filter((l) => { if (!l.rule) return true; const t = plain(l.text); if (seen.has(t)) return false; seen.add(t); return true; });
};

/** prototype weekSheet: the deciding step's lines become the stored day-parts (each its days, words and rule; today's
 *  tagged) and then the row's restatements (the combined-quota note), each once. */
function weekLines(row: HydRow, inp: RegsInputs): StepLine[] {
  const w = row.raw.week!, key = (rix: number | undefined) => (rix == null ? null : inp.rules.get(rix).key);
  const line = (k: string | null, t: Line, tag: string | null, today?: boolean, days?: string): StepLine => {
    const r = k ? inp.rules.byKey?.(k) ?? null : null;
    return { text: t, tag: k ? tag : null, rule: k, won: true, applies: [], book: r ? bookLine(r.facts?.book ?? []) : [],
      ...(today !== undefined ? { today } : {}), ...(days !== undefined ? { days } : {}) };
  };
  return [
    ...(w.parts ?? []).filter((p) => p.says).map((p) =>
      line(key(p.rules?.[0]), [text(p.days), text(" "), text(cap1(p.says))], p.today ? WORDS.TODAY : null, !!p.today, p.days)),
    ...(row.raw.restates ?? []).map((x) => line(key(x.r), [text(x.says)], null)),
  ];
}

/** The ladder as the other: sheet draws it: per section the question, the result, each step's lines (with where they
 *  apply), de-duplicated; a weekday row's first step is its day-parts. */
function sheetLadder(l: LadderVM, row: HydRow, inp: RegsInputs): SheetBlock[] {
  const wk = row.raw.week && (row.raw.week.parts ?? []).some((p) => p.says) ? weekLines(row, inp) : null;
  return l.sections.map((s, si): SheetBlock => ({ k: "ladder", q: s.q, result: s.result,
    steps: s.steps.map((st, i) => ({ level: st.level,
      lines: (si === 0 && i === 0 && wk ? wk : dedupe(st.lines)).map((x) => withApplies(inp, x)) })) }));
}

/** sheetHtml other: — null when this page has no Other fish list or no N-th line in it. */
export function otherSheet(ctx: SheetCtx, key: SheetKey, i: number): SheetVM | null {
  const env = ctx.env, inp = ctx.inp;
  if (!env) return null;
  const card = env.card, ix = splitRows(card).rest?.[i];
  if (ix === undefined) return null;
  const row = card.rows[ix]!, seams = plainRowSeams(inp, env.fish), title = rowTitle(row, inp);
  const blocks: SheetBlock[] = [];
  const pics = pictures(row.members, env.fish, fishNames(inp, env.fish)).filter((p) => !p.sil);
  if (pics.length) blocks.push({ k: "pictures", pics });
  if (row.raw.group?.headline) {
    // sheetHtml other: drops the row title (`<span class="rtitle">` with no scope tag inside): the sheet's title says it
    const group = groupOf(ix, inp, env, drawnRows(card), seams);
    blocks.push({ k: "group", group, titled: !!group.scope });
    return { key, title, sub: null, blocks };
  }

  const vm = plainRowOf(ix, { inp, card, settled: env.settled, seams });
  const scope = scopeBadge(row, card, inp), week = row.raw.week, value = valueLine(row, inp);
  if (week) {
    const wd = WD3[WEEKDAYS.indexOf(ctx.page.weekday)] ?? "";
    blocks.push({ k: "weekBox", day: (ctx.page.today ? WORDS.SHEET_TODAY_DOT : "") + wd, scope,
      status: week.status ?? plain(value), says: week.says.replace(/\.?$/, ".") });
  } else {
    if (scope) blocks.push({ k: "rowTitle", title, scope });
    blocks.push({ k: "value", text: value });
    if (!row.pool && vm.sum) blocks.push(para([text(vm.sum)]));
    // page v36 summaryHtml's pool path, in the two forms ported: a keep pool's "Shared with all of … today." and a no-limit
    // pool's groupBlock fact ("∞ / No daily limit"); a pool with stored conditions (groupBlock / speciesBlock, decisions D19)
    // is not ported
    if (row.pool && row.kind === "nolimit") blocks.push(para([text(WORDS.NOLIMIT_MARK)]), para([text(WORDS.NOLIMIT_SAYS)]));
    const s = row.pool && row.kind === "keep" ? scopeOf(row) : null;
    if (s) blocks.push(para([text(WORDS.SHARED_WITH(s))]));
  }
  if (vm.presence) blocks.push(para(vm.presence));
  if (vm.names) blocks.push(vm.whichFish ? { k: "fold", summary: vm.whichFish, open: false, body: [para([text(vm.names)])] } : para([text(vm.names)]));
  const short = row.pool ? null : shortWindowsOf(row, inp, seams);
  if (short) blocks.push(para([text(short)]));

  blocks.push({ k: "head", text: vm.ladder.label }, ...sheetLadder(vm.ladder, row, inp));
  if (vm.ladder.lost.length) blocks.push({ k: "fold", summary: WORDS.RULES_NOT_APPLY(vm.ladder.lost.length), open: false,
    body: [{ k: "ladder", q: null, result: null, steps: [{ level: "", lines: vm.ladder.lost.map((x) => withApplies(inp,
      { text: [text(x.text)], tag: x.status || null, rule: x.rule, won: false, applies: [], book: x.book })) }] }] });
  blocks.push({ k: "link", line: [{ t: "link", s: vm.more.text, open: vm.more.open }] });
  return { key, title, sub: null, blocks };
}
