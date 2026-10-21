/**
 * "How this was decided" (T19b): the ladder a row folds under it — what decides it, level by level, each rule's sentence,
 * its role and its Rule book page; then "Rules that don’t apply here (N)". The Keep tab's form (no `members`) and the
 * form T36's `other:` sheet uses (`members` + `res`).
 *
 * Named decided.ts, not the ticket's ladder.ts: T07's ladder.ts (ladderAt, the verdict decoder) already holds that name.
 *
 * Owner rule 2026-10-21 (decisions.md): every role here is the PIPELINE's — `rows_decided.roles` [rix, role, by] (which
 * rule governs, contains, narrows, is replaced, agrees, falls away, is moot, and by what) and the ladder's stored lifts
 * (`settled.lifted`). This file gathers them per row (page v36 rowSources), sorts them for reading (the roles the page
 * counts as deciding first, then by level) and words them; it never decides a role. The roles it groups as "deciding"
 * (WINS) and the questions they answer (LADDER_Q) are page v36's reading tables, not a ranking of rules. The result line
 * ("Closed here.", "Release every one here.") is the stored row kind in words.
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): page_v36 rowSources, ladderHtml (+ its line / steps / forFish / subsetOf),
 * WINS, SAYS, QORDER, LOSES, byName, levelHead, say, shortLabel, sameSet; prototype buildKeep restates notes (rnote).
 */
import type { CardModel, HydRow } from "./card";
import { closedGate } from "./describe";
import type { RegsInputs, RuleView } from "./inputs";
import { text, type Line, type Run } from "./line";
import type { LadderStepVM, LadderVM } from "./model";
import { cap, clean } from "./names";
import type { Settled } from "./settle";
import { ruleBook } from "./book";
import { LADDER_Q, LEVEL_HEAD, LOSES, SAYS, WORDS } from "./words";

/** One rule's place in a row's ladder (page v36 rowSources item): its role and what it lost to, for which fish. */
export interface LadderItem {
  readonly r: RuleView; readonly role: string;
  /** The rule that replaced / lifted / agrees with it (page v36 `by`, here the rule itself). */
  readonly by: RuleView | null;
  readonly spp: ReadonlySet<string>;
}

/** The roles page v36 reads as deciding a row (WINS), and their reading order (QORDER). */
export const WINS: readonly string[] = Object.freeze(["also", "governs", "contains", "narrows", "limit", "floor", "season", "possession_cap", "duty", "possession"]);
export const QORDER: readonly string[] = Object.freeze(["governs", "contains", "narrows", "also", "floor", "limit", "season", "possession_cap", "duty", "possession"]);
const wins = (role: string): number => (WINS.includes(role) ? 1 : 0);

/** The seams a sibling lane owns (T17 rowTitle / scopeOf). */
export interface LadderSeams {
  /** T17 grouphead.ts `rowTitle` (page v36 rowTitle). */
  readonly rowTitle: (row: HydRow, inp: RegsInputs) => string;
  /** T17 grouphead.ts `scopeOf` (page v36 scopeOf) — read for a pool row's result line only. */
  readonly scopeOf?: (row: HydRow, inp: RegsInputs) => string | null;
}
export interface LadderEnv { readonly card: CardModel; readonly settled: Settled; readonly inp: RegsInputs; readonly seams: LadderSeams }

/** page v36 say: a rule's plain sentence (answers display.rules[].plain), "" for none. */
export const say = (r: RuleView): string => r.facts?.plain ?? "";

/** page v36 shortLabel(label, row): a rule label shortened for the ladder; the exact words stay in the rule sheet. */
export function shortLabel(label: string, title: string): string {
  let x = clean(label || "");
  if (title && x.toLowerCase().startsWith(title.toLowerCase() + " — ")) x = x.slice(title.length + 3);
  return cap(x.replace(/ per day/g, WORDS.SHORT_A_DAY).replace(/, all species combined/, "")
    .replace(/\(none under (\d+) cm\)/, WORDS.SHORT_CM_PLUS).replace(/\(no more than (\d+) over (\d+) cm\)/, WORDS.SHORT_MAX_OVER)
    .replace(/possession quota is 2 daily quotas/, WORDS.SHORT_HOLD).replace(/ — /g, WORDS.SHORT_COLON).replace(/\s+:/g, ":")
    .replace(/\s+/g, " ").trim());
}

const sameSet = (a: readonly string[], b: readonly string[]): boolean => a.length === b.length && a.every((x) => b.includes(x));

/**
 * page v36 rowSources: every role of every member's stored hatchery and wild answer (the rows frame of the card's
 * segment, once per rule + role + by, with the fish it holds for), plus the day's stored lifts of a rule naming one of the
 * row's fish (role "lifted"); one item per rule — a deciding role before any other — deciding roles first, then by level.
 */
export function rowSources(row: HydRow, card: CardModel, settled: Settled, inp: RegsInputs): readonly LadderItem[] {
  const f = card.ctx.s >= 0 ? inp.year?.rowsBySeg[card.ctx.s] ?? null : null;
  const agg = new Map<string, { r: RuleView; role: string; by: RuleView | null; spp: Set<string> }>();
  for (const S of row.allMembers) {
    if (!card.R[S]) continue;
    const a = f?.fish[S];
    for (const d of [a?.hatchery, a?.wild]) {
      if (!d) continue;
      // page hydRes: the roles keyed by rule key (a later entry for the same key replaces the earlier one, in its place)
      const roles = new Map<string, { r: RuleView; role: string; by: RuleView | null }>();
      for (const [rix, role, by] of d.roles) { const r = inp.rules.get(rix); roles.set(r.key, { r, role, by: by == null ? null : inp.rules.get(by) }); }
      for (const v of roles.values()) {
        const k = `${v.r.key}|${v.role}|${v.by?.key ?? ""}`;
        let it = agg.get(k);
        if (!it) agg.set(k, it = { ...v, spp: new Set() });
        it.spp.add(S);
      }
    }
  }
  for (const [ix, by] of settled.lifted) {
    const r = inp.rules.get(ix);
    if (r.species.some((s) => row.allMembers.includes(s))) agg.set(r.key + "lift", { r, role: "lifted", by: inp.rules.get(by), spp: new Set() });
  }
  const best = new Map<string, LadderItem>();
  [...agg.values()].sort((a, b) => wins(b.role) - wins(a.role)).forEach((it) => { if (!best.has(it.r.key)) best.set(it.r.key, it); });
  return [...best.values()].sort((a, b) => wins(b.role) - wins(a.role) || a.r.rank - b.r.rank);
}

/** page v36 byName: the replacing rule named by its level (its own rank, not the part's). */
function byName(b: RuleView | null, inp: RegsInputs): string {
  if (!b || b.missing) return WORDS.BY_ABOVE;
  const rk = b.baseRank;
  return rk === 0 ? WORDS.BY_WATER(inp.item?.kind === "lake" ? WORDS.WATER_LAKE : WORDS.WATER_RIVER)
    : rk === 3 ? WORDS.BY_REGION(b.export.prov.entry_name || WORDS.REGION)
    : rk === 4 ? WORDS.BY_BC : WORDS.BY_ABOVE;
}

/** page v36 levelHead: a level's words (rank 0: the water's name). */
const levelHead = (k: number, inp: RegsInputs): string =>
  LEVEL_HEAD[String(k)] ?? (inp.item?.name || inp.place?.name || WORDS.THIS_WATER);

type StepLine = LadderStepVM["lines"][number];

/**
 * page v36 ladderHtml. No `members`: the Keep tab's form — the four questions in LADDER_Q order, each with its steps (levels
 * high → low) and the first with the row's result; then the rules that don't apply. With `members` (T36's `other:`
 * sheet): one section of the deciding rules for those fish (no possession), `res` its result, nothing lost.
 */
export function ladderOf(row: HydRow, env: LadderEnv, members?: readonly string[], res?: string): LadderVM {
  const { inp, card, settled, seams } = env;
  const title = seams.rowTitle(row, inp);
  const items: LadderItem[] = rowSources(row, card, settled, inp)
    .filter((it) => !members || !it.spp.size || [...it.spp].some((S) => members.includes(S)));
  if (!items.length && row.win) items.push({ r: row.win, role: "governs", by: null, spp: new Set() });
  // No role is re-classified here (owner rule 2026-10-21): page v36 moved a deciding record_retention rule to "duty";
  // that placement is the pipeline's to emit in rows_decided.roles. `fields.record_retention` only words the tag.

  // the fish a line is listed for when fewer than the row's (or the item's): its sentence for them is the data's (subsets)
  const forFish = (it: LadderItem): string[] | null => {
    if (!it.spp.size || it.spp.size >= row.allMembers.length) return null;
    const spp = [...it.spp];
    if (members && (spp.every((S) => members.includes(S)) || sameSet(spp.filter((S) => members.includes(S)), members))) return null;
    return spp.filter((S) => !members || members.includes(S));
  };
  const words = (it: LadderItem): { text: Line; missing: boolean } => {
    const fish = forFish(it), ss = fish ? (it.r.facts?.subsets ?? []).find((x) => sameSet(x.fish as readonly string[], fish)) ?? null : null;
    if (fish && !ss) return { text: [text(WORDS.MISSING_SUBSET(it.r.key, fish.join(", ")))], missing: true };
    const main = ss?.plain || say(it.r) || shortLabel(it.r.export.label, title);
    const runs: Run[] = [text(main)];
    if (ss && !ss.plain) runs.push(text(" "), text(ss.for));
    return { text: runs, missing: false };
  };
  const applies = (r: RuleView) => [r.facts?.applies, r.facts?.applies_trib].filter((x): x is string => !!x);
  const wonTag = (it: LadderItem): string => {
    let st = SAYS[it.role] ?? it.role;
    if (it.r.f.record_retention) st = WORDS.RECORD_IT;
    if (it.r.export.type === "stop_fishing_after_quota") st = WORDS.STOP_FISHING;
    if (it.role === "governs" && it.r.kind === "gate") st = closedGate(it.r) ? WORDS.TAG_CLOSED : WORDS.TAG_RELEASE;
    return st;
  };
  const lostTag = (it: LadderItem): string =>
    LOSES[it.role] === "Replaced" ? WORDS.REPLACED_BY(byName(it.by, inp)) : LOSES[it.role] ?? WORDS.DOESNT_APPLY;
  const line = (it: LadderItem): StepLine => {
    const w = words(it);
    return w.missing ? { text: w.text, tag: null, rule: null, won: true, applies: [], book: [] }
      : { text: w.text, tag: wonTag(it), rule: it.r.key, won: true, applies: applies(it.r), book: ruleBook(it.r) };
  };
  const steps = (list: readonly LadderItem[]): LadderStepVM[] => {
    const by = new Map<number, LadderItem[]>();
    [...list].sort((a, b) => b.r.rank - a.r.rank).forEach((it) => { const g = by.get(it.r.rank); if (g) g.push(it); else by.set(it.r.rank, [it]); });
    return [...by].map(([k, its]) => {
      const name = its[0]!.r.export.prov.entry_name || "";
      return { level: k === 3 ? name || WORDS.THE_REGION : k === 2 && name.length < 40 ? name : levelHead(k, inp), lines: its.map(line) };
    });
  };

  const sections: { q: string | null; result: string | null; steps: LadderStepVM[] }[] = [];
  if (members) {
    const won = items.filter((i) => WINS.includes(i.role) && i.role !== "possession").sort((a, b) => QORDER.indexOf(a.role) - QORDER.indexOf(b.role));
    sections.push({ q: null, result: res ?? null, steps: steps(won) });
  } else {
    const keepResult = res ?? (row.kind === "nolimit" ? WORDS.RES_NOLIMIT : row.pool ? poolResult(row, env) : row.kind === "closed" ? WORDS.RES_CLOSED : WORDS.RES_RELEASE);
    LADDER_Q.forEach(([q, roles], i) => {
      const its = items.filter((it) => roles.includes(it.role));
      if (its.length) sections.push({ q, result: i === 0 ? keepResult : null, steps: steps(its) });
    });
  }
  const wonSay = new Set(items.filter((i) => WINS.includes(i.role)).map((i) => say(i.r)).filter(Boolean));
  const lost = members ? [] : items.filter((i) => !WINS.includes(i.role) && i.role !== "agrees" && !wonSay.has(say(i.r))
    && !(i.by && !i.by.missing && shortLabel(i.by.export.label, title) === shortLabel(i.r.export.label, title)));
  return {
    label: WORDS.DECIDED, sections,
    lost: lost.map((it) => {
      const w = words(it);
      return w.missing ? { text: w.text.map((r) => (r.t === "text" ? r.s : "")).join(""), status: "", rule: it.r.key, book: [] }
        : { text: w.text.map((r) => (r.t === "text" ? r.s : "")).join(""), status: lostTag(it), rule: it.r.key, book: ruleBook(it.r) };
    }),
  };
}

/** page v36 ladderHtml's pool result: "<daily> a day here", and the region total it counts toward (T17 scopeOf). */
function poolResult(row: HydRow, env: LadderEnv): string {
  const s = env.seams.scopeOf?.(row, env.inp) ?? null, take = env.inp && row.pool ? (row.pool.f.take as number | undefined) : undefined;
  return WORDS.RES_POOL(String(row.daily))
    + (s && take !== row.daily ? WORDS.RES_COUNT_TOWARD(String(take), s) : s ? WORDS.RES_ALL_OF(s) : "") + ".";
}

/** prototype buildKeep (rows 15 Q2): a restating rule's note (`row.restates[]`, the data's words) under the winning line
 *  it restates — the first won line of that rule, in section order. A week row draws its restates in its week block. */
export function withRestates(l: LadderVM, row: HydRow, inp: RegsInputs): LadderVM {
  const rs = row.raw.restates ?? [];
  if (!rs.length || row.raw.week) return l;
  const notes = new Map<StepLine, string[]>();
  for (const x of rs) {
    const key = inp.rules.get(x.of).key;
    const hit = l.sections.flatMap((s) => s.steps.flatMap((st) => st.lines)).find((ln) => ln.won && ln.rule === key);
    if (hit) notes.set(hit, [...(notes.get(hit) ?? []), x.says]);
  }
  if (!notes.size) return l;
  return { ...l, sections: l.sections.map((s) => ({ ...s, steps: s.steps.map((st) => ({ ...st,
    lines: st.lines.map((ln) => (notes.has(ln) ? { ...ln, notes: notes.get(ln)! } : ln)) })) })) };
}
