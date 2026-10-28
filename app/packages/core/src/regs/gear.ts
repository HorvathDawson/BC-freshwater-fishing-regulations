/**
 * T30 — the Gear tab's top block: the day line, an optional fly-only mode line, then Lines · Hooks · Weight on the line,
 * each value big with its ⓘ, its quiet changes line and its own source line; on a day with no fishing, one sentence
 * ("No fishing here until Jul 1."), or — on a water the strip does not govern (tidal) — the Keep banner's own words.
 * It also owns the helpers every Gear / Methods item uses: `sourceLine` (who says so, from the rules' LEVELS), `closedUntil`
 * and `changeVM`.
 *
 * Owner rule 2026-10-21 (decisions.md): nothing here decides a regulation. The day's gear is the PIPELINE's gear frame
 * (`GearAnswer.summary`: its sentence, parts, terms, dated changes and cites), open/closed on a day is the stored display
 * frame's status / strip bit 1 (`displayByDay`), the levels are the rules' stored ranks. This file selects which summary
 * parts the sentence names, words them, groups the changes onto the line they talk about and names the sources.
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): prototype dayLine, closedUntil, levelOf, learnChipWords, downWater,
 * wordOf_, itemSrc, gnote (the "Next"/"Now" chip form; the `chg=a` ◷ variant is not ported), buildGear (the summary half:
 * sm, val, rowOf, notesFor, citesAt, tipFor, the rest-terms splice; the ungoverned/closed branches); gearParts' gRow chip rule set (RANK[rank].t, for the learned chip words).
 */
import { MONTHS, WEEKDAYS } from "../calendar.generated";
import type { GearAnswer, GearValue, RegsInputs, RuleIndex, RuleView } from "./inputs";
import type { BannerVM, ChangeVM, Day, GearLineVM, GearVM, SourceLineVM, Weekday } from "./model";
import { ruleSub } from "./level";
import { cap } from "./names";
import type { SheetKey } from "./sheetKey";
import { BOOK_DAYS, bookIndex, displayOnBookDay, fmtDay } from "./strip";
import { GEAR, GEAR_LINES, RANK_LEVEL, WD3, WORDS } from "./words";

/** level word ("B.C.", "This water’s") → the chip word page v36 drew for it ("Province", "This water") */
export type ChipWords = ReadonlyMap<string, string>;

export interface GearCtx {
  readonly day: Day; readonly weekday: Weekday; readonly today: boolean;
  readonly rules: RuleIndex;
  readonly glossary: ReadonlySet<string>;     // statics.glossary term ids (an ⓘ the glossary lacks is not drawn)
  readonly chips: ChipWords;                  // learnChipWords(gear) — once per page
  /** (D20 b) the Keep tab's banner: a water the strip does not govern (tidal) says its words on Gear and Methods */
  readonly banner: BannerVM | null;
}

/** The gear frame of the day when it is an answer (a tidal key's frame is not). */
export function gearAnswer(inp: RegsInputs): GearAnswer | null {
  const g: GearValue | undefined = inp.frames?.gear;
  return g && !("tidal" in g) ? g : null;
}

/** The page's gear context for one tap (`today`: the page's today; `banner`: the Keep tab's). */
export function gearCtxOf(inp: RegsInputs, o: { readonly today: Day; readonly banner: BannerVM | null }): GearCtx {
  const g = gearAnswer(inp);
  return {
    day: inp.day, weekday: inp.weekday, today: inp.day.month === o.today.month && inp.day.day === o.today.day,
    rules: inp.rules, glossary: new Set(inp.statics.glossary.terms.map((t) => t.id)),
    chips: g ? learnChipWords(g, inp.rules) : new Map(), banner: o.banner,
  };
}

// ── who says so ──────────────────────────────────────────────────────────────────────────────────────────────────────

/** prototype levelOf: the rule sheet's sub (level.ts ruleSub, page v36 levelTag) before " rule · " ("B.C.", "Region 2", "This water’s"). */
export function levelWord(r: RuleView): string {
  return ruleSub(r).split(GEAR.RULE_SUB)[0]!;
}

/** prototype downWater (round UR6): a rule reached from a water downstream is named by that water ("Duncan River"). */
export function downWater(r: RuleView): string {
  return r.rank === 1 ? (ruleSub(r).split(GEAR.RULE_SUB)[1] ?? "").trim() : "";
}

/**
 * prototype learnChipWords: the chip words page v36's gear rows drew (gearParts gRow → gSrc: `RANK[rank].t`), keyed by
 * the level word of the rule each chip names. The rows that carry a chip: the counts of lines, hook-lure-or-fly
 * (else flies), points, weight and hooks per line, and the barbed / lure elements when they are banned. "Region" is never
 * learned.
 */
export function learnChipWords(g: GearAnswer, rules: RuleIndex): ChipWords {
  const out = new Map<string, string>();
  const top = (slot: string): RuleView | null => (g.counts[slot] ? rules.get(g.counts[slot]!.by[0]) : null);
  const banned = (k: string): RuleView | null => (g.elements[k]?.verdict === "ban" ? rules.get(g.elements[k]!.by[0]) : null);
  const chipped = [top("lines_per_angler"), top("terminal_attachments_per_line"), top("points_per_hook"), banned("barb:barbed"),
    banned("lure:artificial_lure"), g.counts.terminal_attachments_per_line ? null : top("flies_per_line"), top("weight_per_line_kg"),
    top("hooks_per_line")];
  for (const r of chipped) {
    if (!r) continue;
    const t = RANK_LEVEL[String(r.rank)];
    if (t !== undefined && t !== GEAR.CHIP_NOT_LEARNED) out.set(levelWord(r), t);
  }
  return out;
}

/** prototype wordOf_: the downstream water's name, else the learned chip word, else the level word. */
export function whoWord(r: RuleView, chips: ChipWords): string {
  return downWater(r) || chips.get(levelWord(r)) || levelWord(r);
}

/**
 * prototype itemSrc: an item's source line — who says so (each rule's word, once, in order; "Sources" when none), which
 * opens the Sources sheet of exactly those rules, then their Rule book pages (each printed page once, its first pdf,
 * ascending). Rules the store does not hold are dropped; none left: no line.
 */
export function sourceLine(open: SheetKey, rixs: readonly number[], ctx: Pick<GearCtx, "rules" | "chips">): SourceLineVM | null {
  const keep = [...new Set(rixs)].filter((ix) => ctx.rules.has(ix));
  if (!keep.length) return null;
  const rs = keep.map((ix) => ctx.rules.get(ix));
  const pages = new Map<number, number>();
  for (const r of rs) for (const [printed, pdf] of r.facts?.book ?? []) if (!pages.has(printed)) pages.set(printed, pdf);
  const who = [...new Set(rs.map((r) => whoWord(r, ctx.chips)))].filter(Boolean);
  return {
    who: `${who.join(GEAR.SUB_SEP) || GEAR.SOURCES} ${GEAR.SRC_ARROW}`,
    aria: GEAR.SOURCES_ARIA + (who.join(", ") || GEAR.SOURCES_RULES),
    open, keys: rs.map((r) => r.key),
    pages: [...pages].sort((a, b) => a[0] - b[0]).map(([printed, pdf]) => ({ printed, pdf })),
  };
}

// ── the day ──────────────────────────────────────────────────────────────────────────────────────────────────────────

/** prototype dayLine: "Today, Oct 10" | "Fri, May 15". */
export function dayLine(ctx: Pick<GearCtx, "day" | "weekday" | "today">): string {
  return `${ctx.today ? WORDS.TODAY : WD3[WEEKDAYS.indexOf(ctx.weekday)]!}, ${fmtDay(ctx.day)}`;
}

/** The stored strip bits of the 365 book days (null: no frame, or a tidal frame) — prototype `st.runs` expanded. */
export function bookBits(inp: RegsInputs): readonly (number | null)[] {
  return Array.from({ length: 365 }, (_, i) => { const f = displayOnBookDay(inp, i); return f && f.status !== "tidal" ? f.strip : null; });
}

/** prototype closedUntil: the first book day after `today365` (cyclic) whose stored bits say open (bit 1 clear). */
export function closedUntil(bits365: readonly (number | null)[], today365: number): string {
  for (let j = 1; j < bits365.length; j++) {
    const i = (today365 + j) % bits365.length, f = bits365[i];
    if (f != null && !(f & 1)) {
      const d = BOOK_DAYS[i]!;
      return GEAR.CLOSED_UNTIL(MONTHS[d.month - 1]!, d.day);
    }
  }
  return GEAR.CLOSED_ALL_YEAR;
}

/** prototype gnote: a dated change — "Next" (or "Now" while it is in force) + its dates + the data's words. */
export function changeVM(when: string, says: string, inForce: boolean): ChangeVM {
  return { chip: inForce ? GEAR.CHIP_NOW : GEAR.CHIP_NEXT, when, says, inForce };
}

/**
 * What a Gear / Methods tab says instead of its items, or null on a day it draws them (prototype buildGear's two early
 * returns): a water the strip does not govern — every book day without bits (tidal) — says the Keep banner's headline
 * and paragraphs; any other day without a gear answer (closed) says when fishing opens.
 */
export function closedFoot(inp: RegsInputs, ctx: Pick<GearCtx, "day" | "banner">): readonly string[] | null {
  const f = inp.frames?.display;
  if (f && f.status !== "closed" && f.status !== "tidal" && gearAnswer(inp)) return null;
  const bits = bookBits(inp);
  if (bits.every((b) => b == null)) return ctx.banner ? [...ctx.banner.big, ...ctx.banner.paras] : [];
  return [closedUntil(bits, bookIndex(ctx.day))];
}

// ── the summary lines ────────────────────────────────────────────────────────────────────────────────────────────────

type Block = { k: "mode" | GearLineVM["k"]; label: string; values: string[]; terms: string[][]; cites: readonly number[] };

/**
 * prototype buildGear's summary half: the parts the day's sentence names (a part the sentence leaves out is not drawn), a
 * fly-only mode first, then Lines · Hooks (attach + hook parts) · Weight on the line, each value without its leading
 * article or repeated label; each dated change on the line it talks about (the mode when there is one); the summary's
 * terms no line used go on the last line when it has a source line and no change (the prototype's splice).
 */
export function gearLines(g: GearAnswer, ctx: GearCtx): { mode: GearVM["mode"]; lines: readonly GearLineVM[] } {
  const sm = g.summary, say = sm.says.toLowerCase();
  const parts = sm.parts.filter((p) => say.includes(p.says.toLowerCase()));
  const of = (ks: readonly string[]) => parts.filter((p) => ks.includes(p.part));
  const tip = (ts: readonly string[]) => ts.filter((t) => ctx.glossary.has(t));
  const val = (says: string, lab: string) => {
    let t = says.replace(/^(a|an)\s+/i, "");
    if (t.toLowerCase().startsWith(lab.toLowerCase() + " ")) t = t.slice(lab.length + 1);
    return cap(t);
  };
  const used = new Set<string>(), blocks: Block[] = [];
  const mode = of(["mode"])[0];
  if (mode) { mode.terms.forEach((t) => used.add(t)); blocks.push({ k: "mode", label: "", values: [mode.says], terms: [tip(mode.terms)], cites: mode.cites }); }
  for (const [k, label, ks] of GEAR_LINES) {
    const ps = ks.flatMap((x) => of([x]));
    if (!ps.length) continue;
    ps.forEach((p) => p.terms.forEach((t) => used.add(t)));
    blocks.push({ k, label, values: ps.map((p) => val(p.says, label)), terms: ps.map((p) => tip(p.terms)), cites: ps.flatMap((p) => p.cites) });
  }
  const drawn = blocks.map((b) => b.k);
  const rowOf = (says: string): Block["k"] => mode ? "mode" : /\bfly\b|\bflies\b|hook|lure/i.test(says) ? "hook"
    : /weight|sinker|float/i.test(says) ? "weight" : /\blines?\b|\brods?\b/i.test(says) ? "lines" : "hook";
  const changeAt = new Map<Block["k"], (typeof sm.changes)[number][]>();
  for (const c of sm.changes) {
    let k = rowOf(c.says);
    if (!drawn.includes(k)) k = drawn[drawn.length - 1]!;
    if (k === undefined) continue;
    changeAt.set(k, [...(changeAt.get(k) ?? []), c]);
  }
  const done = blocks.map((b) => {
    const cs = changeAt.get(b.k) ?? [];
    return { b, changes: cs.map((c) => changeVM(c.when, c.says, false)),
             src: sourceLine(`srcs:gl|${b.k}`, [...b.cites, ...cs.flatMap((c) => c.cites)], ctx) };
  });
  const rest = sm.terms.filter((t) => !used.has(t)), last = done[done.length - 1];
  if (rest.length && last && last.b.k !== "mode" && last.src && !last.changes.length) last.b.terms[last.b.terms.length - 1]!.push(...tip(rest));
  const m = done.find((d) => d.b.k === "mode");
  return {
    mode: m ? { value: m.b.values[0]!, terms: m.b.terms[0]!, changes: m.changes, src: m.src } : null,
    lines: done.filter((d) => d.b.k !== "mode").map((d) => ({ k: d.b.k as GearLineVM["k"], label: d.b.label, values: d.b.values,
      terms: d.b.terms.flat(), valueTerms: d.b.terms, changes: d.changes, src: d.src })),
  };
}

/** The Gear tab without Bait (T31 adds it; `buildRegsPage` composes). */
export function buildGearTop(inp: RegsInputs, ctx: GearCtx, anyway = false): Omit<GearVM, "bait"> {
  // (T227) `anyway`: the day's gear even on a day the game-fish rules close (salmon may be fished there)
  const foot = anyway ? null : closedFoot(inp, ctx), g = gearAnswer(inp);
  if (foot || !g) return { closed: foot?.[0] ?? null, closedMore: foot?.slice(1) ?? [], day: null, mode: null, lines: [] };
  return { closed: null, closedMore: [], day: dayLine(ctx), ...gearLines(g, ctx) };
}
