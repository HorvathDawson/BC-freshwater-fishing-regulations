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
import type { BookBox } from "../answers.generated";
import type { GearAnswer, GearValue, RegsInputs, RuleIndex, RuleView } from "./inputs";
import type { BannerVM, ChangeVM, Day, GearLineVM, GearVM, SourceLineVM, Weekday } from "./model";
import { reportFailure } from "../failure";
import { ruleSub } from "./level";
import { cap, clean } from "./names";
import type { SheetKey } from "./sheetKey";
import { BOOK_DAYS, bookIndex, displayOnBookDay, fmtDay, isTodayOf } from "./strip";
import { GEAR, GEAR_LINES, HOOKS_ALLOWED_TERMS, RANK_LEVEL, WD3, WORDS } from "./words";

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
    day: inp.day, weekday: inp.weekday, today: isTodayOf(inp, o.today),
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
  // (T215) each page's highlight: every kept rule's `book_box` on that PDF page, once each (the same row cited by several
  // rules is one box). Before this the source line's links carried no box and opened the plain book.
  const boxesOn = (pdf: number): BookBox[] => {
    const seen = new Map<string, BookBox>();
    for (const r of rs) for (const b of r.facts?.book_box ?? []) if (b.pdf === pdf) seen.set(JSON.stringify(b), b);
    return [...seen.values()];
  };
  const who = [...new Set(rs.map((r) => whoWord(r, ctx.chips)))].filter(Boolean);
  return {
    who: `${who.join(GEAR.SUB_SEP) || GEAR.SOURCES} ${GEAR.SRC_ARROW}`,
    aria: GEAR.SOURCES_ARIA + (who.join(", ") || GEAR.SOURCES_RULES),
    open, keys: rs.map((r) => r.key),
    pages: [...pages].sort((a, b) => a[0] - b[0]).map(([printed, pdf]) => {
      const box = boxesOn(pdf);
      return box.length ? { printed, pdf, box } : { printed, pdf };
    }),
  };
}

/**
 * (T900) ONE source line for a whole section: the rules of every line given, each level word once (in order), every Rule book
 * page once (ascending, its boxes joined). It opens `open`'s Sources sheet, which lists all their rules. Null when none has one.
 */
export function mergedSource(open: SheetKey, lines: readonly (SourceLineVM | null)[]): SourceLineVM | null {
  const ls = lines.filter((l): l is SourceLineVM => l !== null);
  if (!ls.length) return null;
  const arrow = ` ${GEAR.SRC_ARROW}`;
  const who = [...new Set(ls.flatMap((l) => l.who.replace(arrow, "").split(GEAR.SUB_SEP)))].filter((w) => w && w !== GEAR.SOURCES);
  const pages = new Map<number, { printed: number; pdf: number; box: BookBox[] }>();
  for (const l of ls) for (const p of l.pages) {
    const at = pages.get(p.printed) ?? { printed: p.printed, pdf: p.pdf, box: [] };
    for (const b of p.box ?? []) if (!at.box.some((x) => JSON.stringify(x) === JSON.stringify(b))) at.box.push(b);
    pages.set(p.printed, at);
  }
  return {
    who: `${who.join(GEAR.SUB_SEP) || GEAR.SOURCES}${arrow}`, aria: GEAR.SOURCES_ARIA + (who.join(", ") || GEAR.SOURCES_RULES), open,
    keys: [...new Set(ls.flatMap((l) => l.keys))],
    pages: [...pages.values()].sort((a, b) => a.printed - b.printed).map((p) => (p.box.length ? p : { printed: p.printed, pdf: p.pdf })),
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

/** The frame's hook answers (`GearAnswer.hook`) that leave barbed hooks allowed: no barb ban (p.8: "the use of barbed hooks
 *  in lakes is permitted, unless noted in the Regional Water-Specific Tables"). */
const BARBED_OK: ReadonlySet<GearAnswer["hook"]> = new Set(["trebles_and_barbs", "single", "any"]);
/** … and treble hooks: no hook-point limit (p.80: "use of a treble hook is permitted unless 'single hook' is specified"). */
const TREBLE_OK: ReadonlySet<GearAnswer["hook"]> = new Set(["trebles_and_barbs", "any_barbless"]);

/** (T1051 review) a hook answer the table above does not know: reported, and no line is drawn (W46: never silent). */
export class UnknownHookAnswer extends Error {
  constructor(hook: string) { super(`regs gear: the frame's hook answer ${JSON.stringify(hook)} is not one the Hooks row knows`); this.name = "UnknownHookAnswer"; }
}
const KNOWN_HOOKS: ReadonlySet<string> = new Set(["trebles_and_barbs", "single", "any", "any_barbless", "single_barbless"]);
/** the gear slots that say what a hook may be: its points and its barb */
const HOOK_SLOTS: ReadonlySet<string> = new Set(["points_per_hook", "barb"]);

/** Does a rule hold a clause on a hook's points or barb? */
function speaksOfHooks(rules: RuleIndex, rix: number): boolean {
  const gear = rules.get(rix).export.fields?.gear as readonly { slot?: string }[] | undefined;
  return (gear ?? []).some((c) => HOOK_SLOTS.has(c.slot ?? ""));
}

/**
 * (T1051, owner 2026-11-24 "Say it everywhere") what the day's hook answer leaves allowed, per water, or null: treble and
 * barbed hooks on Dick Lake (`trebles_and_barbs`); treble hooks alone where barbless is required and the points are not
 * limited (`any_barbless`: Quesnel Lake); barbed hooks alone where a single hook is required (`single`). The frame decided
 * the hook (`gear.resolve`); a fly mode (`fly`) is the frame's too, and a fly is "a single-pointed hook that is dressed
 * only with fur, feathers, hair, textiles, tinsel and/or wire" (printed p.80 "artificial fly", PDF p.86 of the repo copy;
 * the older "single or double hook" wording in `gear.py`'s fly-mode comment is not this book's), so no fly-only water says
 * treble hooks. A water whose answer is a single barbless hook (every stream, unless its row lifts that) says nothing more.
 *
 * (T1051 review) The hook answer is the WHOLE water's: a hook rule held only in a part no map draws (`in_part`: François,
 * Mahood, Upper Arrow and Pitt lakes' "Single barbless hook — in part: …"), on one side or some hours (`side`, `timed`), or
 * only in a circumstance (`circumstantial`) would make an unqualified "allowed" untrue somewhere on the water: no line then,
 * as before T1051 (the wording for those waters is the owner's question).
 */
export function hooksAllowed(g: GearAnswer, rules: RuleIndex): { says: string; terms: readonly string[] } | null {
  if (!KNOWN_HOOKS.has(g.hook)) { reportFailure(new UnknownHookAnswer(g.hook)); return null; }
  const partly = [...g.in_part, ...g.side, ...g.timed, ...g.circumstantial.map((c) => c.clause[0])];
  if (partly.some((rix) => speaksOfHooks(rules, rix))) return null;
  const barbed = BARBED_OK.has(g.hook), treble = TREBLE_OK.has(g.hook) && !g.counts.points_per_hook && g.fly === null;
  const H = GEAR.HOOKS_ALLOWED, T = HOOKS_ALLOWED_TERMS;
  if (barbed && treble) return { says: H.both, terms: [T.both] };
  if (treble) return { says: H.treble, terms: [T.treble] };
  if (barbed) return { says: H.barbed, terms: [T.both] };
  return null;
}

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
  // (T670 G4) a hook-size limit the frame decided (`counts.hook_gap_mm`: the Qualicum's "no hooks greater than 15 mm from
  // point to shank", p.18) that the summary sentence does not word: the Hooks line says it in the deciding rule's stored
  // label (the frame's own `says` when it carries one), with that rule among the line's sources
  const gap = g.counts.hook_gap_mm;
  if (gap && !blocks.some((b) => b.values.some((v) => /\bmm\b/.test(v)))) {
    const words = gap.says ?? clean(ctx.rules.get(gap.by[0]).export.label);
    let hook = blocks.find((b) => b.k === "hook");
    if (!hook) {
      hook = { k: "hook", label: GEAR_LINES.find(([k]) => k === "hook")![1], values: [], terms: [], cites: [] };
      const after = blocks.findIndex((b) => b.k === "weight");
      blocks.splice(after < 0 ? blocks.length : after, 0, hook);
    }
    hook.values.push(words); hook.terms.push([]); hook.cites = [...hook.cites, ...gap.cites];
  }
  // (T1051, owner 2026-11-24 "Say it everywhere") the Hooks row's second line: what the frame's hook answer leaves allowed
  // ("Treble and barbed hooks allowed" on Dick Lake, where the sentence names no hook: ROUND13 G), with its ⓘ
  const allowed = hooksAllowed(g, ctx.rules);
  if (allowed) {
    let hook = blocks.find((b) => b.k === "hook");
    if (!hook) {
      hook = { k: "hook", label: GEAR_LINES.find(([k]) => k === "hook")![1], values: [], terms: [], cites: [] };
      const after = blocks.findIndex((b) => b.k === "weight");
      blocks.splice(after < 0 ? blocks.length : after, 0, hook);
    }
    hook.values.push(allowed.says); hook.terms.push(tip(allowed.terms));
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
export function buildGearTop(inp: RegsInputs, ctx: GearCtx, anyway = false): Omit<GearVM, "bait" | "src"> {
  // (T227) `anyway`: the day's gear even on a day the game-fish rules close (salmon may be fished there)
  const foot = anyway ? null : closedFoot(inp, ctx), g = gearAnswer(inp);
  if (foot || !g) return { closed: foot?.[0] ?? null, closedMore: foot?.slice(1) ?? [], day: null, mode: null, lines: [] };
  return { closed: null, closedMore: [], day: dayLine(ctx), ...gearLines(g, ctx) };
}
