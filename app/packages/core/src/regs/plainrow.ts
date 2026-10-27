/**
 * The rows the Keep tab draws without a quota card (T19b): a release-only or closed row ("Char ↩ Release", "Steelhead ⊘
 * Closed") — its title and pill, the one sentence of its status through the year, the release row's fish with their
 * pictures, the steelhead presence line, the names a rule lists, the folded "How this was decided" (decided.ts), and
 * "See every regulation line ›".
 *
 * Owner rule 2026-10-21 (decisions.md): every answer here is the PIPELINE's — the row's kind (release / closed), its
 * members, its winning rule, its stored YEAR STRIP (rows 30 `rows[].year`, decided by `pipeline/deliver/answers/year.py`:
 * keep / release / closed per book day), the water's own closed days (the stored display frames, T07 `isClosedOn`), the
 * presence line, the week split and the restates notes. This file reads the strip as runs and WORDS them (page v36
 * runTxt / closedRuns / runUntil); it ranks no day and decides no status.
 *
 * SEAMS (sibling lanes, not on this branch when written — never duplicated here): `rowTitle` (T17 grouphead.ts),
 * `pictures` / `fishName` (T18 tree.ts, the fish manifest), `runOf` / `bookIndex` / `BOOK_DAYS` (T13 strip.ts), `more`
 * (T19's MORE word: "See every regulation line ›"). T21 binds them once (`PlainRowSeams`).
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): page_v36 rowHtml (non-pool head), valueHtml (the pill), summaryHtml (the
 * `!row.pool` branch), runTxt, closedRuns, runUntil, nextYr, namedIn, presHtml, weekHtml; prototype buildKeep krel28 (the
 * release row's fish line), the .lres drop for a week row, the restates notes.
 */
import { readingDay } from "../calendar.generated";
import type { CardModel, HydRow, YearRun } from "./card";
import { ladderOf, withRestates, type LadderSeams } from "./decided";
import type { RegsInputs } from "./inputs";
import { text, type Line, type Run } from "./line";
import type { Day, FishPicVM, LadderVM, PlainRowVM } from "./model";
import { cap, clean, spName } from "./names";
import type { Settled } from "./settle";
import { isClosedOn } from "./settle";
import type { SheetKey } from "./sheetKey";
import { fmtMd, rangeTxt, winTxt, type Md } from "./time";
import { bookLine } from "./book";
import { RUN_NOW, RUN_THEN, WORDS } from "./words";

/** T13 strip.ts `Run` (page v36 runOf over a 365-long day-state array). */
export interface YearRunOf { readonly now: string; readonly all: boolean; readonly from?: Day; readonly until?: Day; readonly next?: Day; readonly then?: string }

/** The sibling seams a plain row reads (see the file header). */
export interface PlainRowSeams extends LadderSeams {
  /** T18 `pictures(codes, FISH, fishName)` bound to the manifest. */
  readonly pictures: (codes: readonly string[]) => readonly FishPicVM[];
  /** T18 / prototype fishName: the species book's name, else the picture's caption, else the code. */
  readonly fishName: (code: string) => string;
  /** T18 / prototype: a fish with a name or a picture (krel28 lists only those). */
  readonly hasFish: (code: string) => boolean;
  /** T13 runOf / bookIndex / BOOK_DAYS. */
  readonly runOf: (perDay: readonly string[], d: Day) => YearRunOf;
  readonly bookIndex: (d: Day) => number;
  readonly BOOK_DAYS: readonly Day[];
  /** T19 words MORE: "See every regulation line ›". */
  readonly more: string;
}
export interface PlainRowEnv { readonly inp: RegsInputs; readonly card: CardModel; readonly settled: Settled; readonly seams: PlainRowSeams }

/** A row page v36 draws without a quota card and without its legacy pooled path: no pool, no group headline. */
export const isPlainRow = (row: HydRow): boolean => !row.pool && !row.raw.group?.headline;

const md = (d: Day): Md => d.month * 100 + d.day;
const perDay = (runs: readonly YearRun[]): string[] => runs.flatMap(([st, n]) => Array<string>(n).fill(st));

/** The year's words over a row's stored strip and the water's stored closed days (page v36 runTxt / closedRuns / runUntil). */
export class YearWords {
  private readonly water: string[];
  constructor(inp: RegsInputs, private readonly seams: PlainRowSeams) {
    this.water = seams.BOOK_DAYS.map((d) => (isClosedOn(inp, readingDay(d.month, d.day)) ? "closed" : "keep"));
  }
  /** The water's own stored closed state of a book day (page v36 waterSegs, per day). */
  closedOn(i: number): boolean { return this.water[i] === "closed"; }
  /** page v36 nextYr: " next year" when the day comes before the day read. */
  nextYr(d: Day, today: Day): string { return this.seams.bookIndex(d) < this.seams.bookIndex(today) ? WORDS.NEXT_YEAR : ""; }
  /** page v36 runUntil: the last day of today's run, "" when it runs all year. */
  runUntil(sg: readonly string[], today: Day): string {
    const r = this.seams.runOf(sg, today);
    return r.all ? "" : fmtMd(md(r.until!)) + this.nextYr(r.until!, today);
  }
  /** page v36 closedRuns: the strip's closed runs ("All year.", a New-Year-spanning run as one, the whole water's own). */
  closedRuns(sg: readonly string[]): string {
    const runs: [number, number][] = [];
    sg.forEach((st, i) => { if (st !== "closed") return; const r = runs[runs.length - 1]; if (r && r[1] === i - 1) r[1] = i; else runs.push([i, i]); });
    if (!runs.length) return "";
    if (runs.length === 1 && runs[0]![0] === 0 && runs[0]![1] === 364) return WORDS.ALL_YEAR_SENT;
    if (runs.length > 1 && runs[0]![0] === 0 && runs[runs.length - 1]![1] === 364) { const last = runs.pop()!; runs[0] = [last[0], runs[0]![1]]; }
    const D = this.seams.BOOK_DAYS;
    return runs.map(([a, b]) => {
      const whole = this.water[a] === "closed" && this.water[(b + 365) % 365] === "closed";
      const r = rangeTxt(md(D[a]!), md(D[(b + 365) % 365]!));
      return whole ? WORDS.WHOLE_CLOSED(r) : WORDS.CLOSED_RANGE(r);
    }).join(" ");
  }
  /** page v36 runTxt: today's run and what follows, until the fish may be kept again. */
  runTxt(sg: readonly string[], today: Day, tail: boolean): string {
    const { runOf } = this.seams, r = runOf(sg, today);
    if (r.all) return tail ? "" : r.now === "keep" ? "" : WORDS.ALL_YEAR_SENT;
    if (r.now === "keep") return WORDS.KEEP_UNTIL_THEN(fmtMd(md(r.until!)), RUN_THEN[r.then!]!, fmtMd(md(r.next!)));
    let out = tail ? "" : WORDS.NOW_UNTIL(RUN_NOW[r.now]!, fmtMd(md(r.until!)) + this.nextYr(r.until!, today)), cur = r, k = 0;
    const wClosed = (d: Day) => this.water[this.seams.bookIndex(d)] === "closed";
    while (cur.then !== "keep" && k++ < 3) {
      const nx = runOf(sg, cur.next!);
      if (nx.all) break;
      out += nx.now === "closed" && wClosed(nx.from!) ? WORDS.THEN_WHOLE(rangeTxt(md(nx.from!), md(nx.until!)))
        : WORDS.THEN_RANGE(RUN_THEN[nx.now]!, rangeTxt(md(nx.from!), md(nx.until!)));
      cur = nx;
      if (md(nx.next!) === md(r.from!)) break;
    }
    if (cur.then === "keep") out += WORDS.KEEP_FROM(fmtMd(md(cur.next!)) + this.nextYr(cur.next!, today));
    else if (!sg.includes("keep")) out += WORDS.NO_KEEP_TAIL;
    return out.trim();
  }
}

/**
 * A group's year window (prototype round UR5 `.gyear`, lifted from page v36's per-row strip): "Through the year" and page
 * v36 runTxt over the row's STORED year strip ("You can keep them until Oct 31. Then release only from Nov 1."). Null
 * where page v36 draws no strip under the row (miniStrip): a one-run strip, or one that only repeats the water's own
 * closed days. Words and a draw-or-not over two stored answers (the row's year, the water's closed days); decides nothing.
 */
export function yearWindowOf(row: HydRow, inp: RegsInputs, today: Day, seams: PlainRowSeams): { label: string; says: string } | null {
  if (row.year.length < 2) return null;
  const sg = perDay(row.year), water = new YearWords(inp, seams);
  const base = row.year.find(([st]) => st !== "closed")?.[0] ?? "closed";
  if (sg.every((st, i) => st === (water.closedOn(i) ? "closed" : base))) return null;
  const says = water.runTxt(sg, today, false);
  return says ? { label: WORDS.THROUGH_YEAR, says } : null;
}

/**
 * (T36) page v36 miniStrip's words: under a row page v36 draws a mini strip for (the same draw-or-not as `yearWindowOf`),
 * its short keeping windows (shortWindows: each stored `keep` run of 14 days or fewer) — "Keeping allowed only Apr 1–3 and
 * Jul 1–2". Null when no strip is drawn or no run is that short. Words over the stored year; decides nothing.
 */
export function shortWindowsOf(row: HydRow, inp: RegsInputs, seams: PlainRowSeams): string | null {
  if (row.year.length < 2) return null;
  const sg = perDay(row.year), water = new YearWords(inp, seams);
  const base = row.year.find(([st]) => st !== "closed")?.[0] ?? "closed";
  if (sg.every((st, i) => st === (water.closedOn(i) ? "closed" : base))) return null;
  const D = seams.BOOK_DAYS, out: string[] = [];
  let d = 0;
  for (const [st, n] of row.year) { if (st === "keep" && n <= 14) out.push(rangeTxt(md(D[d]!), md(D[d + n - 1]!))); d += n; }
  return out.length ? WORDS.KEEP_ONLY(out.join(WORDS.AND)) : null;
}

/** page v36 summaryHtml (`!row.pool`): the row's status through the year in one sentence, over its stored strip. */
export function runSentence(row: HydRow, inp: RegsInputs, today: Day, seams: PlainRowSeams): string {
  const y = new YearWords(inp, seams);
  const sg = row.members.length ? perDay(row.year) : null;
  const never = !!sg && !sg.includes("keep");
  const un = sg && !never ? y.runUntil(sg, today) : "";
  const rt = sg ? (never ? ((row.kind === "closed" ? "" : WORDS.NEVER_KEEP) + y.closedRuns(sg)).trim() : y.runTxt(sg, today, true)) : "";
  const until = un ? WORDS.UNTIL(un) : "";
  return (row.kind === "closed" ? WORDS.SUM_CLOSED(until) : WORDS.SUM_RELEASE(until))
    + (rt ? " " + rt : un ? "" : row.win?.wins.length ? " " + cap(winTxt(row.win.wins).slice(2)) + "." : "");
}

/** page v36 namedIn: the fish a rule's own words list ("… including: a, b and c"). */
export const namedIn = (verbatim: string): string => {
  const m = clean(verbatim).match(/includ\w*:?\s*(.+)$/i);
  return m ? m[1]!.replace(/\s*·\s*/g, ", ").replace(/\.$/, "") : "";
};

const term = (inp: RegsInputs, id: string): Run[] =>
  inp.statics.glossary.terms.some((t) => t.id === id) ? [{ t: "term", ids: [id] }] : [];

/** page v36 weekHtml: a weekday split in words, each day-part with its rule's Rule book pages, and the restates notes. */
function weekOf(row: HydRow, inp: RegsInputs): PlainRowVM["week"] {
  const w = row.raw.week;
  if (!w) return null;
  const book = (rixs: readonly number[]): Line => rixs.flatMap((ix, i) => [...(i ? [text(" ")] : []), ...bookLine(inp.rules.get(ix).facts?.book ?? [], inp.rules.get(ix).facts?.book_box)]);
  return {
    status: w.status ?? null, says: w.says + ".",
    parts: (w.parts ?? []).map((p) => ({ days: p.days, says: p.says + (p.today ? " " + WORDS.TODAY_MARK : ""), today: p.today, book: book(p.rules) })),
    notes: (row.raw.restates ?? []).map((x) => [text(x.says), ...(bookLine(inp.rules.get(x.r).facts?.book ?? [], inp.rules.get(x.r).facts?.book_box).length ? [text(" "), ...bookLine(inp.rules.get(x.r).facts!.book, inp.rules.get(x.r).facts!.book_box)] : [])]),
  };
}

/** One plain row (rows[rowIx] of the card): the VM T27 draws. */
export function plainRowOf(rowIx: number, env: PlainRowEnv): PlainRowVM {
  const { inp, card, settled, seams } = env;
  const row = card.rows[rowIx]!;
  const sp = inp.statics.exportSpecies;
  const pill: PlainRowVM["pill"] = row.kind === "closed" ? { kind: "closed", text: WORDS.PILL_CLOSED, term: "no_fishing_for" }
    : row.kind === "release" ? { kind: "release", text: WORDS.PILL_RELEASE, term: "catch_and_release" } : null;
  const named = row.prot ? row.prot.map((c) => spName(sp, c)) : !row.members.length && row.win ? namedIn(row.win.export.verbatim).split(/,\s*/).filter(Boolean) : [];
  const cs = row.members.filter(seams.hasFish);
  const release = !row.raw.group?.headline && row.kind === "release" && cs.length
    ? { names: cs.map(seams.fishName).join(WORDS.NAMES_SEP), pictures: seams.pictures(cs) } : null;
  const p = row.raw.presence;
  let ladder: LadderVM = withRestates(ladderOf(row, { card, settled, inp, seams }), row, inp);
  if (row.raw.week) ladder = { ...ladder, sections: ladder.sections.map((s) => ({ ...s, result: null })) };   // prototype: a week row drops .lres
  return {
    row: rowIx, title: seams.rowTitle(row, inp),
    pill: pill && { ...pill, term: term(inp, pill.term).length ? pill.term : "" },
    scope: null,
    sum: row.raw.week ? null : runSentence(row, inp, settled.day, seams),
    week: weekOf(row, inp),
    presence: p ? [text(p.says), ...(term(inp, "steelhead").length ? [text(" "), ...term(inp, "steelhead")] : [])] : null,
    names: named.length ? named.join(WORDS.NAMES_SEP) : null,
    whichFish: named.length > 5 ? WORDS.WHICH_FISH(named.length) : null,
    release, ladder,
    more: { text: seams.more, open: `src:r${rowIx}` as SheetKey },
  };
}
