/**
 * The banner drawn in place of the answers on a closed day (T14): the headline, the run sentence, the chain of closures
 * that together cover the run (each with its level and Rule book page), the reopening link — after the prototype's "say
 * each sentence once" pass against the year strip — and "Closed all year on N stretches"; and the tidal banner.
 *
 * Owner rule 2026-10-21 (decisions.md): every answer drawn here is the PIPELINE's —
 * - closed on a day, and the closures that close it, are the display frame's `status` and decided `closing` (one frame
 *   per book day: `inp.year.displayByDay`); "unless opened" is the frame's decided `unless_opened`;
 * - the day it reopens is the first day whose stored frame is not "closed" (T13 `nextOpen`, a read);
 * - each closure's words are its stored facts (`describe`, T06) and `facts.unless_opened`; its level is the part's rank.
 * Nothing here decides a closure. `closersAt` only SELECTS which of a day's stored closures to list (those naming the most
 * game fish, and any not wholly inside a longer one — a one-fish closure beside them closes nothing more), and the chain
 * lists each once, in day order.
 *
 * Ports (inventory names @ 7fb6809f): page_v36 closersAt, closedBanner (+ closeTxt), mergedHtml, tidalHtml, waterSegs;
 * prototype buildKeep's "say each sentence once" pass and the closechain rewrite (with its integration-1 fix: a sentence
 * the rewrite would leave with no words is kept whole).
 */
import { describe } from "./describe";
import type { DisplayFrame, RegsInputs, RuleView } from "./inputs";
import type { Line, Run } from "./line";
import type { BannerVM, Day, StripVM } from "./model";
import { gameFish } from "./names";
import type { Settled } from "./settle";
import { BOOK_DAYS, bookIndex, displayOnBookDay, fmtDay, nextOpen, runOf, waterSegs } from "./strip";
import { rangeTxt } from "./time";
import { RANK_LEVEL, WORDS } from "./words";

/** A rule's Rule book pages as book runs, one space apart (prototype bookLinks). */
export function bookLine(r: RuleView | null): Line {
  const out: Run[] = [];
  for (const [printed, pdf] of r?.facts?.book ?? []) {
    if (out.length) out.push({ t: "text", s: " " });
    out.push({ t: "book", printed, pdf });
  }
  return out;
}

/**
 * page closersAt over one day's stored display frame: of its decided `closing`, the ones naming the most fish (`top`) —
 * and, when no one closure names every game fish any of them names, each not wholly inside a longer one.
 */
export function closersAt(inp: RegsInputs, frame: DisplayFrame | null): readonly RuleView[] {
  const cl = frame && frame.status === "closed" ? frame.closing : [];
  const top = Math.max(0, ...cl.map(([, fs]) => fs.length));
  const all = gameFish(inp.statics.exportSpecies).filter((S) => cl.some(([, fs]) => (fs as readonly string[]).includes(S))).length;
  return cl.filter(([, fs]) => fs.length === top
      || (top < all && !cl.some(([, gs]) => gs.length > fs.length && fs.every((S) => (gs as readonly string[]).includes(S)))))
    .map(([ix]) => inp.rules.get(ix));
}

/** The banner before "say each sentence once" (page v36 closedBanner / tidalHtml + the closechain rewrite). */
export interface RawBanner {
  readonly kind: BannerVM["kind"];
  readonly big: string | null; readonly para: string | null; readonly golink: string | null;
  readonly chain: BannerVM["chain"]; readonly reopen: Day | null; readonly merged: BannerVM["merged"];
}

/** prototype closechain rewrite: the shared "No fishing for any game fish" goes (the run sentence says it), "from streams,"
 *  is capitalised; a sentence left with no words is kept whole. */
const LEAD = new RegExp(`^${WORDS.NO_FISHING_FOR}${WORDS.ANY_GAME},?\\s*`);
const STREAMS = new RegExp(`^${WORDS.FROM_STREAMS.trim()},\\s*`);
export function chainText(t0: string): string {
  const t1 = t0.replace(LEAD, "").replace(STREAMS, WORDS.FROM_STREAMS_CAP);
  return (/[A-Za-z]/.test(t1) ? t1 : t0).trim();
}

/** page RANK[rank].t: the level a rule speaks at, as this part ranks it. */
export const levelOf = (r: RuleView): string => RANK_LEVEL[String(r.rank)] ?? "";

/** page mergedHtml: several stretches closed all year that are one picker choice, each with the closure that closes it. */
export function mergedOf(inp: RegsInputs): BannerVM["merged"] {
  const g = inp.item?.picker?.choices.find((c) => c.parts.includes(inp.partIx));
  if (!g || !g.closed || g.parts.length < 2 || !inp.mergedParts) return null;
  return {
    label: WORDS.MERGED(g.parts.length, g.sections),
    items: inp.mergedParts.map((p) => ({
      text: `${p.label}${WORDS.DOT}${p.sections} ${p.sections > 1 ? WORDS.SECTIONS : WORDS.SECTION}`,
      level: p.broad ? levelOf(p.broad) : null,
      rule: p.broad?.key ?? null, book: bookLine(p.broad),
    })),
  };
}

/** The chain of closures over the closed run holding `day`, and the stored `unless_opened` rules met on it (page
 *  closedBanner): every day of the run, first to last, its `closersAt` appended in order, each once. */
export function chainOf(inp: RegsInputs, settled: Settled, day: Day, reopen: Day | null):
  { rules: readonly RuleView[]; unless: ReadonlySet<number>; run: { from: Day; until: Day } | null } {
  const unless = new Set<number>();
  const mark = (f: DisplayFrame | null) => { if (f && f.status !== "tidal") for (const ix of f.unless_opened ?? []) unless.add(ix); };
  mark(settled.status);
  const rules: RuleView[] = [];
  const run = reopen ? runOf(waterSegs(inp), day) : null;
  if (run && !run.all) {
    let i = bookIndex(run.from!);
    const end = bookIndex(run.until!);
    for (let g = 0; g < 366; g++) {
      const f = displayOnBookDay(inp, i);
      mark(f);
      for (const r of closersAt(inp, f)) if (!rules.some((x) => x.rix === r.rix)) rules.push(r);
      if (i === end) break;
      i = (i + 1) % BOOK_DAYS.length;
    }
  }
  return { rules, unless, run: run && !run.all ? { from: run.from!, until: run.until! } : null };
}

/** page closedBanner (closed day) or tidalHtml (tidal water), before the dedupe; null where neither is drawn. */
export function rawBanner(inp: RegsInputs, settled: Settled): RawBanner | null {
  const f = settled.status;
  if (inp.key?.tidal) {
    if (f?.status !== "tidal") return null;   // page: missing(…) — the store always carries the tidal frame
    return { kind: "tidal", big: WORDS.TIDAL_WATER, para: f.note, golink: null, chain: [], reopen: null, merged: null };
  }
  if (f?.status !== "closed") return null;
  const merged = mergedOf(inp);
  const b = settled.broad[0];
  if (!b) return { kind: "closed", big: WORDS.STRIP_CLOSED, para: null, golink: null, chain: [], reopen: null, merged };
  const sp = inp.statics.exportSpecies, day = inp.day;
  const reopen = nextOpen(inp, day);
  const { rules, unless, run } = chainOf(inp, settled, day, reopen);
  const closeTxt = (r: RuleView) => unless.has(r.rix) ? (r.facts?.unless_opened || WORDS.NO_WORDS(r.key)) : describe(r, sp) + ".";
  const list = (rules.length ? rules : [b])
    .filter((r, i, a) => a.findIndex((x) => describe(x, sp) === describe(r, sp) && x.rank === r.rank) === i);
  return {
    kind: "closed",
    big: reopen ? WORDS.CLOSED_OPENS_AGAIN(fmtDay(reopen)) : WORDS.CLOSED_ALL_YEAR,
    para: reopen && run ? WORDS.NO_FISHING_RUN(rangeTxt(md(run.from), md(run.until))) + (list.length > 1 ? WORDS.TOGETHER : "") : null,
    golink: reopen ? WORDS.OPENS_AGAIN_LINK(fmtDay(reopen)) : null,
    chain: list.map((r) => ({ text: chainText(closeTxt(r)), scope: levelOf(r), rule: r.key, book: bookLine(r) })),
    reopen, merged,
  };
}
const md = (d: Day) => d.month * 100 + d.day;

/** prototype "say each sentence once": the strip's words first, then the headline's phrases, the paragraphs and the link's
 *  sentences — each said before (case, punctuation and "again" aside) is dropped; a headline with nothing left goes. */
export function sayOnce(strip: StripVM | null, raw: RawBanner): BannerVM {
  const seen = new Set<string>();
  const key = (t: string) => t.toLowerCase().replace(/\bagain\b/g, "").replace(/[^a-z0-9]+/g, " ").trim();
  const fresh = (t: string) => { const k = key(t); if (!k) return true; if (seen.has(k)) return false; seen.add(k); return true; };
  for (const l of strip?.line ?? []) fresh(l.text);
  const big = raw.big === null ? [] : (() => { const ph = raw.big.split(" · ").filter(fresh); return ph.length ? [ph.join(" · ")] : []; })();
  const paras = raw.para !== null && fresh(raw.para) ? [raw.para] : [];
  const reopen = raw.golink !== null && raw.reopen
    ? { text: raw.golink.split(/(?<=\.)\s+/).filter(fresh).join(" "), day: raw.reopen } : null;
  return { kind: raw.kind, big, paras, chain: raw.chain, reopen, merged: raw.merged };
}

/** The banner of the selected day `inp.day`, said once against its year strip; null where none is drawn. */
export function bannerOf(inp: RegsInputs, settled: Settled, strip: StripVM | null): BannerVM | null {
  const raw = rawBanner(inp, settled);
  return raw && sayOnce(strip, raw);
}
