/**
 * The caveat blocks page v36 draws between the strip and the rows on an open day (T15b): "Fishing is different in some
 * spots today" (rules of parts the map can't draw), "⚠ On one side of the channel only", "⚠ Closed to some anglers",
 * "⚠ At certain times" and "? Check: rules for parts of this water that couldn’t be placed on the map".
 *
 * Owner rule 2026-10-21 (decisions.md): every answer here is the PIPELINE's — the undrawn-part rules and whether each is
 * in force today are the stored ladder verdicts (`not_yet_mapped`, T07 `settled.inpart`), the one-side and angler
 * closures are the stored `beside` / `speaks` lists (T07 `settled.side` / `anglerclosed`), each moment's open/closed and
 * its closures are the stored display frames (`status`, decided `closing`), each fish's answer per moment is the stored
 * rows answer (`rows_decided`), the unplaced rules are the store's `item.unresolved`. This file selects them (by family,
 * by moment) and words them; it decides no closure, no answer and no placement. Its one choice is page v36's wording
 * choice of which of a closed moment's stored closures to NAME (those closing every game fish asked, else the one
 * closing the most) — the closed moment itself is the frame's.
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): page_v36 inPartHtml, sideHtml, anglerClosures, timedHtml, uncertainHtml,
 * momentTxt, answerTxt, daysTxt, hoursTxt, renderCard (order, open day only); prototype citeBooks (a Rule
 * book link after each rule button).
 */
import type { Moment } from "../answers.generated";
import { WEEKDAYS } from "../calendar.generated";
import { mainRes, rowsAt, type HydRes } from "./card";
import { closedGate, describe } from "./describe";
import type { RegsInputs, RuleView } from "./inputs";
import { text, type Line } from "./line";
import type { CaveatItemVM, CaveatVM, Day } from "./model";
import { cap, clean, gameFish, spName, type SpeciesBook } from "./names";
import { makePlace } from "./place";
import type { Settled } from "./settle";
import { daySegments, tapDay } from "./tap";
import { fmtDay } from "./strip";
import { sameDay } from "../day";
import { timeCond, winTxt } from "./time";
import { ruleBook } from "./book";
import { WD3, WORDS, pageWaterNoun } from "./words";

export type { CaveatVM } from "./model";

/** The families the Keep tab's caveats speak for (renderCard). */
export const KEEP_FAMS: readonly string[] = Object.freeze(["retention", "access"]);

const family = (r: RuleView): string => r.export.family;
const sp = (inp: RegsInputs): SpeciesBook => inp.statics.exportSpecies;
const parts = (r: RuleView): Readonly<Record<string, string>> => r.export.parts ?? {};

/** page v36 daysTxt: days of the week in words — "Mon–Fri", "Sat–Sun", "Fri–Sun", "Sat" (runs, Monday first). */
export function daysTxt(wds: readonly string[]): string {
  const ix = wds.map((d) => WEEKDAYS.indexOf(d as (typeof WEEKDAYS)[number])).sort((a, b) => a - b), runs: [number, number][] = [];
  ix.forEach((i) => { const r = runs[runs.length - 1]; if (r && r[1] === i - 1) r[1] = i; else runs.push([i, i]); });
  return runs.map(([a, b]) => (a === b ? WD3[a]! : `${WD3[a]}–${WD3[b]}`)).join(", ");
}

/** page v36 hoursTxt: a moment's hours window as timeCond says it. */
export const hoursTxt = (h: NonNullable<Moment["hours"]>): string => timeCond({ hours: h });

/** page v36 momentTxt: a moment in words — its weekdays (none when all 7) and its hours ("other hours" outside them). */
export function momentTxt(m: Moment | null | undefined): string {
  if (!m) return "";
  const d = m.weekdays.length === 7 ? "" : daysTxt(m.weekdays);
  return [d, m.hours ? (m.hours.in ? hoursTxt(m.hours) : WORDS.OTHER_HOURS) : ""].filter(Boolean).join(", ");
}

/** page v36 answerTxt: a fish's stored answer in one or two words. */
export function answerTxt(d: HydRes | null): string {
  if (!d) return WORDS.NO_RULE;
  return d.status === "keep" ? `${d.daily}${WORDS.A_DAY}` : d.status === "nolimit" ? WORDS.ANSWER_NOLIMIT
    : d.status === "release" ? WORDS.ANSWER_RELEASE : WORDS.ANSWER_CLOSED;
}


/**
 * (T208) A rule held on an undrawn part the book gives an area for: an outlet or inlet area, or a part the book draws on a
 * map. It reads "Outlet area only (see map 1): …" with the map's synopsis page linked (the export's `not_yet_mapped.map`,
 * its printed page through the guide's printed → PDF map); the rule stays a note on the whole water, as before.
 */
export function areaOf(r: RuleView, inp: RegsInputs): CaveatItemVM["area"] | null {
  const part = r.part ?? "", mp = r.export.not_yet_mapped?.map ?? null;
  const edge = /\boutlet\b/i.test(part) ? WORDS.AREA_OUTLET : /\binlet\b/i.test(part) ? WORDS.AREA_INLET : null;
  if (!mp && !edge) return null;
  const lead = edge ?? WORDS.AREA_PART(pageWaterNoun(inp));
  return { tag: WORDS.AREA_ONLY(lead, mp ? mp.name : false),   // (T208) no map: never "see the map"
           map: mp ? [text(WORDS.AREA_MAP(mp.name)), { t: "book", printed: mp.page, pdf: inp.statics.pdfPages[mp.page] ?? null, bare: true }] : null };
}

const item = (what: Line, where: string | null, r: RuleView | null, fish: string | null = null): CaveatItemVM =>
  ({ what, where: where || null, rule: r ? r.key : null, fish, book: r ? ruleBook(r) : [] });

/** (T372) One entry of the in-part block, either family's: its item, whether it is in force on the day (the pipeline's
 *  answer: trout's stored ladder verdict, salmon's `n` runs), and whether it is a boat rule. */
export interface InPartEntry { readonly item: CaveatItemVM; readonly now: boolean; readonly boat?: boolean }

/**
 * page v36 inPartHtml over entries: parts of the water the map can't draw yet, those in force today first ("Other dates
 * (N)" folds the rest). (T372, owner 2026-11-03: one mechanism) trout's undrawn rules (`inPartOf`) and salmon's undrawn
 * DFO places (core `salmonCard`) both come through here: the same selection, words and VM. `noun`: the page's water noun.
 */
export function inPartBlock(u: readonly InPartEntry[], noun: string): CaveatVM | null {
  if (!u.length) return null;
  const now = u.filter((x) => x.now), later = u.filter((x) => !x.now);
  const hot = now.some((x) => !x.boat), onlyBoat = now.length > 0 && now.every((x) => x.boat);
  const items = (now.length ? now : later).map((x) => x.item), rest = now.length && later.length ? later.map((x) => x.item) : null;
  // (T208) the block's areas, said on its folded head so a rule of one area is never only behind the fold
  const areas = [...new Set([...items, ...(rest ?? [])].flatMap((x) => (x.area ? [x.area.tag] : [])))];
  return {
    kind: "inpart", hot,
    title: hot ? WORDS.INPART_HOT : onlyBoat ? WORDS.INPART_BOAT : WORDS.INPART_SOME,
    calm: hot ? WORDS.CALM_HOT : onlyBoat ? WORDS.CALM_BOAT(noun) : WORDS.CALM_LATER,
    items, later: rest,
    ...(areas.length ? { areas, areaNow: items.some((x) => !!x.area) && now.length > 0 } : {}),
  };
}

/** One undrawn-part rule as a caveat line: its words and dates ("No fishing · Sep 1–Dec 31"), where ("Where: Salmon Arm
 *  Bay, …"), and (T208) the area the book maps it in. */
function inPartItem(r: RuleView, inp: RegsInputs): CaveatItemVM {
  const p = cap(r.part ?? "").replace(/\(map ([A-Z])\)/, WORDS.MAP_IN_REGS);
  const where = /various|do not identify|buoys and signs/i.test(p) ? WORDS.WHERE_SIGNS : WORDS.WHERE(p);
  const words: Line = [text(clean(parts(r).what || r.export.label).replace(/ — in part:.*$/, "")),
    ...(r.wins.length ? [text(" · " + winTxt(r.wins).slice(2))] : [])];
  const area = areaOf(r, inp);
  return area ? { ...item(words, where, r), area } : item(words, where, r);
}

/**
 * (T670 G3) THE GEAR AND BOAT RULES OF AN UNDRAWN PART: the day's gear frame lists them (`GearAnswer.in_part`: in force on
 * the day, held on a part of the water no map draws — Mahood Lake's "Bait ban, Jul 1-Dec 31" within the signs at its
 * western tip, Cowichan Lake's 10 km/h within 60 m of shore, p.15). No screen read the list. Each is an entry of the same
 * in-part block as the keeping rules, in the same words (`inPartItem`: the rule's stored label, dates and part).
 */
export function gearInPart(inp: RegsInputs): readonly InPartEntry[] {
  const g = inp.frames?.gear;
  if (!g || "tidal" in g) return [];
  return [...new Set(g.in_part)].map((ix) => inp.rules.get(ix)).filter((r) => !r.missing)
    .map((r) => ({ item: inPartItem(r, inp), now: true, boat: family(r) === "vessel" }));
}

/** page v36 inPartHtml for the trout card: the stored undrawn-part rules (`settled.inpart`) of `fams` as entries, then
 *  (T670 G3) the day's gear and boat rules of an undrawn part (`gearInPart`), each rule once. (T623 F4)
 *  A CLOSURE of an undrawn part is the part-closed warning's (`partClosedOf`), never said twice. */
export function inPartOf(s: Settled, inp: RegsInputs, fams: readonly string[]): CaveatVM | null {
  const boat = (r: RuleView) => family(r) === "vessel";
  const kept = s.inpart.filter((x) => fams.includes(family(x.rule)) && !closedGate(x.rule))
    .map((x): InPartEntry => ({ item: inPartItem(x.rule, inp), now: x.now, boat: boat(x.rule) }));
  const said = new Set(kept.map((x) => x.item.rule));
  return inPartBlock([...kept, ...gearInPart(inp).filter((x) => !said.has(x.item.rule))], pageWaterNoun(inp));
}

/**
 * (T623 F4, owner 2026-11-11: "part-of-lake closures (Shuswap, Quesnel, every lake like them): a clear warning on the card
 * for all such lakes now") THE PART-CLOSED WARNING: every closure the pipeline holds on an undrawn part of this water
 * (the stored `not_yet_mapped` verdicts over the key's year, `settled.inpart`, that are closure gates — the answers' own
 * `facts.closure`), drawn at the top of the Keep tab on EVERY open day: a standing warning, its title saying whether one
 * is in force on the day shown (the stored verdict `now`), each closure's words, dates and part as the book prints them —
 * in force first, the rest under "Other dates". A closed day says nothing more (its banner closes the whole water), nor
 * does tidal water. Null when the water has none. Decides nothing: the rules, the part words, the day's status and
 * verdict are the pipeline's.
 */
export function partClosedOf(s: Settled, inp: RegsInputs, today: Day): CaveatVM | null {
  const st = s.status?.status;
  if (!st || st === "closed" || st === "tidal") return null;
  const shut = s.inpart.filter((x) => closedGate(x.rule));
  if (!shut.length) return null;
  const noun = pageWaterNoun(inp), now = shut.filter((x) => x.now), later = shut.filter((x) => !x.now);
  const when = sameDay(s.day, today) ? WORDS.TODAY_LC : WORDS.ON_DAY(fmtDay(s.day));
  return {
    kind: "partclosed", hot: now.length > 0,
    title: now.length ? WORDS.PART_CLOSED_NOW(noun, when) : WORDS.PART_CLOSED_LATER(noun),
    calm: WORDS.PART_CLOSED_CALM(noun),
    items: (now.length ? now : later).map((x) => inPartItem(x.rule, inp)),
    later: now.length && later.length ? later.map((x) => inPartItem(x.rule, inp)) : null,
  };
}

/** page v36 sideHtml: rules that hold on one side of the channel only. */
export function sideOf(s: Settled, fams: readonly string[]): CaveatVM | null {
  const u = s.side.filter((r) => fams.includes(family(r)));
  if (!u.length) return null;
  return { kind: "side", hot: false, title: WORDS.SIDE_TITLE, calm: null, later: null,
    items: u.map((r) => item([text(clean(parts(r).what || r.export.label))],
      cap(parts(r).side || WORDS.HALF_ONLY(String(r.f.side))) + (parts(r).where ? " · " + parts(r).where : ""), r)) };
}

/** page v36 anglerClosures: the day's angler closures that speak (some anglers may not fish here). */
export function anglerOf(s: Settled): CaveatVM | null {
  if (!s.anglerclosed.length) return null;
  return { kind: "angler", hot: false, title: WORDS.ANGLER_TITLE, calm: null, later: null,
    items: s.anglerclosed.map((r) => item([text(r.export.label)], null, r)) };
}

const describeShort = (r: RuleView, inp: RegsInputs) => cap(describe(r, sp(inp)).replace(/ \(.*\)$/, ""));

/**
 * page v36 timedHtml: the day reads differently by weekday or hour (the key has moments and the day ≥ 2 segments). The
 * card is the tap's own moment; each other moment of the day is named with what the store decides there: (a) a closed
 * moment while the card's is open — its stored closures; (b) a fish whose stored answer differs between the day's open
 * moments; (c) an angler closure that speaks at some of the day's moments, not the card's. `settledAtSeg(j)` is T07's
 * settle at segment j of the same day (its display frame and ladder).
 */
export function timedOf(inp: RegsInputs, s: Settled, settledAtSeg: (seg: number) => Settled): CaveatVM | null {
  const M = inp.year?.moments ?? null;
  if (!M || !inp.year || s.s < 0) return null;
  const ss = daySegments(inp.year.segStarts, tapDay(s.day, inp.calendarYear).day);
  if (ss.length < 2) return null;
  const cur = s.s, items: CaveatItemVM[] = [], R = inp.rules;
  const dispOf = (j: number) => inp.year!.displayBySeg[j] ?? null;
  const st = (j: number) => dispOf(j)?.status ?? null;
  const closing = (j: number) => { const f = dispOf(j); return f && f.status !== "tidal" ? f.closing : []; };

  // (a) a closure of some hours or some days: as the banner names them — those closing every game fish asked, else the
  // one closing the most — less any that also closes the card's own moment (it is on the card already)
  const asked = gameFish(sp(inp)).filter((S) => ((inp.statics.fish ?? []) as readonly string[]).includes(S));
  for (const j of ss) {
    if (j === cur || st(j) !== "closed" || st(cur) === "closed") continue;
    const cl = closing(j).map(([ix, fs]) => [R.get(ix), fs] as const);
    const all = cl.filter(([, fs]) => asked.every((S) => (fs as readonly string[]).includes(S)));
    const pick = (all.length ? all : [...cl].sort((a, b) => b[1].length - a[1].length).slice(0, 1)).map(([r]) => r);
    const whole = pick.filter((r) => !closing(cur).some(([ix]) => ix === r.rix));
    for (const r of whole) items.push(item([text(describeShort(r, inp))], momentTxt(M[j]), r));
    if (!whole.length) items.push(item([text(WORDS.MISSING_CLOSER(momentTxt(M[j])))], null, null));
  }

  // (b) a fish whose answer differs between the day's moments (a moment the water is closed at is said once, above)
  const place = makePlace(inp);
  const rs = ss.filter((j) => j === cur || st(j) !== "closed").map((j) => [j, rowsAt(inp, place, j)] as const);
  const spp = [...new Set(rs.flatMap(([, r]) => (r ? r.spp : [])))];
  const weekly = new Set(rs.flatMap(([, r]) => (r ? r.rows.flatMap((x) => x.raw.week?.species ?? []) : [])));
  for (const S of spp) {
    if (weekly.has(S)) continue;   // its row says the weekday window (rows[].week)
    const by = rs.map(([j, r]) => [j, r ? mainRes(r.R[S]) : null] as const);
    const words = by.map(([j, d]) => [j, answerTxt(d)] as const);
    if (new Set(words.map(([, t]) => t)).size < 2) continue;
    const grp = new Map<string, number[]>();
    words.forEach(([j, t]) => { const g = grp.get(t); if (g) g.push(j); else grp.set(t, [j]); });
    const line = [...grp].map(([t, js]) => `${t} ${daysTxt([...new Set(js.flatMap((j) => M[j]!.weekdays))])}`
      + (js.every((j) => M[j]!.hours) ? (js.every((j) => M[j]!.hours!.in) ? " " + hoursTxt(M[js[0]!]!.hours!) : " " + WORDS.OTHER_HOURS) : ""))
      .join("; ");
    const today = by.find(([j]) => j === cur)?.[1] ?? null;
    items.push(item([text(`${spName(sp(inp), S)}: ${line.replace(/ Mon–Sun/g, "")}`)],
      WORDS.TODAY_PAREN(momentTxt(M[cur])) + ": " + answerTxt(today), null, S));
  }

  // (c) an angler closure of some days: speaking at some of the day's moments, never at the card's own
  const spk = (j: number) => {
    const out = new Set<number>(), L = settledAtSeg(j).L;
    if (L) for (const o of L.values()) for (const [ix, v] of o.none) if (v.state === "speaks" && R.get(ix).kind === "anglerclosure") out.add(ix);
    return out;
  };
  const sets = ss.map((j) => [j, spk(j)] as const), every = new Set(sets.flatMap(([, x]) => [...x]));
  for (const ix of every) {
    const on = sets.filter(([, x]) => x.has(ix)).map(([j]) => j);
    if (on.length === ss.length || on.includes(cur)) continue;
    const r = R.get(ix);
    items.push(item([text(describeShort(r, inp))], r.tcond || daysTxt([...new Set(on.flatMap((j) => M[j]!.weekdays))]), r));
  }
  return items.length ? { kind: "timed", hot: false, title: WORDS.TIMED_TITLE, calm: null, items, later: null } : null;
}

/**
 * The Keep tab's caveat blocks (page v36 renderCard): only on a keyed, non-tidal day that is open, in the page's order —
 * in-part, one side, some anglers, certain times. (T378, 2026-11-04) The unplaced rules are the in-part block's: a rule
 * whose part is not drawn is `not_yet_mapped` (pipeline `undrawn_part`), carried on the water; page v36's separate
 * "couldn't be placed" block read a store field nothing fills, and is gone.
 */
export function caveatsOf(inp: RegsInputs, s: Settled, settledAtSeg: (seg: number) => Settled): readonly CaveatVM[] {
  const st = s.status?.status;
  if (!st || st === "closed" || st === "tidal") return [];
  return [inPartOf(s, inp, KEEP_FAMS), sideOf(s, KEEP_FAMS), anglerOf(s), timedOf(inp, s, settledAtSeg)]
    .filter((c): c is CaveatVM => c !== null);
}

/**
 * (T473) The offsets of a daylight-only closure, read from the item's own words ("60 min after sunset–60 min before
 * sunrise", the rule's `where`): minutes closed after sunset and before sunrise; null for any other timed item.
 */
export function daylightOffsets(x: CaveatItemVM): { after: number; before: number } | null {
  const what = x.what.map((r) => (r.t === "text" ? r.s : "")).join("").trim();
  const m = x.where ? /^(\d+) min after sunset\s*[–-]\s*(\d+) min before sunrise$/.exec(x.where.trim()) : null;
  return m && /^no fishing/i.test(what) ? { after: +m[1]!, before: +m[2]! } : null;
}

/**
 * (T318, owner 2026-10-31) A TIMED ITEM AS ONE LINE under the year bar (not a boxed note): a closure of the hours around
 * sunset ("No fishing for any game fish" · "60 min after sunset–60 min before sunrise") reads "Daylight only: closed from
 * 1 h after sunset to 1 h before sunrise"; any other timed item reads its words and its moment, "<what>: <when>".
 */
export function timedLineText(x: CaveatItemVM): string {
  const what = x.what.map((r) => (r.t === "text" ? r.s : "")).join("").trim();
  const d = daylightOffsets(x);
  if (d) return WORDS.TIMED_DAYLIGHT(WORDS.TIMED_H(d.after), WORDS.TIMED_H(d.before));
  return x.where ? `${what}: ${x.where}` : what;
}
