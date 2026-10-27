/**
 * (T216) "How this was decided", for an angler: the rules that applied to this row today, in layers — the province's
 * and the region's base rules, an area's, then this water's own row (or the row of the river it flows into), then the
 * exceptions (federal law, a rule on part of the water) — each with what it did today, why, and its printed page.
 *
 * Owner rule 2026-10-21: every role is the PIPELINE's (`rows_decided.roles` [rix, role, by] and the stored lifts,
 * gathered by decided.ts rowSources). This file decides nothing: it groups the stored items by the level their rule
 * speaks at and words each stored role. A "why" names the stored `by` rule by its level and says which of the book's
 * plain principles that pairing is (a closure beats an opening, release beats keep, the more local rule beats the
 * region, else the stricter one wins) — read off the two rules' stored kind and rank, never re-ranked.
 */
import type { LadderItem } from "./decided";
import { closedGate } from "./describe";
import type { RegsInputs, RuleView } from "./inputs";
import { text, type Line } from "./line";
import type { DecidedLayerVM, DecidedStepVM, DecidedStoryVM } from "./model";
import { ruleBook } from "./book";
import { DECIDE as D } from "./decidedWords";
import { QORDER } from "./decided";

/** A region code as a name ("3" → "Region 3", "7a" → "Region 7A"); a zone keeps its own name (T190). */
const regionName = (r: RuleView): string => {
  const ent = r.export.prov.entry_name || "";
  if (/^(Region|Zone) /.test(ent)) return ent;
  const code = r.export.prov.region;
  return code ? `Region ${code.toUpperCase()}` : ent;
};

/** A rule named by its level, for "why": "Fraser River’s own rule", "The Region 3 rule", "The B.C.-wide rule". */
function who(b: RuleView | null, water: string): string {
  if (!b || b.missing) return D.WHO_ABOVE;
  const ent = b.export.prov.entry_name || water;
  return b.baseRank === -1 ? D.WHO_FEDERAL : b.baseRank <= 1 ? D.WHO_OWN(ent) : b.baseRank === 2 ? D.WHO_AREA(ent)
    : b.baseRank === 3 ? D.WHO_REGION(regionName(b)) : D.WHO_BC;
}

/** Why a rule lost to `b` (role "replaced"): the book's principle the pair shows. */
function overridden(r: RuleView, b: RuleView | null, water: string): string {
  const w = who(b, water);
  if (!b || b.missing) return D.OV_STRICTER(w);
  if (b.rank === -1 && r.rank !== -1) return D.OV_FEDERAL(w);
  if (closedGate(b) && !closedGate(r)) return D.OV_CLOSURE(w);
  if (b.kind === "gate" && r.kind !== "gate") return D.OV_RELEASE(w);
  if (b.rank < r.rank) return b.rank <= 1 ? D.OV_OWN(w) : b.rank === 2 ? D.OV_AREA(w) : D.OV_REGION(w);
  return D.OV_STRICTER(w);
}

/** Why a deciding rule decides (role "governs"). */
function decides(r: RuleView): string {
  if (r.kind === "gate") return closedGate(r) ? D.WHY_CLOSED : D.WHY_RELEASE;
  if (r.rank === -1) return D.WHERE_FEDERAL;
  if (r.rank === 0) return D.WHY_OWN;
  if (r.rank === 1) return D.WHY_DOWN(r.export.prov.entry_name || "");
  if (r.rank === 2) return D.WHY_AREA;
  return D.WHY_BASE;
}

const WON: Readonly<Record<string, readonly [string, string]>> = Object.freeze({
  contains: [D.V_APPLIES, D.WHY_CONTAINS], narrows: [D.V_LOWER, D.WHY_NARROWS], also: [D.V_APPLIES, D.WHY_ALSO],
  floor: [D.V_APPLIES, D.WHY_FLOOR], limit: [D.V_CAP, D.WHY_LIMIT], season: [D.V_APPLIES, D.WHY_MORE],
  possession_cap: [D.V_APPLIES, D.WHY_MORE], duty: [D.V_APPLIES, D.WHY_MORE], possession: [D.V_APPLIES, D.WHY_POSSESSION],
});
/** A step's reading order inside its layer: the deciding roles (QORDER), then lifted, replaced, moot, not needed, same. */
const LOST = ["lifted", "replaced", "moot", "falls", "agrees"];
const orderOf = (role: string): number => {
  const i = QORDER.indexOf(role), j = LOST.indexOf(role);   // read at call time: decided.ts imports this file
  return i >= 0 ? i : j >= 0 ? QORDER.length + j : QORDER.length + LOST.length;
};

/** The step one stored item reads as. `says` is the ladder's line for it (decided.ts `words`). */
function stepOf(it: LadderItem, says: Line, water: string): DecidedStepVM {
  const r = it.r, b = it.by && !it.by.missing ? it.by : null;
  const byLine = (): Line => (b ? [text(it.role === "lifted" ? D.WHERE_LIFTED_BY : D.WHERE_BY), text(b.facts?.plain || b.export.label), ...(ruleBook(b).length ? [text(" "), ...ruleBook(b)] : [])] : []);
  let verdict: string, why: string, tone: DecidedStepVM["tone"] = "won", by: Line = [];
  switch (it.role) {
    case "governs": verdict = D.V_DECIDES; why = decides(r); break;
    case "replaced": verdict = D.V_OVERRIDDEN; why = overridden(r, it.by, water); tone = "lost"; by = byLine(); break;
    case "lifted": verdict = D.V_LIFTED; why = b && b.rank < r.rank ? D.LIFT_OWN(who(b, water)) : D.LIFT(who(it.by, water)); tone = "lost"; by = byLine(); break;
    case "moot": verdict = D.V_MOOT; why = D.MOOT(who(it.by, water)); tone = "lost"; by = byLine(); break;
    case "falls": verdict = D.V_UNNEEDED; why = D.UNNEEDED(who(it.by, water)); tone = "lost"; by = byLine(); break;
    case "agrees": verdict = D.V_SAME; why = D.SAME(who(it.by, water)); tone = "same"; break;
    default: [verdict, why] = WON[it.role] ?? [D.V_APPLIES, D.WHY_MORE];
  }
  const where: string[] = [];
  if (r.part) where.push(D.WHERE_PART(r.part));
  if (r.rank === -1 && it.role !== "governs") where.push(D.WHERE_FEDERAL);
  if (r.f.source === "dfo") where.push(D.WHERE_DFO);
  return { says, verdict, tone, why, where, book: ruleBook(r), by, rule: r.key };
}

/**
 * The story of one row: its stored items grouped into layers (province, each region, an area, the river it flows into,
 * this water's own row, then federal law and rules on part of the water), each layer's steps in reading order; the
 * notes (two regions, tidal water) and the row's answer (`answer`, the ladder's result line) around them.
 */
export function storyOf(items: readonly LadderItem[], says: (it: LadderItem) => Line, inp: RegsInputs,
  answer: string | null): DecidedStoryVM {
  const water = inp.item?.name || inp.place?.name || "";
  // a layer per level — and at the region level, per region (T190); this water's own row per region when it is in two
  const ownRegions = new Set(items.filter((i) => i.r.rank === 0 && !i.r.part).map((i) => i.r.export.prov.region ?? ""));
  const layers = new Map<string, { sort: number; head: string; its: LadderItem[] }>();
  for (const it of items) {
    const r = it.r, ent = r.export.prov.entry_name || "";
    let key: string, sort: number, head: string;
    if (r.part) { key = "parts"; sort = 9; head = D.L_PARTS; }
    else if (r.rank === -1) { key = "fed"; sort = 8; head = D.L_FEDERAL; }
    else if (r.rank === 4) { key = "4"; sort = 0; head = D.L_PROVINCE; }
    else if (r.rank === 3) { const n = regionName(r); key = `3|${r.export.prov.region ?? n}`; sort = 1; head = D.L_REGION(n); }
    else if (r.rank === 2) { key = `2|${ent}`; sort = 2; head = D.L_AREA(ent); }
    else if (r.rank === 1) { key = `1|${ent}`; sort = 3; head = D.L_DOWN(ent); }
    else {
      const reg = r.export.prov.region ?? "";
      key = `0|${ent}|${reg}`; sort = ent === water ? 5 : 4;
      head = ent !== water ? D.L_LISTED(ent) : ownRegions.size > 1 && reg ? D.L_WATER_IN(ent, regionName(r)) : D.L_WATER(ent || water);
    }
    const g = layers.get(key);
    if (g) g.its.push(it); else layers.set(key, { sort, head, its: [it] });
  }
  const out: DecidedLayerVM[] = [...layers.values()]
    .sort((a, b) => a.sort - b.sort || a.head.localeCompare(b.head))
    .map((g, i) => ({ n: i + 1, head: g.head,
      steps: [...g.its].sort((a, b) => orderOf(a.role) - orderOf(b.role)).map((it) => stepOf(it, says(it), water)) }));
  const notes: string[] = [];
  const regions = [...new Set(items.filter((i) => i.r.rank === 3).map((i) => regionName(i.r)))].sort();
  const codes = new Set(items.filter((i) => i.r.rank === 3).map((i) => i.r.export.prov.region ?? ""));
  if (codes.size >= 2 && regions.length >= 2) notes.push(D.TWO_REGIONS(regions[0]!, regions.slice(1).join(" and ")));
  if (inp.key?.tidal) notes.push(D.TIDAL);
  return { intro: D.INTRO, notes, layers: out, answer: answer ? D.ANSWER + answer : null };
}
