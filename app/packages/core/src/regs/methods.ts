/**
 * T32 — the Methods tab: Ways to fish (one ✓ / ✕ list, each allowed way's conditions small under it), Boats (only when
 * the water has boat rules) and the closed Reference fold "Handling fish · N rules that apply everywhere" (the conduct
 * rules by moment). Each item ends in its own source line; a day with no fishing says the Gear tab's sentence.
 *
 * Owner rule 2026-10-21 (decisions.md): which ways are allowed and why, their conditions (the clauses, conduct, devices,
 * the rules they are allowed under), the boat rules and the conduct rules are the PIPELINE's gear frame (`GearAnswer.ways`,
 * `vessel`, `conduct`, `caught`) and the rules it cites. This file words them; it decides nothing.
 *
 * Ports (inventory names, regs v38 @ 7fb6809f): page v36 countTxt, tgtTxt, clauseOf, gearParts' ways chips / boats / the
 * always cards (MOMENT_HEAD, PHRASE, the caught card, alwaysN), renderKit's always tile; prototype buildGear's ways /
 * boats / reference cells, WAYS. The "!" limited-way glyph of page v36 is not drawn: every ✓ row is "✓" and the warning
 * is the name's suffix (as the prototype).
 */
import { closedFoot, gearAnswer, sourceLine, type GearCtx } from "./gear";
import type { GearAnswer, RegsInputs, RuleView } from "./inputs";
import type { MethodsVM } from "./model";
import { cap, clean, expand, lc, lcNames, type SpeciesBook } from "./names";
import { ELEM, METHODS, METHODS_TEST, WORDS } from "./words";
import { yesNo } from "./yesno";

/** One rule's gear clause (`export.fields.gear[i]`), as page v36 reads it. */
export type GearClause = { slot?: string; max?: number; min?: number; unlimited?: boolean; must_be?: readonly string[]; members?: readonly string[]; unless?: readonly unknown[] };
/** One way to fish as page v36's chips hold it: the method, allowed or not, its rule, its conditions. */
export interface WayChip { readonly method: string; readonly allow: boolean; readonly rix: number | null; readonly info: string }

const C = METHODS.COUNT;
/** page v36 countTxt: a clause's count in words ("up to 1 kg", "Single point only (no trebles)", "no limit"). */
export function countTxt(slot: string, c: GearClause | null): string {
  if (!c) return C.MISSING;
  if (c.unlimited) return C.NO_LIMIT;
  const n = c.max ?? c.min, pre = "min" in c ? C.AT_LEAST : "";
  switch (slot) {
    case "points_per_hook": return n === 1 ? C.SINGLE_POINT : pre + C.POINTS(String(n));
    case "hook_gap_mm": return pre + C.CM(String(n! / 10));
    case "weight_per_line_kg": case "bait_possession_kg": return (pre || C.UP_TO) + C.KG(String(n));
    case "light_to_hook_mm": return C.WITHIN_M(n! / 1000);
    default: return `${pre}${n}`;
  }
}

/** page v36 tgtTxt: each code a group word ("game fish") or its fish names; two or more read "a, b or c". */
export function tgtTxt(codes: readonly string[], species: SpeciesBook): string {
  const a = codes.map((c) => METHODS.GROUPWORD[c] ?? lcNames(species, expand(species, [c])));
  return a.length > 1 ? a.slice(0, -1).join(METHODS.LIST) + METHODS.OR + a[a.length - 1] : a[0] ?? "";
}

/** page v36 clauseOf: [rule, clause index] → the clause, or null. */
export function clauseOf(rules: GearCtx["rules"], ref: readonly [number, number]): GearClause | null {
  const g = rules.get(ref[0]).export.fields?.gear as readonly GearClause[] | undefined;
  return g?.[ref[1]] ?? null;
}

const mustWords = (xs: readonly string[]) => xs.map((m) => METHODS.MUST[m] ?? m.replace(/_/g, " ")).join(METHODS.LIST);

/** page v36 gearParts ways: per way in the frame's order, allowed or not and its conditions in words. */
export function wayChips(g: GearAnswer, ctx: GearCtx & { readonly species: SpeciesBook; readonly exportConduct: Readonly<Record<string, string>> }): readonly WayChip[] {
  return g.ways.map((x): WayChip => {
    const rix = x.by ? x.by[0] : null;
    if (!x.allowed) return { method: x.method, allow: false, rix, info: (x.why && METHODS.WAYWHY[x.why]) || "" };
    if (x.why === "fly_fishing_only") return { method: x.method, allow: true, rix, info: METHODS.WAYWHY.fly_fishing_only! };
    const bits: string[] = [];
    if (x.not_for) bits.push(METHODS.NOT_FOR(tgtTxt(x.not_for, ctx.species)));
    if (x.for) bits.push(METHODS.FOR(tgtTxt(x.for, ctx.species)));
    for (const e of x.while ?? []) {
      const c = clauseOf(ctx.rules, e.clause);
      if (!c) continue;
      bits.push(c.must_be ? METHODS.MUST_BE(mustWords(c.must_be))
        : METHODS.SLOT_COUNT((METHODS.SLOT_LABEL[c.slot!] ?? c.slot ?? "").toLowerCase(), countTxt(c.slot!, c)));
    }
    for (const a of x.conduct ?? []) bits.push(ctx.exportConduct[a] ?? a);
    for (const r2 of (x.while_rules ?? []).map((ix) => ctx.rules.get(ix)))
      bits.push(METHODS_TEST.CRAYFISH.test(r2.export.label ?? "") ? METHODS.CRAYFISH_ONLY : clean(r2.export.label));
    for (const [d, v] of Object.entries(x.devices ?? {}))
      if (v) bits.push(METHODS.WITH_DEVICE(d, mustWords(v.must_be ?? [])) + (v.within_mm != null ? METHODS.LIST + countTxt("light_to_hook_mm", { max: v.within_mm }) : ""));
    return { method: x.method, allow: true, rix, info: [...new Set(bits)].join(METHODS.SEP) };
  });
}

/** prototype WAYS: ✓ ways (name + "— not for trout or other game fish" when limited; conditions under it), then ✕ ways;
 *  the source line names the ✓ ways' rules, then the ✕ ways'. */
export function waysVM(chips: readonly WayChip[], ctx: GearCtx): MethodsVM["ways"] {
  const ok = chips.filter((c) => c.allow), no = chips.filter((c) => !c.allow);
  const name = (m: string) => ELEM[m] ?? m;
  return {
    label: METHODS.WAYS,
    list: yesNo(ok.map((c) => [name(c.method) + (METHODS_TEST.NOT_GAME.test(c.info) ? METHODS.NOT_GAME : ""), c.info] as const),
                no.map((c) => [cap(lc(name(c.method))), ""] as const)),
    src: sourceLine("srcs:ways", [...ok, ...no].flatMap((c) => (c.rix == null ? [] : [c.rix])), ctx),
  };
}

/** page v36 gearParts boats: the water's boat rules (in force, then timed), each its label; null when it has none. */
export function boatsVM(g: GearAnswer, ctx: GearCtx): MethodsVM["boats"] {
  const rixs = [...g.vessel.active, ...g.vessel.timed];
  if (!rixs.length) return null;
  return { label: METHODS.BOATS, items: rixs.map((ix) => clean(ctx.rules.get(ix).export.label)), src: sourceLine("srcs:boats", rixs, ctx) };
}

/** The static gear moments (`gear.moments`: [moment, [[act, short, long]…]]) as act → its short phrase (page v36 PHRASE). */
function phrases(moments: unknown): ReadonlyMap<string, string> {
  const out = new Map<string, string>();
  for (const [, acts] of (moments as readonly (readonly [string, readonly (readonly [string, string, string?])[]])[] | null) ?? [])
    for (const [a, p] of acts) out.set(a, p);
  return out;
}

/** page v36 always cards + the kit tile: the conduct rules grouped by moment (a snagged fish's duty first); null with none. */
export function referenceVM(g: GearAnswer, ctx: GearCtx & { readonly statics: RegsInputs["statics"] }): MethodsVM["reference"] {
  const moments = Object.entries(g.conduct) as [string, readonly (readonly [string, readonly number[]])[]][];
  const caught: RuleView[] = g.caught.map((ix) => ctx.rules.get(ix));
  const n = moments.reduce((a, [, acts]) => a + acts.length, 0) + caught.length;
  if (!n) return null;
  const PHRASE = phrases(ctx.statics.gearMoments), st = ctx.statics;
  const groups = [
    ...(caught.length ? [{ head: METHODS.CAUGHT_HEAD, items: caught.map((r) => r.facts?.plain || WORDS.NO_WORDS(r.key)) }] : []),
    ...moments.map(([m, acts]) => ({ head: METHODS.MOMENT_HEAD[m] ?? m,
      items: acts.map(([a]) => PHRASE.get(a) || st.conductMeans[a] || st.exportConduct[a] || a) })),
  ];
  return { label: METHODS.REFERENCE as "Reference", title: METHODS.HANDLING, sub: METHODS.RULES_EVERYWHERE(n), groups,
           src: sourceLine("srcs:always", [...g.caught, ...moments.flatMap(([, acts]) => acts.map(([, rs]) => rs[0]!))], ctx) };
}

/** The Methods tab. */
export function buildMethods(inp: RegsInputs, ctx: GearCtx): MethodsVM {
  const foot = closedFoot(inp, ctx), g = gearAnswer(inp);
  if (foot || !g) return { closed: foot?.[0] ?? null, closedMore: foot?.slice(1) ?? [], ways: null, boats: null, reference: null };
  const chips = wayChips(g, { ...ctx, species: inp.statics.exportSpecies, exportConduct: inp.statics.exportConduct });
  return { closed: null, closedMore: [], ways: waysVM(chips, ctx), boats: boatsVM(g, ctx), reference: referenceVM(g, { ...ctx, statics: inp.statics }) };
}
