/**
 * T209 — the boats line: one chip per kind of vessel restriction the water holds ("Electric motors only", "No powered
 * boats", "Engine limit 7.5 kW (10 hp)", "Speed limit 8 km/h", "No towing", "No vessels"), drawn near the top of the page
 * for the tapped section. It words the `vessel_rule` facts the bundle carries (`fields.aspect`, `level`, `max_kmh`,
 * `max_power_kw`); it never reads the rule's words and decides nothing. The gear frame's `vessel` lists are already this
 * section's (a rule bound to a stretch is not in the lists of the rest of the water). The Methods "Boats" cell stays the
 * full list with sources.
 */
import type { GearAnswer, RegsInputs, RuleView } from "./inputs";
import type { BoatChipVM, BoatKind } from "./model";
import { gearAnswer } from "./gear";
import { clean } from "./names";
import { winTxt } from "./time";
import { BOATS } from "./words";

const KINDS: readonly Exclude<BoatKind, "in_boat">[] = ["none", "unpowered", "electric_only", "power_capped", "speed", "towing"];

/** The kind a vessel rule belongs to, from its facts alone; null: not a vessel rule or an aspect this line does not know. */
export function boatKind(r: RuleView): Exclude<BoatKind, "in_boat"> | null {
  if (r.export.type !== "vessel_rule") return null;
  const aspect = r.f.aspect;
  if (aspect === "speed") return "speed";
  if (aspect === "towing") return "towing";
  if (aspect !== "propulsion") return null;
  const level = r.f.level;
  return level === "none" || level === "unpowered" || level === "electric_only" || level === "power_capped" ? level : null;
}

const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
const g = (n: number): string => String(+n.toFixed(2));

function words(kind: Exclude<BoatKind, "in_boat">, rs: readonly RuleView[]): string {
  switch (kind) {
    case "none": return BOATS.NONE;
    case "unpowered": return BOATS.UNPOWERED;
    case "electric_only": return BOATS.ELECTRIC;
    case "towing": return BOATS.TOWING;
    case "power_capped": {
      const kw = rs.map((r) => num(r.f.max_power_kw)).filter((x): x is number => x !== null);
      return kw.length ? BOATS.POWER(g(Math.min(...kw)), Math.round(Math.min(...kw) * BOATS.HP_PER_KW)) : BOATS.POWER_NO_CAP;
    }
    case "speed": {
      const v = [...new Set(rs.map((r) => num(r.f.max_kmh)).filter((x): x is number => x !== null))].sort((a, b) => a - b);
      if (!v.length) return BOATS.SPEED_NO_NUMBER;
      // two limits of one kind on the whole water: the book does not say where each holds, so never the stricter alone
      return v.length > 1 ? BOATS.SPEEDS_ON_PARTS(g(v[0]!), g(v[v.length - 1]!)) : BOATS.SPEED(g(v[0]!));
    }
  }
}

/**
 * (T209, owner 2026-11-12: "It should be in the vessel restriction section but it should say the locator text there")
 * THE VESSEL RULES OF A PART NO MAP DRAWS: the day's gear frame lists them in `in_part` (the pipeline's answer: in force
 * on the day, held on a part of the water, never on the whole of it — Okanagan Lake's "Speed restrictions or no vessels
 * (as buoyed and signed), various locations", Alouette Lake's "No vessels in swimming areas, as buoyed and signed"). They
 * are vessel restrictions, so they are drawn here and not as a loose note (`caveats.gearInPart` leaves them out), each
 * chip carrying the book's own locator words (`fields.undrawn_part`) so it never reads as the whole water. One chip per
 * kind and locator, in the stored order. Selects and words; decides nothing.
 */
export function inPartVessels(ga: GearAnswer, inp: Pick<RegsInputs, "rules">): readonly RuleView[] {
  return [...new Set(ga.in_part)].map((ix) => inp.rules.get(ix)).filter((r) => !r.missing && boatKind(r) !== null && !!r.part);
}

/** The chips of a gear answer's vessel rules (in force, then timed), one per kind in the fixed order; after each kind's
 *  whole-water chip, its chips for parts no map draws (`inPartVessels`), with their locator words. */
export function boatChips(ga: GearAnswer, inp: Pick<RegsInputs, "rules">): readonly BoatChipVM[] {
  const by = new Map<Exclude<BoatKind, "in_boat">, RuleView[]>();
  for (const ix of [...ga.vessel.active, ...ga.vessel.timed]) {
    const r = inp.rules.get(ix), k = boatKind(r);
    if (k) by.set(k, [...(by.get(k) ?? []), r]);
  }
  const parts = new Map<Exclude<BoatKind, "in_boat">, Map<string, RuleView[]>>();
  for (const r of inPartVessels(ga, inp)) {
    const k = boatKind(r)!, at = parts.get(k) ?? new Map<string, RuleView[]>(), p = r.part!.trim();
    at.set(p, [...(at.get(p) ?? []), r]);
    parts.set(k, at);
  }
  const partChips = (k: Exclude<BoatKind, "in_boat">): BoatChipVM[] =>
    [...(parts.get(k) ?? new Map<string, RuleView[]>()).entries()].map(([p, rs]): BoatChipVM => {
      const when = rs.every((r) => r.wins.length) ? [...new Set(rs.map((r) => winTxt(r.wins).slice(2)))].join("; ") : "";
      return { kind: k, text: words(k, rs) + BOATS.AT_PART(p) + (when ? ` · ${when}` : ""), rule: rs[0]!.key };
    });
  const wholeChip = (k: Exclude<BoatKind, "in_boat">, rs: readonly RuleView[]): BoatChipVM => {
    const t = words(k, rs);
    const cond = (r: RuleView) => [r.tcond, winTxt(r.wins).slice(2)].filter(Boolean).join(", ");
    const when = rs.every(cond) ? [...new Set(rs.map(cond))].join("; ") : "";
    const part = rs.every((r) => r.part) && !t.endsWith(BOATS.ON_PARTS);
    // (T623 F15) a rule the book prints "on parts" and never says which is held on the whole water (owner 2026-10-26):
    // its chip keeps the book's words (the pipeline's label part `on_parts`), never reading as whole-water
    const on = !part && !t.endsWith(BOATS.ON_PARTS) && rs.every((r) => !!r.export.parts?.on_parts)
      ? ` ${rs[0]!.export.parts!.on_parts}` : "";
    return { kind: k, text: t + (part ? BOATS.IN_PART : on) + (when ? ` · ${when}` : ""), rule: rs[0]!.key };
  };
  return [...inBoatRules(ga, inp).map((r): BoatChipVM => ({ kind: "in_boat", text: clean(r.export.label), rule: r.key })),
          ...KINDS.flatMap((k) => [...(by.has(k) ? [wholeChip(k, by.get(k)!)] : []), ...partChips(k)])];
}

/**
 * (T670 G1) THE RULES THAT HOLD FOR AN ANGLER IN A BOAT: the gear frame's `circumstantial` clauses the pipeline holds
 * "while in_boat" — the Kispiox's "No angling from boats" (p.51), the Thompson's (p.32), Kootenay Lake's rods from a
 * boat. The frame carried them and no screen read them. Each rule once, in the stored order; its words are its stored
 * label ("No angling (from a boat), Aug 15-Dec 31"), the same words its rule sheet shows. Selects; decides nothing.
 */
export function inBoatRules(ga: GearAnswer, inp: Pick<RegsInputs, "rules">): readonly RuleView[] {
  const seen = new Set<number>(), out: RuleView[] = [];
  for (const c of ga.circumstantial) {
    const ix = c.clause[0];
    if (!(c.while ?? []).includes("in_boat") || seen.has(ix)) continue;
    seen.add(ix); out.push(inp.rules.get(ix));
  }
  return out;
}

/** The page's boats line: empty for a water with no vessel rule (or no gear frame: tidal, outside B.C.). */
export function boatsLine(inp: RegsInputs): readonly BoatChipVM[] {
  const ga = gearAnswer(inp);
  return ga ? boatChips(ga, inp) : [];
}
