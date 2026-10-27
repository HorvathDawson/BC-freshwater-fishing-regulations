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
import { winTxt } from "./time";
import { BOATS } from "./words";

const KINDS: readonly BoatKind[] = ["none", "unpowered", "electric_only", "power_capped", "speed", "towing"];

/** The kind a vessel rule belongs to, from its facts alone; null: not a vessel rule or an aspect this line does not know. */
export function boatKind(r: RuleView): BoatKind | null {
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

function words(kind: BoatKind, rs: readonly RuleView[]): string {
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

/** The chips of a gear answer's vessel rules (in force, then timed), one per kind in the fixed order. */
export function boatChips(ga: GearAnswer, inp: Pick<RegsInputs, "rules">): readonly BoatChipVM[] {
  const by = new Map<BoatKind, RuleView[]>();
  for (const ix of [...ga.vessel.active, ...ga.vessel.timed]) {
    const r = inp.rules.get(ix), k = boatKind(r);
    if (k) by.set(k, [...(by.get(k) ?? []), r]);
  }
  return KINDS.filter((k) => by.has(k)).map((k): BoatChipVM => {
    const rs = by.get(k)!;
    const t = words(k, rs);
    const cond = (r: RuleView) => [r.tcond, winTxt(r.wins).slice(2)].filter(Boolean).join(", ");
    const when = rs.every(cond) ? [...new Set(rs.map(cond))].join("; ") : "";
    const part = rs.every((r) => r.part) && !t.endsWith(BOATS.ON_PARTS);
    return { kind: k, text: t + (part ? BOATS.IN_PART : "") + (when ? ` · ${when}` : ""), rule: rs[0]!.key };
  });
}

/** The page's boats line: empty for a water with no vessel rule (or no gear frame: tidal, outside B.C.). */
export function boatsLine(inp: RegsInputs): readonly BoatChipVM[] {
  const ga = gearAnswer(inp);
  return ga ? boatChips(ga, inp) : [];
}
