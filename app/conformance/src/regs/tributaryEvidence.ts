/**
 * (T892) What a fish sheet's rail SAYS decides the answer, read off its drawn steps only (never flow.ts): the checks that
 * the "What you can keep" row's tributary line is the sheet's (trout.release.test.ts, tools/release-tributary-lines.test.ts).
 *
 * Per origin the rail speaks for ("Hatchery ones: …" / "Wild ones: …" steps; an untagged step holds for both; no tagged
 * step: one group; a twin row: its own origin only), the steps that set the answer are the deciding ones with no tag
 * (the rule that sets it; a weekday part of today). A "Lowers the number" step is not read here: the rail draws one
 * whether or not it lowered today's number; the stored answer's `narrow` says (`storedElsewhere`). `setByDown`: every
 * group has a setting step and each is kicked "<water> · its tributaries" (or a tributaries row's own name, `downWater`); `names`: those waters; `mentions`: every
 * deciding tributary water the rail draws.
 */
import type { Decided, FlowStepVM, FlowVM, RuleIndex } from "@app/core";

const DOWN = " · its tributaries";
const ORIGIN = /^(Hatchery|Wild) ones(?:: )?/;

const originOf = (s: FlowStepVM): "Hatchery" | "Wild" | null => (ORIGIN.exec(s.tag ?? "")?.[1] as "Hatchery" | "Wild" | undefined) ?? null;
const bareTag = (s: FlowStepVM): string | null => (s.today ? null : (s.tag ?? "").replace(ORIGIN, "") || null);
const TRIBUTARY = /\btributar(?:y|ies)\b/i;
/**
 * (T946) The water a deciding step's kicker names as carried down to its tributaries — core's `DECIDE.K_DOWN` read back:
 * "<water> · its tributaries", or (T919 a) a row whose own name already says it is about tributaries, named once ("Lake
 * Revelstoke's Tributaries", "Rancheria River's Tributaries"). Every other kicker is "<name> · <what>" or a fixed phrase
 * with no "tributar" in it. Reading only the first form, the T892 sweep failed every such row's line after T919 a.
 */
export const downWater = (kicker: string): string | null =>
  kicker.endsWith(DOWN) ? kicker.slice(0, -DOWN.length) : TRIBUTARY.test(kicker) && !kicker.includes(" · ") ? kicker : null;
const water = (s: FlowStepVM): string | null => downWater(s.kicker);

export function railSetters(flow: FlowVM | null, origin: "hatchery" | "wild" | null = null): { setByDown: boolean; names: string[]; mentions: string[] } {
  const won = (flow?.steps ?? []).filter((s) => s.tone === "won");
  const mentions = [...new Set(won.map(water).filter((x): x is string => !!x))];
  const tagged = won.some((s) => originOf(s) !== null);
  // a twin row (`origin`) speaks for its own origin only
  const os = origin ? [origin === "hatchery" ? "Hatchery" : "Wild"] as const : (["Hatchery", "Wild"] as const);
  const groups = tagged || origin ? os.map((o) => won.filter((s) => originOf(s) === null || originOf(s) === o)) : [won];
  const names = new Set<string>();
  let setByDown = groups.length > 0;
  for (const g of groups) {
    const sets = g.filter((s) => bareTag(s) === null);
    if (!sets.length || !sets.every((s) => water(s) !== null)) setByDown = false;
    for (const s of sets) if (water(s)) names.add(water(s)!);
  }
  return { setByDown, names: setByDown ? [...names] : [], mentions };
}

/**
 * (T892) A stored answer (one fish × origin, as Python decided it) set by a rule that is not another water's tributary
 * one: the rule that wins it; where its number was lowered (`narrow`), a rule stored as narrowing it; or (review 3) a rule
 * stored as lowering the number ("narrows", or "limit" on a subcap) that the rail draws as deciding (`railKeys`, the
 * won steps' rule keys). Read straight off the rows frame, for the sweep's proof that a row with no line is partial.
 */
export function storedElsewhere(d: Pick<Decided, "win" | "narrow" | "roles">, rules: Pick<RuleIndex, "get">,
  railKeys: ReadonlySet<string>): boolean {
  const sets = [d.win, ...(d.narrow != null ? d.roles.filter(([, r]) => r === "narrows").map(([ix]) => ix) : [])];
  if (sets.some((ix) => rules.get(ix).rank !== 1)) return true;
  return d.roles.some(([ix, role]) => {
    const r = rules.get(ix);
    return (role === "narrows" || (role === "limit" && r.kind === "subcap")) && railKeys.has(r.key) && r.rank !== 1;
  });
}

/** The rule keys of the steps a rail draws as deciding. */
export const railKeys = (flow: FlowVM | null): ReadonlySet<string> =>
  new Set((flow?.steps ?? []).filter((s) => s.tone === "won" && s.rule).map((s) => s.rule!));
