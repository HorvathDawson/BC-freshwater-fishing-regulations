/**
 * THE status function. One implementation, used by every surface.
 *
 * A coherence review of five parallel design decks counted nine status surfaces across
 * four vocabularies — map line, tap card, search row, rule line, gauge strip — each
 * inventing its own words for the same states. AGENTS.md rule 23 exists because of it.
 * Navigation may differ between phone and desktop; the answer may not.
 *
 * Two things the type keeps apart that v1 fused into `default_only`:
 *
 *   OUTCOME     what you may do here today          -> the colour
 *   PROVENANCE  whether anyone wrote a rule for it  -> the line weight
 *
 * A water nobody wrote a rule about is OPEN with provenance "general". It is not a
 * fourth colour and it is not unknown — the regional and provincial rules still apply.
 */
import { inForce, type PlainDate, type Window } from "./dates";

export type Outcome = "closed" | "restricted" | "open" | "unknown";

/**
 * Every outcome, once, in the order a legend reads them — worst news first.
 *
 * THE TYPE AND THE LIST HAVE TO SHIP TOGETHER. `Shell` held one copy to draw the legend and
 * `outcome-colour.test.ts` held its own to check that every outcome is coloured in every
 * theme — so the test proved a property of ITS list, not of the app's. Add a fifth outcome
 * to the union and both copies keep compiling, the legend silently omits it, and the test
 * that exists to catch exactly that stays green. Derived from the union here, so a new
 * member is a type error until it is listed.
 */
export const OUTCOMES: readonly Outcome[] =
  ["closed", "restricted", "open", "unknown"] as const;
export type Provenance = "specific" | "general";

/**
 * The province regulates trout, char, whitefish and coarse fish. DFO regulates salmon.
 * They are two authorities over two different sets of species, so a DFO closure does not
 * override a provincial rule — it answers a question the provincial rule never asked. A
 * river can be open for rainbow trout and closed for coho on the same afternoon, and
 * blending those into one colour is how somebody gets fined.
 */
export type SpeciesGroup = "provincial" | "salmon";

/** Where a rule attaches. The scope is a property of the geometry, not of the rule —
 *  which is how one row closes every stream in a management unit. */
export type ScopeKind = "section" | "mu" | "area";

export type RuleKind =
  | "closure"
  | "gear_restriction"
  | "harvest"
  | "vessel_restriction"
  | "licensing"
  | "note";

export interface Rule {
  readonly id: string;
  readonly kind: RuleKind;
  readonly scope: ScopeKind;
  readonly group: SpeciesGroup;
  /** Empty = all year. */
  readonly windows: readonly Window[];
  /**
   * What this rule governs, e.g. "bait" or "rainbow-trout-quota". A rule written for one
   * water REPLACES a zone default about the same subject — that is how a lake gets a
   * quota of 6 where its management unit says 2, deliberately and more permissively.
   */
  readonly subject?: string;
  /**
   * An absolute prohibition: an area closure, a no-access polygon, an in-season notice.
   * Never replaced by a more permissive rule, only ever the answer.
   */
  readonly absolute?: boolean;
  /** We could not place this rule, or could not check the feed that carries it. */
  readonly uncertain?: boolean;
}

export interface Status {
  readonly outcome: Outcome;
  readonly provenance: Provenance;
  /** Present only when outcome is "unknown" — the three reasons need different words. */
  readonly because?: "unplaceable" | "near-name" | "feed-unreachable";
  /** The rules that produced this answer, most restrictive first. */
  readonly from: readonly Rule[];
}

const SEVERITY: Record<RuleKind, number> = {
  closure: 3,
  gear_restriction: 2,
  harvest: 2,
  vessel_restriction: 2,
  licensing: 2,
  note: 1,
};

const OUTCOME_OF: Record<number, Outcome> = { 3: "closed", 2: "restricted", 1: "open" };

/** Rank for "most restrictive wins". Unknown sits above open because we must not
 *  present a thing we could not check as a thing we checked. */
const RANK: Record<Outcome, number> = { closed: 3, unknown: 2, restricted: 1, open: 0 };

// Module-private: used by `evaluate` in this file and nowhere else.
function moreRestrictive(a: Outcome, b: Outcome): Outcome {
  return RANK[a] >= RANK[b] ? a : b;
}

export interface EvaluateInput {
  readonly rules: readonly Rule[];
  readonly on: PlainDate;
  readonly group: SpeciesGroup;
  /** True when a live override feed could not be reached, so the answer may be stale. */
  readonly feedUnreachable?: boolean;
}

/**
 * The whole combining rule, in one place:
 *
 *   1. keep only this species group's rules that are in force today
 *   2. a section-scoped rule REPLACES an mu-scoped default about the same subject
 *   3. absolute rules (area closure, no-access, in-season) override everything
 *   4. otherwise the most restrictive outcome wins
 *   5. anything uncertain, or an unreachable feed, is UNKNOWN — never open
 */
export function evaluate({ rules, on, group, feedUnreachable }: EvaluateInput): Status {
  const mine = rules.filter((r) => r.group === group);
  /* A rule we could not place applies to NOTHING — it must not vote on the outcome, or an
     unplaceable closure reads exactly like a placed one. It only ever raises `unknown`. */
  const live = mine.filter((r) => !r.uncertain && inForce(r.windows, on));

  // 2 — a rule written for this water displaces the zone default it contradicts
  const replaced = new Set(
    live
      .filter((r) => r.scope === "section" && r.subject !== undefined)
      .map((r) => r.subject!),
  );
  const effective = live.filter(
    (r) => !(r.scope === "mu" && r.subject !== undefined && replaced.has(r.subject)),
  );

  const provenance: Provenance = mine.some((r) => r.scope === "section")
    ? "specific"
    : "general";

  if (feedUnreachable) {
    return { outcome: "unknown", provenance, because: "feed-unreachable", from: effective };
  }
  /* A closure we DID place is a real answer and outranks a doubt. Anything short of a
     closure does not: "restricted, plus something here we could not place" is unknown. */
  const placedClosure = effective.some((r) => r.kind === "closure");
  if (mine.some((r) => r.uncertain) && !placedClosure) {
    return { outcome: "unknown", provenance, because: "unplaceable", from: effective };
  }

  const ordered = [...effective].sort((a, b) => SEVERITY[b.kind] - SEVERITY[a.kind]);
  const absolute = ordered.filter((r) => r.absolute);
  const deciding = absolute.length > 0 ? absolute : ordered;

  let outcome: Outcome = "open";
  for (const r of deciding) outcome = moreRestrictive(outcome, OUTCOME_OF[SEVERITY[r.kind]]!);

  return { outcome, provenance, from: ordered };
}

/** The word a person reads. Every surface uses this one — never its own wording. */
export function statusWord(s: Status): string {
  switch (s.outcome) {
    case "closed":
      return "CLOSED";
    case "restricted":
      return "RESTRICTED";
    case "unknown":
      return "UNKNOWN";
    case "open":
      return s.provenance === "general" ? "OPEN · GENERAL RULES" : "OPEN";
  }
}
