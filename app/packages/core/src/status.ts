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
import { holdsOn, type PlainDate, type When } from "./dates";

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

/**
 * WHERE THE RULE WAS WRITTEN — its specificity, which is what drives precedence: a rule
 * written for this water displaces a zone default it contradicts.
 *
 * Every rule in the corpus today is `section`; `mu` arrives with zone regulations ("in MU
 * 4-5, no bait"). Kept as a type rather than assumed, because the precedence rule below
 * already depends on it and would otherwise have to be rediscovered.
 */
export type ScopeKind = "section" | "mu" | "area";

/**
 * HOW A RULE REACHES ONE SECTION — its provenance, which is what a reader is TOLD.
 *
 * Orthogonal to `ScopeKind`, and an earlier draft had one field trying to be both. A
 * water-specific closure reaches its own water (`reach`) and everything joining it
 * (`trib`), and those deserve different sentences: "no fishing here" against "no fishing
 * here, because this creek joins a closed stretch of the Skeena". 98.6% of all bindings in
 * the province are tributary ones, so this is the common case, not the footnote.
 */
export type RuleVia = "reach" | "trib";

/**
 * THE FOURTEEN TYPES, and the six families they group into. Licensing is NOT a rule type (it is
 * `licensing` on the catalogue entry and never votes on open/closed); `angler_closure` is the
 * one closure to a kind of angler, filed under `access`.
 *
 * This replaced `RuleKind`, whose six coarse values (closure / harvest / gear_restriction /
 * vessel_restriction / licensing / note) said the SHAPE of a rule and never its content.
 *
 * NOTE WHAT IS NOT HERE: `closure`. A closure is not a kind of rule — it is a retention
 * limit of zero that you may not fish for, and catch-and-release is a retention limit of
 * zero that you may. Those two differ only in `mayTarget`, which is why it is on the rule
 * and why `severityOf` reads it. Treating take=0 as "closed" on its own once turned 605
 * closures into permissions.
 */
export type RuleType =
  | "retention_limit" | "stop_fishing_after_quota"
  | "bait_restriction" | "tackle_restriction" | "method_rule"
  | "vessel_rule" | "angling_from_vessel_prohibited" | "navigation_duty"
  | "angler_closure"
  | "handling_rule"
  | "hazard" | "advisory" | "program_membership" | "facility";

/** The reader sees these as sections, worst news first within each. */
export type RuleFamily =
  | "retention" | "gear_and_method" | "vessel" | "access" | "conduct" | "information";

export const RULE_TYPES: readonly RuleType[] = [
  "retention_limit", "stop_fishing_after_quota",
  "bait_restriction", "tackle_restriction", "method_rule",
  "vessel_rule", "angling_from_vessel_prohibited", "navigation_duty",
  "angler_closure",
  "handling_rule",
  "hazard", "advisory", "program_membership", "facility",
] as const;

/** The order a screen shows families in. Derived from the union, so a new family is a
 *  type error here rather than a section that silently never renders. */
/**
 * Which family a type belongs to.
 *
 * THE BUNDLE SHIPS `family` ON EVERY RULE, so nothing at runtime needs this — it is here for
 * constructing a rule (tests, fixtures) and for a surface that has a type and no row. The
 * authority is `pipeline/regs/parsing/catalogue.py::_FAMILY`, and a python test asserts this
 * table matches it, because two copies of a mapping are two answers waiting to disagree.
 */
export const FAMILY_OF: Record<RuleType, RuleFamily> = {
  retention_limit: "retention",
  stop_fishing_after_quota: "retention",
  bait_restriction: "gear_and_method",
  tackle_restriction: "gear_and_method",
  method_rule: "gear_and_method",
  vessel_rule: "vessel",
  angling_from_vessel_prohibited: "vessel",
  navigation_duty: "vessel",
  angler_closure: "access",
  handling_rule: "conduct",
  hazard: "information",
  advisory: "information",
  program_membership: "information",
  facility: "information",
};

export const RULE_FAMILIES: readonly RuleFamily[] =
  ["retention", "gear_and_method", "vessel", "access", "conduct", "information"] as const;

export interface Rule {
  readonly id: string;
  readonly type: RuleType;
  /** Which section the reader sees this under. Shipped, not derived, so the client does
   *  not carry its own copy of the type-to-family mapping. */
  readonly family: RuleFamily;
  readonly scope: ScopeKind;
  /** How this rule reaches the section being asked about. See `RuleVia`. */
  readonly via: RuleVia;
  /**
   * WHAT IT SAYS, in the curator's own words — "Bait ban", "Single barbless hook",
   * "No fishing, one hour after sunset to one hour before sunrise".
   *
   * NOT `subject`, which is the precedence key below and comes from a different curated
   * field. Writing this sentence into `subject` — which is what the first version of the
   * bundler did — made every rule look like it governed a unique subject, so the rule that
   * lets a water-specific rule displace a zone default could never match anything.
   *
   * Present on all 3,050 rules, averaging 24 characters, and the best copy in the corpus.
   * The screen headlined `kind` instead and printed raw enum names joined by dots —
   * "closure · gear restriction · vessel restriction" — which says the shape of a rule and
   * never its content.
   */
  readonly label: string;
  /**
   * The rule as written, verbatim. An exact substring of the entry's paragraph in 99.7% of
   * cases, which is what lets the panel highlight the sentence a rule was read from without
   * the bundle carrying clause offsets.
   */
  readonly verbatim?: string;
  /** The reach in the page's own words, when no cut-point could express it. */
  readonly extentText?: string;
  /**
   * The species it names. 823 rules; the other 2,227 name NONE, and that must not be
   * rendered as "all species" — the parser established no such thing.
   */
  readonly species?: readonly string[];
  readonly group: SpeciesGroup;
  /** When it binds. `ALL_YEAR` (no dates, no weekdays) = every day. See `When`. */
  readonly when: When;
  /**
   * What this rule governs, e.g. "bait" or "rainbow-trout-quota". A rule written for one
   * water REPLACES a zone default about the same subject — that is how a lake gets a
   * quota of 6 where its management unit says 2, deliberately and more permissively.
   */
  readonly dimension: string;
  /**
   * How many you may keep. `0` with `mayTarget: false` is a closure; `0` with
   * `mayTarget: true` is catch-and-release. Undefined on every non-retention rule, and on a
   * size limit whose count comes from the region.
   */
  readonly take?: number;
  /** Whether you may fish for it at all. See `take`. */
  readonly mayTarget?: boolean;
  /**
   * The acts this rule binds WHILE doing — `["spear_fishing"]`, `["ice_fishing"]`. LOAD-BEARING
   * FOR THE OUTCOME: "only non-game fish may be speared" is a retention limit of zero on every
   * game fish WHILE spear fishing, and without this field it is indistinguishable from "No
   * fishing". Absent or empty = whatever you are doing. (It was `method`, a field the catalogue
   * retired; the data layer went on looking for it and every river in B.C. read closed.)
   */
  readonly while?: readonly string[];
  /**
   * The rule holds everywhere, at places no dataset can draw — "no fishing within 23 m downstream
   * of any fishway". It is SHOWN on every water and NEVER decides one's outcome: bound to every
   * section as the closure it literally is, it painted the whole province CLOSED.
   */
  readonly standing?: boolean;
  /**
   * An absolute prohibition: an area closure, a no-access polygon, an in-season notice.
   * Never replaced by a more permissive rule, only ever the answer.
   */
  readonly absolute?: boolean;
  /** We could not place this rule, or could not check the feed that carries it. */
  readonly uncertain?: boolean;
  /**
   * WHAT THIS RULE LIFTS where it is in force — "Exempt from spring closure". Each names the
   * entry it lifts, and one rule of it when `rule` is set; the bundle resolved both from the
   * catalogue's `default_id` / `target`, so nothing here matches a bare name. See `evaluate`.
   */
  readonly exempts?: readonly Lift[];
}

/**
 * One thing a rule lifts: every rule of zone entry `entry` (a named zone default), or the one
 * rule `rule` of `entry`. Rule ids are unique only within an entry (AGENTS rule 8), so the entry
 * is always named.
 */
export interface Lift {
  readonly entry: string;
  readonly rule?: string;
}

export interface Status {
  readonly outcome: Outcome;
  readonly provenance: Provenance;
  /** Present only when outcome is "unknown" — the reasons need different words. */
  readonly because?: "unplaceable" | "unreadable-season" | "near-name" | "feed-unreachable";
  /** The rules that produced this answer, most restrictive first. */
  readonly from: readonly Rule[];
}

/**
 * How bad the news is: 3 closed, 2 restricted, 1 open.
 *
 * A FUNCTION, not a table, because the worst outcome in the corpus is not a type. "No
 * fishing for bull trout" and "bull trout catch and release" are both `retention_limit`
 * with `take: 0`, and they differ only in `mayTarget` — so a lookup keyed on the type alone
 * cannot tell a closure from a release rule, and would have to call one of them wrong.
 */
function severityOf(r: Rule): number {
  // A rule with no knowable place tells you something about every water and decides none.
  if (r.standing) return 1;
  // A LIFT AND NOTHING ELSE restricts nobody. "Exempt from spring closure" is a retention rule
  // that sets no number — 77 of them — and read as a quota it painted a lifted water RESTRICTED.
  if (r.type === "retention_limit" && r.take === undefined && r.mayTarget === undefined
      && (r.exempts ?? []).length > 0) return 1;
  if (r.type === "retention_limit") {
    if (r.take === 0) return closesTheWater(r) ? 3 : 2;
    return 2;
  }
  // Information tells you something; it does not restrict you. Everything else does.
  return r.family === "information" ? 1 : 2;
}

/**
 * Does a retention limit of zero shut the WATER, or only one way of fishing it?
 *
 * IT MATTERED ON EVERY RIVER IN THE PROVINCE. "Only non-game fish (such as carp) may be speared"
 * is take=0 on every game fish — correctly, that is what it says — and it carries
 * `while: ["spear_fishing"]`. It reaches 1,674 of the bundle's 1,693 rulesets, so a severity that
 * reads `take === 0 && mayTarget === false` and stops there returns `closed` for essentially
 * every section in British Columbia, every day of the year. You may still angle.
 *
 * `ALL_GAME_FISH` is the whole closed list and so is NOT a narrowing; a shorter species list is.
 * The design prototype learned both of these first; core did not, and the two must agree or the
 * map and the sheet contradict each other about the same water.
 */
function closesTheWater(r: Rule): boolean {
  if (r.mayTarget !== false) return false;
  // Closed WHILE doing one thing is closed to that thing, not to the water.
  if ((r.while ?? []).length > 0) return false;
  // Closed for PART of the day ("21:00 to 05:00") is not a closed day. Six rules, all night
  // closures on Region 2 rivers; read as a whole-day closure they shut the Harrison at noon.
  if (r.when.hours) return false;
  const sp = r.species ?? [];
  return sp.length === 0 || sp.includes("ALL_GAME_FISH");
}

/**
 * A rule no answer may rest on: one we could not place, or one whose season we could not read.
 * An unread season is not "all year" — that reading would enforce a closure on days it does not
 * hold and, worse, lift nothing on days it does.
 */
export function isUncertain(r: Rule): boolean {
  return r.uncertain === true || r.when.unparsed.length > 0;
}

/**
 * Does `by` lift `r`? Only a rule in force lifts, and only one that holds whatever you are doing
 * and all day: a lift WHILE set lining, or for part of the day, still leaves the rule it lifts
 * standing the rest of the time, and this answer is for the whole day and any method.
 *
 * NEVER ITSELF, and a zone default is never lifted by a rule of its own entry.
 * `z6:steelhead_stream_closure.r1` names its own slug as the default it lifts; honoured, it would
 * delete itself on every stream in the region.
 */
function lifts(by: Rule, r: Rule): boolean {
  if (by === r || by.id === r.id) return false;
  for (const l of by.exempts ?? []) {
    const prefix = `${l.entry}.`;
    if (l.rule !== undefined) {
      if (r.id === prefix + l.rule) return true;
    } else if (r.id.startsWith(prefix) && !by.id.startsWith(prefix)) {
      return true;
    }
  }
  return false;
}

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
 *   0. a rule LIFTED by another rule in force here does not count at all (`exempts`)
 *   1. keep only this species group's rules that are in force today
 *   2. a section-scoped rule REPLACES an mu-scoped default about the same subject
 *   3. absolute rules (area closure, no-access, in-season) override everything
 *   4. otherwise the most restrictive outcome wins
 *   5. anything uncertain, or an unreachable feed, is UNKNOWN — never open
 */
export function evaluate({ rules, on, group, feedUnreachable }: EvaluateInput): Status {
  const ours = rules.filter((r) => r.group === group);
  /* 0 — EXEMPTIONS. "Exempt from spring closure" beside the region's spring closure: while the
     lift is in force, the closure is not a rule of this water — not a vote, not a doubt. Applied
     nowhere before, so the North Thompson read CLOSED on May 1 beside its own exemption. */
  const lifters = ours.filter((r) => (r.exempts ?? []).length > 0 && !isUncertain(r)
    && holdsOn(r.when, on) && (r.while ?? []).length === 0 && !r.when.hours);
  const mine = lifters.length === 0 ? ours
    : ours.filter((r) => !lifters.some((by) => lifts(by, r)));
  /* A rule we could not place applies to NOTHING — it must not vote on the outcome, or an
     unplaceable closure reads exactly like a placed one. It only ever raises `unknown`. The
     same holds for a rule whose SEASON could not be read: in force or not, we cannot say. */
  const live = mine.filter((r) => !isUncertain(r) && holdsOn(r.when, on));

  /* 2 — a rule written for this water displaces the zone default it contradicts.
     THE KEY IS (type, dimension), both halves. Type alone collides — a water's daily quota
     and its possession quota are both `retention_limit` — and dimension alone crosses types
     that never compete. It used to key on `subject`, which the bundle populated on 2 rules
     out of 3,273, so this branch existed for two years and never once fired. */
  const key = (r: Rule) => `${r.type}\u0000${r.dimension}`;
  const replaced = new Set(live.filter((r) => r.scope === "section").map(key));
  const effective = live.filter((r) => !(r.scope === "mu" && replaced.has(key(r))));

  const provenance: Provenance = mine.some((r) => r.scope === "section")
    ? "specific"
    : "general";

  if (feedUnreachable) {
    return { outcome: "unknown", provenance, because: "feed-unreachable", from: effective };
  }
  /* A closure we DID place is a real answer and outranks a doubt. Anything short of a
     closure does not: "restricted, plus something here we could not place" is unknown. */
  const placedClosure = effective.some((r) => severityOf(r) === 3);
  if (mine.some((r) => r.uncertain) && !placedClosure) {
    return { outcome: "unknown", provenance, because: "unplaceable", from: effective };
  }
  if (mine.some(isUncertain) && !placedClosure) {
    return { outcome: "unknown", provenance, because: "unreadable-season", from: effective };
  }

  const ordered = [...effective].sort((a, b) => severityOf(b) - severityOf(a));
  const absolute = ordered.filter((r) => r.absolute);
  const deciding = absolute.length > 0 ? absolute : ordered;

  let outcome: Outcome = "open";
  for (const r of deciding) outcome = moreRestrictive(outcome, OUTCOME_OF[severityOf(r)]!);

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
