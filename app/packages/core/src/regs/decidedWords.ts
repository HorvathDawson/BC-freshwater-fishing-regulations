/**
 * (T216) The words of "How this was decided", the angler's explanation (story.ts): the layer heads, what each rule did
 * today, and why — in plain words. Verbatim, frozen; spread into ALL_WORDS (words.ts) so the no-hand-facts test knows
 * them. No imports: words.ts imports this file.
 */
export const DECIDE = Object.freeze({
  INTRO: "Rules stack in layers: the region’s base rules first, then this water’s own rules, then any exceptions. A later layer can change an earlier one.",
  ANSWER: "Today’s answer: ",
  TWO_REGIONS: (a: string, b: string) => `This stretch is in ${a} and ${b}, so both regions’ rules apply here.`,
  TIDAL: "Tidal water: Fisheries and Oceans Canada’s tidal rules apply here, not the B.C. rules below.",

  // layer heads (a step per level, as the ladder's; T190: one per region)
  L_PROVINCE: "All of B.C.: the province’s base rules",
  L_REGION: (name: string) => `${name}: the region’s base rules`,
  L_AREA: (name: string) => `${name}: rules for this area of the region`,
  L_DOWN: (name: string) => `${name}: its rules also cover the streams that flow into it`,
  L_WATER: (name: string) => `${name}: this water’s own rules`,
  L_WATER_IN: (name: string, region: string) => `${name}: this water’s own rules (${region} table)`,
  L_LISTED: (name: string) => `${name}: rules for a listed water that takes in this one`,
  L_FEDERAL: "Exceptions: federal law",
  L_PARTS: "Exceptions: on part of this water only",

  // what each rule did today
  V_DECIDES: "Decides it",
  V_APPLIES: "Also applies",
  V_LOWER: "Lowers the limit",
  V_CAP: "Caps some fish",
  V_OVERRIDDEN: "Overridden",
  V_SAME: "Says the same",
  V_LIFTED: "Lifted here",
  V_MOOT: "Doesn’t matter today",
  V_UNNEEDED: "Not needed here",

  // why it decides
  WHY_CLOSED: "A closure decides it: a closure beats any limit that would let you keep fish.",
  WHY_RELEASE: "Release-only decides it: it beats any limit that would let you keep fish.",
  WHY_OWN: "This water’s own rule sets it, and a water’s own rule beats the region’s.",
  WHY_DOWN: (name: string) => `${name}’s rules cover the streams that flow into it, so they set the answer here.`,
  WHY_AREA: "A rule for this area of the region sets it, and it beats the region-wide rule.",
  WHY_BASE: "Nothing on this water changes it, so the base rule stands.",
  WHY_CONTAINS: "The overall daily limit: these fish count toward it.",
  WHY_NARROWS: "A lower limit for this kind of water. When two limits apply, the stricter one wins.",
  WHY_ALSO: "It applies as well, alongside the limit.",
  WHY_FLOOR: "A size rule, on top of the daily limit.",
  WHY_LIMIT: "A smaller cap inside the daily limit: these fish count toward the total, but no more than this.",
  WHY_MORE: "It applies as well.",
  WHY_POSSESSION: "How many you may have at once, counting fish kept on earlier days.",

  // why it lost: `who` is the rule that beat it, named by its level
  OV_CLOSURE: (who: string) => `${who} closes it, and a closure beats an opening.`,
  OV_RELEASE: (who: string) => `${who} says release, and release-only beats a limit that lets you keep fish.`,
  OV_OWN: (who: string) => `${who} replaces it: a water’s own rule beats the region’s.`,
  OV_AREA: (who: string) => `${who} replaces it: a rule for a smaller area beats the region-wide one.`,
  OV_REGION: (who: string) => `${who} replaces it: the region’s rule beats the B.C.-wide one.`,
  OV_FEDERAL: (who: string) => `${who} replaces it: federal law beats a B.C. rule.`,
  OV_STRICTER: (who: string) => `${who} is stricter, and the stricter rule wins.`,
  SAME: (who: string) => `${who} says the same, so nothing changes.`,
  LIFT_OWN: (who: string) => `${who} opens it here: a water’s own rule beats the region’s.`,
  LIFT: (who: string) => `${who} lifts it here.`,
  MOOT: (who: string) => `${who} already decides these fish today.`,
  UNNEEDED: (who: string) => `${who} already covers these fish.`,

  // who: a rule named by its level
  WHO_OWN: (name: string) => `${name}’s own rule`,
  WHO_AREA: (name: string) => `The rule for ${name}`,
  WHO_REGION: (name: string) => `The ${name} rule`,
  WHO_BC: "The B.C.-wide rule",
  WHO_FEDERAL: "Federal law",
  WHO_ABOVE: "A rule above",

  // where a rule holds
  WHERE_PART: (part: string) => `Only on part of this water: ${part}.`,
  WHERE_FEDERAL: "Federal law: it applies over the B.C. rules.",
  WHERE_DFO: "Set by Fisheries and Oceans Canada (DFO), not the B.C. rule book.",
  WHERE_BY: "The rule that beat it: ",
  WHERE_LIFTED_BY: "The rule that opens it: ",

  // (T883, proposal) the one "How this was decided" every fish sheet draws (flow.ts): each step's kicker (who says it),
  // what it did, the closing answer, and the short lists under it
  K_BC: "All of B.C.",
  K_BASE: (name: string) => `${name} · base rule`,
  K_AREA: (name: string) => `${name} · area rule`,
  // (owner 2026-11-18 ~23:40) a rule another water's row carries down to its tributaries says so, never "this water's"
  K_DOWN: (name: string) => `${name} · its tributaries`,
  // (T892) the same fact on the "What you can keep" table's row: under the fish's name, one line
  ROW_DOWN: (names: readonly string[]) => names.length === 1 ? `${names[0]}’s rule for its tributaries`
    : `${names.slice(0, -1).map((n) => `${n}’s`).join(", ")} and ${names[names.length - 1]}’s rules for their tributaries`,
  K_WATER: (name: string) => `${name} · this water’s rule`,
  K_LISTED: (name: string) => `${name} · listed water`,
  K_FEDERAL: "Federal law",
  K_PART: "Part of this water",
  T_COUNTS: "Counts toward this total",
  T_LOWERS: "Lowers the number",
  T_CAPS: "A cap inside the total",
  T_SIZE: "Size rule",
  T_ALSO: "Also applies",
  T_YEARLY: "Yearly limit",
  T_DUTY: "Must do",
  T_REPLACED: (who: string) => `Replaced by ${who}`,
  T_LIFTED: (who: string) => `Lifted by ${who}`,
  T_REPLACED_ABOVE: "Replaced by a rule above",
  T_REPLACED_PLAIN: "Replaced",
  T_LIFTED_PLAIN: "Lifted",
  T_SAME: "Says the same",
  T_ORIGIN: (origin: "hatchery" | "wild", tag: string | null) => `${origin === "wild" ? "Wild ones" : "Hatchery ones"}${tag ? `: ${tag}` : ""}`,
  T_RESTATES: (who: string) => `Restates ${who}’s rule`,
  T_MOOT: "Doesn’t matter today",
  T_UNNEEDED: "Not needed here",
  RESULT: "So: ",
  W_DAYS: (days: string) => `${days}: `,
  OTHER: "Other rules",
  QUIET: (n: number) => `Rules that don’t apply here (${n})`,
});
