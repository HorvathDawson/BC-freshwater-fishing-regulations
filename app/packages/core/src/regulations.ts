/**
 * REGULATIONS — NOT INTEGRATED. This file is the place they plug back in.
 *
 * The app currently reads NO regulation data: no rules, no rule sets, no licensing, no gear,
 * no `while`/method scoping, no exemptions, no closures and no open/closed/restricted status.
 * The map draws water in one neutral style and a water's sheet shows a placeholder panel
 * (`RegulationsPlaceholder` in @app/ui-native). The previous integration (core `status.ts`,
 * `dates.ts`, `stretches.ts`, the data layer's rule and licensing readers, `StatusPill`, the
 * date pill, the map's `closure` colour mode) was deleted outright; git history keeps it.
 *
 * TODO(regulations): build the integration against the data as it ships today.
 *
 *   · THE BUNDLE — `pipeline/deliver/bundle/schema.sql` is the contract. Rules: `entry`,
 *     `rule`, `ruleset`, `section_ruleset` (a section names ONE interned set; sets list their
 *     (entry_id, rule_id, via) members). Licensing: `licence`, `designation`,
 *     `not_classified`, `requirement`, `licence_terms`, `exemption`, `alternative`,
 *     `licensing_set`, `section_licensing`. The dev fixture (`pnpm fixture`) creates these
 *     tables EMPTY; a fixture with regulation rows comes back with the integration.
 *   · THE EXPORT — `data/generated/regs/ui-rules-export.json`, written by
 *     `pipeline/tools/export_ui_rules.py`. Its `guide` explains every field: rule types and
 *     families, the competition ladder, gear, `lengths`, `when`, species, retention, vessel,
 *     `exempts`, `standing`, `angler_closure`, licensing and placement (`via`: reach / trib /
 *     trib_pending / contested). It settles nothing — "no open/closed verdicts, no colours" —
 *     so every verdict this app shows will have to be derived, once, in core.
 *
 * Rules the integration has to keep (settled with the user, see the licensing brief):
 * licensing never affects open/closed; the angler is always unknown, so answers are
 * conditional; every label is generated from structured fields with the verbatim kept
 * underneath; ids are unique only within an entry, so a record is always `entry_id` + its id.
 *
 * The types below are the SHAPE THE UI WILL CONSUME, deliberately minimal: identity, the
 * generated label and the printed sentence. Grow them from the bundle when the work starts;
 * do not guess fields the data does not carry.
 */

/** Whether regulations are wired into this build. There is one value until they are. */
export type RegulationsAvailability = "not-integrated";

export const REGULATIONS: RegulationsAvailability = "not-integrated";

/**
 * One regulation record as a screen will show it — a rule or a licensing record.
 *
 * `key` is the full key the export uses (`entry_id::rule_id` for a rule, `entry_id#record_id`
 * for licensing): the short id alone collides across entries.
 */
export interface RegulationRecord {
  readonly key: string;
  readonly entryId: string;
  readonly kind: "rule" | "licensing";
  /** Generated from the record's fields by the pipeline — never stored prose. */
  readonly label: string;
  /** The sentence as printed in the synopsis. */
  readonly verbatim: string | null;
}

/** Everything the regulations panel of one water will receive. */
export interface WaterRegulations {
  /** The durable registry id (`item_id`). */
  readonly item: string;
  readonly rules: readonly RegulationRecord[];
  readonly licensing: readonly RegulationRecord[];
}
