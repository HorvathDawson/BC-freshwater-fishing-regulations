// TypeScript mirror of the API.md / pipeline entry_models.py shapes.
// Kept in one file on purpose (throwaway internal tool).

export type Op = "whole" | "upstream_of" | "downstream_of" | "between" | "within";

export type RestrictionType =
  | "closure"
  | "harvest"
  | "gear_restriction"
  | "vessel_restriction"
  | "licensing"
  | "note";

export type Status =
  | "no_registry"
  | "needs_review"
  | "unused_splits"
  | "unreviewed"
  | "confirmed";

export type RegistryStatus = "matched" | "no_registry";

// ---- Entry model (entry_models.py) ----------------------------------------

export interface Extent {
  op: Op;
  splits: string[];
  item_id?: string | null;
  /** several registry ids, when a reach's two cut-points sit on different waters. Mutually
   *  exclusive with item_id — scoping such a reach to one water puts the other end out of scope. */
  item_ids?: string[];
  area_id?: string | null;
  area_kind?: string | null;
  feature_types?: string[];
}

export interface Rule {
  rule_id: string;
  restriction_type: RestrictionType;
  details: string;
  extents: Extent[];
  dates: string[];
  includes_tributaries?: boolean | null;
  tributaries_only?: boolean;
  tributary_excludes?: Extent[];
  sections_override?: string[] | null;
  needs_review: boolean;
  review_reason: string;
  rule_text: string;
  location_text: string;
  exception: string;
  display_location: string;
  unresolved_locators: string[];
  /** normalized ids of the DEFAULT restrictions this rule lifts (spring_closure, bait_ban, ...).
   *  A regional closure applies unless a water is exempted, so this has to be machine-readable —
   *  "is this river open?" cannot be answered from the closure rules alone. */
  exempts_from: string[];
  species: string[];
}

/** The controlled vocabulary for Rule.exempts_from — mirrors entry_models.Rule.exempts_from. */
export const EXEMPTS_FROM_VOCAB = [
  "spring_closure",
  "summer_closure",
  "trout_char_release",
  "bull_trout_release",
  "bait_ban",
  "single_barbless_hook",
  "kokanee_stream_quota",
] as const;

export interface Identity {
  name: string;
  region: string;
  mus: string[];
}

export interface Tributaries {
  included: boolean;
  only: boolean;
  excludes: Extent[];
}

export interface ReviewIssue {
  severity: string;
  problem: string;
  fix: string;
}

export interface ParseReview {
  verdict: string; // "" | pass | changes_requested
  model: string;
  reviewed_at: string;
  issues: ReviewIssue[];
}

export interface Entry {
  entry_id: string;
  identity: Identity;
  regs_verbatim: string;
  /** Where the row is PRINTED. Nested because it is all one fact about the book — it was a flat
   *  `source_symbols` until a page number joined it. */
  source: { pages: number[]; symbols: string[]; row_image: string };
  locked: boolean;
  reviewed_by: string;
  reviewed_at: string;
  revisit: boolean;
  revisit_note: string;
  /** This row carries no regulations of its own — it points at another entry under a different
   *  name ("BEAR LAKE: See Cowichan Lake"). The backend has always saved it; the type omitted it,
   *  so every read of `entry.reference_only` in EntryDetail was a typecheck error. */
  reference_only: boolean;
  parse_review: ParseReview;
  matched: string[];
  registry_status: RegistryStatus;
  registry_note: string;
  tributaries: Tributaries;
  scope: Extent[];
  rules: Rule[];
  audit_log: string[];
}

// ---- API response shapes --------------------------------------------------

export interface RegionSummary {
  id: string;
  total: number;
  by_status: Partial<Record<Status, number>>;
}

export interface QueueRow {
  entry_id: string;
  region: string;
  name: string;
  mus: string[];
  status: Status;
  locked: boolean;
  revisit?: boolean;
  /** a "See X" pointer row — carries no regulations of its own */
  reference_only?: boolean;
  registry_status: string;
  n_rules: number;
  matched_item_id: string | null;
  matched_item_name: string | null;
  also_item_ids: string[]; // a combined override's OTHER items (Chilliwack/Vedder, Fraser + channels)
  unused_curated_splits: number;
}

export interface Boundary {
  id: string;
  label: string;
  kind: string;
  ref: string;
  wbk: string;
  curated: boolean;
  item_id?: string; // which registry item this cut-point belongs to (combined entries)
  meta?: Record<string, unknown> | null; // AS-BUILT split metadata (anchor_type, route_measure, blk, …)
  in_graph?: boolean; // baked into the current graph build
  in_splits?: boolean; // present in the live splits.json source
  live?: { label?: string; kind?: string; anchor?: unknown; note?: string } | null; // live splits.json values
}

export interface SpeciesOption {
  code: string;
  name: string;
  is_group: boolean;
}

export interface SplitRef {
  entry_id: string;
  region: string;
  rule_id: string;
  details: string;
  entry_name: string;
}

export interface Item {
  id: string;
  name: string;
  kind: string;
  variants: string[];
  mus: string[];
  boundaries: Boundary[];
}

export interface MatchInfo {
  item_id: string | null;
  status: string;
  reason: string;
  candidates: unknown[];
  also: string[]; // other registry items a combined override pinned for this row
}

export interface AlsoItem {
  id: string;
  name: string;
  kind: string;
}

/** another synopsis row covering the same water — review it alongside this one */
export interface RelatedEntry {
  entry_id: string;
  region: string;
  name: string;
  locked: boolean;
  n_rules: number;
  /** its regs are just a cross-reference ("See Chilliwack River") */
  pointer: boolean;
  shared_items: { id: string; name: string }[];
}

export interface UnusedSplit {
  id: string;
  label: string;
  anchor_type: string;
}

export interface EntryDetail {
  entry: Entry;
  region: string;
  match: MatchInfo;
  item: Item | null; // `boundaries` is the union over the item AND `also_items`
  also_items: AlsoItem[]; // a combined override's other items — the entry covers these too
  related_entries: RelatedEntry[]; // other synopsis rows over the same water
  unused_curated_splits: UnusedSplit[];
  source_image: string | null; // synopsis row-crop image filename, if found
}

/** One split whose cut lands at more than one measure on the same blue line, so the reach shown is
 *  one of two honest readings (an area boundary the stream crosses twice). */
export interface AmbiguousCut {
  split_id: string;
  used: number;
  also_at: number[];
}

/** What one extent actually selects on the map. `unclassified` = pieces that genuinely straddle the
 *  reach's end, surfaced rather than silently included or dropped. */
export interface ResolvedExtent {
  sections: string[];
  unclassified: string[];
  ambiguous_cut: AmbiguousCut[];
  /** the named waters this reach actually lands on, biggest share first */
  waters: string[];
}

/** A distinct reach within one entry, shared by every rule that resolves to the same sections.
 *  `key` is a short stable label (A, B, C…) so the rules list and the map agree on what to call it. */
export interface ReachIdentity {
  key: string;
  color: string;
  waters: string[];
  n: number;
}

/** GET /api/entries/{id}/reaches — per rule, one entry per extent (null = not determinable). */
export interface EntryReaches {
  covered: string[];
  rules: Record<string, (ResolvedExtent | null)[]>;
  /** the BUILDER's answer per rule — outcome + the sections that actually ship, tributaries
   *  already expanded. `rules` above is the raw per-extent resolve and has no tributaries. */
  verdict?: Record<string, RuleVerdict>;
}

export interface RuleVerdict {
  outcome: string;
  reason: string | null;
  detail: string;
  n_sections: number;
  sections: string[];
  tributaries_pending: boolean;
  diagnostics: { kind: string; [k: string]: unknown }[];
}

/** One EXCEPT clause and what it removed. `above` is everything upstream of the named water,
 *  which a carve-out always takes with it. */
export interface CarveOut {
  extent: unknown;
  resolved: boolean;
  n_sections: number;
  above: number;
}

/** What one rule resolves to: IN (`sections`) and OUT (`n_excluded` + the drawn exclusions). */
export interface RuleResolved {
  entry_id: string;
  rule_id: string;
  outcome: string;
  wants_tributaries: boolean;
  tributaries_only: boolean;
  within_area: boolean;
  /** how many EXCEPT carve-outs the entry/rule authors */
  n_carve_outs_authored: number;
  /** ...and whether they bite on THIS rule. They only apply where tributaries are expanded,
   *  so a mainstem-only rule authors them but is not narrowed by them. */
  carve_outs_apply: boolean;
  /** the extents alone, before tributary/area expansion */
  n_direct: number;
  /** what the builder ADDED — tributaries, or an area's other waters */
  n_added: number;
  n_total: number;
  /** sections the EXCEPT carve-outs removed */
  n_excluded: number;
  /** bound sections the item layer never had, so they exist only in this payload */
  n_offitem: number;
  carve_outs: CarveOut[];
  sections: string[];
  truncated: boolean;
  limit: number;
  geojson: GeoJSON;
  error?: string;
}

export interface ItemSearchResult {
  id: string;
  name: string;
  kind: string;
  mus: string[];
}

export interface SaveResult {
  ok: boolean;
  errors: string[];
}

export interface GeoJSON {
  type: string;
  features: unknown[];
  _todo?: string;
}

// ---- Graph rebuild (pipeline.atlas.build --full, run as a background subprocess) ----

export interface RebuildStage {
  label: string;
  done: boolean;
  seconds: number | null;
}

export interface RebuildStatus {
  status: "idle" | "running" | "done" | "error";
  elapsed_s: number;
  current: string;
  stages: RebuildStage[];
  n_done: number;
  n_total: number;
  returncode: number | null;
  error: string;
  log_tail: string[];
}
