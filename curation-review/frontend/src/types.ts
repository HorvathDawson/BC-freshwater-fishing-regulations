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
  tributary_excludes?: Extent[];
  sections_override?: string[] | null;
  needs_review: boolean;
  review_reason: string;
  rule_text: string;
  location_text: string;
  exception: string;
  display_location: string;
  unresolved_locators: string[];
  species: string[];
}

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
  source_symbols: string[];
  locked: boolean;
  reviewed_by: string;
  reviewed_at: string;
  revisit: boolean;
  revisit_note: string;
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
  registry_status: string;
  n_rules: number;
  matched_item_id: string | null;
  matched_item_name: string | null;
  unused_curated_splits: number;
}

export interface Boundary {
  id: string;
  label: string;
  kind: string;
  ref: string;
  wbk: string;
  curated: boolean;
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
  item: Item | null;
  unused_curated_splits: UnusedSplit[];
  source_image: string | null; // synopsis row-crop image filename, if found
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

// ---- Graph rebuild (pipeline.build --full, run as a background subprocess) ----

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
