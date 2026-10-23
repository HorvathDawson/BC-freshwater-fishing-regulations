// TypeScript mirror of the API.md shapes — the entry is a `CatalogueEntry`
// (pipeline/regs/parsing/catalogue.py), in its own shape. Nothing of the retired prose model is
// declared here: the backend refuses those fields, and a field declared here invites a control
// that saves one.
// Kept in one file on purpose (throwaway internal tool).

/** An extent op — the values come from `/api/vocab` (`entry_models.Op`), not from here. */
export type Op = string;

/** What the PARSE and the build left (`reuse.entry_status`) — never review progress; the marks are
 *  `VerifyStatus`. T135 renamed "flagged" (parser-uncertain) and "unreviewed" (plain). */
export type Status =
  | "no_registry"
  /** a rule or licensing record carries a `review_reason` */
  | "parser-uncertain"
  | "unused_splits"
  /** nothing the parser or the build flagged */
  | "plain"
  /** A regional or provincial rule. It names no water BY DEFINITION, so it is a category,
   *  not a problem — it used to show as `no_registry` and sit at the top of the queue. */
  | "zone";

/** Which part of the book an entry came from. `zone` = a region chapter or the provincial
 *  pages; `water` = a row of a water table. They need different questions asked of them. */
export type EntryKind = "zone" | "water";

// ---- Entry model (catalogue.py) -------------------------------------------
// Every key below is the model's own JSON key (`while`, `except` — the aliases), so a validator
// error's `path` (`rules.2.gear.0.except`) names the control that edits it.

/** One extent: a plain dict on the model, read by the resolver as `entry_models.Extent`. */
export interface Extent {
  op: Op;
  splits?: string[];
  item_id?: string | null;
  /** several registry ids, when a reach's two cut-points sit on different waters. Mutually
   *  exclusive with item_id — scoping such a reach to one water puts the other end out of scope. */
  item_ids?: string[];
  area_id?: string | null;
  area_kind?: string | null;
  feature_types?: string[];
  within_area?: string | null;
  outside_area?: string | null;
  outside_areas?: string[];
  /** registry item ids (waters) subtracted — e.g. a lake that only reaches into the area */
  outside_items?: string[];
  /** an admin-area FAMILY subtracted, e.g. national_parks (the mirror of `area_kind`) */
  outside_area_kind?: string | null;
  /** op `rest` only: the rule ids (same entry) whose sections this extent is the complement of —
   *  "Other parts" is the rule's water minus what these rules bind (their walks included). */
  siblings?: string[];
  /** on upstream_of / downstream_of / between scoped to ONE river: the river's WATERSHED on that
   *  side of the cut(s), not the river alone. Refused with includes_tributaries (own or inherited),
   *  with item_ids, and with area_id/area_kind. */
  watershed?: boolean;
  /** an AMBIGUOUS CUT decided by hand (ruling E16 refinement): split id -> `lower` / `upper` /
   *  a measure (m) within 25 m of one of the run's candidates. Absent: the ruling picks. */
  cut_pick?: Record<string, "lower" | "upper" | number>;
  [key: string]: unknown;
}

/** An inclusive range of calendar days, no year (catalogue `DateRange`). */
export interface DateRange {
  from_month: number;
  from_day: number;
  to_month: number;
  to_day: number;
}

/** A time of day: a clock time OR a solar time with an offset in minutes (negative = before). */
export interface Clock {
  at?: string;
  solar?: "sunrise" | "sunset";
  offset_min?: number;
}

/** WHEN A RULE BINDS (catalogue `When`). Empty parts are omitted; `unparsed` is a season the
 *  parser could not read, which is NOT all year. */
export interface When {
  dates?: DateRange[];
  hours?: { start: Clock; end: Clock };
  weekdays?: string[];
  unparsed?: string[];
}

/** One range of fish lengths (inclusive; null = open) and how many of them you may keep. */
export interface LengthBand {
  min_cm?: number | null;
  max_cm?: number | null;
  take?: number | null;
}

/** When a gear clause applies — a closed vocabulary; `note` costs a review_reason. */
export interface GearWhen {
  water?: string | null;
  method?: string | null;
  targeting?: string[];
  angler?: string | null;
  gear_in_use?: string | null;
  note?: string;
}

/** How the thing a spec clause names must be built or carried. */
export interface GearSpec {
  attached_to?: string | null;
  attachment?: string | null;
  within_m_of_hook?: number | null;
  opening_shape?: string | null;
  note?: string;
}

/** One thing constrained, and how far. Ordered within `gear`; FIRST MATCH WINS per slot. */
export interface GearClause {
  slot: string;
  of?: string[];
  allow?: string[] | null;
  only?: string[] | null;
  ban?: string[] | null;
  except?: string[];
  members?: string[];
  max?: number | null;
  min?: number | null;
  unlimited?: boolean;
  when?: GearWhen | null;
  requires?: GearSpec | null;
  must_be?: string[];
  unless?: GearWhen[];
}

/** What a rule LIFTS: a zone default by slug, OR one rule by id (in `entry_id` if another's). */
export interface Exempts {
  default_id?: string | null;
  target?: string | null;
  entry_id?: string | null;
  note?: string;
}

/** WHICH ANGLERS — a set on every axis; an axis left out means any member. */
export interface Who {
  residency?: string[];
  age?: string[];
  guidance?: string[];
  status?: string[];
  /** the angler's role — `companion` of an authorized angler (Youth/Disabled Accompanied Waters) */
  role?: string[];
}

export interface Rule {
  rule_id: string;
  type: string;
  /** GENERATED (catalogue.label) — served and recomputed live, never stored. */
  label?: string;
  verbatim: string;
  obligation?: string;
  species?: string[];
  species_except?: string[];
  closed_to?: Who | null;
  /** the anglers inside `closed_to` the water stays open to */
  closed_to_except?: Who[];
  gear?: GearClause[];
  derived_from?: string | null;
  condition_of?: string | null;
  while?: string[];
  conduct?: string[];
  when?: When | null;
  take?: number | null;
  unlimited?: boolean;
  may_target?: boolean | null;
  period?: string;
  per_daily?: number | null;
  within?: string | null;
  lengths?: LengthBand[] | null;
  record_retention?: boolean;
  water?: string | null;
  origin?: string | null;
  /** a region's blanket closure only: which seasonal closure it is by name (spring | summer | winter) */
  closure_kind?: string | null;
  when_targeting?: string[];
  aspect?: string | null;
  level?: string | null;
  max_power_kw?: number | null;
  max_kmh?: number | null;
  includes_tributaries?: boolean | null;
  tributaries_only?: boolean;
  extents?: Extent[] | null;
  tributary_excludes?: Extent[];
  exempts?: Exempts[];
  standing?: boolean;
  authority?: string | null;
  notice?: string | null;
  suspended_while?: string | null;
  extent_text?: string;
  /** holds only in this part of what `extents` draw; nothing draws the part (a note, never coloured) */
  undrawn_part?: string;
  /** holds on this half of the river's channel only (north | south | east | west) */
  side?: string | null;
  /** a life stage the book defines ("adult" chinook, p.77) — the rule holds for that stage only */
  life_stage?: string | null;
  unresolved_locators?: string[];
  review_reason?: string;
}

// ---- Licensing (catalogue `LicensingRecord`, discriminated by `kind`) ----

export interface Ref { entry_id: string; id: string }
export interface Quote { verbatim: string }
export interface StampPeriod { when: When; verbatim: string }
export interface Suspension { rule_id: string; verbatim: string }
export interface Accompaniment { who: Who; holding?: string }
/** One way to satisfy a requirement: exactly one of hold / accompanied_by / as. */
export interface Path {
  hold?: string[];
  accompanied_by?: Accompaniment | null;
  as?: Who | null;
  quota?: string | null;
}
export interface Doing {
  act: string;
  species?: string[];
  species_except?: string[];
  origin?: string | null;
  lengths?: LengthBand[] | null;
}

interface RecordBase {
  id: string;
  verbatim: string;
  review_reason?: string;
  /** GENERATED (catalogue.licensing_label) — served and recomputed live, never stored. */
  label?: string;
}
export interface Designation extends RecordBase {
  kind: "designation";
  classified: string;
  unit: string;
  unit_name: string;
  when?: When | null;
  extents?: Extent[] | null;
  includes_tributaries?: boolean | null;
  tributaries_only?: boolean;
  tributary_excludes?: Extent[];
  steelhead_stamp_during?: StampPeriod | null;
  steelhead_stamp_waived?: Quote | null;
  suspended_while?: Suspension[];
}
export interface NotClassified extends RecordBase {
  kind: "not_classified";
  extents?: Extent[] | null;
}
export interface Requirement extends RecordBase {
  kind: "requirement";
  satisfied_by?: Path[];
  conduct?: string[];
  who?: Who | null;
  who_except?: Who | null;
  doing: Doing;
  extents?: Extent[] | null;
  includes_tributaries?: boolean | null;
  water?: string | null;
  on?: string | null;
  authority?: string | null;
  when?: When | null;
  restates?: Ref | null;
}
export interface LicenceTerms extends RecordBase {
  kind: "licence_terms";
  document: string;
  who?: Who | null;
  classified?: string | null;
  units?: string[];
  sold?: string | null;
  covers?: string | null;
  max_consecutive_days?: number | null;
  max_days_per_licence_year?: number | null;
  max_per_licence_year?: number | null;
  max_units_per_licence_year?: number | null;
  unlimited_days?: boolean;
  allocation?: string | null;
  needs?: string[];
  fee_cad?: number | null;
}
export interface Exemption extends RecordBase {
  kind: "exemption";
  who: Who;
  documents: string[];
}
export interface Alternative extends RecordBase {
  kind: "alternative";
  alternative_to: Ref;
  satisfied_by: Path[];
  extents: Extent[];
}
export type LicensingRecord =
  | Designation | NotClassified | Requirement | LicenceTerms | Exemption | Alternative;

export interface Entry {
  entry_id: string;
  /** pass-through from the synopsis row (see the Identity panel) — shown, not edited */
  name: string;
  display_name?: string;
  region?: string;
  scope_note?: string;
  regs_verbatim: string;
  source_pages?: number[];
  /** the printed glyphs, 1:1 — read-only */
  symbols?: string[];
  matched?: string[];
  extents?: Extent[];
  includes_tributaries?: boolean | null;
  rules?: Rule[];
  licensing?: LicensingRecord[];
  /** POINTERS ("See Lonzo Creek") — not rules; they bind nothing (catalogue.See). A row whose
   *  only content is a pointer has no rules and no licensing. */
  see?: See[];
  /** anadromous rainbow trout are found here: a rainbow over 50 cm IS a steelhead (p.80;
   *  catalogue.CatalogueEntry.anadromous_rainbow). Set per row where known, never inferred. */
  anadromous_rainbow?: boolean;
}

/** catalogue.See: the printed pointer, and the entries it names — OR why it names none. */
export interface See {
  verbatim: string;
  entry_ids?: string[];
  unresolved?: string;
}

// ---- Model vocabulary (GET /api/vocab — read off catalogue.py) ----------

export interface Vocab {
  rule_types: { type: string; family: string }[];
  licensing_kinds: string[];
  slots: { slot: string; shape: "set" | "spec" | "count" | "measured" }[];
  methods: string[];
  while: string[];
  /** the `means` / `devices` a `while` clause may name (catalogue.WHILE_MEANS / WHILE_DEVICES) */
  while_means: string[];
  while_devices: string[];
  conduct: { act: string; words: string }[];
  documents: { doc: string; words: string; provincial: boolean }[];
  periods: string[];
  water_kinds: string[];
  channel_sides: string[];
  life_stages: string[];
  /** catalogue.ClosureKind — the season a blanket closure is NAMED for (rule `closure_kind`) */
  closure_kinds: string[];
  origins: string[];
  obligations: string[];
  vessel_aspects: string[];
  propulsion_levels: string[];
  angler_states: string[];
  solar: string[];
  weekdays: string[];
  who_axes: Record<string, string[]>;
  exemptable_defaults: string[];
  doing_acts: string[];
  requirement_on: string[];
  authority: string[];
  classified: string[];
  terms_sold: string[];
  terms_covers: string[];
  terms_allocation: string[];
  terms_needs: string[];
  path_quota: string[];
  extent_ops: string[];
  extent_keys: string[];
  feature_types: string[];
  species: SpeciesOption[];
  /** OPEN strings in the model: tokens the corpus already uses, offered as suggestions only */
  gear_members: Record<string, string[]>;
  must_be: Record<string, string[]>;
  area_kinds: string[];
}

/** A validator error addressed to the field it is about: `rules.2.gear.0.max`. */
export interface FieldError {
  path: string;
  msg: string;
}

/** POST /api/check — what a save would refuse, and the labels the app will show. */
export interface CheckResult {
  ok: boolean;
  errors: FieldError[];
  warnings: FieldError[];
  labels: { rules: (string | null)[]; licensing: (string | null)[] };
}

// ---- API response shapes --------------------------------------------------

export interface RegionSummary {
  id: string;
  total: number;
  by_status: Partial<Record<Status, number>>;
  /** the reviewer's marks over the region (see `VerifyStatus`) */
  by_verify?: Partial<Record<VerifyStatus, number>>;
}

/** The reviewer's mark on an entry, from the marks store `marks.jsonl` (never in the entry; T97).
 *  `stale` = it was marked, then something it was held against changed — `mark_reasons` says what. */
export type VerifyStatus = "unmarked" | "verified" | "wrong" | "unsure" | "deferred" | "stale";
/** Why a mark no longer holds (`pipeline.regs.review.marks.StaleReason`). */
export type StaleReason =
  | "book text changed" | "rule changed" | "binding moved" | "answer changed" | "awaiting build";
/** risk: the review order (tier, the most urgent risk factor, the severity-A pre-checks, then the
 *  book — `tiers.queue_rank`); book: region, printed page, then the page's order. */
export type QueueOrder = "risk" | "book";

/** ONE VINTAGE ON SCREEN (T133, `pipeline.regs.review.vintage.entry_state`): whether the reach
 *  run, the bundle and the release on disk were made from this entry as it is now. Until
 *  "current", the binding and the answer are another vintage's and only T and R can be marked. */
export interface Vintage {
  state: "current" | "awaiting build";
  /** `vintage.Waiting` values — which artifact is not this entry's */
  reasons: string[];
  /** the splits it reads that the served atlas does not hold as authored */
  splits: string[];
  /** "anchors" when the atlas records each split's anchor; "ids" for an older atlas (a moved
   *  split is not seen there) */
  splits_checked: "anchors" | "ids";
  /** what the human runs (the tool never runs it) */
  commands: string[];
}

/** GET /api/build — the pending-build panel (PLAN §5). */
export interface PendingBuild {
  built: Record<string, unknown>;
  entries: { entry_id: string; region: string; name: string; reasons: string[]; splits: string[] }[];
  splits: { split_id: string; reasons: string[] }[];
  splits_checked: "anchors" | "ids";
  commands: string[];
}

/** One mark in force, with its status against its target NOW (`GET /api/marks`). */
export interface MarkView {
  scope: "entry" | "rule" | "split";
  target: string;
  rule_id: string | null;
  verdict: "verified" | "wrong" | "unsure" | "deferred";
  answers: Partial<Record<"T" | "R" | "P" | "A", "y" | "n" | "?">>;
  problem: string | null;
  note: string;
  reviewer: string;
  date: string;
  pages: number[];
  carried_by: string | null;
  /** the reviewer's time on the target before this mark (the screen's clock; T136 pilot) */
  seconds?: number | null;
  status: VerifyStatus;
  reasons: StaleReason[];
}

export interface QueueRow {
  entry_id: string;
  region: string;
  name: string;
  mus: string[];
  status: Status;
  kind?: EntryKind;
  n_rules: number;
  matched_item_id: string | null;
  matched_item_name: string | null;
  also_item_ids: string[]; // a combined override's OTHER items (Chilliwack/Vedder, Fraser + channels)
  unused_curated_splits: number;
  /** the first page the row is printed on */
  page?: number | null;
  /** a "See X" row with no rules of its own */
  pointer?: boolean;
  verify?: VerifyStatus;
  verify_note?: string;
  mark_reasons?: StaleReason[];
  /** T133: is this entry built (`Vintage.state`), and if not why */
  vintage?: Vintage["state"];
  vintage_reasons?: string[];
  /** T134: the review tier (PLAN §3) */
  tier?: Tier;
  /** T135: the risk factors, most urgent first (`tiers.RISKS`) */
  risks?: string[];
  /** T135: how many pre-check findings, by severity (A facts, B heuristics) */
  prechecks?: { A: number; B: number };
}

export interface Boundary {
  id: string;
  label: string;
  kind: string;
  ref: string;
  wbk: string;
  curated: boolean;
  /** a cut the build minted itself (gauge__…, length:…, area:…) — never in splits.json */
  minted?: boolean;
  item_id?: string; // which registry item this cut-point belongs to (combined entries)
  meta?: Record<string, unknown> | null; // AS-BUILT split metadata (anchor_type, route_measure, blk, …)
  in_graph?: boolean; // baked into the current graph build
  in_splits?: boolean; // present in the live splits.json source
  live?: {
    label?: string; kind?: string; anchor?: unknown; note?: string; review?: SplitReview;
  } | null; // live splits.json values
}

export interface SpeciesOption {
  code: string;
  name: string;
  is_group: boolean;
  members?: string[];
}

export interface SplitRef {
  entry_id: string;
  region: string;
  rule_id: string;
  label: string;
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
  /** a live-match SUGGESTION, only when `matched` is empty — never the entry's water */
  match: MatchInfo | null;
  item: Item | null; // `boundaries` is the union over the item AND `also_items`
  also_items: AlsoItem[]; // a combined override's other items — the entry covers these too
  /** Which part of the book this came from — see `EntryKind`. */
  kind?: EntryKind;
  related_entries: RelatedEntry[]; // other synopsis rows over the same water
  unused_curated_splits: UnusedSplit[];
  source_image: string | null; // synopsis row-crop image filename, if found
  /** the marks in force on this entry (its entry mark, its rule marks) */
  marks?: MarkView[];
  /** T133: whether the binding and the answer on screen are this entry's */
  vintage?: Vintage;
}

// ---- What an angler is told (the live bundle) -------------------------------------------------

export interface BundleMeta {
  built: string;
  version?: string;
  reach_digest?: string;
  [k: string]: string | undefined;
}

/** GET /api/entries/{id}/bundle — the bundle's copy of this entry beside the file's. */
export interface EntryInBundle {
  /** T133: the entry's vintage; while it awaits its build only this (and `awaiting`) is sent */
  vintage: Vintage;
  awaiting: boolean;
  bundle?: BundleMeta;
  in_bundle?: boolean;
  /** false when a rule's label OR its section count in the bundle differs from the file now
   *  (`vintage.bundle_matches`) */
  bundle_matches: boolean;
  /** rule_id -> what differs ("label", "sections: 1 in the bundle, 2 now", "not in the bundle") */
  mismatches?: Record<string, string[]>;
  /** rules one entry alone cannot count now (a complement, the steelhead set) */
  unchecked?: string[];
  rules?: { rule_id: string; label_bundle: string | null; label_now: string | null;
            n_sections: number; n_sections_now: number | null; unresolved: string | null }[];
  licensing?: { kind: string; id: string; label: string; unresolved: string | null;
                n_sections: number; n_waters: number; waters: { name: string; n: number }[] }[];
}

export interface AnswerPiece {
  sid: number;
  n_sections: number;
  rules: { rule_id: string; via: string }[];
}

export interface AnswerWater {
  item_id: string;
  name: string;
  n_sections: number;
  pieces: AnswerPiece[];
}

/** GET /api/answer — `read.effective_rules` at one section, one day, one fish. */
export interface Answer {
  sid: number;
  date: string;
  fish: string;
  waters: string[];
  tidal: string | null;
  rules: { entry_id: string; rule_id: string; entry_name: string; state: string; type: string;
           label: string; verbatim: string | null; via: string | null;
           undrawn_part: string | null; standing: boolean }[];
  licensing: { kind: string; entry_id: string; id: string; via: string; label: string }[];
  bundle: BundleMeta;
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
  errors: FieldError[];
}

export interface GeoJSON {
  type: string;
  features: unknown[];
  _todo?: string;
}

/** What the curator left on a cut-point (`reuse.split_review`): its `_note` — how it was placed and
 *  what is still to verify — and the auto-proposal grade the note opens with (`L` low, `M`, `H`),
 *  null when the cut is not an open auto-proposal. */
export interface SplitReview {
  note: string;
  grade: "L" | "M" | "H" | null;
}

// ---- The review screen (T134, PLAN §4 phase 3) ------------------------------------------------

export type Check = "T" | "R" | "P" | "A";
export type CheckAnswer = "y" | "n" | "?";
export type Tier = "S0" | "P1" | "Z" | "P2" | "P3" | "P4" | "sweep";

/** GET /api/mode — review (marks only) or the owner's fix mode. */
export interface Mode {
  fix: boolean;
  /** the app the answer is checked in (its start link) */
  app_url: string;
  /** the checks each scope answers (`marks.CHECKS`) */
  checks: Record<"entry" | "rule" | "split", Check[]>;
  /** the problem categories a "wrong" mark names, by check (`marks.PROBLEMS`) */
  problems: Record<Check, string[]>;
  /** T135: the pre-check codes (`precheck.CHECKS`) */
  prechecks: Record<string, { severity: "A" | "B"; what: string }>;
  /** T135: what the screen PRINTS for the human to run (commit the marks, export tickets) */
  commands: Record<"commit_marks" | "tickets" | "prechecks", string>;
}

/** One pre-check finding (`precheck.prechecks`). */
export interface Precheck {
  code: string;
  severity: "A" | "B";
  rule_id: string | null;
  what: string;
}

/** One ambiguous cut the reach RUN chose between (rule_diagnostic `ambiguous_cut`, T135) — every
 *  candidate and the side it took; the live resolver's view of the same cut is `AmbiguousCut`. */
export interface RunAmbiguousCut {
  split_id: string;
  extent: number;
  used: number;
  candidates: number[];
  /** the ruling's side (a split id or `mouth`), or `curated` when the extent's cut_pick decided */
  toward: string;
  also_at?: number[];
}

/** Where an entry is printed and how to see it (`evidence.locate`). */
export interface Evidence {
  kind: "row" | "passage" | "none" | "missing";
  printed?: number | null;
  pdf_page?: number;
  boxes?: number[][];
  /** a passage: the words of the entry's text found on the page / asked */
  found?: number | null;
  total?: number | null;
  /** none / missing: why, with the command that fixes it when there is one */
  why?: string;
  crop_url?: string;
  page_url?: string;
  /** the extractor's own full-width crop, offered beside the legible one */
  row_image?: string | null;
}

export interface CoverageSegment { text: string; kind: "quoted" | "unquoted" | "gap" }

/** The app's own page for the answer: a start link (owner Q34 default: a new tab). */
export interface AnswerLink {
  url: string | null;
  why?: string;
  item?: string;
  part?: number;
  sections?: number;
  via?: string;
}

/** GET /api/entries/{id}/review — what the T/R/P/A checklist is answered from. */
export interface ReviewView {
  entry_id: string;
  /** THE RAW NAME — the heading ("ADAM RIVER (except Eve River)") */
  name: string;
  display_name?: string | null;
  kind: EntryKind;
  region: string;
  tier: Tier;
  risks: string[];
  evidence: Evidence;
  coverage: { segments: CoverageSegment[]; unquoted_chars: number; missing: string[] };
  vintage: Vintage;
  answer: AnswerLink;
  date: string;
  /** a zone entry's rules carry their impact line and their own answer link */
  rules: { rule_id: string; impact?: string; answer?: AnswerLink }[];
  /** T135: the pre-checks, the save gate included */
  prechecks: Precheck[];
  /** T135: rule id -> the cuts the builder chose between */
  ambiguous_cuts: Record<string, RunAmbiguousCut[]>;
  /** T135: the second-pass agent's findings on the parse (none exist yet) */
  parse_findings: string[];
  /** T135: the reach run the risk factors were read from, or why there is none */
  run: QueueFacts;
}

/** Which reach run the queue's run-based factors came from (`reuse.queue_facts`). */
export interface QueueFacts {
  run: string | null;
  why: string | null;
}

/** GET /api/review/splits — a split card (tier S0). */
export interface SplitCard {
  split_id: string;
  label: string | null;
  kind: string | null;
  waterbody: string | null;
  grade: "L" | "M" | "H" | null;
  note: string;
  s0: boolean;
  item_id: string | null;
  status: VerifyStatus;
  verdict: string | null;
  reasons: string[];
  vintage: Vintage | null;
}

export interface TierProgress { tier: Tier | "all"; total: number; done: number; stale: number; todo: number }
export interface Progress { tiers: TierProgress[]; overall: TierProgress; facts?: QueueFacts }

/** What the reviewer sends (`POST /api/marks`); the server stamps the rest. No `verdict`: the
 *  answers decide it (`marks.verdict_from_answers`) — except `deferred`, which is the reviewer's own call. */
export interface MarkPost {
  scope: "entry" | "rule" | "split";
  target: string;
  rule_id?: string | null;
  verdict?: "deferred";
  answers: Partial<Record<Check, CheckAnswer>>;
  problem?: string | null;
  note?: string;
  reviewer: string;
  /** seconds on the target before this mark — the screen's clock (T136: the pilot's time per entry) */
  seconds?: number;
}
