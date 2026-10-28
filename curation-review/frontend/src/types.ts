// TypeScript mirror of the API.md shapes — the entry is a `CatalogueEntry`
// (pipeline/regs/parsing/catalogue.py), in its own shape, READ ONLY: the screen writes one thing, a
// mark (T185 retired the editors). Nothing of the retired prose model is declared here.
// Kept in one file on purpose (internal tool).

/** An extent op (`entry_models.Op`). */
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
// Every key below is the model's own JSON key (`while`, `except` — the aliases), so a gate
// error's `path` (`rules.2.gear.0.except`) names the field it is about.

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

/** An error addressed to the field it is about (`rules.2.gear.0.max`), or to none (`""`). */
export interface FieldError {
  path: string;
  msg: string;
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
/** The review filter: a mark's status, `todo` (not human-reviewed now), or `recheck` (human-reviewed
 *  once, changed since — back for a re-check, T184). */
export type ReviewFilter = VerifyStatus | "todo" | "recheck" | "";

/** THE HUMAN-REVIEWED STATE (T184, `reuse._entry_mark_state`): who verified the entry on every check
 *  and when; `holds` false once its text, rules or extent changed since (T97 staleness) — then it
 *  is back in the queue for a re-check. */
export interface Reviewed {
  by: string[];
  on: string;
  holds: boolean;
}

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
  scope: "entry" | "rule" | "split" | "dfo" | "tidal";
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
  tributaries?: Record<string, TribCall>;
  lake_split_later?: boolean;
  extent_fix?: ExtentFix[];
  /** T210: new splits this mark proposed that another mark had already proposed (reused ids) */
  reused?: { ref: string; id: string; name: string }[];
  status: VerifyStatus;
  reasons: StaleReason[];
}

export interface QueueRow {
  /** owner 2026-10-27: the second method's verdict on the entry's split proposals */
  pv?: Pv | null;
  entry_id: string;
  region: string;
  name: string;
  mus: string[];
  status: Status;
  kind?: EntryKind;
  n_rules: number;
  matched_item_id: string | null;
  matched_item_name: string | null;
  /** MATCHED BY AREA: no registry item, but every rule binds through these areas */
  matched_areas?: AreaHit[];
  also_item_ids: string[]; // a combined override's OTHER items (Chilliwack/Vedder, Fraser + channels)
  unused_curated_splits: number;
  /** the first page the row is printed on */
  page?: number | null;
  /** a "See X" row with no rules of its own */
  pointer?: boolean;
  verify?: VerifyStatus;
  verify_note?: string;
  mark_reasons?: StaleReason[];
  /** T184: human-reviewed (by whom, when, and whether it still holds), or null */
  reviewed?: Reviewed | null;
  /** T133: is this entry built (`Vintage.state`), and if not why */
  vintage?: Vintage["state"];
  vintage_reasons?: string[];
  /** T134: the review tier (PLAN §3) */
  tier?: Tier;
  /** T135: the risk factors, most urgent first (`tiers.RISKS`) */
  risks?: string[];
  /** T135: how many pre-check findings, by severity (A facts, B heuristics; I notes are not counted) */
  prechecks?: { A: number; B: number };
  /** owner 2026-10-26: is there a real issue to review (`severity.entry_severity`) — the one chip */
  severity?: EntrySeverity;
  /** why, most severe first ("high: parser-uncertain (x.r1)") — the chip's hover */
  severity_reasons?: string[];
}

/** An entry's severity (`severity.LEVELS`), the queue's first order (owner 2026-10-26). */
export type EntrySeverity = "high" | "medium" | "low" | "none";
/** the queue's severity filter (`severity.FILTERS`): a level, or high + medium */
export type SeverityFilter = EntrySeverity | "issues" | "";

export interface Boundary {
  id: string;
  label: string;
  kind: string;
  ref: string;
  wbk: string;
  curated: boolean;
  /** a cut the build minted itself (gauge__…, length:…, area:…) — never in splits.json */
  minted?: boolean;
  /** every other id this cut-point answers to (a rule naming any of them binds it) */
  aliases?: string[];
  /** the other NAMES at this place (merged gauge/area/border/length, lake, a second authored point) */
  names?: string[];
  /** T207: how sure the curator is of where the cut stands (absent: high) */
  confidence?: "high" | "medium" | "low";
  item_id?: string; // which registry item this cut-point belongs to (combined entries)
  meta?: Record<string, unknown> | null; // AS-BUILT split metadata (anchor_type, route_measure, blk, …)
  in_graph?: boolean; // baked into the current graph build
  in_splits?: boolean; // present in the live splits.json source
  live?: {
    label?: string; kind?: string; anchor?: unknown; note?: string; review?: SplitReview;
  } | null; // live splits.json values
}

export interface Item {
  id: string;
  name: string;
  kind: string;
  variants: string[];
  mus: string[];
  boundaries: Boundary[];
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
  /** missing-split / ambiguous-cut proposals (`entry-split-suggestions.json`), read only */
  split_suggestions?: SplitSuggestion[];
  entry: Entry;
  region: string;
  item: Item | null; // `boundaries` is the union over the item AND `also_items`
  also_items: AlsoItem[]; // a combined override's other items — the entry covers these too
  /** MATCHED BY AREA: no registry item, but every rule binds through these areas */
  matched_areas?: AreaHit[];
  /** Which part of the book this came from — see `EntryKind`. */
  kind?: EntryKind;
  related_entries: RelatedEntry[]; // other synopsis rows over the same water
  unused_curated_splits: UnusedSplit[];
  source_image: string | null; // synopsis row-crop image filename, if found
  /** the marks in force on this entry (its entry mark, its rule marks) */
  marks?: MarkView[];
  /** T184: human-reviewed (by whom, when, and whether it still holds), or null */
  reviewed?: Reviewed | null;
  /** T133: whether the binding and the answer on screen are this entry's */
  vintage?: Vintage;
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

/** One split the entry's rules name (GET /api/entries/{id}/splits, `reuse.entry_split_points`) —
 *  what the map labels so the reviewer checks the cut BY PROXY (T184: no split is a review target). */
export interface NamedSplit {
  split_id: string;
  /** the built label (else splits.json's, else the boundary's) — what the map writes */
  label: string;
  /** its splits.json waterbody (a lake boundary: the water it is on) */
  water: string;
  /** "WATER: label" */
  name: string;
  /** the rule ids that name it, in rule order */
  rules: string[];
  /** any other part that names it: "entry scope", a licensing record's id */
  parts: string[];
  /** `"verify": true` or a stale mark (`tiers.split_needs_verify`); a `_note` never counts */
  to_verify: boolean;
  grade: "L" | "M" | "H" | null;
  item_id: string | null;
}

export interface EntrySplits {
  type: "FeatureCollection";
  features: { type: "Feature"; geometry: { type: "Point"; coordinates: [number, number] };
              properties: NamedSplit }[];
  /** named, but the served atlas has no point for it — listed, never dropped */
  unplaced: (NamedSplit & { why: string })[];
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

export interface GeoJSON {
  type: string;
  features: unknown[];
}

/** What the curator left on a cut-point (`reuse.split_review`): its `_note` — how it was placed and
 *  how it was placed — and the auto-proposal grade the note opens with (`L` low, `M`, `H`),
 *  null when the cut is not an open auto-proposal. */
export interface SplitReview {
  note: string;
  grade: "L" | "M" | "H" | null;
}

// ---- The review screen (T134, PLAN §4 phase 3) ------------------------------------------------

export type Check = "T" | "R" | "P" | "A";
export type CheckAnswer = "y" | "n" | "?";
/** the review tiers (PLAN §3, `tiers.TIERS`) — entries only (T184: the S0 split tier is gone) */
export type Tier = "P1" | "Z" | "P2" | "P3" | "P4" | "sweep";

/** GET /api/mode — what the server reviews with (review mode, the only mode). */
export interface Mode {
  /** the app the answer is checked in (its start link) */
  app_url: string;
  /** the checks each scope a reviewer marks answers (`marks.CHECKS`; an entry, a zone entry's rule) */
  checks: Record<"entry" | "rule", Check[]> & { dfo?: Check[]; tidal?: Check[] };
  /** the problem categories a "wrong" mark names, by check (`marks.PROBLEMS`) */
  problems: Record<Check, string[]>;
  /** T135: the pre-check codes (`precheck.CHECKS`) */
  prechecks: Record<string, { severity: Severity; what: string }>;
  /** T135: what the screen PRINTS for the human to run (commit the marks, export tickets) */
  commands: Record<"commit_marks" | "tickets" | "prechecks", string>;
}

/** A pre-check's severity (`precheck.SEVERITIES`): A a fact, B a hint, I a note — nothing to fix (T207). */
export type Severity = "A" | "B" | "I";

/** One pre-check finding (`precheck.prechecks`). */
export interface Precheck {
  code: string;
  severity: Severity;
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
  /** owner 2026-10-26: the one severity chip and its reasons */
  severity: EntrySeverity;
  severity_reasons: string[];
  evidence: Evidence;
  coverage: { segments: CoverageSegment[]; unquoted_chars: number; missing: string[] };
  vintage: Vintage;
  answer: AnswerLink;
  date: string;
  /** a zone entry's rules carry their impact line and their own answer link */
  rules: { rule_id: string; impact?: string; answer?: AnswerLink }[];
  /** T135: the pre-checks, the gate included */
  prechecks: Precheck[];
  /** T189: the entry's extent and every rule's, in plain words (`scope_words`) */
  scope: { entry: EntryScopeWords; rules: Record<string, RuleScopeWords> };
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

export interface TierProgress { tier: Tier | "all"; total: number; done: number; stale: number; todo: number }
export interface Progress { tiers: TierProgress[]; overall: TierProgress; facts?: QueueFacts }

/** What the reviewer sends (`POST /api/marks`); the server stamps the rest. No `verdict`: the
 *  answers decide it (`marks.verdict_from_answers`) — except `deferred`, which is the reviewer's own call. */
export interface MarkPost {
  scope: "entry" | "rule" | "dfo" | "tidal";
  target: string;
  rule_id?: string | null;
  verdict?: "deferred";
  answers: Partial<Record<Check, CheckAnswer>>;
  problem?: string | null;
  note?: string;
  reviewer: string;
  /** seconds on the target before this mark — the screen's clock (T136: the pilot's time per entry) */
  seconds?: number;
  /** T189: the reviewer's call on tributaries — "entry" or a rule id -> include | exclude */
  tributaries?: Record<string, TribCall>;
  /** T189: the target needs a lake split later (a lake divided into areas; RULINGS K5) */
  lake_split_later?: boolean;
  /** T191: the reviewer's proposed extent(s), one per scope — `marks.ExtentFix` */
  extent_fix?: ExtentFix[];
  /** T164: NEW splits proposed (`marks.NewSplit`) — proposals only, never splits.json */
  new_splits?: NewSplit[];
}

/** T191: the catalogue's extent ops (`marks.OP_POINTS`), by their own names */
export type ExtentOp = "whole" | "upstream_of" | "downstream_of" | "between" | "within" | "rest"
  | "steelhead_waters";
export type FixPointKind = "split" | "confluence" | "lake_inlet" | "lake_outlet" | "coord" | "new";
export interface FixPoint {
  kind: FixPointKind;
  split_id?: string;
  name?: string;
  item_id?: string;
  /** kind coord: [lon, lat] */
  coord?: [number, number];
}
/** T191: a proposed extent, recorded IN THE MARK (never a file edit; T185 proposes the edit) */
export interface ExtentFix {
  scope: string;              // "entry" or a rule id
  op: ExtentOp;
  points: FixPoint[];
  items: string[];
  tributaries?: boolean | null;
  area?: string | null;
}
export interface ExtentOpInfo { op: ExtentOp; points: number; words: string }
export interface SplitHit { id: string; label: string; water: string; kind?: string | null }
export interface ItemHit { id: string; name: string; kind: string; mus: string[] }
/** One atlas area (`/api/areas/search`): the "within an area" op's pick list. */
export interface AreaHit { id: string; name: string }
export interface ExtentPreview {
  resolvable: boolean;
  why: string | null;
  n_sections: number | null;
  outcomes?: Record<string, string>;
  truncated?: boolean;
  geojson?: GeoJSON;
}

/** T189: a reviewer's tributary call — the water's tributaries should be in, or out */
export type TribCall = "include" | "exclude";

export interface NamedRef { id: string; name: string }

/** T189 (`scope_words.rule_scope`): where one rule holds, in words. */
export interface RuleScopeWords {
  where: string;
  /** the book's own words for the place (`extent_text`), when the parse kept them */
  book_says: string | null;
  items: NamedRef[];
  splits: NamedRef[];
  /** holds only in this part, which nothing draws */
  in_part: string | null;
  /** T207: false when the book never says which part ("on parts") — an honest note, not a problem */
  in_part_identified: boolean;
  side: string | null;
  unresolved: string[];
  /** the reach builder's answer (`classify.wants_tributaries`) and where it comes from */
  tributaries: { yes: boolean; only: boolean; why: string };
  carve_outs: string[];
  /** the zone defaults it is not subject to (`exempts`) */
  exceptions: string[];
}

/** T189 (`scope_words.entry_scope`): the entry's own scope, which clips every rule. */
export interface EntryScopeWords {
  items: NamedRef[];
  where: string;
  tributaries: { yes: boolean; stated: boolean };
  carve_outs: string[];
  /** the heading's own exception ("except Eve River") */
  name_except: string | null;
  /** the rules that walk their tributaries */
  rules_walking: string[];
}

// --------------------------------------------------------------------------- T164: DFO review mode

/** what a NEW split is anchored to (`marks.AnchorKind`) */
export type AnchorKind = "confluence" | "lake_inlet" | "lake_outlet" | "bridge" | "dam" | "sign"
  | "coord" | "split";
/** T164: a NEW split proposed in a mark (`marks.NewSplit`) — the server checks its anchor and
 *  stamps where it resolved (blk, measure, coord) */
export interface NewSplit {
  ref: string;
  anchor: AnchorKind;
  name?: string | null;
  item_id?: string | null;
  split_id?: string | null;
  /** the water it cuts (a registry id); none = the location's water */
  on_item?: string | null;
  /** [lon, lat] */
  coord?: [number, number] | null;
  blk?: string | null;
  measure?: number | null;
  /** which side of the cut the extent lies on */
  side?: "upstream" | "downstream" | null;
  /** + upstream, - downstream of the anchor */
  offset_m?: number | null;
  /** how sure the proposer is of where the cut stands (owner 2026-10-27); none = the anchor's
   *  default — a hand-placed point medium, a registry anchor high (`marks.default_confidence`) */
  confidence?: SplitConfidence | null;
  quote?: string;
  note?: string;
}
/** a proposed split's confidence (`marks.Confidence`, splits.json's `confidence`) */
export type SplitConfidence = "high" | "medium" | "low";
/** GET /api/dfo/locations/{id}/anchor */
export interface AnchorCheck {
  ok: boolean;
  why: string | null;
  coord?: [number, number];
  blk?: string;
  measure?: number;
  on_item?: string;
  distance_m?: number;
  words?: string;
  item_id?: string;
}
/** T210: a PROPOSED split, shared — every mark's `new_splits` deduped by anchor (`marks.proposals`),
 *  the first proposal's name kept, with the marks that proposed it */
export interface ProposedSplit {
  id: string;
  anchor: AnchorKind;
  name: string | null;
  item_id: string | null;
  split_id: string | null;
  on_item: string | null;
  coord: [number, number] | null;
  blk: string | null;
  measure: number | null;
  side: "upstream" | "downstream" | null;
  offset_m: number | null;
  quote: string;
  marks: { target: string; ref: string; reviewer: string; date: string }[];
  n_marks: number;
  /** the proposed split's confidence (an owner override, the latest set, else the anchor default) */
  confidence: SplitConfidence;
}
export type DfoState = "needs_cut" | "bound" | "unbound" | "no_water";
export interface DfoRow {
  pv?: Pv | null;
  id: string;
  region: string;
  kind: string;
  water: string;
  water_id: string | null;
  items: string[];
  section: string | null;
  status: "active" | "dormant";
  text: string;
  op: string | null;
  anchors: string[];
  n_rows: number;
  n_rules: number;
  need: { op: string | null; points: number | null; have: number; bound: boolean; missing: number };
  state: DfoState;
  duplicate_of: string | null;
  verify: VerifyStatus;
  reasons: string[];
  new_splits: number;
  /** the confidence of the location's SUGGESTED extent (`dfo-suggestions.json`), when there is one */
  suggested?: SuggestConfidence | null;
}
export type SuggestConfidence = "high" | "medium" | "low" | "none";
/** A SUGGESTED extent (`pipeline.regs.dfo_salmon.suggest`): never applied — "accept suggestion"
 *  pre-fills the extent-fix editor (`suggestionFix`); the owner's save is the only write. */
export interface DfoSuggestion {
  location_id: string;
  /** existing (every point a cut that exists or the owner proposed) | new | whole | partial | none */
  matched: "existing" | "new" | "whole" | "partial" | "none";
  confidence: SuggestConfidence;
  reason: string;
  suggestion: {
    op: ExtentOp;
    points: FixPoint[];
    items: string[];
    tributaries: boolean | null;
    /** the NEW splits its `kind: new` points name, by ref */
    new_splits: NewSplit[];
    words?: string;
    /** op within: the area */
    area?: string;
  } | null;
  /** each point's label and, for a new one, where the data pins it */
  points?: { label: string; coord: [number, number] | null; kind: string; split_id?: string }[];
  /** THE SPLIT HUNT (owner 2026-10-27, `dfo-split-hunt.json`): hand-researched cuts, each a proposal */
  hunt?: DfoHunt | null;
  /** COVERAGE (owner 2026-10-27, `dfo_salmon/coverage.py`): why the location had no extent */
  why?: string;
  /** read before accepting: what the proposal cannot say (tributaries-only, radii kept as text, …) */
  caveat?: string;
  /** the rewordings of this place on the same water (DFO rewords a reach year to year) */
  cluster?: DfoCluster;
  /** the matcher's own reading, when the cluster replaced it */
  before?: { suggestion: DfoSuggestion["suggestion"]; confidence: string; matched: string; reason: string } | null;
}
export interface DfoCluster {
  id: string;
  members: string[];
  /** members with a curated extent */
  placed: string[];
  canonical: string;
  /** inherits (a curated member's extent) | adopts (the cluster's better suggestion) | agrees | canonical | member */
  role: "inherits" | "adopts" | "agrees" | "canonical" | "member";
  links: Record<string, string>;
}
/** One hand-researched cut: where (lat/lon, snapped to the FWA line), how sure, and why. */
export interface HuntProposal {
  ref: string;
  anchor: AnchorKind;
  name: string;
  confidence: SuggestConfidence;
  description: string;
  sources: string[];
  notes: string;
  lat: number;
  lon: number;
  /** [lon, lat] drawn on the map (snapped, offset applied) */
  coord: [number, number] | null;
  offset_m?: number | null;
  split_id?: string | null;
  /** metres from the given lat/lon to the water's FWA line */
  snap_m?: number | null;
  warning?: string;
}
export interface DfoHunt {
  status: "found" | "not_found";
  /** not found: why */
  why?: string;
  /** found: how the notice's words were read */
  notes?: string;
  sources?: string[];
  proposals?: HuntProposal[];
  matcher?: { matched: string; reason: string };
}
export interface DfoCount {
  total: number;
  done: number;
  by_verify: Record<string, number>;
  by_state: Record<string, number>;
  new_splits: number;
}
export interface DfoProgress { all: DfoCount; active: DfoCount; regions: Record<string, DfoCount> }
export interface DfoExtent { op: string; splits?: string[]; item_id?: string; item_ids?: string[] }
export interface DfoDetail {
  row: DfoRow;
  location: {
    location_id: string; water: string; section: string | null; status: string; kind: string;
    source_text: { waters?: string; specific_area?: string; op?: string; anchor_types?: string[] };
    binding: { extents: DfoExtent[]; tributaries: boolean | null; tributaries_only: boolean;
               tributary_excludes: DfoExtent[]; notes: string[]; spatial_caveat: boolean };
    duplicate_of: string | null;
  };
  water: { water_id: string; name: string; item_ids: string[]; tributaries: boolean | null;
           match: { status?: string; via?: string } } | null;
  items: { id: string; name: string; kind: string | null; mus: string[]; n_sections: number }[];
  page_url: string | null;
  verbatim: { waters: string; specific_area: string };
  rows: string[];
  rules: { rule_id: string; label: string; verbatim: string }[];
  need: DfoRow["need"];
  splits: EntrySplits;
  /** T210: the proposed splits on this river, from ALL marks, deduped */
  proposed: ProposedSplit[];
  hints: { name: string; anchor: AnchorKind; items: ItemHit[] }[];
  siblings: { id: string; text: string; status: string; section: string | null }[];
  /** the SUGGESTED extent, or null (never applied) */
  suggestion?: DfoSuggestion | null;
  mark: (MarkView & { new_splits?: NewSplit[] }) | null;
}

// --- T212: the Tidal review mode (`curation-review/backend/tidal.py`) ---
export type TidalConfidence = "high" | "medium" | "low";
/** One word source a proposed tidal boundary is checked against (the synopsis box, a DFO page or
 *  notice, a synopsis row, a curated split, a tidal gauge); located ones carry measure / coord. */
export interface TidalSource {
  kind: string;
  source: string;
  words: string;
  measure?: number | null;
  coord?: [number, number] | null;
  located_by?: string | null;
  agrees?: boolean | null;
  polygon_minus_source_m?: number | null;
  osm?: { osm: string; crossings: string[]; offset_m?: number | null } | null;
}
/** A web finding on where tidal water ends (`tidal-web.json`, T212 follow-up), located on the line
 *  when it can be (`located`); `agrees` with the polygon's crossing within 300 m. */
export interface TidalWeb {
  marker: string;
  kind?: string;
  quote?: string;
  url: string;
  site: string;
  confidence: "high" | "medium" | "low";
  located?: { measure: number; coord?: [number, number] | null; located_by: string } | null;
  /** a DFO boundary drawn as a line between two landmarks: its ends [lon, lat] and their names */
  line?: [number, number][];
  endpoints?: string[];
  agrees: boolean | null;
}
/** The triage class (`pipeline.tools.audit.tidal_triage`): was the polygon hand-edited at this stream,
 *  and do the book / web words agree with it. */
export type TidalTriageClass = "missed" | "sanity" | "multi-crossing" | "owner-edited" | "book agrees" | "no claim" | "owner-decided";
export interface TidalTriage {
  class: TidalTriageClass;
  /** null: the edit fingerprint can't see this stretch of coast */
  edited: boolean | null;
  edit_sites: string[];
  why: string;
  web: TidalWeb[];
}
/** One proposed tidal boundary (`tidal-candidates.json`) */
export interface TidalCandidate {
  id: string;
  water: string;
  blk: string;
  wsc: string;
  enters: "ocean" | "tidal_water";
  items: string[];
  item_names: string[];
  zones: string[];
  proposed: { measure: number; coord: [number, number]; above_mouth_m: number; basis: string;
              split_label: string; split_name: string; aliases: string[];
              /** a curated split within 25 m on the same line: aliased "tidal boundary", no new cut */
              reuse?: { split_id: string; label: string; distance_m: number; measured: string;
                        split_measure: number; tidal_measure: number; alias: string } };
  polygon: { crossing: string; measure: number | null; coord: [number, number] | null; n_cuts: number;
             inside_pieces: number; outside_below_m: number; mouth_gap_m: number;
             /** every crossing (m above the mouth), the first 12 */
             cuts?: number[];
             /** the line crosses in, out and in again (T212 follow-up 2): the outside gaps below the top
              *  are filled, and pieces the polygon does not join to the run are ignored */
             multi_crossing?: boolean; gaps_filled?: [number, number][]; ignored_pieces?: [number, number][] };
  /** a tributary of the Fraser / Skeena / Nass joining below that river's tidal top (m on the mainstem) */
  joins_big_river_tidal_reach_at_m?: number;
  mouth: { measure: number; coord: [number, number] };
  sources: TidalSource[];
  confidence: TidalConfidence;
  why: string;
  agreement: string;
  words_disagree: boolean;
  triage?: TidalTriage;
}
/** GET /api/tidal/queue */
export interface TidalRow {
  pv?: Pv | null;
  id: string;
  water: string;
  blk: string;
  enters: string;
  zones: string[];
  items: string[];
  confidence: TidalConfidence;
  agreement: string;
  crossing: string;
  above_mouth_m: number;
  n_words: number;
  words_disagree: boolean;
  verify: string;
  reasons: string[];
  triage: TidalTriageClass | null;
  edited: boolean | null;
  n_web: number;
  triage_why: string | null;
  multi_crossing?: boolean;
  n_cuts?: number;
}
/** GET /api/tidal/progress */
export interface TidalProgress {
  total: number;
  done: number;
  by_verify: Record<string, number>;
  by_confidence: Record<string, number>;
  with_words: number;
  by_triage: Record<TidalTriageClass, number>;
  coverage: Record<string, unknown>;
}
/** GET /api/tidal/candidates/{id} */
export interface TidalDetail {
  row: TidalRow;
  candidate: TidalCandidate;
  items: { id: string; name: string; kind: string | null; mus: string[] }[];
  points: EntrySplits;
  mark: (MarkView & { new_splits?: NewSplit[] }) | null;
}

/* PRE-VERIFIED (owner 2026-10-27): a second, independent method's verdict (`backend/preverify.py`) */
export type PvStatus = "pre-verified" | "disagree" | "unchecked";
export interface Pv {
  status: PvStatus;
  why: string;
  method: string | null;
  first: string | null;
  delta_m: number | null;
  tolerance_m: number | null;
  duplicate_of: string | null;
  /** an entry's row: how many of its proposals, and how many pre-verified */
  n?: number;
  n_pre?: number;
}
/** one missing-split / ambiguous-cut proposal on an entry (`pipeline.regs.review.split_suggest`) */
export interface SplitSuggestion {
  id: string;
  rule_id: string;
  kind: string;
  phrase: string;
  suggestion: {
    op: string; points: { kind: string; split_id: string }[]; items: string[];
    new_splits: { ref: string; name?: string; coord?: [number, number]; measure?: number; confidence?: string; anchor?: string }[];
    words?: string;
  } | null;
  confidence: "high" | "medium" | "low" | "none";
  reason: string;
  evidence: string[];
  preverify: Pv | null;
  duplicate_of: string | null;
  pick: { builder: number | null; proposed: number | null; agrees: boolean } | null;
}
