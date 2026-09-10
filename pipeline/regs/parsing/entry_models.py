"""Frozen-parse data models — the rebuilt parser's output shape (DESIGN-regs-to-sections.md §4).

This is the NEW curation surface that replaces the flat `location_text` of the Gemini parser
(`models.py`). A rule binds to its reach by **`extents` (a list of `op + split ids`, unioned)** — a
*constrained selection* over the waterbody's curated split ids, not blind geometry extraction. The
file is frozen and checked in (`pipeline/regs/parsing/entries/region-N.json`); re-parsing is a reviewed
merge, never an overwrite.

Shape:
    EntryFile{ region, entries:[ Entry{ identity, regs_verbatim, source, tributaries,
                                        scope:[Extent], rules:[ Rule{ extents:[Extent], … } ],
                                        parse_review, locked/reviewed_by/revisit } ] }

The anti-hallucination chain-of-custody validators from `models.py` are preserved:
    rule_text ⊆ regs_verbatim   ·   location_text ⊆ rule_text   ·   each date ⊆ rule_text
Split-id existence is NOT checked here (the model can't know the allowed ids); the ingest step
validates each `extent.splits` against the waterbody's `splits.json` — see `validate_entry_splits`.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Mapping, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from pipeline.regs.parsing.dates import DateWindow, date_parse_errors, parse_date_windows
from pipeline.regs.parsing.species import KNOWN_SPECIES_CODES


# ---------------------------------------------------------------------------
# Shared enum + verbatim-validation normalizers (self-contained; the Gemini
# parser's models.py is being retired)
# ---------------------------------------------------------------------------


class RestrictionType(str, Enum):
    """Classification of a fishing regulation restriction."""

    CLOSURE = "closure"
    HARVEST = "harvest"
    GEAR_RESTRICTION = "gear_restriction"
    VESSEL_RESTRICTION = "vessel_restriction"
    LICENSING = "licensing"
    NOTE = "note"


def _normalize(text: str) -> str:
    """Normalize for flexible substring checks: strip bold markers, collapse whitespace, lowercase.
    Validation only — never mutates stored data."""
    if not text:
        return ""
    return " ".join(text.replace("**", "").replace("\n", " ").split()).lower()


def _normalize_date(text: str) -> str:
    """Like _normalize but also strips commas/periods (date comparison)."""
    return _normalize(text).replace(",", "").replace(".", "")


# ---------------------------------------------------------------------------
# Extent — the op + split binding
# ---------------------------------------------------------------------------


_FEATURE_TYPES = frozenset({"stream", "lake", "wetland"})


class Op(str, Enum):
    """How a rule/scope selects sections from the entry's reach.

    THE REACH IS `entry.matched` — a LIST. One synopsis row can cover several registry items
    ("CHILLIWACK / VEDDER RIVERS" = the Chilliwack + the Vedder + the Vedder Canal), so "the matched
    item" is not a single water and the ops must be defined across all of them:

    - `whole` = every section of every covered item.
    - The directional ops FOLLOW THE WATER, not the item boundary: `downstream_of X` is every covered
      section downstream of X in the flow graph, crossing from one covered item into the next. That is
      what the synopsis means — "from the Brilliant Dam to the confluence with the Columbia River"
      runs to the Kootenay's mouth regardless of which blue line carries it — and it is why a
      name-change junction (Chilliwack -> Vedder) needs no special case.
    - `item_id` NARROWS an extent to one covered item, and is how a reach that stops at a junction is
      expressed: "downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge" is
      `downstream_of tamihi` + `item_id=<Chilliwack>`, because the Chilliwack ENDS at Vedder Crossing.
      A split named by an `item_id`-scoped extent must be a cut-point on that item (enforced by
      `validate_entry_splits`).

    NOTE: nothing resolves these ops to sections yet — they are recorded intent. This docstring is the
    contract that resolver must implement.
    """

    WHOLE = "whole"                 # every section of every covered item (no split refs)
    UPSTREAM_OF = "upstream_of"     # sections above split s, following the water  (1 split)
    DOWNSTREAM_OF = "downstream_of" # sections below split s, following the water  (1 split)
    BETWEEN = "between"             # sections between a and b                     (2 splits)
    WITHIN = "within"              # sections inside an area/polygon (area, not splits)


class Extent(BaseModel):
    """One `op + split ids` binding. A rule's `extents` is a list → UNION (covers "A plus B").

    `item_id` scopes this extent to ONE registry item — either one of the several the entry covers
    (`entry.matched`), or a different item entirely ("…plus Tenas Lake", a named side channel).
    `item_ids` is the same thing over SEVERAL items, for a reach whose two ends sit on different
    waters: "downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge" is bounded by a cut on the
    Chilliwack and a cut on the Vedder, so scoping it to either one alone puts the other end out of
    scope and the reach cannot be resolved at all. Set one or the other, never both.
    Without either, a directional op follows the water across every covered item; see `Op`.
    `area_id`/`area_kind` carry a `within(area)`.
    """

    model_config = ConfigDict(frozen=True)

    op: Op
    splits: List[str] = Field(default_factory=list, description="curated split ids this extent binds to")
    item_id: Optional[str] = Field(default=None, description="registry id, if this extent scopes a different item")
    item_ids: List[str] = Field(
        default_factory=list,
        description="registry ids, when this extent spans SEVERAL items (a reach whose two cut-points "
        "sit on different waters). Mutually exclusive with item_id.",
    )
    area_id: Optional[str] = Field(default=None, description="area id (op=within), e.g. 'area:watershed:liard_river'")
    area_kind: Optional[str] = Field(
        default=None,
        description="admin area FAMILY (op=within), used INSTEAD of area_id when a regulation "
        "is written against every area of a kind: 'national_parks', 'ecological_reserves', "
        "'restricted_land_access', 'indigenous_land', 'wma', 'park'. The resolver takes the "
        "union of every `area:<kind>:*` item, so 'prohibited in National Parks' is one extent "
        "rather than seven ids that go stale when the province gazettes another one.",
    )
    feature_types: List[str] = Field(
        default_factory=list,
        description="op=within only: restrict the area's members to these feature kinds "
        "(subset of stream/lake/wetland); empty = all features inside the area",
    )

    @property
    def scope_ids(self) -> List[str]:
        """The registry items this extent is scoped to — [] meaning "every item the entry covers"."""
        if self.item_ids:
            return list(self.item_ids)
        return [self.item_id] if self.item_id else []

    @model_validator(mode="after")
    def _check_arity(self) -> "Extent":
        n = len(self.splits)
        if self.item_id and self.item_ids:
            raise ValueError("set item_id or item_ids, not both")
        if len(set(self.item_ids)) != len(self.item_ids):
            raise ValueError(f"item_ids has duplicates: {self.item_ids}")
        if self.op in (Op.UPSTREAM_OF, Op.DOWNSTREAM_OF) and n != 1:
            raise ValueError(f"op {self.op.value} needs exactly 1 split id, got {n}")
        if self.op == Op.BETWEEN and n != 2:
            raise ValueError(f"op between needs exactly 2 split ids, got {n}")
        if self.op == Op.WHOLE and n != 0:
            raise ValueError(f"op whole takes no split ids, got {n}")
        if self.op == Op.WITHIN and not (self.area_id or self.area_kind or self.splits):
            raise ValueError("op within needs an area, an area_kind, or bounding split ids")
        if self.feature_types:
            if self.op != Op.WITHIN:
                raise ValueError("feature_types is only valid for op=within")
            bad = [t for t in self.feature_types if t not in _FEATURE_TYPES]
            if bad:
                raise ValueError(f"invalid feature_types {bad}; allowed: {sorted(_FEATURE_TYPES)}")
        return self


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class LimitKind(str, Enum):
    """Over what period the count is counted."""

    DAILY = "daily"
    POSSESSION = "possession"        # how many you may have, usually a multiple of daily
    ANNUAL = "annual"                # per licence year


class Water(str, Enum):
    """Which kind of water a limit applies in — a limit may differ between the two."""

    STREAM = "stream"
    LAKE = "lake"


class Origin(str, Enum):
    """Hatchery fish are marked; wild ones are not, and the regulations treat them apart."""

    HATCHERY = "hatchery"
    WILD = "wild"


class Limit(BaseModel):
    """HOW MANY MAY BE KEPT, as a number rather than a sentence.

    Every quota in the synopsis lives only in prose today — "Trout and Char: 5 (all species
    combined), including not more than 1 over 50 cm" — so `details` is the only place the 5
    exists and nothing can compare it, override it, or lay it out as a table. Measured over
    the 141 zone harvest rules, six fields express every one of them.

    A SUB-LIMIT IS A LIMIT, which is the whole shape of the thing. The sentence above is one
    limit of 5 with a child that narrows by size; "not more than 1 rainbow or cutthroat over
    50 cm" narrows by species AND size. So limits nest, and `within` names the parent.

    THE TWO SIZE BOUNDS ARE NOT ONE FIELD WITH A SIGN. "none under 60 cm" protects small fish
    and "not more than 1 over 50 cm" caps large ones; storing a single number would make
    those indistinguishable, and they are opposite instructions.

    BOTH BOUNDS TOGETHER IS A REAL SHAPE, and this class used to refuse it on the stated
    grounds that "nothing in the synopsis writes" a band. That was wrong, and wrong in the
    way a validator must never be — it was asserted from recall rather than measured. The
    corpus writes a slot eight times ("Lake trout daily quota = 2 (none under 40 cm or over
    60 cm)") and a PROTECTED band ten times ("only 1 over 90 cm, none between 60 cm and 90
    cm"). The protected band cannot be decomposed at all: "none over 60" and "none under 90"
    as siblings forbid every fish. So a limit may carry both, and `band` says which of the
    two readings is meant.
    """

    model_config = ConfigDict(frozen=True)

    kind: LimitKind = Field(default=LimitKind.DAILY)
    take: Optional[int] = Field(
        default=None,
        description="how many may be kept. 0 = release all. None = no number is stated "
        "(use `unlimited` for an explicit 'unlimited').",
    )
    unlimited: bool = Field(default=False, description="the synopsis says unlimited")
    per_daily: Optional[int] = Field(
        default=None,
        description="possession only: this many daily quotas ('possession = 2 daily quotas'), "
        "which is a MULTIPLE and not a count — the count depends on the daily limit in force",
    )
    species: List[str] = Field(
        default_factory=list,
        description="narrows the RULE's species for this limit only; empty = the rule's own. "
        "A trout-and-char quota with a rainbow-only sub-limit needs both.",
    )
    combined: bool = Field(
        default=False,
        description="the count is shared across the species, not one each ('all species combined')",
    )
    over_cm: Optional[int] = Field(default=None, description="applies to fish LONGER than this")
    under_cm: Optional[int] = Field(default=None, description="applies to fish SHORTER than this")
    band: bool = Field(
        default=False,
        description="how to read a limit carrying BOTH bounds. False (a slot) = the fish must "
        "be BETWEEN them: 'none under 40 cm or over 60 cm' keeps the middle. True (a protected "
        "band) = the fish must be OUTSIDE them: 'none between 60 cm and 90 cm' keeps the ends. "
        "The same two numbers, opposite meanings, so it cannot be inferred.",
    )
    water: Optional[Water] = Field(default=None, description="only in this kind of water")
    origin: Optional[Origin] = Field(default=None)
    within: Optional[str] = Field(
        default=None,
        description="the `id` of the limit this one sits inside, for a sub-limit",
    )
    id: Optional[str] = Field(default=None, description="so a sub-limit can name its parent")

    @model_validator(mode="after")
    def _check(self) -> "Limit":
        if self.unlimited and self.take is not None:
            raise ValueError("a limit is `unlimited` or has a `take`, not both")
        if self.per_daily is not None and self.kind is not LimitKind.POSSESSION:
            raise ValueError("`per_daily` is only meaningful on a possession limit")
        if self.band and not (self.over_cm is not None and self.under_cm is not None):
            raise ValueError("a `band` limit needs both over_cm and under_cm")
        if self.within and not self.within.strip():
            raise ValueError("`within` names a parent limit id")
        bad = [c for c in self.species if c not in KNOWN_SPECIES_CODES]
        if bad:
            raise ValueError(f"unknown species code(s) {sorted(bad)} on a limit")
        return self


class Rule(BaseModel):
    """A single parsed restriction, bound to its reach by `extents` (op + split ids)."""

    model_config = ConfigDict(frozen=True)

    rule_id: str = Field(..., description="stable id, unique within the entry (e.g. 'atnarko_main.r5')")
    restriction_type: RestrictionType
    details: str = Field(..., description="concise normalized summary, e.g. 'No powered boats'")
    extents: List[Extent] = Field(
        default_factory=list,
        description="op+split bindings, unioned. Empty ONLY when needs_review or sections_override is set; "
        "a whole-reach rule must be explicit: extents=[{op:'whole'}].",
    )
    dates: List[str] = Field(default_factory=list, description="date strings exactly as in rule_text")
    limits: List[Limit] = Field(
        default_factory=list,
        description="the quota as NUMBERS, for a harvest rule. Empty is not 'no limit' — it "
        "means nobody has structured this rule yet, and `details` is still the only place "
        "the count exists. A rule may carry several: a daily quota, its size sub-limit and "
        "a possession multiple are three limits, and the sub-limit names its parent.",
    )
    includes_tributaries: Optional[bool] = Field(
        default=None,
        description="per-rule tributary override (null=inherit entry, true/false=override for this rule)",
    )
    tributaries_only: bool = Field(
        default=False,
        description="rule applies ONLY to tributaries, not the mainstem reach (per-rule parallel of "
        "Tributaries.only). Implies includes_tributaries=True. The 4 states a curator picks are: inherit "
        "(includes_tributaries=null), yes (true), no (false), only (this flag).",
    )
    tributary_excludes: List[Extent] = Field(
        default_factory=list,
        description="per-rule HAND-CURATED carve-outs subtracted from THIS rule's tributary set (only "
        "meaningful when the rule extends to tributaries). Unlike entry-wide Tributaries.excludes (which "
        "applies to every rule), these scope to one rule — e.g. a seasonal 'No Fishing in any tributaries "
        "(except Quinsam River)'. A whole-tributary carve-out names the item: item_id=<registry id>, op=whole; "
        "a partial carve-out references the boundary split(s). The parser leaves this empty (curator-filled).",
    )
    sections_override: Optional[List[str]] = Field(
        default=None,
        description="escape hatch: name section ids directly when no split can express the reach",
    )
    needs_review: bool = Field(
        default=False,
        description="parser could not confidently bind this rule -> hand-curation queue",
    )
    review_reason: str = Field(default="", description="why it needs review (required iff needs_review)")
    # verbatim provenance (self-contained; never needs the ephemeral extraction to curate)
    rule_text: str = Field(..., description="exact contiguous substring of regs_verbatim for this rule")
    location_text: str = Field(default="", description="verbatim phrase the extents came from ('upstream of X')")
    exception: str = Field(default="", description="verbatim carve-out qualifying THIS rule (⊆ rule_text)")
    display_location: str = Field(
        default="",
        description="human-readable, USER-FACING reach label (e.g. 'Above Talchako River confluence'). "
        "NOT verbatim-constrained and curator-editable — unlike location_text (verbatim provenance). Lets "
        "a rule keep a readable location for the app even when its extents fall back to whole-stream "
        "(locator unresolved). The output layer shows this, falling back to location_text when empty.",
    )
    unresolved_locators: List[str] = Field(
        default_factory=list,
        description="verbatim locator phrases the parser could NOT bind to a split/boundary (e.g. "
        "'the outlet', 'signs 500 m below the falls'). Non-empty forces needs_review — the hand-curation "
        "queue maps each to a curated split id. A rule still parses (restriction + display_location); the "
        "unbound locator is recorded here, never silently dropped.",
    )
    exempts_from: List[str] = Field(
        default_factory=list,
        description="normalized ids of the DEFAULT restrictions this rule lifts, e.g. "
        "['spring_closure']. A regional closure applies unless a water is exempted from it, so "
        "'is this river open?' cannot be answered from the closure rules alone — the exemption has to "
        "be machine-readable, not a sentence in `details`. Vocabulary: spring_closure, summer_closure, "
        "trout_char_release, bull_trout_release, bait_ban, single_barbless_hook, kokanee_stream_quota. "
        "Empty on a rule that exempts from a NAMED water's own closure (recorded in `details` only).",
    )
    species: List[str] = Field(
        default_factory=list,
        description="species codes this rule applies to (pipeline/regs/parsing/species.py); empty = ALL "
        "species. Validated against the known BC species table so an unrecognized code surfaces as an "
        "error rather than being stored silently wrong.",
    )

    @model_validator(mode="before")
    @classmethod
    def _only_implies_include(cls, data):
        if isinstance(data, dict) and data.get("tributaries_only"):
            data = dict(data); data["includes_tributaries"] = True   # 'only' definitely reaches tributaries
        return data

    @model_validator(mode="after")
    def _validate_chain(self) -> "Rule":
        errors: List[str] = []
        if not self.rule_text or not self.rule_text.strip():
            raise ValueError("rule_text is empty")
        if not self.details or not self.details.strip():
            errors.append("details is empty")

        for field_name in ("rule_text", "location_text", "exception"):
            val = getattr(self, field_name)
            if val and "..." in val:
                errors.append(f"{field_name} contains '...' — verbatim fields must not be truncated")

        if self.location_text and _normalize(self.location_text) not in _normalize(self.rule_text):
            errors.append(f"location_text not found in rule_text. Location: '{self.location_text[:80]}'")
        if self.exception and _normalize(self.exception) not in _normalize(self.rule_text):
            errors.append(f"exception not found in rule_text. Exception: '{self.exception[:80]}'")

        for date in self.dates:
            if "\n" in date:
                errors.append(f"Date '{date}' contains a newline")
            elif _normalize_date(date) not in _normalize_date(self.rule_text):
                errors.append(f"Date '{date}' not found in rule_text. Rule: '{self.rule_text[:100]}'")
        # every verbatim date must resolve to a real calendar window (hallucination guard)
        errors.extend(date_parse_errors(self.dates))

        # binding completeness: a confidently-parsed rule must express its reach somehow
        if not self.needs_review and not self.extents and not self.sections_override:
            errors.append("rule has no extents and no sections_override but needs_review is False "
                          "— a whole-reach rule must set extents=[{op:'whole'}]")
        if self.needs_review and not self.review_reason.strip():
            errors.append("needs_review is True but review_reason is empty")

        # an unbound locator must go to review, never masquerade as a confident parse
        if self.unresolved_locators and not self.needs_review:
            errors.append("unresolved_locators is set but needs_review is False — an unbound locator "
                          "must be flagged for review, not stored as a confident binding")

        bad_species = [s for s in self.species if s not in KNOWN_SPECIES_CODES]
        if bad_species:
            errors.append(f"unknown species code(s) {sorted(bad_species)} — not in pipeline/regs/parsing/species.py")

        if errors:
            raise ValueError("; ".join(errors))
        return self

    def date_windows(self) -> List[DateWindow]:
        """The structured form of `dates`, derived deterministically from the verbatim strings
        (validated to parse at construction time). Empty = the rule has no seasonal window."""
        return parse_date_windows(self.dates)


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------


class Identity(BaseModel):
    """Who the entry is about — the matcher uses (name, region, mus) to find the registry item.

    `name` is the SYNOPSIS's own words and is the key; `display_name` is what the registry
    calls the water it resolved to. Keeping both is not cosmetic: several differently-named
    synopsis rows can resolve to registry items that share one collective name, and writing
    that collective name into `name` makes them look like duplicates of each other.

    Measured on the 2026-08 corpus: INDATA, TCHENTLO, TSAYTA and CHUCHI LAKE are four rows
    that resolve to four different polygons in the Nation Lakes chain, every one of which
    the registry displays as "Nation Lakes" — so all four entries read `name: "Nation
    Lakes"` and the source names were gone. Same for HAYNES/HYDRAULIC/MINNOW LAKE
    ("McCulloch Reservoir") and SATURDAY/FRIDAY LAKE ("Tepee Lakes")."""

    model_config = ConfigDict(frozen=True)
    name: str = Field(..., description="waterbody name VERBATIM from the synopsis row — the key")
    display_name: str = Field(
        default="",
        description="what the registry calls the matched item ('Nation Lakes'); '' when the "
        "synopsis name is already the display name or there is no registry match",
    )
    region: str = Field(default="", description="management region, e.g. '5'")
    mus: List[str] = Field(default_factory=list, description="management units, e.g. ['5-4']")


class ReviewIssue(BaseModel):
    """One finding from the agent reviewer's second pass (see review_exporter / CATALOGUE_REVIEW_PROMPT.md)."""

    model_config = ConfigDict(frozen=True)
    severity: str = Field(..., description="high | medium | low")
    problem: str = Field(..., description="what the reviewer believes is wrong")
    fix: str = Field(default="", description="the concrete correction the reviewer suggests")


class ParseReview(BaseModel):
    """Durable record of the parser's AGENT review pass, persisted onto the entry at ingest so the
    review state survives even if the ephemeral parse/review outputs are lost. Distinct from human
    curation (locked/reviewed_by/revisit): this is the automated second-pass reviewer's verdict."""

    model_config = ConfigDict(frozen=True)
    verdict: str = Field(default="", description="'' (not reviewed) | pass | changes_requested")
    model: str = Field(default="", description="reviewer model, e.g. 'haiku'")
    reviewed_at: str = Field(default="", description="ISO timestamp of the review pass")
    issues: List[ReviewIssue] = Field(default_factory=list, description="reviewer findings for this entry")


class Tributaries(BaseModel):
    """Entry-wide tributary scope, grouped. `excludes` are HAND-CURATED carve-outs subtracted from
    the tributary set (e.g. 'EXCEPT Burnt Bridge upstream of Sitkatapa'); the parser leaves them
    empty. An exclude extent just references the boundary split(s) — the excepted item is inferred
    from the split's own scope, so no `item` is needed."""

    model_config = ConfigDict(frozen=True)
    included: bool = Field(default=False, description="do this entry's rules extend to tributaries?")
    only: bool = Field(default=False, description="entry governs ONLY tributaries (e.g. \"X LAKE'S TRIBUTARIES\")")
    excludes: List[Extent] = Field(default_factory=list, description="carve-outs subtracted from the tributary set")

    @model_validator(mode="before")
    @classmethod
    def _only_implies_included(cls, data):
        if isinstance(data, dict) and data.get("only") and not data.get("included"):
            data = dict(data); data["included"] = True
        return data


class Source(BaseModel):
    """WHERE THIS ROW IS PRINTED. Provenance, nested, because it is all one thing.

    These started flat — `source_symbols` shipped on its own — and the moment a second one
    arrived it was clear they are not siblings of `locked` and `reviewed_by` but members of a
    single fact: the row in the book this entry was read from. Nested, a reader of the schema
    can see at a glance which fields are OURS and which are the SOURCE's, and the next piece of
    provenance has an obvious home instead of another `source_` prefix.

    Everything here is injected at ingest from the batch item and never trusted from the model —
    the same rule that governs `entry_id`, `regs_verbatim` and `identity.name`.
    """

    model_config = ConfigDict(frozen=True)
    pages: List[int] = Field(
        default_factory=list,
        description="synopsis page number(s) this row is printed on. A LIST because seven MU 6-1 "
        "lakes (Basalt, Chipmunk, Gatcho, Naglico, Pettry, Squirrel, Toms) are printed on two "
        "pages each; recording one would be choosing between two true answers.",
    )
    symbols: List[str] = Field(
        default_factory=list,
        description="verbatim synopsis-row symbols ('Incl. Tribs', 'Classified', 'Stocked'), so "
        "tributaries.included stays re-validatable against the source without the extraction file.",
    )
    row_image: str = Field(
        default="",
        description="filename of the cropped image of this row under "
        "data/generated/regs/extraction/row_images/ — the last resort when the wording is "
        "ambiguous, because it is a picture of the printed line itself.",
    )


class Entry(BaseModel):
    """One frozen parse for a synopsis row: identity + rules. Both the parser's output and the
    file a curator hand-edits (fix an extent, add a sections_override, correct a matched)."""

    model_config = ConfigDict(frozen=True)

    entry_id: str = Field(..., description="stable id (e.g. 'atnarko_main'); rule_ids namespace under it")
    identity: Identity
    regs_verbatim: str = Field(..., description="exact copy of the input raw_regs")
    source: Source = Field(
        default_factory=lambda: Source(),
        description="where this row is printed in the book. Injected by validate/ingest from the "
        "batch item — authoritative, never trusted from the model, the same rule that governs "
        "`entry_id`, `regs_verbatim` and `identity.name`.",
    )
    locked: bool = Field(
        default=False,
        description="human-curated lock. Starts False (fresh parse). A curator flips it True once the "
        "entry is reviewed/confirmed/edited; a re-parse MUST NOT overwrite a locked entry (the merge "
        "tool preserves it). This is how a hand-authored entry (e.g. the full Atnarko system) is frozen.",
    )
    reviewed_by: str = Field(default="", description="curator who confirmed this entry (set by the review tool alongside locked)")
    reviewed_at: str = Field(default="", description="ISO timestamp of the confirm (set by the review tool)")
    revisit: bool = Field(
        default=False,
        description="conditional-accept flag: the entry was confirmed but a curator wants it revisited "
        "later (e.g. a low-confidence binding they accepted to keep moving). Defaults False.",
    )
    revisit_note: str = Field(default="", description="why it should be revisited (free text; set alongside revisit)")
    parse_review: ParseReview = Field(default_factory=ParseReview, description="durable agent-review pass state (verdict/issues), persisted by ingest")
    matched: List[str] = Field(default_factory=list, description="registry ids — written by the matcher, [] from the parser")
    reference_only: bool = Field(
        default=False,
        description="this row does not carry its own regulations — it is the synopsis pointing at "
        "another entry under a different name ('VEDDER RIVER: See Chilliwack River'). It stays a real "
        "entry so a search for that name finds something; the flag tells the review queue and the app "
        "not to treat it as unregulated or as a second, conflicting set of rules.",
    )
    registry_status: str = Field(
        default="matched",
        description="'matched' (a registry item + its boundaries were available) or 'no_registry' "
        "(the row had no registry match, so the reg text is still split into rules but NO locators "
        "could be bound — a curator attaches an item + extents later). Injected by ingest from the "
        "batch item; never trusted from the model.",
    )
    registry_note: str = Field(
        default="",
        description="why there is no registry match (matcher status + reason), e.g. "
        "'ambiguous: 2 candidates for MACKENZIE CREEK' or 'skip: variant_of ZYMOETZ (Copper) RIVER'. "
        "Required when registry_status='no_registry'. Injected by ingest.",
    )
    tributaries: Tributaries = Field(default_factory=Tributaries, description="entry-wide tributary scope (included/only/excludes)")
    scope: List[Extent] = Field(default_factory=list, description="entry-wide extents; composes by ∩ with each rule's extents")
    rules: List[Rule] = Field(..., min_length=1)
    audit_log: List[str] = Field(default_factory=list, description="parsing ambiguities / source issues only")

    @model_validator(mode="after")
    def _validate_entry(self) -> "Entry":
        errors: List[str] = []
        if not self.regs_verbatim or not self.regs_verbatim.strip():
            raise ValueError("regs_verbatim is empty")

        if self.registry_status not in ("matched", "no_registry"):
            errors.append(f"registry_status must be 'matched' or 'no_registry', got '{self.registry_status}'")
        if self.registry_status == "no_registry":
            if not self.registry_note.strip():
                errors.append("registry_status is 'no_registry' but registry_note is empty (record why)")
            # content-only: with no registry item there is no reach to bind — every rule stays unbound
            # and flagged, so a curator attaches an item + extents later (never a silent whole-reach).
            for rule in self.rules:
                if rule.extents or rule.sections_override:
                    errors.append(f"rule {rule.rule_id}: no_registry entry must not bind extents/"
                                  "sections_override (there is no registry reach) — leave extents empty")
                if not rule.needs_review:
                    errors.append(f"rule {rule.rule_id}: no_registry entry must set needs_review on every rule")

        regs_norm = _normalize(self.regs_verbatim)
        for i, rule in enumerate(self.rules):
            if _normalize(rule.rule_text) not in regs_norm:
                errors.append(f"rule[{i}].rule_text not found in regs_verbatim. Rule: '{rule.rule_text[:80]}'")

        # rule_ids unique within the entry
        rids = [r.rule_id for r in self.rules]
        dupes = {r for r in rids if rids.count(r) > 1}
        if dupes:
            errors.append(f"duplicate rule_id(s): {sorted(dupes)}")

        # keyword coverage — a known restriction phrase in the source must appear in some rule
        _KEYWORDS = ["no fishing", "class i water", "class ii water", "bait ban", "fly fishing only",
                     "catch and release", "no powered boats", "single barbless hook", "daily quota",
                     "steelhead stamp mandatory"]
        all_rules_norm = " \n ".join(_normalize(r.rule_text) for r in self.rules)
        for kw in _KEYWORDS:
            if kw in regs_norm and kw not in all_rules_norm:
                errors.append(f"Keyword '{kw}' in regs_verbatim but missing from all rule_text — a restriction may be missed")

        if errors:
            raise ValueError("; ".join(errors))
        return self


# ---------------------------------------------------------------------------
# EntryFile — the per-region checked-in artifact
# ---------------------------------------------------------------------------


class EntryFile(BaseModel):
    """The frozen per-region parse: pipeline/regs/parsing/entries/region-N.json."""

    region: str
    entries: List[Entry]

    @model_validator(mode="after")
    def _unique_entry_ids(self) -> "EntryFile":
        ids = [e.entry_id for e in self.entries]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"duplicate entry_id(s) in region {self.region}: {sorted(dupes)}")
        return self


# ---------------------------------------------------------------------------
# Ingest-time validation the models can't do alone
# ---------------------------------------------------------------------------


def validate_entry_splits(entry: Entry, allowed_split_ids: set[str],
                          allowed_by_item: Optional[Mapping[str, set]] = None) -> List[str]:
    """Every id referenced by an extent (entry scope + each rule) must exist in the waterbody's
    curated splits. Returns error strings (empty = clean). `sections_override` is NOT checked here
    (section ids only exist after the graph build).

    `allowed_by_item` ({item_id: split ids}) tightens the check for a COMBINED entry, where the flat
    `allowed_split_ids` is the union over several waters. An extent that names one of them via
    `item_id` must bind a cut-point ON THAT WATER — otherwise a reach can be scoped to the Atnarko
    while bounded by a confluence that only exists on the Bella Coola, which the union check happily
    accepts and which is exactly the wrong-but-confident binding the parser is told to avoid."""
    errors: List[str] = []

    def _check(extents: List[Extent], where: str) -> None:
        for ex in extents:
            # A multi-item scope allows a cut on ANY of the named items — that is the point of it:
            # the two ends of the reach are on different waters, and each end is checked against the
            # union so neither is rejected for living on the other's blue line.
            ids = ex.scope_ids
            scoped = None
            if ids and allowed_by_item is not None:
                scoped = set()
                for i in ids:
                    scoped |= set(allowed_by_item.get(i) or ())
            for sid in ex.splits:
                if sid not in allowed_split_ids:
                    errors.append(f"{where}: extent op={ex.op.value} references unknown split id '{sid}'")
                elif scoped is not None and sid not in scoped:
                    named = ex.item_id or ", ".join(ids)
                    errors.append(
                        f"{where}: extent op={ex.op.value} is scoped to '{named}' but "
                        f"'{sid}' is not a cut-point on it (drop the scope, add the item that "
                        f"carries '{sid}', or bind a cut-point that is on that water)")

    _check(entry.scope, f"entry {entry.entry_id} scope")
    _check(entry.tributaries.excludes, f"entry {entry.entry_id} tributaries.excludes")
    for rule in entry.rules:
        _check(rule.extents, f"entry {entry.entry_id} rule {rule.rule_id}")
        _check(rule.tributary_excludes, f"entry {entry.entry_id} rule {rule.rule_id} tributary_excludes")
    return errors


def unused_splits(entry: Entry, allowed_split_ids: set[str]) -> List[str]:
    """ADVISORY coverage check (not an error): curated split ids for the matched item that no extent
    references. A parse that leaves splits unused may have missed a reach — surfacing them guards the
    worst failure mode, a wrong parse stored as confident. Some unused splits are legitimate (consumed
    by a tributary rule or a different entry), so the caller warns rather than rejecting."""
    used: set[str] = set()

    def _collect(extents: List[Extent]) -> None:
        for ex in extents:
            used.update(ex.splits)

    _collect(entry.scope)
    _collect(entry.tributaries.excludes)
    for rule in entry.rules:
        _collect(rule.extents)
        _collect(rule.tributary_excludes)
    return sorted(allowed_split_ids - used)
