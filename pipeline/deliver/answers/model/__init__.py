"""THE ANSWERS FILE'S TYPES (answers/2, DATAFLOW P7) — every frame and table a section ships, as a
strict pydantic model: `strict=True` (no coercion: a "2" is not a 2), `extra="forbid"` (a stray
field is refused), `frozen=True`, and every `X | None` paired, in a model validator, with the
condition that makes it null. Vocabularies are the closed enums of `pipeline.deliver.types`.

The build validates every DISTINCT value of every section through its model BEFORE encoding
(`validate_section`), so a wrong shape stops the build naming the section and the field; the
JSON Schema of each model ships in the file's `spec` (`json_schemas`) and is emitted for the app
(`python -m pipeline.deliver.answers.model --check`, AGENTS 40).

Field sets are the measured ones of the shipped file (every key, every optional key), not a guess.

THE MODULES: `base` (Model, aliases, vocabularies), one per section — `rows`, `gear`, `licence`, `display` —
`top` (moment, glossary, unnamed water), `frames` (FRAMES, TOP, STATICS and the validation), `emit` (the app's
generated copy; `python -m pipeline.deliver.answers.model [--check]`).
"""
# The package's face: every name its modules define, so `<package>.X` keeps meaning what it did.
# A test that switches a module-level value must set it on the module that READS it (see each module).
# ruff: noqa: F401
from pipeline.deliver.answers.model.base import (
    Model, RuleIx, LicIx, Count, Positive, Cm, Fish, ClauseRef, _enum, FishLit, Decided_, RoleLit, LineLit,
    CondLit, BadgeLit, SteelLineLit, OriginLit,
)
from pipeline.deliver.answers.model.rows import (
    _LineBase, LRel, LCap, LOuterSize, LOuterCap, LAlso, LCaution, LTnote, LBare, Line, _FactBase, FRel,
    FCap, FOuterSize, FOuterCap, FAlso, FCaution, FTnote, FOrigin, FOrigin2, FExc, FXref, FBare, Fact,
    KeepBand, _CondBase, COrigin, CSize, CBack, CGroup, CCap, CSubcap, COuterCap, COuterSize, COrigin2,
    CStreamcap, Cond, LiftNote, RoleOf, Decided, OriginLine, Item, SharedCap, SharedCount, RealDaily, Badge,
    Group, SubcapLift, Subcap, Headline, HeadAlso, CardLiftEach, CardLift, Presence, CardKind, SubGroup,
    NoteThen, BucketNote, SizeThen, SizeGroup, Replaces, Bucket, Stamp, NotKept, Worked, CardState,
    TableBand, WhyLit, TableCell, CardTable, CapKind, CapPlaces, Cap, TreeThen, TreeExtra, TreeSizes,
    AlsoCounts, TreeKind, TreeTier, GroupPossession, Header, GroupTotal, _trout_char, Elsewhere, Week,
    WeekPart, Restated, YearStatus, YearRun, BOOK_YEAR_DAYS, Row, FishAnswer, RowsFrame,
)
from pipeline.deliver.answers.model.gear import (
    Circumstance, CountPick, ElementPick, ExceptedFrom, BaitPick, Device, Way, Vessel, Overruled,
    SummaryPart, SummaryChange, GearSummary, GearAnswer, TidalState, GearFrame,
)
from pipeline.deliver.answers.model.licence import (
    Length, When, Prices, DocumentOr, DocumentNeed, Accompanied, Path_, RequirementPath, Exempt,
    PrintedRequirement, ProfileAnswer, TidalProfile, Holds, TidalHolds, WithAdult, BuyOr, BuyDoc,
    UnderSixteen, LicenceFrame,
)
from pipeline.deliver.answers.model.display import (
    InheritedLift, StatusFrame, StripState, ReasonWords, TidalStatus, DisplayFrame, SubsetSay, RuleFacts,
    PartFacts, LicensingFacts, Choice, Picker, WaterFacts,
)
from pipeline.deliver.answers.model.top import (
    WeekdayLit, ClockTime, MomentHours, Moment, GlossaryTerm, Glossary, RegionLit, PlaceNames, Place,
    Unnamed,
)
from pipeline.deliver.answers.model.frames import (
    FRAMES, TOP, STATICS, ShapeError, _wire, validate_section, json_schemas, validate_top,
)
from pipeline.deliver.answers.model.emit import (
    _CORE, SCHEMA_OUT, TS_OUT, emitted_schema, _ts, emitted_ts, main,
)
from pydantic import TypeAdapter  # callers build their adapters from the package, as they did from model.py
