"""The rule catalogue — 13 types in 6 families, their conditions, and what makes two rules
comparable.

Replaces hand-written `Rule.details` with a TYPE plus named CONDITIONS. The label is generated
from those (see `label()`), so it cannot drift from the numbers the way the prose did:
`r6:bennett_lake` said "tiered size limit" while its own verbatim said "only 1 over 90 cm, none
between 60 cm and 90 cm".

Spec: pipeline/docs/18-how-regulations-are-stored.md and pipeline/docs/CURRENT-STATE.md. Source
text: data/curated/regulations/reference/.

THE ONE INVARIANT: a type boundary is a wall the override cannot cross. Two rules that could ever
displace one another must share a type and differ only in `dimension`; two rules that never compete
must not. `z4:bass_closed` (closure) and `r4:wasa_lake` (harvest) are one subject at two values, and
filing them apart is why the override never fired.
"""

from __future__ import annotations

import re

from enum import Enum
from typing import Annotated, Dict, List, Literal, Optional, Union


from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator


# THE CATALOGUE IS A PACKAGE OF PARTS (`catalogue_parts/`); this module is its one import point.
# Every name of every part is re-exported here, public and private, so `catalogue.X` and
# `from pipeline.regs.parsing.catalogue import X` read exactly what they read when this was one file.
from .catalogue_parts.vocab import (  # noqa: F401
    RuleType, Method, Period, WaterKind, ClosureKind, _PRINTED_CLOSURE_KIND, printed_closure_kinds,
    Origin, ChannelSide, LifeStage, LIFE_STAGE_FISH, _ADULT_CHINOOK, HALF_OF_CHANNEL, VesselAspect,
    PropulsionLevel, Document, Obligation, _Terse
)
from .catalogue_parts.text import (  # noqa: F401
    _DASH, EXTRACTION_MARKUP, clean_verbatim, squash
)
from .catalogue_parts.dates import (  # noqa: F401
    _MONTHS, _LAST_DAY, parse_clock, parse_date_range, _point, _month, _day_index, range_days,
    complement, _md, Solar, Clock, DateRange, Hours, When, _days
)
from .catalogue_parts.species import (  # noqa: F401
    BOOK_FAMILIES, BOOK_SPECIES, GAME_FISH, SCIENTIFIC_NAMES, SPECIES_GROUPS, NAMING_GROUPS,
    _CHAR_NAMED, _TROUT_GROUP, _TROUT_WORD, _TROUT_KIND, _is_group_trout, mentions_char_apart,
    trout_word, _EXCLUDES_CHAR, rule_aspects, _dates_meet, related_rules, char_rules_apart,
    trout_scope_problems, _PRINTS_STEELHEAD, _size_key, steelhead_scope_problems, OPEN_SUBJECTS, _s,
    SALMON_FISH, PROTECTED_FISH, KNOWN_SPECIES, protected_fish_printed, REFUSED_SPECIES,
    FEDERAL_SALMON, species_problems, DEFINITIONAL_SIZE, expand_species, _FAMILY, _COUNTED_TYPES,
    SOURCE_ARTEFACTS, source_artefact_problems
)
from .catalogue_parts.gear import (  # noqa: F401
    Slot, _SET_SLOTS, _SPEC_SLOTS, _MEASURED, WHILE_MEANS, WHILE_DEVICES, WHILE_TOKENS, _SNAGGED,
    CAUGHT_HOW, AnglerState, _ANGLER_WORDS, GearWhen, GearSpec, GearClause, CONDUCT_ACTS,
    DOCUMENT_ACTS, _PRESUMED_SAID
)
from .catalogue_parts.lengths import (  # noqa: F401
    EXACT_BOUND_IS_LEGAL, LengthBand
)
from .catalogue_parts.checks import (  # noqa: F401
    _RANGE, printed_ranges, _dates_are_printed, _ALL_YEAR_SAID, _CUT_DATE, _year_days,
    _own_dates_carried, _ALL_SPECIES_SAID, _all_species_is_game_fish, WINDOWS_HELD_ELSEWHERE,
    _whens_in, _unique_span, _JOIN_NEXT, _BARE_GAP, _dates_lost, _extents_the_resolver_reads,
    LIST_MARKER, PLACE_VALUE, UNIDENTIFIED_PART, part_identifies_place, part_words,
    strip_list_marker, bare_whole
)
from .catalogue_parts.licensing import (  # noqa: F401
    WHO_AXES, _PARTITION_AXES, Residency, Age, Guidance, Status, Role, Who, residency_said,
    guidance_said, _check_residency, _SLUG, slug, PROVINCIAL_ANGLER_DOCUMENTS, Ref, Quote,
    StampPeriod, Suspension, _CLASS_SAID, _WAIVED_SAID, _DURING_SAID, _CONDITIONAL_WAIVER,
    _UNIT_SAID, Designation, NotClassified, Doing, Accompaniment, Path, Requirement, LicenceTerms,
    Exemption, Alternative, _STATUS_SAID, _STATUS_RESIDENCY, _ANY_DOCUMENT_SAID, _ANNUAL_SAID,
    LicensingRecord, licensing_verbatims
)
from .catalogue_parts.see import (  # noqa: F401
    See, _POINTER_WORDS, _NOT_A_WATER, _letters, see_relation
)
from .catalogue_parts.rule import (  # noqa: F401
    EXEMPTABLE_DEFAULTS, Exempts, CatalogueRule
)
from .catalogue_parts.labels import (  # noqa: F401
    _DOC_WORDS, _HP, _SPECIES_WORDS, _FAMILY_WORDS, species_menu, is_protected_list, species_words,
    protected_words, _lower_fish, _gear_words, _when_words, _scope, _size, _where, _side_words,
    _suspended, _EMPHASIS, _is_closure, _lifted_rule_words, _lift_name, _lifted_names, _lifts,
    _lift_covers_fish, LABEL_PARTS, label_parts, compose, label
)
from .catalogue_parts.licensing_labels import (  # noqa: F401
    _docs, _a, _cap, unit_words, _path_words, _doing_words, LICENSING_PARTS, recorded_fish,
    licensing_parts, compose_licensing, licensing_label
)
from .catalogue_parts.entry import (  # noqa: F401
    GLYPH_INCLUDES_TRIBUTARIES, _extent_errors, CatalogueEntry, CatalogueFile
)


import sys as _sys
import types as _types

_UNSET = object()


class _Catalogue(_types.ModuleType):
    """A NAME PATCHED HERE REACHES THE PART THAT READS IT. When this was one file, setting
    `catalogue.EXACT_BOUND_IS_LEGAL` (or any name) changed what every function in it read at call
    time, and tests rely on that (`monkeypatch.setattr(C, "SOURCE_ARTEFACTS", ())`). A part reads
    its own globals, so an assignment here is passed on to every part still holding the old object
    under that name — the part that defines it and every part that imported it. `monkeypatch`'s
    undo is an assignment too, so it restores them all."""

    def __setattr__(self, name, value):
        old = self.__dict__.get(name, _UNSET)
        if old is not _UNSET:
            prefix = __name__ + "_parts."
            for key, part in list(_sys.modules.items()):
                if key.startswith(prefix) and part is not None \
                        and part.__dict__.get(name, _UNSET) is old:
                    setattr(part, name, value)
        super().__setattr__(name, value)


_sys.modules[__name__].__class__ = _Catalogue
