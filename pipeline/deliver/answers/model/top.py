"""The answers file's top-level tables: a segment's MOMENT (answers 2.1), the GLOSSARY (2.2, user decision J)
and THE UNNAMED WATER (2.4, S1 — every section has an answer)."""
from __future__ import annotations

from typing import Annotated, Literal, Optional, Tuple
from pydantic import ConfigDict, Field, StrictBool, StrictInt, StrictStr, model_validator
from pipeline.deliver import types as T
from pipeline.deliver.calendar import WEEKDAYS
from pipeline.deliver.answers.model.base import Count, Model, Positive, _enum


# --------------------------------------------------------------------------------------------
# The top level: a segment's MOMENT (answers 2.1)
# --------------------------------------------------------------------------------------------

WeekdayLit = Literal[WEEKDAYS]                     # THE CALENDAR SPEC's names, Monday first


class ClockTime(Model):
    at: Optional[StrictStr] = None                   # "HH:MM" — or a solar event:
    solar: Optional[Literal["sunrise", "sunset"]] = None
    offset_min: StrictInt

    @model_validator(mode="after")
    def _one(self):
        if (self.at is None) == (self.solar is None):
            raise ValueError("a time is a clock time (`at`) or a solar event (`solar`), not both")
        return self


class MomentHours(Model):
    start: ClockTime
    end: ClockTime
    in_: StrictBool = Field(alias="in")              # inside the window (else every other hour)
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, populate_by_name=True)


class Moment(Model):
    """When in the week and the day a segment holds (`calendar.Moment.as_json`)."""
    weekdays: Tuple[WeekdayLit, ...] = Field(min_length=1, max_length=7)
    hours: Optional[MomentHours]                     # None: every hour of those days


# --------------------------------------------------------------------------------------------
# The top level: the GLOSSARY (answers 2.2, user decision J)
# --------------------------------------------------------------------------------------------

class GlossaryTerm(Model):
    """One piece of jargon the page shows, in plain words (`glossary.py`: generated from the data
    and the book's text, never written per water)."""
    id: Annotated[StrictStr, Field(pattern=r"^[a-z0-9_]+$")]
    term: StrictStr
    says: StrictStr                                   # the plain-language explanation
    example: Optional[StrictStr] = None
    pages: Tuple[Positive, ...] = Field(min_length=1)  # PRINTED book pages
    quote: StrictStr                                  # the book's own words, verbatim
    source: StrictStr                                 # the data it was generated from


class Glossary(Model):
    version: Positive
    terms: Tuple[GlossaryTerm, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique(self):
        ids = [t.id for t in self.terms]
        if len(set(ids)) != len(ids):
            raise ValueError("two glossary terms share an id")
        return self


# --------------------------------------------------------------------------------------------
# The top level: THE UNNAMED WATER (answers 2.4, S1 — every section has an answer)
# --------------------------------------------------------------------------------------------

RegionLit = _enum(T.Region, "Region")


class PlaceNames(Model):
    """The whole name of an unnamed answer in a region set, per water kind (`display.place_label`)."""
    lake: StrictStr
    stream: StrictStr
    wetland: StrictStr


class Place(Model):
    """A REGION SET sections lie in (the bundle's `region_place`, S1b), with its words ("Region 5")
    and the name of an unnamed answer there per water kind: "Unnamed stream · Region 5"."""
    regions: Tuple[RegionLit, ...] = Field(min_length=1)
    label: StrictStr
    names: PlaceNames


class Unnamed(Model):
    """The unnamed water's keys (S1b, keyed by content): every key from `first` on is a content
    only unnamed parts have; `parts[up]` the key (index into `keys`) of the bundle's unnamed part
    `up` — a named part's key where the content is the same."""
    first: Count
    parts: Tuple[Count, ...]
