"""The `licence` section's models: what holds for each angler profile (`LicenceFrame`) and the documents to
buy."""
from __future__ import annotations

from typing import Annotated, Any, Dict, Literal, Optional, Tuple, Union
from pydantic import Field, StrictBool, StrictFloat, StrictInt, StrictStr, model_validator
from pipeline.deliver.answers.model.base import Cm, LicIx, Model
from pipeline.deliver.answers.model.gear import TidalState


class Length(Model):
    min_cm: Cm


class When(Model):
    act: Literal["fishing", "targeting", "retaining", "retaining_recorded", "guiding"]
    species: Optional[Tuple[StrictStr, ...]] = None
    lengths: Optional[Tuple[Length, ...]] = None
    on: Optional[Literal["classified_period", "steelhead_period"]] = None


class Prices(Model):
    year: Optional[Union[StrictInt, StrictFloat]] = None
    day: Optional[Tuple[Union[StrictInt, StrictFloat], ...]] = None
    eight_days: Optional[Tuple[Union[StrictInt, StrictFloat], ...]] = None


class DocumentOr(Model):
    """licence decision L9: another way to satisfy what the document is bought for."""
    need: Tuple[StrictStr, ...] = Field(min_length=1)
    alt: Optional[LicIx] = None                      # the alternative record that offers it
    prices: Dict[StrictStr, Prices]


class DocumentNeed(Model):
    doc: StrictStr
    when: When                                       # the broadest requirement's (decision L10)
    base: StrictBool
    prices: Prices
    also_when: Optional[Tuple[When, ...]] = None     # narrower requirements needing it too
    or_: Optional[Tuple[DocumentOr, ...]] = Field(default=None, alias="or")


class Accompanied(Model):
    holding: StrictStr
    who: Dict[StrictStr, Any]


class Path_(Model):
    need: Optional[Tuple[StrictStr, ...]] = None
    freed: Optional[Tuple[StrictStr, ...]] = None
    accompanied_by: Optional[Accompanied] = None
    as_: Optional[Dict[StrictStr, Any]] = Field(default=None, alias="as")
    quota: Optional[Literal["own", "counts_to_companion"]] = None
    alt: Optional[LicIx] = None


class RequirementPath(Model):
    req: LicIx
    when: When
    paths: Tuple[Path_, ...]
    displaced_by: Optional[Tuple[LicIx, ...]] = None
    presumes_freed: Optional[Literal[True]] = None
    presumes_by: Optional[LicIx] = None
    terms: Optional[Tuple[LicIx, ...]] = None


class Exempt(Model):
    by: Tuple[LicIx, ...]
    from_: Tuple[StrictStr, ...] = Field(alias="from")


class PrintedRequirement(Model):
    """gap G2 (answers 2.3): another angler's or a guide's requirement, with how it is met as the
    record prints it (`satisfied_by`; nothing freed, nothing to buy: it is not this angler's)."""
    req: LicIx
    paths: Tuple[Path_, ...]


class ProfileAnswer(Model):
    documents: Tuple[DocumentNeed, ...]
    none_needed: StrictBool
    requirements: Tuple[RequirementPath, ...]
    exempt: Optional[Exempt] = None
    others: Optional[Tuple[PrintedRequirement, ...]] = None
    guiding: Optional[Tuple[PrintedRequirement, ...]] = None


class TidalProfile(Model):
    tidal: Literal[True]


class Holds(Model):
    holds: Tuple[LicIx, ...]
    designations: Tuple[LicIx, ...]
    stamp_period: StrictBool
    contested: StrictBool
    considered: Tuple[LicIx, ...]
    wrong_water: Tuple[LicIx, ...]
    waived: Tuple[LicIx, ...]
    not_yet_mapped: Tuple[LicIx, ...]
    displaced: Dict[StrictStr, Tuple[LicIx, ...]]
    also_printed: Dict[StrictStr, Tuple[LicIx, ...]]


class TidalHolds(Model):
    tidal: TidalState


class WithAdult(Model):
    who: StrictStr = Field(min_length=1)
    counts: StrictBool


class BuyOr(Model):
    """licence 7: another way to meet what an under-16's `buy` document is bought for — the 16+ answer's DocumentNeed
    `or` (decision L9), with each document's name ("Yukon angling licence"). Draw the document "or" its alternatives."""
    need: Tuple[StrictStr, ...] = Field(min_length=1)
    names: Tuple[Annotated[StrictStr, Field(min_length=1)], ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _one_name_each(self):
        if len(self.names) != len(self.need):
            raise ValueError("an alternative names each document it needs, once")
        return self


class BuyDoc(Model):
    doc: StrictStr = Field(min_length=1)
    name: StrictStr = Field(min_length=1)
    prices: Prices
    #: licence 7: the document's alternatives (absent where it has none)
    or_: Optional[Tuple[BuyOr, ...]] = Field(default=None, alias="or", min_length=1)


class UnderSixteen(Model):
    """ROUND15 2 (owner): what an under-16 angler of one residency needs here (not guided, no status), read
    off the profiles' answers (`licence.under_16_summary`): fish with a licensed adult (`with_adult`), or
    buy `buy` (`own_limit`: a limit of their own), or nothing; `says` the sentence."""
    residency: Literal["resident", "non_resident", "non_resident_alien"]
    label: StrictStr = Field(min_length=1)
    none_needed: StrictBool
    with_adult: Optional[WithAdult]
    buy: Tuple[BuyDoc, ...]
    own_limit: StrictBool
    says: StrictStr = Field(min_length=1)


class LicenceFrame(Model):
    holds: Union[Holds, TidalHolds]
    profiles: Tuple[Union[ProfileAnswer, TidalProfile], ...]
    under_16: Optional[Tuple[UnderSixteen, ...]] = None     # ROUND15 2; absent on tidal water

    @model_validator(mode="after")
    def _sixty(self):
        if len(self.profiles) != 60:
            raise ValueError(f"{len(self.profiles)} profiles, not the 60 angler profiles")
        tidal = isinstance(self.holds, TidalHolds)
        if any(isinstance(p, TidalProfile) != tidal for p in self.profiles):
            raise ValueError("a tidal frame is the documented state on every profile, only there")
        return self
