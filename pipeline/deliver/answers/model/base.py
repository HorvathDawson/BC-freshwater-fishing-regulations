"""The base of every answers/2 model: `Model` (strict, no extra field, frozen), the index and size
aliases, and the closed vocabularies as `Literal`s (`pipeline.deliver.types`)."""
from __future__ import annotations

from typing import Annotated, Literal, Tuple, Union
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StrictStr
from pipeline.deliver import types as T


class Model(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


RuleIx = Annotated[StrictInt, Field(ge=0)]
LicIx = Annotated[StrictInt, Field(ge=0)]
Count = Annotated[StrictInt, Field(ge=0)]
Positive = Annotated[StrictInt, Field(gt=0)]
Cm = Union[StrictInt, StrictFloat]                   # band edges: whole or half cm
Fish = StrictStr                                      # a FishCode value (checked below)
ClauseRef = Tuple[RuleIx, Count]                      # (rule, clause index into its `gear`)


def _enum(enum, name: str):
    """A str field restricted to an enum's values (the JSON Schema says `enum`)."""
    return Literal[tuple(m.value for m in enum)]  # type: ignore[valid-type]


FishLit = _enum(T.FishCode, "FishCode")
Decided_ = _enum(T.DecidedStatus, "DecidedStatus")
RoleLit = _enum(T.Role, "Role")
LineLit = _enum(T.LineType, "LineType")
CondLit = _enum(T.CondType, "CondType")
BadgeLit = _enum(T.BadgeOf, "BadgeOf")
SteelLineLit = _enum(T.SteelheadLine, "SteelheadLine")
OriginLit = Literal["hatchery", "wild"]
