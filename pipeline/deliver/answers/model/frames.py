"""Every section's frame model (`FRAMES`), the top-level and static tables (`TOP`, `STATICS`), and the
validation that refuses a wrong shape naming the section and the field (`validate_section`, `validate_top`),
with each model's JSON Schema (`json_schemas`)."""
from __future__ import annotations

import json
from typing import Dict, Optional, Tuple
from pydantic import StrictStr, TypeAdapter
from pipeline.deliver import types as T
from pipeline.deliver.answers.model.base import _enum
from pipeline.deliver.answers.model.rows import RowsFrame
from pipeline.deliver.answers.model.gear import GearFrame
from pipeline.deliver.answers.model.licence import LicenceFrame
from pipeline.deliver.answers.model.display import (
    DisplayFrame, LicensingFacts, ReasonWords, RuleFacts, StripState, WaterFacts
)
from pipeline.deliver.answers.model.top import Glossary, Moment, Place, Unnamed


#: One adapter per section's frame (per (key, segment) value), and per static table.
FRAMES: Dict[str, TypeAdapter] = {
    "rows": TypeAdapter(RowsFrame),
    "gear": TypeAdapter(GearFrame),
    "licence": TypeAdapter(LicenceFrame),
    "display": TypeAdapter(DisplayFrame),
}


#: The top-level tables' models (not sections): `moments`, `glossary`, `places`, `unnamed`.
TOP: Dict[str, TypeAdapter] = {"moments": TypeAdapter(Tuple[Moment, ...]),
                               "glossary": TypeAdapter(Glossary),
                               "places": TypeAdapter(Tuple[Place, ...]),
                               "unnamed": TypeAdapter(Unnamed)}

STATICS: Dict[Tuple[str, str], TypeAdapter] = {
    ("display", "rules"): TypeAdapter(Tuple[RuleFacts, ...]),
    ("display", "waters"): TypeAdapter(Dict[StrictStr, WaterFacts]),
    ("display", "strip"): TypeAdapter(Tuple[StripState, ...]),
    ("gear", "count_nouns"): TypeAdapter(Dict[StrictStr, Tuple[StrictStr, StrictStr]]),
    ("display", "licensing"): TypeAdapter(Tuple[LicensingFacts, ...]),
    ("display", "reasons"): TypeAdapter(Dict[_enum(T.LossReason, "LossReason"), ReasonWords]),
}


class ShapeError(ValueError):
    """A section value the answers/2 model refuses: the build stops, naming it."""


def _wire(x) -> str:
    """The value as the wire holds it (JSON): what the models validate, in strict JSON mode."""
    return json.dumps(x)


def validate_section(name: str, values, statics: Optional[dict] = None) -> int:
    """Validate every DISTINCT value of a section (and its static tables) against its model.
    Returns how many distinct values were validated; raises `ShapeError` on the first refusal."""
    ad = FRAMES.get(name)
    seen = set()
    n = 0
    if ad is not None:
        for v in values:
            s = json.dumps(v, sort_keys=True)
            if s in seen:
                continue
            seen.add(s)
            try:
                ad.validate_json(_wire(v))
            except Exception as e:                      # pydantic's ValidationError, named
                raise ShapeError(f"answers/2: a `{name}` frame is not its model: {e}") from None
            n += 1
    for t, v in (statics or {}).items():
        sa = STATICS.get((name, t))
        if sa is None:
            continue
        try:
            sa.validate_json(_wire(v))
        except Exception as e:
            raise ShapeError(f"answers/2: `{name}.{t}` is not its model: {e}") from None
    return n


def json_schemas() -> dict:
    """The JSON Schema of every frame and static model (shipped in the file's `spec`)."""
    out = {name: ad.json_schema(by_alias=True) for name, ad in FRAMES.items()}
    out.update({f"{n}.{t}": ad.json_schema(by_alias=True) for (n, t), ad in STATICS.items()})
    out.update({f"top.{t}": ad.json_schema(by_alias=True) for t, ad in TOP.items()})
    return out


def validate_top(name: str, value) -> None:
    """Validate a top-level table (`TOP`) against its model."""
    try:
        TOP[name].validate_json(_wire(value))
    except Exception as e:
        raise ShapeError(f"answers/2: top-level `{name}` is not its model: {e}") from None
