"""Every section's frame model (`FRAMES`), the top-level and static tables (`TOP`, `STATICS`), and the
validation that refuses a wrong shape naming the section and the field (`validate_section`, `validate_top`),
with each model's JSON Schema (`json_schemas`)."""
from __future__ import annotations

import json
from typing import Dict, Iterable, Optional, Tuple
from pydantic import StrictStr, TypeAdapter
from pipeline.deliver import types as T
from pipeline.deliver.answers.model.base import _enum
from pipeline.deliver.answers.model.rows import rows_frame_model
from pipeline.deliver.answers.model.gear import GearFrame
from pipeline.deliver.answers.model.licence import LicenceFrame
from pipeline.deliver.answers.model.display import (
    DisplayFrame, LicensingFacts, ReasonWords, RuleFacts, StripState, WaterFacts
)
from pipeline.deliver.answers.model.top import Glossary, Moment, Place, Unnamed
from pipeline.deliver.answers.model.trout import DateSet, FishSheet, TroutFrame, TroutFrameWire, TroutTableWire


#: One adapter per section's frame (per (key, segment) value), and per static table.
FRAMES: Dict[str, TypeAdapter] = {
    "rows": TypeAdapter(rows_frame_model()),        # F0: RowsFrame34 from rows 34
    "gear": TypeAdapter(GearFrame),
    "licence": TypeAdapter(LicenceFrame),
    "display": TypeAdapter(DisplayFrame),
    # T810 ticket I: the trout and char table (contract 2.3); required since the cutover (ticket L)
    "trout": TypeAdapter(TroutFrame),
}

#: The OPTIONAL sections' frames (contract 2.1: `answers.SECTIONS`' `optional` ones, held equal by a test): a file
#: without one carries none of its schemas. None since the trout cutover (T810 ticket L: `trout` is required); the rule
#: stays for the next additive section.
OPTIONAL_FRAMES: frozenset = frozenset()

#: The sections whose cell may be null (contract 2.6, Q-D3: the trout table is null where no table is drawn).
NULLABLE_FRAMES = frozenset({"trout"})

#: (T810 ticket I, contract 2.3 "wire against decoded") the shapes the trout section's tables hold on the wire — a
#: `trout_frame` row, a `trout_table` body, a `trout_dates` date set, a `trout_sheet` fish sheet — emitted beside the
#: decoded `TroutFrame` so the app's decoder types what it reads with generated types (`TroutWireValue`).
TROUT_WIRE = TypeAdapter(Tuple[TroutFrameWire, TroutTableWire, DateSet, FishSheet])


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
    # T517: every end token -> [words, other names…] (`answers.display.end_table`)
    ("display", "ends"): TypeAdapter(Dict[StrictStr, Tuple[StrictStr, ...]]),
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
            if v is None and name in NULLABLE_FRAMES:
                continue                                # Q-D3: no table at this cell
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


def json_schemas(present: Optional[Iterable[str]] = None) -> dict:
    """The JSON Schema of every frame and static model (shipped in the file's `schemas`). `present`: the sections a file
    holds — an optional section it lacks (`OPTIONAL_FRAMES`) is left out, with its wire shapes; None: every model (the
    app's generated copy, `emit`)."""
    keep = (lambda n: True) if present is None else (lambda n, p=set(present): n not in OPTIONAL_FRAMES or n in p)
    out = {name: ad.json_schema(by_alias=True) for name, ad in FRAMES.items() if keep(name)}
    if keep("trout"):
        out["trout.wire"] = TROUT_WIRE.json_schema(by_alias=True)
    out.update({f"{n}.{t}": ad.json_schema(by_alias=True) for (n, t), ad in STATICS.items()})
    out.update({f"top.{t}": ad.json_schema(by_alias=True) for t, ad in TOP.items()})
    return out


def validate_top(name: str, value) -> None:
    """Validate a top-level table (`TOP`) against its model."""
    try:
        TOP[name].validate_json(_wire(value))
    except Exception as e:
        raise ShapeError(f"answers/2: top-level `{name}` is not its model: {e}") from None
