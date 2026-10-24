"""The shipped pair: `encode` (the model -> data + guide, `codec.compact`), `guide_path` and `load`
(a pair decoded back to the model — how a test or tool reads it, never `json.load` of the data file alone).
"""
from __future__ import annotations

import json
from pathlib import Path
from pipeline.regs.parsing import catalogue as C
from pipeline.deliver.bundle.read import Authority, Scope, Source
from pipeline.deliver.export import codec as K
from pipeline.deliver.export.contract import GUIDE_NAME, OUT


def codec_tables() -> dict:
    """The two small tables a decoder needs, FROM THE MODEL: a rule type's family
    (`catalogue._FAMILY`) and a provenance's rank (`read.Source.rank` for every authority and
    scope)."""
    return {"family_of_type": {t.value: f for t, f in C._FAMILY.items()},
            "rank": {f"{a.value}/{s.value}": Source(a, s).rank for a in Authority for s in Scope}}


def encode(doc: dict) -> tuple[dict, dict]:
    """The model -> (data, guide) as they ship (`codec.compact`)."""
    return K.compact(doc, **codec_tables())


def guide_path(out: Path) -> Path:
    return Path(out).parent / GUIDE_NAME


def load(out: Path = OUT, guide: Path | None = None) -> dict:
    """A shipped pair, decoded to the model (`codec.expand`)."""
    g = guide or guide_path(out)
    return K.expand(json.loads(Path(out).read_text(encoding="utf-8")),
                    json.loads(Path(g).read_text(encoding="utf-8")))
