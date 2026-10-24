"""The unnamed water's names (answers 2.4, S1): "Unnamed {kind} · {regions}" and the table of them."""
from __future__ import annotations

from typing import List, Sequence
from pipeline.common import entry_id as EID
from pipeline.deliver.answers.common import AnswersError


#: How an unnamed answer is named: the water kind, then the regions it is in (the owner:
#: "for labels … mus just stored in a separate spot, just show regions the section is in"). One
#: template, here; the app shows the whole string (AGENTS 33).
PLACE_TEMPLATE = "Unnamed {kind} · {regions}"


def regions_label(regions: Sequence[str]) -> str:
    """The regions a section is in, as the book names them: "Region 5", "Regions 3 and 8"."""
    if not regions:
        raise AnswersError("display: a place in no region has no name")
    return EID.region_words(regions)


def place_label(kind: str, regions: Sequence[str]) -> str:
    """The name of an unnamed section's answer, from its key's water kind and its region set (the
    bundle's `region_place`): "Unnamed stream · Region 5"; a lake on a region line "Unnamed lake ·
    Regions 3 and 8"; "Unnamed wetland · Region 7A". No MU (`mu_set`, stored apart)."""
    return PLACE_TEMPLATE.format(kind=kind, regions=regions_label(regions))


def places_table(region_places) -> List[dict]:
    """The answers' top-level `places`: per region set (`types.RegionPlace`, in place order) its
    regions, their words (`label`) and the whole name of an unnamed answer there per water kind
    (`names`)."""
    from pipeline.deliver.types import WaterKind
    return [{"regions": list(w.regions), "label": regions_label(w.regions),
             "names": {k.value: place_label(k.value, w.regions) for k in WaterKind}}
            for w in region_places]
