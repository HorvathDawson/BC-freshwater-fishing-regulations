"""DOES A WATER FLOW — the ONE definition, read by the registry build and nothing downstream.

The book's stream definition (p.80, "Definitions") makes a slough a stream, and the user ruled
(2026-10-03) that a slough, canal or channel is a stream for EVERY regulation. FWA cannot tell
such a water from a lake — every one of them is `WATERBODY_TYPE L`, `GB15300000` in the lakes
layer — so THE NAME IS THE ONLY SIGNAL: a water whose name's HEAD NOUN (its last word, before any
"at …"/"near …" phrase) is slough, canal, channel, river or creek flows. "Vedder Canal", "Gravel
Slough", "Sumas Lake Canal", "Rancheria River" flow; "Bear Creek Reservoir", "Corn Creek Marsh",
"River Lakes", "OKANAGAN RIVER OXBOWS" and "Pete's Pond Unnamed Lake At The Head Of San Juan River"
do not (2026-10-03: the old any-word test made all of those flow). Measured on the registry's 7,797
lake/wetland items: 119 flow, 0 false positives, 0 false negatives (FREV/sloughs §0).

WHERE IT IS READ. `pipeline.atlas.registry.flowing` turns every flowing polygon into a `stream`-kind
item (joined to its river where one threads it), and `registry.build` uses it to tell a river's
name from a lake's. After the registry is written, `item.kind` IS the water kind and every consumer
— the reach (`reach.water_kind.kind_of`), the bundle, the export, the tiles, the apps — reads it;
NOTHING recomputes `flows` (`test_flows_is_decided_in_the_registry_only`). The one other reader is
the known-steelhead list generator (`regs.steelhead.known_waters`), which is a separate step over
the same registry.

A beaver pond and a stream crossing a reservoir's drawdown zone are streams by the book too; neither
can be told from the data yet (the obstacles layer is not fetched), so nothing here claims them.
"""
from __future__ import annotations

import re

#: A name whose HEAD NOUN is one of these flows.
FLOWING = re.compile(r"\b(slough|canal|channel|river|creek)$", re.I)
_LOCATIVE = re.compile(r"\s+(at|near)\b.*$", re.I)


def flows(kind, name) -> bool:
    """Flowing water: a stream, or a water of another kind whose name's head noun says it flows."""
    if str(getattr(kind, "value", kind) or "").lower() == "stream":
        return True
    head = _LOCATIVE.sub("", str(name or "").strip())
    return bool(FLOWING.search(head))


def head_noun(name) -> str:
    """The head noun of a flowing name, lower-cased ("slough" for "Nicomen Slough"); "" otherwise."""
    m = FLOWING.search(_LOCATIVE.sub("", str(name or "").strip()))
    return m.group(1).lower() if m else ""
