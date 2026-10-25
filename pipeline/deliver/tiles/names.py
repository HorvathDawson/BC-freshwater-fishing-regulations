"""The DISPLAY NAME a tile feature carries — `name` goes on the map and must look right:
"Wahleach Lake". Search is the bundle's (`alias`, `item`), so the tile carries no search
haystack any more (`alt`, dropped 2026-10-03: nothing in the app read it).

`normalise` is THE name key, `pipeline.common.names.key` (T61 2.8), kept under this name for its
callers (the bundle's alias dedupe, lake facts, the gauge / stocking / bathymetry matchers). It
used to turn an apostrophe into a SPACE ("St. Mary's" -> "st mary s"), so "Joe's Lake" and "Joes
Lake" were two names here and one in the regulation matcher; the one key deletes it.
"""

from __future__ import annotations

import re

from pipeline.common.names import key
from pipeline.deliver.bundle.place_names import display_case

_WS = re.compile(r"\s+")


def normalise(s: str) -> str:
    """THE name key (`pipeline.common.names.key`): accents folded, case folded, apostrophes deleted,
    every other punctuation run a space. Comparison only — `display` is the shown name.

    Folding is not cosmetic: without it "Pouce Coupé" would key as "pouce coup e" and "Barrière" as
    "barrie re", and BC is full of them (Doré, Barrière, Pouce Coupé)."""
    return key(s)


def display(name: str) -> str:
    """A name fit for the map, cased by THE ONE CASER (`bundle.place_names.display_case`, T113): the
    card shows the same words (the export's `waters[].name` is `display_case` of the same registry
    name), so the map label and the card can never disagree. Registry variants arrive SHOUTING
    ("O'ROURKE LAKE" -> "O'Rourke Lake", "MCDONALD CREEK PARK" -> "McDonald Creek Park"); a name
    already in mixed case keeps it ('Sts'a'í:les', 'McArthur') save its shouted runs. This used to be
    a plain title-caser of its own, which gave "O'rourke Lake" and "Mcdonald Creek Park" on ~90
    features while the card said otherwise. Only whitespace is this module's own."""
    return display_case(_WS.sub(" ", (name or "").strip()))
