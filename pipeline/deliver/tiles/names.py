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

_WS = re.compile(r"\s+")


def normalise(s: str) -> str:
    """THE name key (`pipeline.common.names.key`): accents folded, case folded, apostrophes deleted,
    every other punctuation run a space. Comparison only — `display` is the shown name.

    Folding is not cosmetic: without it "Pouce Coupé" would key as "pouce coup e" and "Barrière" as
    "barrie re", and BC is full of them (Doré, Barrière, Pouce Coupé)."""
    return key(s)


def display(name: str) -> str:
    """A name fit for the map. Registry variants arrive SHOUTING; titlecase them, but
    never touch a name that already has mixed case — 'Sts'a'í:les' and 'McArthur' are
    correct as given and titlecasing would wreck both."""
    n = _WS.sub(" ", (name or "").strip())
    if not n or n != n.upper():
        return n
    return " ".join(w if len(w) <= 1 else w[0] + w[1:].lower() for w in n.split(" "))
