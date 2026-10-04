"""The DISPLAY NAME a tile feature carries — `name` goes on the map and must look right:
"Wahleach Lake". Search is the bundle's (`alias`, `item`), so the tile carries no search
haystack any more (`alt`, dropped 2026-10-03: nothing in the app read it); `normalise` stays as the
one spelling of "what search compares".
"""

from __future__ import annotations

import re
import unicodedata

_WS = re.compile(r"\s+")
_PUNCT = re.compile(r"[^a-z0-9]+")


def normalise(s: str) -> str:
    """Lowercase, fold accents, collapse punctuation and whitespace. Comparison only.

    Folding is not cosmetic. `_PUNCT` turns anything outside `[a-z0-9]` into a SPACE, so
    without this "Pouce Coupé" would normalise to "pouce coup e" and "Barrière" to
    "barrie re" — both stop matching their unaccented spellings, and BC is full of them
    (Doré, Barrière, Pouce Coupé). Decomposing splits é into "e" plus a combining mark;
    the mark is deleted outright rather than spaced, which is the difference between
    "barriere" and "barrie re".
    """
    folded = unicodedata.normalize("NFKD", s or "")
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    return _WS.sub(" ", _PUNCT.sub(" ", folded.lower())).strip()


def display(name: str) -> str:
    """A name fit for the map. Registry variants arrive SHOUTING; titlecase them, but
    never touch a name that already has mixed case — 'Sts'a'í:les' and 'McArthur' are
    correct as given and titlecasing would wreck both."""
    n = _WS.sub(" ", (name or "").strip())
    if not n or n != n.upper():
        return n
    return " ".join(w if len(w) <= 1 else w[0] + w[1:].lower() for w in n.split(" "))
