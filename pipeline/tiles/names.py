"""Display name and search haystack — two fields, because they want opposite things.

`name` goes on the map and must look right: "Wahleach Lake".
`alt`  goes to search and must match whatever a person types: "jones", "wahleach (jones) l.".

Fusing them gives you either an ugly map or a search that misses. The registry carries
both cases of the same string ("EAST WHITE RIVER" and "East White River"), so the
haystack normalises and dedupes before joining.
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


def haystack(display_name: str, variants: list[str] | tuple[str, ...]) -> str:
    """`|`-separated lowercase search string, deduped, display name first.

    Returns "" when the variants add nothing — most features, and an empty attribute
    is one tippecanoe drops entirely.
    """
    seen: list[str] = []
    keys: set[str] = set()
    for raw in (display_name, *variants):
        k = normalise(raw)
        if not k or k in keys:
            continue
        keys.add(k)
        seen.append(k)
    if len(seen) <= 1:
        return ""
    return "|".join(seen)
