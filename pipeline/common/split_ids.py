"""THE SPLIT ID FUNCTION (T355, d9b7c7c40): a cut's id follows from its anchor, never from the order marks were
made or read, and is never stored anywhere but splits.json. One home, so the review proposals and the atlas (T326:
tidal cuts and mouth aliases) mint the same id for the same place.

T523: an OFFSET cut ends in its direction and distance — `_u100m` / `_d100m`, the hand ids' convention
(`puntledge_river__morrison_creek_into_puntledge_river_u100m`) — and a name is cut back to a whole word, never
mid-word, so two cuts are told apart by their words and `_2` is left for truly identical names."""
from __future__ import annotations

import re
from typing import Iterable, Optional

#: the name part's longest slug (whole words); the water part's is 40
NAME_MAX = 60

#: the words `amend` / a label adds for an offset ("weir (100 m upstream)"): the id says them as `_u100m`
_OFFSET_WORDS = re.compile(r"\s*\(\s*[\d.]+\s*m\s+(?:upstream|downstream)\s*\)\s*$", re.IGNORECASE)


def slug(s: str, n: int = 60) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")[:n].strip("_")


def readable_slug(s: str) -> str:
    """THE AREA / REGISTRY SLUG (T560 item 5): lower case, every run of other characters one `_`, never cut —
    `area_catalog.area_id`'s `area:{kind}:{slug}`, the registry's `area:{slug}` and item/lake boundary ids, and the
    review's `split_use.area_slug` (which must reproduce registry area ids) all call this one function; they were three
    byte-identical copies. (`slug` above cuts to a length; that is the split id's rule, not this one.)"""
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", (s or "").lower())).strip("_")


def word_slug(s: str, n: int = NAME_MAX) -> str:
    """`slug`, cut back to the last whole word that fits in `n` (a single word longer than `n` is cut at `n`)."""
    full = re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")
    if len(full) <= n:
        return full
    head = full[:n + 1]
    return (head[:head.rfind("_")] if "_" in head else full[:n]).strip("_")


def offset_suffix(offset_m: Optional[float]) -> str:
    """`_u100m` (+ upstream) / `_d100m` (- downstream); "" for no offset."""
    if not offset_m:
        return ""
    return f"_{'u' if offset_m > 0 else 'd'}{abs(float(offset_m)):g}m"


def base(water: str, name: str, offset_m: Optional[float] = None) -> str:
    """THE SLUG a new split lands under, before a clash suffix: `<water>__<name>[_u<m>m|_d<m>m]` (splits.json's
    scheme). `offset_m` is + upstream / - downstream; the name's own "(100 m upstream)" words are dropped for it."""
    if offset_m:
        name = _OFFSET_WORDS.sub("", str(name))
    return f"{slug(water.split('(')[0], 40)}__{word_slug(name)}{offset_suffix(offset_m)}"


def assign_ids(rows: Iterable[tuple], taken: Iterable[str]) -> dict:
    """REAL IDS (T355): rows (key, base, digest) -> {key: id}. `base` if free, else `base_2`, `_3` … past every
    id in `taken` (splits.json) and every other pending cut. Clashing cuts are numbered in DIGEST order, so an
    id follows from the anchors in play, never from the order the marks were made or read."""
    used, out = set(taken), {}
    for key, b, _dig in sorted(rows, key=lambda r: (r[1], r[2], str(r[0]))):
        sid, k = b, 1
        while sid in used:
            k += 1
            sid = f"{b}_{k}"
        used.add(sid)
        out[key] = sid
    return out
