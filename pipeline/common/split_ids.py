"""THE SPLIT ID FUNCTION (T355, d9b7c7c40): a cut's id follows from its anchor, never from the order marks were
made or read, and is never stored anywhere but splits.json. One home, so the review proposals and the atlas (T326:
tidal cuts and mouth aliases) mint the same id for the same place."""
from __future__ import annotations

import re
from typing import Iterable


def slug(s: str, n: int = 60) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")[:n].strip("_")


def base(water: str, name: str) -> str:
    """THE SLUG a new split lands under, before a clash suffix: `<water>__<name>` (splits.json's scheme)."""
    return f"{slug(water.split('(')[0], 40)}__{slug(name, 32)}"


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
