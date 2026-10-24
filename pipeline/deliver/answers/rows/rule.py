"""Rules as the page reads them: `R`, a rule's fields as attributes, and the small readings of one rule the
other modules share (its size bands, whether it keeps, whether it covers a fish)."""
from __future__ import annotations

import math
from typing import List, Optional, Tuple
from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import expand
from pipeline.deliver.answers.display import bands as rule_bands
from pipeline.deliver.answers.display import kind_of
from pipeline.deliver.bundle import read


INF = math.inf


class R:
    """One rule of the part, with what the page derives once (`species`, `k`, `rank`, `wins`)."""
    __slots__ = ("key", "x", "f", "k", "rank", "species", "except_", "via", "entry_id", "rule_id",
                 "type", "dimension", "wins")

    def __init__(self, key: str, x: dict, via: str):
        self.key, self.x, self.f, self.via = key, x, x, via
        self.entry_id, self.rule_id = x["entry"], x["rule"]
        self.type, self.dimension = x.get("type"), x.get("dimension")
        self.k = kind_of(x)
        self.rank = read.place(x, via)
        self.species = expand(x.get("species"))
        self.except_ = expand(x.get("species_except"))
        self.wins = common.when_dates(x.get("when"))

    def take(self):
        return self.f.get("take")


def _keepish(s: Optional[str]) -> bool:
    return s in common.KEEPS


def _covers(r: R, S: str) -> bool:
    return S in r.species and S not in r.except_


def _ge(a, b) -> bool:
    """JavaScript `a >= b` for a number b and a possibly-missing a (null reads 0; undefined is
    never >=)."""
    if a is None:
        return False
    return a >= b


def _bands(r: R) -> List[Tuple[float, float, Optional[float]]]:
    return [(a, INF if b is None else b, t) for a, b, t in rule_bands(r.x)]


def size_lines(r: R, pool_take, into: list) -> None:
    """The page's `sizeLines`: a band of take 0 is a release line, a band keeping fewer than the
    day (or any band of a clause) a cap line."""
    for a, b, t in _bands(r):
        if t == 0:
            into.append({"t": "rel", "a": a, "b": b, "r": r})
        elif t is not None and t > 0 and (r.f.get("within") or t < pool_take):
            into.append({"t": "cap", "a": a, "b": b, "take": t, "r": r})


def _js(v) -> str:
    """A number as JavaScript prints it in a template string."""
    if v is None:
        return "null"
    if v == INF:
        return "Infinity"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _keep_range(r: R) -> Tuple[float, float]:
    """A rule's own keep range from its bands (a band of 0 or of no number keeps nothing)."""
    keep = [(a, b) for a, b, t in _bands(r) if t]
    return (keep[0][0], keep[-1][1]) if keep else (0, INF)
