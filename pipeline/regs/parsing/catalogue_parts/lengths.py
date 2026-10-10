"""Length bands — `LengthBand`, the ordered size ranges with their own take.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, ConfigDict, model_validator


#: POLICY (user ruling Q9, 2026-10-07): A FISH EXACTLY ON A PRINTED SIZE BOUND IS LEGAL. "None under
#: 30 cm" keeps a 30.0 cm fish; "no trout over 50 cm" keeps a 50.0 cm one. One reading for the
#: whole corpus, decided here and nowhere else: a band that keeps NONE (`take: 0` — a floor, a
#: ceiling, a hole) does not hold its own finite bounds, so the bound itself falls to the band that
#: keeps (or to no band: the rule says nothing about it). Before, `[{max_cm: 30, take: 0}]` alone
#: denied 30.0 while `[{min_cm: 30}, {max_cm: 30, take: 0}]` granted it (order settled the shared
#: endpoint) — two answers for one printed phrase. A plain half-open `[min, max)` would have made
#: "no trout over 50 cm" deny 50.0, so the openness belongs to the denying band, not to one side.
#: The book's "X cm OR MORE" / "X cm OR LESS" put X inside the band (Inland, Khartoum, Lois, Ruby
#: "40 cm or more"; Chilliwack "(50 cm or less)"): those bands say so with `closed: true`.
EXACT_BOUND_IS_LEGAL = True


class LengthBand(BaseModel):
    """ONE RANGE OF FISH LENGTHS, AND HOW MANY OF THEM YOU MAY KEEP.

    `min_cm` and `max_cm` bound the range, and null is open at that end. `take` is how many of
    THESE you may keep; omitted, the rule's own `take` applies to them.

    A BOUND IS INCLUSIVE, EXCEPT ON A BAND THAT KEEPS NONE (`take: 0`): a fish exactly on such a
    band's bound is not in it (`EXACT_BOUND_IS_LEGAL`, user ruling Q9) — unless the book prints
    "X cm or more" / "X cm or less", which the band records as `closed: true`.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    min_cm: Optional[int] = None
    max_cm: Optional[int] = None
    take: Optional[int] = None
    #: the book's "X cm OR MORE" / "X cm OR LESS": this take-0 band holds X itself (only on a
    #: take-0 band — every other band already holds its bounds; True or absent, never False)
    closed: Optional[bool] = None

    @model_validator(mode="after")
    def _real(self) -> "LengthBand":
        if self.min_cm is None and self.max_cm is None:
            raise ValueError("a length band open at both ends is every fish; say nothing instead")
        if self.min_cm is not None and self.max_cm is not None and self.min_cm >= self.max_cm:
            raise ValueError(f"min_cm {self.min_cm} >= max_cm {self.max_cm} is an empty range")
        if self.take is not None and self.take < 0:
            raise ValueError("take cannot be negative")
        if self.closed is not None and (self.closed is not True or self.take != 0):
            raise ValueError("`closed` is only `true`, and only on a band that keeps none "
                             "(take 0): the book's 'X cm or more' / 'X cm or less'")
        return self

    def holds(self, cm: float) -> bool:
        """Whether a fish of `cm` is in this band. A take-0 band does not hold its own finite
        bounds unless `closed` (`EXACT_BOUND_IS_LEGAL`): the bound is legal."""
        if self.take == 0 and not self.closed and EXACT_BOUND_IS_LEGAL:
            return ((self.min_cm is None or cm > self.min_cm) and
                    (self.max_cm is None or cm < self.max_cm))
        return ((self.min_cm is None or cm >= self.min_cm) and
                (self.max_cm is None or cm <= self.max_cm))
