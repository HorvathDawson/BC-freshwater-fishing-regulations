"""SIZE, WITH ITS POLARITY DECIDED — because two loose ints carry four different laws.

`over_cm` and `under_cm` do not mean one thing. What they mean depends on `take`, `within`,
`band` and `period`, and `catalogue._size()` is the corpus's own statement of it:

    take = 0                the size says WHICH FISH GO BACK      "none over 50 cm"
    take = n, within        the size says which fish the CAP COUNTS
                            ...and it is ASYMMETRIC: `over_cm` caps how many BIG fish the
                            allowance includes; `under_cm` is a FLOOR on every fish kept
    take = n, not daily     the size says which fish the ANNUAL ceiling counts — it does not
                            forbid smaller ones, which the daily quota governs
    band                    a HOLE: the middle is forbidden, the outside allowed

The first version of this module kept `Size(lo=over_cm, hi=under_cm)` and read one meaning off
it — "keep only fish over lo, under hi". On 112 rules that is the opposite of the law. The
Shuswap rendered "Char · keep 1 · under 60 cm" where the book says the one char you keep must be
OVER 60 cm; `r2:cultus_lake` is the case the catalogue cites for exactly this, and it exists
because getting it backwards puts an angler's hands on the fish the rule protects.

So polarity is decided ONCE, here, where the fields that decide it are still in scope — and
what travels afterwards is a value that can only be read one way.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Size:
    kind: str = "any"        # any | none_over | none_under | counts_over | counts_under
                             #     | slot | band
    lo: Optional[int] = None
    hi: Optional[int] = None

    @property
    def is_any(self) -> bool: return self.kind == "any"

    @property
    def is_gate(self) -> bool:
        """A PROHIBITION on a size class, as opposed to a size class a NUMBER counts.

        "none under 30 cm" sends a fish back; "1 over 50 cm" inside a 4 says how many big ones
        the four may include. The first is a GATE — a take of zero on a size class, true beside
        whatever the count is — and belongs to no Subject (see gates.py). The second is a
        SELECTOR: it names the fish a number is about, and stays on the subject of that number.
        """
        return self.kind in ("none_over", "none_under", "slot", "band")

    @property
    def bound(self) -> str:
        """Which kind of bound this is — so two gates on one fish can be told apart from one
        gate said twice. A floor and a ceiling are both true at once; two floors are one
        statement, and the closer authority's is the one that stands."""
        return {"none_under": "floor", "none_over": "ceiling", "counts_over": "ceiling",
                "counts_under": "floor"}.get(self.kind, self.kind)

    def covers(self, s: "Size") -> bool:
        return self.is_any or self == s

    def words(self) -> str:
        k, lo, hi = self.kind, self.lo, self.hi
        return {
            "any":           "",
            "none_over":     f"none over {lo} cm",
            "none_under":    f"none under {hi} cm",
            "counts_over":   f"counting those over {lo} cm",
            "counts_under":  f"counting those over {hi} cm",
            "slot":          f"{hi}–{lo} cm only",
            "band":          f"none between {hi} cm and {lo} cm",
        }[k]


ANY = Size()


def size_of(over_cm, under_cm, *, take=None, within=None, band=False, period="daily") -> Size:
    """The polarity table from `catalogue._size()`, as a value instead of a sentence."""
    if over_cm and under_cm:
        return Size("band", over_cm, under_cm) if band else Size("slot", over_cm, under_cm)
    if not over_cm and not under_cm:
        return ANY
    if take == 0:
        # Which fish go back. This is the only reading where the bound is a prohibition.
        return Size("none_over", lo=over_cm) if over_cm else Size("none_under", hi=under_cm)
    if period and period != "daily" and take:
        # An annual ceiling COUNTS a size class; it sets no minimum on the daily one.
        return Size("counts_over", lo=over_cm) if over_cm else Size("counts_under", hi=under_cm)
    if within and take:
        # Asymmetric on purpose — see the module docstring.
        return Size("counts_over", lo=over_cm) if over_cm else Size("none_under", hi=under_cm)
    # A bare allowance with a bound is a FLAT PROHIBITION — `_size`'s last line, "(none over X)"
    # / "(none under X)". Which is the same thing said the other way: `r2:cultus_lake` stores
    # "1 bull trout over 60 cm" as `under_cm=60`, and "none under 60 cm" IS "the one you keep
    # must be over 60". `under_cm` is always a floor and never a ceiling; reading it as one put
    # "each one under 60 cm" on the Shuswap, which is the rule upside down on the fish it
    # exists to protect.
    return Size("none_over", lo=over_cm) if over_cm else Size("none_under", hi=under_cm)
