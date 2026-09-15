"""PROTOTYPE 2 — OUTCOME: what you may do, as one point on a total order.

WHAT IT HANDLES
  Today the outcome is `take` (an int, or 0, or None) plus `may_target` plus `unlimited`, and
  "is this stricter than that" is written out longhand wherever it is needed — 29 separate
  comparisons against `take===0` / `take!==0` / `may_target` in the page. The kokanee defect
  was one of those written the wrong way round:

      if(shutAll.length && l.take!=null && l.take!==0){ keep="0"; }   <- skips every release

  A closure is stricter than a release. That is not an opinion to re-implement per call site;
  it is an ORDER. Put the four possible answers on it once and `stricter()` is `min()`.

      Closed        you may not fish for it            (strictest)
      ReleaseAll    you may fish, you keep none
      Quota(n)      you keep up to n                   (ordered among themselves by n)
      Unlimited     no limit                           (weakest)

  `rank` is the sort key and smaller is stricter, so a fold is `min(...)` and nothing has to
  decide anything. There is no place left to write the wrong test.

  SIZE-ONLY AND PAPERWORK RULES ARE NOT OUTCOMES AT ALL. `take is None` today means "this rule
  says nothing about how many", and the page had to guard `take!=null` everywhere or it
  rendered "0 (normally —)" — a number the rule never had. Here they are simply not Outcomes;
  they are qualifiers on a Subject, which is what they always were.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple


#: "per daily" is not English. The book says "per day".
_PERIOD = {"daily": "day", "annual": "licence year", "possession": "day in possession"}


@dataclass(frozen=True)
class Outcome:
    kind: str                       # closed | release | quota | unlimited
    n: Optional[int] = None
    period: str = "daily"
    pooled: bool = False            # `combined`: n SHARED across the species, not n of each

    def __post_init__(self):
        # A QUOTA OF ZERO IS NOT A QUOTA. It is a release or a closure, and both already have a
        # point on this order — strictly stricter than any quota. Left representable, `Quota(0)`
        # ranked (2,0), WEAKER than release, which is the order upside down. And `Quota(None)`
        # printed the word "None" into the keep column.
        if self.kind == "quota" and not self.n:
            raise ValueError("a quota of zero or None is a release or a closure, not a quota")

    # smaller == stricter. One definition, used by every fold.
    @property
    def rank(self) -> Tuple[int, int, int]:
        # POOLED IS STRICTER AT THE SAME NUMBER. "6 in the aggregate" and "6 of each" ranked
        # equal, so the winner was whichever the candidate list happened to hold first — and on
        # Lois Lake it printed "keep up to 6 of each" for a rule that allows 6 between them.
        # That is the five-times-the-legal-limit read, decided by list order.
        return {"closed": (0, 0, 0), "release": (1, 0, 0),
                "quota": (2, self.n or 0, 0 if self.pooled else 1),
                "unlimited": (3, 0, 0)}[self.kind]

    def __lt__(self, o: "Outcome") -> bool: return self.rank < o.rank

    def same_answer(self, o: "Outcome") -> bool:
        """`period` is meaningless on a closure, a release and an unlimited, yet it takes part in
        `__eq__` — so two identical releases could fail to be called "says the same thing"."""
        if self.kind != o.kind: return False
        return (self.n, self.pooled) == (o.n, o.pooled) if self.kind == "quota" else True
    def stricter(self, o: "Outcome") -> "Outcome": return self if self.rank <= o.rank else o

    def word(self) -> str:
        return {"closed": "0", "release": "release",
                "unlimited": "∞", "quota": str(self.n)}[self.kind]

    def sentence(self) -> str:
        return {"closed": "you may not fish for it",
                "release": "you may fish for it, and must release every one",
                "unlimited": "no limit",
                # "OF EACH" IS A CLAIM, AND IT IS USUALLY NOT IN THE DATA. `combined` is unset
                # on 224 of 248 group quotas — "Char daily quota = 1" says nothing about
                # whether that is one char or one of each kind of char. Defaulting to "of each"
                # answers the question in the permissive direction on every one of them: one
                # bull trout AND one Dolly Varden AND one lake trout, which is the
                # five-times-the-legal-limit read this type was written to prevent.
                #
                # `combined` set is a fact and says "between them". `combined` unset is silence,
                # and the number alone is what the book gives the reader.
                "quota": f"keep up to {self.n}"
                          + (" between them" if self.pooled else "")
                          + f" per {_PERIOD.get(self.period, self.period)}"
                }[self.kind]


CLOSED = Outcome("closed")
RELEASE = Outcome("release")


def outcome_of(take, may_target, unlimited, period, pooled=False) -> Optional[Outcome]:
    """The one place the flat fields become an Outcome. `None` = this rule is not about how
    many, so it never reaches the keep column and never needs a guard there."""
    if unlimited: return Outcome("unlimited", period=period or "daily", pooled=pooled)
    if take is None: return None
    if take == 0:
        # `may_target` is declared Optional[bool] and ships as int 0/1. Absent = you may fish
        # for it. Present and falsy = you may not. Every consumer used to need to know that;
        # now exactly one line does, and downstream there is only `Outcome`.
        return CLOSED if (may_target is not None and not may_target) else RELEASE
    return Outcome("quota", take, period or "daily", pooled)
