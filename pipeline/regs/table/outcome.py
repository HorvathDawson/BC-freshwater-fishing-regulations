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


@dataclass(frozen=True)
class Outcome:
    kind: str                       # closed | release | quota | unlimited
    n: Optional[int] = None
    period: str = "daily"
    pooled: bool = False            # `combined`: n SHARED across the species, not n of each

    # smaller == stricter. One definition, used by every fold.
    @property
    def rank(self) -> Tuple[int, int]:
        return {"closed": (0, 0), "release": (1, 0),
                "quota": (2, self.n or 0), "unlimited": (3, 0)}[self.kind]

    def __lt__(self, o: "Outcome") -> bool: return self.rank < o.rank
    def stricter(self, o: "Outcome") -> "Outcome": return self if self.rank <= o.rank else o

    def word(self) -> str:
        return {"closed": "0", "release": "release",
                "unlimited": "∞", "quota": str(self.n)}[self.kind]

    def sentence(self) -> str:
        return {"closed": "you may not fish for it",
                "release": "you may fish for it, and must release every one",
                "unlimited": "no limit",
                "quota": f"keep up to {self.n}"
                          + (" between them" if self.pooled else " of each")
                          + f" per {self.period.replace('annual','licence year')}"
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
