"""The hand review of gauge matches — what a person decided, beside what the machine did.

    pipeline/gauge_review.json     authored by a human (via the review page)
    pipeline/gauge_match.json      written by the matcher, which READS the file above

WHY A SEPARATE FILE FROM THE MATCH. `gauge_match.json` is regenerated wholesale every time
the matcher runs; anything written into it is destroyed by the next run. A review has to
survive that — it is the expensive half. So the review is an INPUT to matching, and the
match is output. One direction, and the review is never rewritten by a program.

FOUR VERDICTS, AND THE LAST TWO ARE THE ONES PEOPLE COLLAPSE:

    confirmed   the automatic match is right. NOT a no-op — it pins the water, so a later
                atlas release that quietly moves this station fails loudly instead of
                silently. Half the value of a review is in the rows that were already fine,
                and recording only the corrections throws that half away.

    bind        the right answer is known: force this station onto these FWA keys. One or
                several — a lake outlet legitimately describes both the lake and the stream
                leaving it, and KOOTENAY LAKE OUTFLOW NEAR CORRA LINN is exactly that case.

    wrong       the automatic match is WRONG and the right water is not recorded yet. This
                is a TODO, and it becomes `unresolved` in the match — a gap to close.

    none        there is genuinely no such water. Comox Harbour is tidal salt water and no
                amount of alias work will find it a freshwater node. Becomes `no_match`.

`wrong` AND `none` ARE NOT THE SAME CLAIM, and merging them costs in both directions. Filing
a `wrong` as `no_match` says "we checked and there is nothing here", which stops anyone ever
looking again — the Kootenay below Corra Linn would stay ungauged forever on the strength of
a review that actually said "this needs the outlet stream, which we cannot address yet".
Filing a `none` as unresolved has the opposite cost: somebody re-investigates Comox Harbour
every year and reaches the same conclusion.

WHAT `bind` DOES NOT NEED. It writes `wsc`/`wbk` straight into the match without consulting
the graph, because those keys ARE the match — `StationMatch` is deliberately addressed in
FWA's own terms rather than by node id, so a human who knows the answer can simply state it.
No geometry, no build, no second matching pass.

MULTI-WATER BINDINGS ARE RECORDED BUT NOT YET CONSUMED. `wsc`/`wbk` on `StationMatch` are
single, and widening them reaches `split_defs`, `nodes_for` and the shed walk. So a binding
with several keys stores the first as the match and the rest in `also`, which nothing reads
yet. That is deliberate: the curation is captured faithfully now, and the code does not
pretend to act on it. See `pipeline/docs/HANDOFF-curated-layout.md` §8.
"""

from __future__ import annotations

import functools
import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ROOT = Path(__file__).resolve().parents[2]
REVIEW_FILE = ROOT / "pipeline" / "gauge_review.json"

Verdict = Literal["confirmed", "bind", "wrong", "none"]


class Decision(BaseModel):
    """One station, one person's decision about it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    verdict: Verdict
    #: What the station is called, so the file reads without a join.
    station_name: str = ""
    #: What the AUTOMATIC match said when this was reviewed. Review aid and drift evidence:
    #: a `confirmed` row whose match no longer agrees is the thing worth shouting about.
    matched: str | None = None
    #: WHAT THESE MEAN DEPENDS ON THE VERDICT, and both readings are worth keeping:
    #:
    #:   bind       the water to force this station onto. Several is legal.
    #:   confirmed  the water it was pinned TO — what a later drift is measured against.
    #:   wrong      the water it was wrongly matched to AT REVIEW TIME. Never a binding;
    #:              it is the evidence. If a future atlas makes this station resolve
    #:              somewhere else, the rejection was about a different question and
    #:              deserves re-reading.
    #:   none        unused.
    wsc: tuple[str, ...] = ()
    wbk: tuple[str, ...] = ()
    note: str = ""
    reviewed: str = ""

    @field_validator("wsc", "wbk", mode="before")
    @classmethod
    def _listify(cls, v):
        """A bare string is one key. Curators write both, and both are unambiguous."""
        if v is None:
            return ()
        return (v,) if isinstance(v, str) else tuple(v)

    @model_validator(mode="after")
    def _binds_must_name_a_water(self):
        if self.verdict == "bind" and not (self.wsc or self.wbk):
            raise ValueError("verdict 'bind' needs at least one wsc or wbk — a binding that "
                             "names no water is an unresolved match with extra steps")
        return self

    @property
    def keys(self) -> tuple[str, ...]:
        """Every FWA key this decision names, lakes first — a lake is the more specific.

        Empty for a `wrong` row: those keys record what was REJECTED, so reading them as a
        binding would point the station at the very water the review threw out.
        """
        return () if self.verdict in ("wrong", "none") else self.wbk + self.wsc

    @property
    def rejected(self) -> tuple[str, ...]:
        """For a `wrong` row, the water it was wrongly matched to. Empty otherwise."""
        return self.wbk + self.wsc if self.verdict == "wrong" else ()

    @property
    def is_todo(self) -> bool:
        """Does this decision still need a right answer? `wrong` is a worklist."""
        return self.verdict == "wrong"


class Review(BaseModel):
    """The whole file."""

    model_config = ConfigDict(frozen=True)

    about: str = Field("", alias="_about")
    stations: dict[str, Decision] = Field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.stations)

    def counts(self) -> dict[str, int]:
        import collections
        return dict(collections.Counter(d.verdict for d in self.stations.values()))


@functools.lru_cache(maxsize=4)
def load(path: Path | None = None) -> Review:
    """The review, or an empty one if nobody has curated anything yet.

    An empty review is a legitimate state — a fresh checkout, or a matcher run before any
    hand pass — so a missing file is not an error. A MALFORMED one is: it means somebody
    edited curation into a shape the matcher will ignore, which is the silent-empty failure
    this repo keeps paying for.
    """
    path = path or REVIEW_FILE
    if not path.exists():
        return Review()
    return Review.model_validate(json.loads(path.read_text(encoding="utf-8")))


def save(review: Review, path: Path | None = None) -> Path:
    """Write it back, sorted by station so a diff reads as a list of decisions.

    Only for migrations and tooling. The matcher never writes here — the whole point is that
    the review outlives the thing it corrects.
    """
    path = path or REVIEW_FILE
    body = {
        "_about": review.about or (
            "Hand review of gauge matches, and an INPUT to `pipeline.hydro.match` rather "
            "than output: `gauge_match.json` is rewritten wholesale on every run, so a "
            "decision recorded there would not survive. verdict is `confirmed` (the "
            "automatic match is right — pinned so a later atlas release cannot move it "
            "silently), `bind` (force this station onto these FWA keys; several is legal, "
            "a lake outlet describes both the lake and the stream below it), `wrong` (the "
            "match is wrong and the right water is not recorded YET — a worklist, and it "
            "becomes `unresolved`, never `no_match`), or `none` (there is genuinely no such "
            "water; Comox Harbour is tidal). Edited by hand and by the gauge review page."),
        "stations": {
            # Drop empties so a decision reads as what it IS. `model_dump` renders the
            # tuples as lists, so this must test `[]`, not `()`.
            s: {k: v for k, v in d.model_dump(mode="json").items()
                if v not in ([], "", None)}
            for s, d in sorted(review.stations.items())
        },
    }
    path.write_text(json.dumps(body, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    load.cache_clear()
    return path
