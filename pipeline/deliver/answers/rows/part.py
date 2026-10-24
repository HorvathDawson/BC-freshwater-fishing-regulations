"""`Part`: the part, one reading — its rules (as `R`), water kind, steelhead presence and whether a rainbow
over 50 cm is a steelhead there, as every other module of the rows layer asks them."""
from __future__ import annotations

from typing import List, Optional
from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.rows.rule import R


class Part:
    """What the rows read of one part on one reading: its rules in the page's order, its water
    kind and steelhead facts, the reader's verdict per fish and origin."""

    def __init__(self, B, set_id: int, steelhead_water: bool, steelhead_rules: bool, kind,
                 presence, ladder_value: dict, md, open_states: Optional[dict] = None,
                 at=None):
        self.B = B
        self.kind, self.presence = kind, presence
        self.steelhead_water = steelhead_water
        self.steelhead_rules = steelhead_rules
        # the export says `steelhead_rules: false` only on a KNOWN part none applies to
        self.rules_off_in_export = presence == "known" and not steelhead_rules
        self.md = md
        #: the moment the reading is asked at (`calendar.Moment`; answers 2.1): a weekday or
        #: hours rule is in force at the moments it covers, out at the others
        self.at = at
        bound = B.sets[set_id]
        reach = [(e, r, v) for e, r, v in bound if v == "reach"]
        trib = [(e, r, v) for e, r, v in bound if v == "trib"]
        self.cands: List[R] = []
        for e, r, v in reach + trib:                       # decision R3: the page's order
            x = B.rules[(e, r)]
            if x.get("family") == "information":
                continue
            self.cands.append(R(f"{e}::{r}", x, v))
        self.by_key = {r.key: r for r in self.cands}
        self.all_rules = {f"{e}::{r}" for e, r in B.rules}
        self.ladder = ladder_value
        self.open_states = open_states or {}
        self._notes: Optional[dict] = None

    def applies(self, r: R) -> bool:
        """M3: a rule bound here is for this kind of water — its binding enforced `water`
        (feature_types). One that is not is a binding defect: refused, never filtered."""
        if r.f.get("water") and self.kind and r.f["water"] != self.kind:
            raise AnswersError(f"rows: {r.key} is for {r.f['water']}s, bound to a {self.kind}")
        return True

    def in_dates(self, r: R) -> bool:
        """The rule's dates hold today (the reader's calendar, `read.in_force`; some hours count —
        the page reads only the dates here)."""
        from pipeline.deliver.bundle import read
        return read.in_force(r.f.get("when"), self.md, self.at) != "no"

    def sp_name(self, S: str) -> str:
        return common.sp_name(S)

    def verdict(self, S: str, o: str) -> dict:
        v = self.ladder.get(S)
        if v is None:
            raise AnswersError(f"rows: the ladder does not answer {S} for this part (decision R4)")
        return v[o]

    def lift_notes(self, S: str, o: str, win: R) -> List[dict]:
        return self.lift_notes_of(win)

    def lift_notes_of(self, win: R) -> List[dict]:
        """Decision R6: the qualified lifts of rules in force here today that name `win`."""
        from pipeline.deliver.bundle import read
        if self._notes is None:
            self._notes = {}
            for L in self.cands:
                if not self.applies(L) or read.in_force(L.f.get("when"), self.md, self.at) != "yes" \
                        or read.not_yet_mapped(L.f) or L.f.get("side"):
                    continue
                for e in L.f.get("exempts") or []:
                    t = f"{e['entry_id']}::{e['rule_id']}"
                    if t == L.key or ("when" in e
                                      and read.in_force(e["when"], self.md, self.at) == "no"):
                        continue
                    q = {k: e[k] for k in ("when_targeting", "while", "lengths") if e.get(k)}
                    if q:
                        self._notes.setdefault(t, []).append({"by": L.key, "q": q})
        return self._notes.get(win.key, [])

    def open_gates(self) -> List[R]:
        """Decision R5: the open-subject gate rules that speak (the page's `ctx.active`)."""
        from pipeline.regs.parsing.catalogue import PROTECTED_FISH, SPECIES_GROUPS, OPEN_SUBJECTS
        out = []
        for r in self.cands:
            sp = r.f.get("species") or []
            if not (r.type == "retention_limit" and r.k == "gate" and sp and not r.f.get("while")
                    and not r.f.get("caught") and self.applies(r)):
                continue
            if not all((c in OPEN_SUBJECTS and c != "ALL_FIN_FISH") or c in PROTECTED_FISH
                       for c in sp):
                continue
            st = self.open_states.get(r.key)
            if st is None:
                raise AnswersError(f"rows: no reader answer for the open-subject rule {r.key}")
            if st[0] == "speaks":
                out.append(r)
        out.sort(key=lambda r: r.rank)
        return out


def open_fish(x: dict) -> str:
    """The fish the reader is asked about for an open-subject rule (decision R5)."""
    from pipeline.regs.parsing.catalogue import SALMON_FISH
    sp = list(x.get("species") or [])
    for c in sp:
        if c not in ("SALMON", "PROTECTED_SPECIES", "ALL_FIN_FISH"):
            return c
    if "SALMON" in sp:
        return next(f for f, g in sorted(SALMON_FISH.items()) if g == "SALMON")
    return sp[0]
