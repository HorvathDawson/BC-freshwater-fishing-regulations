"""The three shapes a retention rule takes when it carries NO COUNT OF ITS OWN.

`outcome_of` returns None for these, and the first version of this module let that stand — they
were binned as "not a quota" beside the boat rules, and the compliance check passed while 294
retention rules went unread. They are not "not quotas"; they are statements ABOUT a quota, and
each attaches to the allowance it qualifies:

    A POSSESSION MULTIPLE      "possession quota is 2 daily quotas" (`per_daily`, 169 rules).
                               Not a number — a multiplier on whatever the daily number turns
                               out to be, which is why it cannot be a quota and cannot compete
                               with one. It binds at the same time as the daily limit, so it is
                               a CEILING in the same sense annual quotas are.

    A DUTY ON KEEPING          "record your retention on your licence at once" (102 rules).
                               Discharged at the moment you keep a fish, so it belongs on the
                               row whose outcome permits keeping — and nowhere at all on a row
                               that says release.

    A BARE SIZE GATE           "Bull trout, Dolly Varden and Lake trout (none under 60 cm)"
                               (23 rules). A constraint on WHICH fish the allowance may be made
                               of, with no count of its own — which is a sub-limit without a
                               number, and reads on the row it qualifies.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional

from pipeline.regs.table.subject import Subject


@dataclass(frozen=True)
class Qualifier:
    kind: str                 # possession | duty | size_gate
    subject: Subject
    text: str
    verbatim: str
    rule_id: str
    n: Optional[int] = None   # the multiplier, for a possession multiple

    def sentence(self, name=None) -> str:
        if self.kind == "possession":
            # "1 days' worth" is not English, and the plural is load-bearing on a 2.
            d = "a day's" if self.n == 1 else f"{self.n} days'"
            return f"you may have {d} worth in possession"
        if self.kind == "size_gate" and name is not None:
            # A SIZE GATE WITHOUT ITS FISH IS A FLOOR ON EVERYTHING. "none under 60 cm" is about
            # bull trout, Dolly Varden and lake trout; hung on a row headed "Trout and char" with
            # no subject it read as a minimum size for every trout on the water.
            who, _ = self.subject.words(name)
            return f"{who.lower()}: {self.text}"
        return self.text


def qualifier_of(x: dict, subject: Subject, key: str) -> Optional[Qualifier]:
    """The one place a count-less retention rule becomes something the table can carry."""
    if x.get("take") is not None or x.get("unlimited"):
        return None
    lab, verb = (x.get("label") or ""), (x.get("verbatim") or "")
    if x.get("per_daily"):
        return Qualifier("possession", subject, lab, verb, key, int(x["per_daily"]))
    if x.get("record_retention") or x.get("on_retention"):
        return Qualifier("duty", subject, lab, verb, key)
    if x.get("over_cm") or x.get("under_cm") or x.get("band"):
        return Qualifier("size_gate", subject, subject.size.words() or lab, verb, key)
    return None


def attach(rows, quals: List[Qualifier]) -> List[Qualifier]:
    """Hang each qualifier on every row it speaks about.

    On EVERY such row, not the first: "possession is 2 daily quotas" is true of the trout row
    and the char row alike, and a qualifier that lands on one of them is a rule the reader of
    the other never sees. A duty is the exception — it is discharged by keeping, so it has no
    business on a row that says release or closed.
    """
    unattached = []
    for q in quals:
        landed = False
        for r in rows:
            # EITHER DIRECTION. A possession multiple on "all game fish" speaks about the trout
            # row because it covers it; a duty on steelhead speaks about the "Trout and char"
            # row because that row covers IT. Testing only one way lost every qualifier written
            # about a fish narrower than the row it belongs on — which is most of them, since
            # rows are merged upward.
            if not (q.subject.covers(r.subject) or r.subject.covers(q.subject)):
                continue
            # NOTHING TO KEEP, NOTHING TO CARRY. A possession multiple is a multiplier on a
            # daily limit, and there is no daily limit on a row that says release or closed;
            # "you may have 2 days' worth in possession" sat on 123 closed and 331 release rows.
            # A duty discharged by keeping has the same problem, and always skipped them.
            if q.kind in ("duty", "possession") and r.outcome.kind not in ("quota", "unlimited"):
                continue
            bag = "duties" if q.kind == "duty" else "quals"
            cur = list(getattr(r, bag, None) or [])
            if q.rule_id not in {getattr(c, "rule_id", None) for c in cur}:
                cur.append(q)
                setattr(r, bag, cur)
            landed = True
        if not landed:
            unattached.append(q)
    # A DUTY WITH NOTHING TO TRIGGER IT. "Record your retention of hatchery steelhead at once"
    # has no row to sit on where steelhead is release-only — there is nothing to record. That is
    # a real answer and not a loss, but the caller has to be TOLD, or it is indistinguishable
    # from a rule quietly dropped, which is the one thing this table may not do.
    return unattached
