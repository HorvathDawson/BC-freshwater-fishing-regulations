"""T779: THE PIECES A DFO PLACE WITHHOLDS, at save time (the review app's preview and the side check `apply` trusts).

The reach run stops on a piece a place cannot place unless `unplaced_piece_calls.json` `accepted` names it (T632,
`pipeline.atlas.reach.build.unplaced_gate`). The in-memory check a mark is landed on (`reuse.dfo_binding_change`, the
review preview `dfo._bind`) calls the same `build_reach`, which returns that diagnostic (`unclassified`, not
included) — and both threw it away, so two Middle Shuswap places were landed "bound" and the full build then stopped
fourteen minutes into its reach run. `rows` reads the diagnostics; `unaccepted` and `stale` apply the gate's own rule
(`build.unplaced_verdict`, keyed on `build.UNPLACED_KEY`) — `stale` to this place's accepted rows alone, which the
location key makes possible — and `check` is what both save-time callers report, so the check refuses what the gate
would."""
from __future__ import annotations

from typing import Iterable, Mapping, Optional


def rows(location_id: str, diags: Iterable) -> list[dict]:
    """Every piece the place withheld, in the gate's shape: {entry_id (the location), rule_id "", piece, reason, kind
    "dfo"}, from `build_reach`'s diagnostics (objects with `kind` and `payload`)."""
    from pipeline.atlas.reach import dfo_salmon as DS
    return DS.unplaced([{"location_id": location_id, "kind": d.kind, "payload": d.payload} for d in diags])


def unaccepted(pieces: Iterable[Mapping], accepted: Optional[Iterable[Mapping]] = None) -> list[dict]:
    """The withheld pieces no `accepted` row names — what `unplaced_gate` would stop the build on. `accepted` defaults to
    the curated file; a file that cannot be read raises, never reads as 'nothing accepted'."""
    from pipeline.atlas.reach.build import load_unplaced_calls, unplaced_verdict
    return [dict(x) for x in unplaced_verdict(list(pieces), accepted if accepted is not None
                                              else load_unplaced_calls())[0]]


def stale(location_id: str, pieces: Iterable[Mapping], accepted: Optional[Iterable[Mapping]] = None) -> list[dict]:
    """The `accepted` rows OF THIS PLACE (keyed by its location, rule "") that name no piece it withholds now — the gate
    stops on a stale acceptance too (the piece moved, was placed, or its reason changed). The rows are keyed by location,
    so the gate's rule (`unplaced_verdict`) runs on this place's rows alone."""
    from pipeline.atlas.reach.build import UNPLACED_KEY, load_unplaced_calls, unplaced_verdict
    mine = [r for r in (accepted if accepted is not None else load_unplaced_calls())
            if (r.get("entry_id") or "") == location_id and not r.get("rule_id")]
    return [dict(zip(UNPLACED_KEY, k)) for k in unplaced_verdict(list(pieces), mine)[1]]


def check(location_id: str, pieces: Iterable[Mapping], accepted: Optional[Iterable[Mapping]] = None) -> dict:
    """What the save-time check reports for one place's binding: `withheld` (every piece, `accepted` or not),
    `unaccepted` (those the gate stops on) and `stale_acceptances` (its accepted rows that name no piece now). The review
    preview (`dfo._bind`) and the landing check (`reuse.dfo_binding_change`) both call this; `apply_proposals` refuses a
    mark with either list non-empty."""
    from pipeline.atlas.reach.build import load_unplaced_calls
    pieces = list(pieces)
    acc = list(accepted if accepted is not None else load_unplaced_calls())
    bad = {(x["piece"], x["reason"]) for x in unaccepted(pieces, acc)}
    rows = [{"piece": x["piece"], "reason": x["reason"], "accepted": (x["piece"], x["reason"]) not in bad}
            for x in pieces]
    return {"withheld": rows, "unaccepted": [x for x in rows if not x["accepted"]],
            "stale_acceptances": [{"piece": r["piece"], "reason": r["reason"]} for r in stale(location_id, pieces, acc)]}


def words(bad: Iterable[Mapping]) -> str:
    return "; ".join(f"{x['piece']} ({x['reason']})" for x in bad)
