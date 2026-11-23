"""T779: THE PIECES A DFO PLACE WITHHOLDS, at save time (the review app's preview and the side check `apply` trusts).

The reach run stops on a piece a place cannot place unless `unplaced_piece_calls.json` `accepted` names it (T632,
`pipeline.atlas.reach.build.unplaced_gate`). The in-memory check a mark is landed on (`reuse.dfo_binding_change`, the
review preview `dfo._bind`) calls the same `build_reach`, which returns that diagnostic (`unclassified`, not
included) — and both threw it away, so two Middle Shuswap places were landed "bound" and the full build then stopped
fourteen minutes into its reach run. `rows` reads the diagnostics, `unaccepted` applies the gate's own rule (the key is
`build.UNPLACED_KEY`), and the check refuses what the gate would."""
from __future__ import annotations

from typing import Iterable, Mapping, Optional


def rows(location_id: str, diags: Iterable) -> list[dict]:
    """Every piece the place withheld, in the gate's shape: {entry_id (the location), rule_id "", piece, reason, kind
    "dfo"}, from `build_reach`'s diagnostics (objects with `kind` and `payload`)."""
    from pipeline.atlas.reach import dfo_salmon as DS
    return DS.unplaced([{"location_id": location_id, "kind": d.kind, "payload": d.payload} for d in diags])


def unaccepted(pieces: Iterable[Mapping], accepted: Optional[Iterable[Mapping]] = None) -> list[dict]:
    """The withheld pieces no `accepted` row names — what `unplaced_gate` would stop the build on (it also stops on a
    stale acceptance, which is a property of a whole run and not of one place). `accepted` defaults to the curated file;
    a file that cannot be read raises, never reads as 'nothing accepted'."""
    from pipeline.atlas.reach.build import UNPLACED_KEY, load_unplaced_calls
    ok = {tuple(r.get(k) or "" for k in UNPLACED_KEY) for r in (accepted if accepted is not None
                                                                else load_unplaced_calls())}
    return [dict(x) for x in pieces if tuple(x.get(k) or "" for k in UNPLACED_KEY) not in ok]


def words(bad: Iterable[Mapping]) -> str:
    return "; ".join(f"{x['piece']} ({x['reason']})" for x in bad)
