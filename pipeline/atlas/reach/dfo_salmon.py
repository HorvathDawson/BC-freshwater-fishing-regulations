"""THE DFO SALMON LOCATIONS, PLACED (T211): every curated DFO location with an authored extent, resolved to sections by
the SAME `build_reach` the provincial rules go through (`entries.to_reach_input` shapes it; nothing here re-implements a
cut, a walk or a clip).

    data/curated/dfo_salmon/places/region-*.json   the curated geography (read only)
      └─ to_reach_input → build_reach ─▶ <reach run>/dfo_salmon.jsonl.gz   one line per location

Written beside the reach run's tables and OUTSIDE the rules' digest (like `tidal`, `steelhead_presence`): a DFO location
is not a provincial rule, and adding the salmon to a run must not move the rules' digest. The bundle reads it
(`pipeline.deliver.salmon`) and never resolves again.

A LOCATION WITH NO EXTENT IS WRITTEN TOO, with `sections: []` and `mapped: false` — the app says "location not yet mapped"
for it on the card, and never draws it — unless the owner accepted it as WHOLLY IN TIDAL WATER (T449, `tidal_water`):
then `outcome: tidal_water`, and it is not missing anywhere. A VARIANT (`duplicate_of`, the same reach reworded) takes its target's extent and
water when it has none of its own: it is the same place.

A PLACE OF SEVERAL PIECES (T524: "the Chilliwack below the Slesse line, including the Sumas below Barrow Town Pump
Station") is its `extents` list — each piece one op on its own water(s) — and binds their UNION; a piece that binds
nothing on its own STOPS the run (`dead_pieces`), never drops out of the union unseen.

`regional=False`: a DFO row's extent is its own words (a region default says `within area:region:6`, a closure
`within_area`), and a water's own row applies along the whole water (user ruling 2026-09-25) — the provincial region
limit is a provincial rule's, not DFO's.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

FILE = "dfo_salmon.jsonl.gz"
#: T546 item 4: what `build_reach` said of each placed location (ambiguous_cut picks, unclassified pieces, the
#: walk, …) — it used to be discarded. Beside FILE, outside the rules' digest; the binding diff compares it.
DIAG_FILE = "dfo_salmon_diagnostic.jsonl"


def _slugs() -> List[str]:
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    return [s for s in ALL_SLUGS if not PAGES[s].is_stub]


def resolve_all(registry, graph, *, entries_dir: Optional[Path] = None, outside=None,
                diagnostics: Optional[list] = None) -> List[dict]:
    """One row per curated location, sorted by location_id: `{location_id, region, mapped, outcome, reason, sections}`.
    `diagnostics`: a list to fill with `{location_id, region, kind, payload}` per diagnostic `build_reach` emitted
    for a placed location (T546 item 4; `write_diagnostics`)."""
    from pipeline.atlas.reach.build import build_reach
    from pipeline.atlas.reach.models import Outcome
    from pipeline.atlas.reach.outside import outside_bc
    from pipeline.regs.dfo_salmon import entries as E

    if outside is None:
        outside = outside_bc(registry, graph)
    rows: List[dict] = []
    gone: List[str] = []
    dead: List[str] = []
    checked: set = set()                 # the places whose pieces were checked: a variant riding its target's is not again
    for slug in _slugs():
        ef = E.load(slug, entries_dir or E.ENTRIES_DIR, registry=registry)
        gone += stale_bindings(ef, registry)
        by_id = {x.location_id: x for x in ef.locations}
        for loc in ef.locations:
            target = loc
            if not loc.binding.resolved and loc.is_variant and loc.duplicate_of in by_id:
                target = by_id[loc.duplicate_of]
            water = ef.water(target.water_id) if target.water_id else None
            # T449: a place the owner accepted as wholly in tidal water binds nothing and is not missing
            row = {"location_id": loc.location_id, "region": slug, "mapped": False,
                   "outcome": "tidal_water" if loc.tidal_water else "no_extent", "reason": None, "sections": []}
            if target.binding.resolved:
                entry, rules = E.to_reach_input(target, [{}], water, siblings=by_id)
                entry = {**entry, "entry_id": loc.location_id}
                binding, diags = build_reach(entry, rules[0], registry, graph, outside=outside, regional=False)
                if diagnostics is not None:
                    diagnostics.extend({"location_id": loc.location_id, "region": slug, "kind": d.kind,
                                        "payload": d.payload} for d in diags)
                if target.location_id not in checked:
                    checked.add(target.location_id)
                    dead += [_piece_words(slug, target.location_id, rules[0]["extents"], i)
                             for i in dead_pieces(entry, rules[0], registry, graph, outside=outside)]
                row["outcome"] = binding.outcome.value
                if binding.outcome is Outcome.bound:
                    row["mapped"] = True
                    row["sections"] = sorted(binding.sections)
                else:
                    row["reason"] = binding.reason.value if binding.reason else None
            rows.append(row)
    if gone:
        raise ValueError(f"dfo_salmon: {len(gone)} DFO water binding(s) name an item this registry does not have — "
                         "re-bind them (the review app's DFO mode) before building:\n  " + "\n  ".join(gone))
    if dead:
        raise ValueError(f"dfo_salmon: {len(dead)} piece(s) of a DFO place bind nothing on their own (T524: the place "
                         "binds the UNION of its pieces, and a dead piece would drop silently) — fix the piece in the "
                         "review app's DFO mode before building:\n  " + "\n  ".join(dead))
    rows.sort(key=lambda r: r["location_id"])
    return rows


def _piece_words(slug: str, location_id: str, extents: List[dict], i: int) -> str:
    """One dead piece, named for the refusal: the place, which piece, its op, cuts and waters."""
    ex = extents[i]
    on = ex.get("item_ids") or ex.get("item_id") or "the water"
    return f"r{slug} {location_id}: piece {i + 1} of {len(extents)} ({ex.get('op')} {ex.get('splits') or ''} on {on})"


def dead_pieces(entry: dict, rule: dict, registry, graph, *, outside=None) -> List[int]:
    """T524: THE PIECES OF A PLACE THAT BIND NOTHING — the index of every extent of a SEVERAL-piece place that, resolved
    alone by the same `build_reach`, is not bound to a section of its own. The place binds the union of its pieces
    (`classify` unions extents), so a piece naming a cut on the wrong water, or a water the place does not hold, would
    vanish inside the union with nothing looking wrong; the build refuses it instead (`resolve_all`). Each piece is
    resolved WITHOUT the tributary walk: what is checked is that the piece lands on its own water — the walk is the
    union's, and it is the slow part. One piece is the place itself (its outcome already says it): not checked here."""
    from pipeline.atlas.reach.build import build_reach
    from pipeline.atlas.reach.models import Outcome

    extents = rule.get("extents") or []
    if len(extents) < 2:
        return []
    out = []
    for i, ex in enumerate(extents):
        one = {**rule, "rule_id": f"{rule['rule_id']}~piece{i + 1}", "extents": [ex],
               "includes_tributaries": False, "tributaries_only": False}
        b, _ = build_reach({**entry, "includes_tributaries": False}, one, registry, graph, outside=outside,
                           regional=False)
        if b.outcome is not Outcome.bound or not b.sections:
            out.append(i)
    return out


def stale_bindings(ef, registry) -> List[str]:
    """T337 (owner 2026-11-05: the DFO matches are done and verified): a water's `item_ids` are the owner's binding, kept
    as written; this build only CHECKS each still names an item of its registry (an absorbed item already reads as its
    absorber, `entries.load(registry=)`). One line per id that does not, naming the water, so the build stops on it
    instead of placing nothing there. Never re-matched by name here."""
    return [f"r{w.region} {w.name} ({w.water_id}): {i}"
            for w in ef.waters for i in w.item_ids if i not in registry]


def write(out_dir: Path, rows: Iterable[dict]) -> str:
    """Write `dfo_salmon.jsonl.gz` (gzip mtime 0: byte-identical reruns) and return its digest."""
    body = "".join(json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n" for r in rows)
    raw = body.encode("utf-8")
    p = Path(out_dir) / FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(gzip.compress(raw, compresslevel=6, mtime=0))
    return hashlib.sha256(raw).hexdigest()[:16]


def write_diagnostics(out_dir: Path, diags: Iterable[dict]) -> int:
    """Write `dfo_salmon_diagnostic.jsonl`, sorted (location, kind, payload): byte-identical reruns. Returns the count."""
    rows = sorted((json.dumps(d, sort_keys=True, ensure_ascii=False, separators=(",", ":")) for d in diags),
                  key=lambda line: (json.loads(line)["location_id"], json.loads(line)["kind"], line))
    p = Path(out_dir) / DIAG_FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("".join(r + "\n" for r in rows), encoding="utf-8")
    return len(rows)


def read(run_dir: Path) -> Iterator[dict]:
    """The run's DFO placements. RAISES when the run predates them — a run with no file is not a run with no salmon."""
    p = Path(run_dir) / FILE
    if not p.exists():
        raise SystemExit(f"{p} is missing — the reach run predates the DFO salmon placements (T211). Re-run the "
                         f"reach builder:\n    python -m pipeline.atlas.reach.cli --build <atlas> --out {run_dir}")
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def entries_digest(entries_dir: Optional[Path] = None) -> str:
    """The curated DFO entry files by content — the reach stage's key must move when a curator places a location."""
    from pipeline.regs.dfo_salmon import entries as E
    root = Path(entries_dir or E.ENTRIES_DIR)
    h = hashlib.sha256()
    for p in sorted(root.glob("region-*.json")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:16]
