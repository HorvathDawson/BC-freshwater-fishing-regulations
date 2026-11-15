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

TIDAL WATER IS SUBTRACTED FROM EVERY PLACE (T632, owner 2026-11-11: "always subtract, unless there is a stated reason
not to for a place"): the run's tidal sections (`build_reaches`' `tidal`, the same map every provincial row loses) come
out of each place after its walk, by the same `build_reach`. A place keeps its tidal water only when
`data/curated/dfo_salmon/tidal_calls.json` (`CURATED.dfo_salmon.tidal_calls`) states a reason for it. A place the
subtraction EMPTIES stops the run (`drowned`): it lies wholly in tidal water, which is the owner's T449 call
(`tidal_water`), never decided here.

CLOSED AREAS INSIDE A PLACE (T573 entry B, `regs.dfo_salmon.closures`): each LIVE closed area of a place is a row of
its own (`<water_id>#closed:<key>`, `resolve_closures`) — a confirmed stretch bound between its cuts, a confirmed
circle on the sections wholly inside it; anything else `carried: true` with its `reason` (and a circle's `marked`
sections), which the bundle puts on the water as an undrawn part. It wins over its place on its sections by the
salmon chain's own order (same tier, fewer sections: `deliver.salmon.chain_order`).
"""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

FILE = "dfo_salmon.jsonl.gz"
#: T546 item 4: what `build_reach` said of each placed location (ambiguous_cut picks, unclassified pieces, the
#: walk, …) — it used to be discarded. Beside FILE, outside the rules' digest; the binding diff compares it.
DIAG_FILE = "dfo_salmon_diagnostic.jsonl"


def _slugs() -> List[str]:
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    return [s for s in ALL_SLUGS if not PAGES[s].is_stub]


def load_tidal_calls(path: Optional[Path] = None) -> Dict[str, str]:
    """T632: THE PLACES THAT KEEP THEIR TIDAL WATER — `{location_id: reason}` from `tidal_calls.json` `keep_tidal`.
    A missing file raises (a curated loader never returns empty, AGENTS 37); a row without a stated reason is refused:
    the owner's rule is "always subtract, unless there is a stated reason not to"."""
    from pipeline.common.curated import CURATED
    p = Path(path) if path is not None else Path(CURATED.dfo_salmon.tidal_calls)
    data = json.loads(p.read_text(encoding="utf-8"))
    keep = data.get("keep_tidal")
    if not isinstance(keep, dict):
        raise ValueError(f"{p}: `keep_tidal` must be an object {{location_id: {{reason, by, on}}}}")
    out: Dict[str, str] = {}
    for lid, row in sorted(keep.items()):
        reason = (row or {}).get("reason") if isinstance(row, dict) else None
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(f"{p}: {lid} keeps its tidal water with no stated `reason` — state one or remove the row")
        out[lid] = reason.strip()
    return out


def resolve_all(registry, graph, *, entries_dir: Optional[Path] = None, outside=None,
                diagnostics: Optional[list] = None, tidal=None,
                keep_tidal: Optional[Dict[str, str]] = None,
                closed_areas: Optional[List[dict]] = None) -> List[dict]:
    """One row per curated location, sorted by location_id: `{location_id, region, mapped, outcome, reason, sections}`
    — and (T573) one per live CLOSED AREA of a place (`resolve_closures`; `closed_areas`: the confirmed placements,
    default the curated file `review.closed_areas.confirmed()`).
    `diagnostics`: a list to fill with `{location_id, region, kind, payload}` per diagnostic `build_reach` emitted
    for a placed location (T546 item 4; `write_diagnostics`).

    `tidal` (T632): the run's tidal sections (`ReachResult.tidal`, {section: owner}), taken out of every place but
    those `keep_tidal` ({location_id: reason}, `load_tidal_calls`) names. `None` takes nothing out (a caller that did
    not compute it). A `keep_tidal` id that is no place, and a place left with no water by the subtraction, stop it."""
    from pipeline.atlas.reach.build import build_reach
    from pipeline.atlas.reach.models import Outcome, Reason
    from pipeline.atlas.reach.outside import outside_bc
    from pipeline.regs.dfo_salmon import entries as E

    if outside is None:
        outside = outside_bc(registry, graph)
    keep_tidal = dict(keep_tidal or {})
    confirmed = load_closed_areas() if closed_areas is None else list(closed_areas)
    rows: List[dict] = []
    gone: List[str] = []
    dead: List[str] = []
    drowned: List[str] = []
    seen: set = set()
    checked: set = set()                 # the places whose pieces were checked: a variant riding its target's is not again
    for slug in _slugs():
        ef = E.load(slug, entries_dir or E.ENTRIES_DIR, registry=registry)
        gone += stale_bindings(ef, registry)
        by_id = {x.location_id: x for x in ef.locations}
        seen.update(by_id)
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
                # T632: tidal water out, unless the place (or the place it is a variant of) has a stated reason
                lim = None if (loc.location_id in keep_tidal or target.location_id in keep_tidal) else tidal
                binding, diags = build_reach(entry, rules[0], registry, graph, outside=outside, regional=False,
                                             tidal=lim)
                if diagnostics is not None:
                    diagnostics.extend({"location_id": loc.location_id, "region": slug, "kind": d.kind,
                                        "payload": d.payload} for d in diags)
                if target.location_id not in checked:
                    checked.add(target.location_id)
                    dead += [_piece_words(slug, target.location_id, rules[0]["extents"], i)
                             for i in dead_pieces(entry, rules[0], registry, graph, outside=outside, tidal=lim)]
                row["outcome"] = binding.outcome.value
                if binding.outcome is Outcome.bound:
                    row["mapped"] = True
                    row["sections"] = sorted(binding.sections)
                else:
                    row["reason"] = binding.reason.value if binding.reason else None
                    if binding.reason is Reason.tidal:
                        drowned.append(f"r{slug} {loc.location_id}: {binding.detail}")
            rows.append(row)
        # T573: the region's live CLOSED AREAS (entry B), each its own location — bound, or carried on its water
        rows += resolve_closures(slug, ef, registry, graph, outside=outside, tidal=tidal, keep_tidal=keep_tidal,
                                 diagnostics=diagnostics, confirmed=confirmed)
    stale = sorted(set(keep_tidal) - seen)
    if stale:
        raise ValueError(f"dfo_salmon: {len(stale)} `keep_tidal` row(s) in tidal_calls.json name no DFO place — "
                         "remove or re-point them:\n  " + "\n  ".join(stale))
    if drowned:
        raise ValueError(f"dfo_salmon: {len(drowned)} DFO place(s) lie wholly in tidal water once it is subtracted "
                         "(T632) — the owner's call: mark the place tidal water in the review app's DFO mode (T449), "
                         "or state a reason to keep its tidal water in tidal_calls.json:\n  " + "\n  ".join(drowned))
    if gone:
        raise ValueError(f"dfo_salmon: {len(gone)} DFO water binding(s) name an item this registry does not have — "
                         "re-bind them (the review app's DFO mode) before building:\n  " + "\n  ".join(gone))
    if dead:
        raise ValueError(f"dfo_salmon: {len(dead)} piece(s) of a DFO place bind nothing on their own (T524: the place "
                         "binds the UNION of its pieces, and a dead piece would drop silently) — fix the piece in the "
                         "review app's DFO mode before building:\n  " + "\n  ".join(dead))
    rows.sort(key=lambda r: r["location_id"])
    return rows


def load_closed_areas() -> List[dict]:
    """T573: the closed areas the owner confirmed (`data/curated/dfo_salmon/closed_areas.json`, `PieceGeometry` fields
    only); [] before his first confirm (the file is born with it)."""
    from pipeline.regs.review import closed_areas as CA
    return CA.confirmed()


def resolve_closures(slug: str, ef, registry, graph, *, outside=None, tidal=None,
                     keep_tidal: Optional[Dict[str, str]] = None, diagnostics: Optional[list] = None,
                     confirmed: Iterable[dict] = ()) -> List[dict]:
    """T573: ONE ROW PER LIVE CLOSED AREA of the region's places (`closures.build_pieces`), in the places' shape plus
    `closure: {of, key, kind, state}`:

        bound    a confirmed stretch, `between` its two cuts by the same `build_reach` (no tributaries; the place's
                 tidal limit); a confirmed circle, the sections of its water WHOLLY inside it
        carried  `carried: true` + `reason` (why), and `marked` (the sections a circle only touches): the bundle puts
                 the piece on its water as an undrawn part ("parts not mapped") — never widened to a whole section,
                 never dropped. Unconfirmed, reworded, a lake line (deferred), a cut not in this atlas, a circle
                 partly over a section: all carried.

    An item no live row names makes no row (`closures.attach`)."""
    from pipeline.atlas.reach.build import build_reach
    from pipeline.atlas.reach.models import Outcome, Reason
    from pipeline.regs.dfo_salmon import closures as K
    from pipeline.regs.dfo_salmon import entries as E

    keep_tidal = keep_tidal or {}
    rows: List[dict] = []
    for p in K.build_pieces(getattr(ef, "locations", ()), confirmed):
        lim = None if (p.derived_from in keep_tidal or p.location_id in keep_tidal) else tidal
        row = {"location_id": p.location_id, "region": slug, "mapped": False, "outcome": "closure_carried",
               "reason": None, "sections": [],
               "closure": {"of": p.derived_from, "key": p.piece_key, "kind": p.piece_kind, "state": p.state}}
        why, marked = p.why, []
        if not why and p.piece_kind == "stretch":
            water = ef.water(p.water_id) if p.water_id else None
            entry, rules = E.to_reach_input(p, [{}], water, siblings={})
            b, diags = build_reach(entry, rules[0], registry, graph, outside=outside, regional=False, tidal=lim)
            if diagnostics is not None:
                diagnostics.extend({"location_id": p.location_id, "region": slug, "kind": d.kind,
                                    "payload": d.payload} for d in diags)
            if b.outcome is Outcome.bound and b.sections:
                row.update(mapped=True, outcome="bound", sections=sorted(b.sections))
            else:
                new = [x["split_id"] for x in (p.geometry or {}).get("points") or () if x.get("kind") == "new"]
                why = (f"its stretch did not bind ({b.reason.value if b.reason else b.outcome.value})"
                       + (f"; its new cut(s) {new} must be in the cuts file and the atlas rebuilt" if new else "")
                       + ("; it lies wholly in tidal water" if b.reason is Reason.tidal else "")
                       + ": carried on its water")
        elif not why and p.piece_kind == "circle":
            inside, marked, why = _circle(p.geometry, registry, graph, lim)
            if inside:
                row.update(mapped=True, outcome="bound", sections=inside)
        if why:
            row.update(carried=True, reason=why)
            if marked:
                row["marked"] = marked
        rows.append(row)
    return rows


def _circle(g: dict, registry, graph, lim) -> tuple:
    """(inside, touched, why) of a confirmed circle on its water (`closures.circle_sections`, the atlas's own section
    geometry, `reach.position`). No geometry in the run, or a water this registry lacks: nothing bound, all carried."""
    from pipeline.atlas.reach import position
    from pipeline.regs.dfo_salmon import closures as K
    item = registry.get(g["on_item"]) if hasattr(registry, "get") else None
    if item is None:
        return [], [], f"its water {g['on_item']} is not in this registry: carried on its water"
    geoms = position._geoms(graph)
    if geoms is None:
        return [], [], "no section geometry in this run (no atlas build attached): carried on its water"
    outlines, lines = geoms
    shapes = {}
    for s in item.section_ids:
        shp = outlines.get(s) if s.startswith("lake:") else None
        shapes[s] = shp if shp is not None else lines.get(s)
    inside, touched = K.circle_sections(g["centre"], g["radius_m"], shapes)
    inside = [s for s in inside if not lim or s not in lim]
    if not inside and not touched:
        return [], [], f"the circle touches no section of {g['on_item']}: carried on its water"
    if touched:
        return inside, touched, (f"{len(touched)} section(s) only partly inside the circle: marked, carried on its "
                                 "water (a section is never bound whole for a circle)")
    return inside, [], ""


def _piece_words(slug: str, location_id: str, extents: List[dict], i: int) -> str:
    """One dead piece, named for the refusal: the place, which piece, its op, cuts and waters."""
    ex = extents[i]
    on = ex.get("item_ids") or ex.get("item_id") or "the water"
    return f"r{slug} {location_id}: piece {i + 1} of {len(extents)} ({ex.get('op')} {ex.get('splits') or ''} on {on})"


def dead_pieces(entry: dict, rule: dict, registry, graph, *, outside=None, tidal=None) -> List[int]:
    """T524: THE PIECES OF A PLACE THAT BIND NOTHING — the index of every extent of a SEVERAL-piece place that, resolved
    alone by the same `build_reach`, is not bound to a section of its own. The place binds the union of its pieces
    (`classify` unions extents), so a piece naming a cut on the wrong water, or a water the place does not hold, would
    vanish inside the union with nothing looking wrong; the build refuses it instead (`resolve_all`). Each piece is
    resolved WITHOUT the tributary walk: what is checked is that the piece lands on its own water — the walk is the
    union's, and it is the slow part. One piece is the place itself (its outcome already says it): not checked here.
    `tidal` (T632): the place's tidal limit — a piece wholly in tidal water binds nothing once it is subtracted, and
    would vanish from the union the same way."""
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
                           regional=False, tidal=tidal)
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


def unplaced(diags: Iterable[dict]) -> List[dict]:
    """T632: EVERY PIECE A DFO PLACE WITHHELD — one row per (place, piece) of an `unclassified` diagnostic that was not
    included, in `build.unplaced_pieces`' shape (`entry_id` is the location id, `rule_id` "", `kind` "dfo"), for the
    run's unplaced gate (`build.unplaced_gate`)."""
    out = []
    for d in diags:
        pay = d.get("payload") or {}
        if d.get("kind") != "unclassified" or pay.get("included"):
            continue
        reasons = pay.get("reasons") or {}
        for p in pay.get("pieces") or ():
            out.append({"entry_id": d["location_id"], "rule_id": "", "piece": p,
                        "reason": reasons.get(p, "unknown"), "kind": "dfo"})
    return sorted({json.dumps(x, sort_keys=True): x for x in out}.values(),
                  key=lambda x: (x["entry_id"], x["piece"]))


def entries_digest(entries_dir: Optional[Path] = None) -> str:
    """The curated DFO entry files by content — the reach stage's key must move when a curator places a location —
    and (T632) the places that keep their tidal water (`tidal_calls.json`)."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.dfo_salmon import entries as E
    root = Path(entries_dir or E.ENTRIES_DIR)
    h = hashlib.sha256()
    for p in sorted(root.glob("region-*.json")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    h.update(b"tidal_calls")
    h.update(Path(CURATED.dfo_salmon.tidal_calls).read_bytes())
    # T573: the confirmed closed areas (a piece confirmed or moved must move the key; absent before his first confirm)
    from pipeline.regs.review import closed_areas as CA
    h.update(b"closed_areas")
    h.update(CA.path().read_bytes() if CA.path().exists() else b"-")
    return h.hexdigest()[:16]
