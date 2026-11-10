"""THE DFO REVIEW MODE's reads (T164): every scraped DFO salmon extent, on the map, with its splits.

    queue(...)            one row per DFO location (water, the notice's words, rules, extent, mark)
    progress()            marked / to do overall, per region, and how many still need a cut
    detail(id)            one location: the notice's words verbatim, its scraped rows, its curated
                          extent, the cut-points on its water (labelled map points), anchor hints,
                          the mark in force
    reach(id)             what its extent binds NOW (the builder's own `build_reach`), as geometry
    check_anchor(id, n)   a proposed NEW split (`marks.NewSplit`) resolved on the map — validated
                          server-side, cheap: a snap to the water, a tributary's mouth, a lake's edge
    extent_preview(id, f) a proposed extent (`marks.ExtentFix`) resolved, read only
    mark_for(body, ...)   the `Mark` a DFO mark payload makes (anchors validated, resolved points
                          stamped) — `reuse.post_mark` commits it, the ONE write

READ ONLY. The curated geography (`data/curated/dfo_salmon/places/`) and splits.json
are never written here: a new split is a PROPOSAL in the mark, and T185 turns marks into reviewed
changes (AGENTS 66/67). The rules are the re-scrape's (`<generated>/regs/dfo_salmon/typed/`).
"""
from __future__ import annotations

import json
import os
import re
from collections import Counter
from functools import lru_cache
from pathlib import Path

from pipeline.common.curated import GENERATED

import preverify
import reuse
from pipeline.atlas.reach import dfo_salmon as DS
from pipeline.regs.review import dfo as D

#: the typed feed the app reads (generated); `CURATION_DFO_TYPED` points it at a copy (tests). The DFO entry files are
#: curated: `reuse._dfo_dir()` (`CURATED.dfo_salmon.places`, moved only with the whole tree by `CURATED_ROOT`, T422)
DFO_TYPED = Path(os.environ["CURATION_DFO_TYPED"]) if os.environ.get("CURATION_DFO_TYPED") else None

#: THE SUGGESTED EXTENTS (`pipeline.regs.dfo_salmon.suggest`, a frozen, reviewed file): shown on each
#: location, never applied — "accept suggestion" only pre-fills the editor. `CURATION_DFO_SUGGESTIONS`
#: points it at a copy (tests).
SUGGESTIONS_PATH = (Path(os.environ["CURATION_DFO_SUGGESTIONS"]) if os.environ.get("CURATION_DFO_SUGGESTIONS")
                    else GENERATED.review / "dfo-suggestions.json")

#: how far (m) a map point may sit from the water it cuts — further is a click on another water
SNAP_MAX_M = 500.0
#: how far (m) a tributary's mouth may sit from the water — further, it is not its tributary
MOUTH_MAX_M = 300.0
#: how close (m) a section end must be to a lake's outline to be its inlet / outlet
LAKE_TOUCH_M = 40.0
#: the queue filters
#: T449: `tidal` — a place in tidal water the owner accepted (binds nothing, not missing)
NEEDS = ("needs_cut", "bound", "unbound", "no_water", "tidal")
VERIFIES = ("unmarked", "verified", "wrong", "unsure", "deferred", "stale", "todo")

TRIB_RE = re.compile(r"\b([A-Z][A-Za-z'\-]*(?:\s+[A-Z][A-Za-z'\-]*)*\s+(?:River|Creek|Slough))\b")
LAKE_RE = re.compile(r"\b([A-Z][A-Za-z'\-]*(?:\s+[A-Z][A-Za-z'\-]*)*\s+Lake)\b")


def _locations():
    return D.locations(reuse._dfo_dir())


def _states() -> dict:
    """location_id -> the DFO mark in force on it."""
    from pipeline.regs.review import marks as M
    return {m.target: m for m in M.latest(M.load(reuse.MARKS_PATH)).values() if m.scope == "dfo"}


def _proposals() -> list[dict]:
    """T210: THE PROPOSED SPLITS of EVERY mark (superseded ones too), deduped by anchor
    (`marks.proposals`): one per distinct cut, the first proposal's name kept, with the marks that
    proposed it. Read only — the store is never edited."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import marks as M
    ov = CURATED.review.proposal_overrides
    # once per marks / overrides file (owner 2026-10-28: ~0.7 s, read by every DFO progress badge)
    return _PROPOSALS_MEMO.get((reuse._stamp((reuse.MARKS_PATH, ov)), M.proposals, M.load),
                               lambda: M.proposals(M.load(reuse.MARKS_PATH), M.load_overrides(ov)))


_PROPOSALS_MEMO = reuse._Memo()
_WITHDRAWN_MEMO = reuse._Memo()


def withdrawn_on(items: list[str]) -> list[dict]:
    """T589: the cut proposals on this river the owner TOOK BACK (`marks.withdrawn_groups`): {id, name, why, by, date}
    — listed where proposals are listed, never offered by a picker"""
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import marks as M
    ov = CURATED.review.proposal_overrides
    rows = _WITHDRAWN_MEMO.get((reuse._stamp((reuse.MARKS_PATH, ov)), M.withdrawn_groups, M.load),
                               lambda: M.withdrawn_groups(M.proposal_groups(M.load(reuse.MARKS_PATH)),
                                                          M.load_overrides(ov)))
    blks = {str(n.blk) for _nid, n, _g in _line_sections(items)} if items else set()
    return [{"id": g["id"], "name": g["split"].name or g["id"], "why": g["withdrawn"]["note"],
             "by": g["withdrawn"]["by"], "date": g["withdrawn"]["date"]} for g in rows if _on_river(g, items, blks)]


def _proposal_view(p: dict) -> dict:
    n = p["split"]
    # EVERY field of the split (T355): a hand-picked list dropped an area_boundary's area, and its save
    # was refused (Little Qualicum, 59e75d36b) — a field added to `NewSplit` now travels by itself
    fields = {k: v for k, v in n.model_dump(mode="json").items() if k not in ("ref", "note", "confidence")}
    # T355: the id shown is the REAL one (the splits.json id it lands as), never the mark's local ref
    return {**fields, "id": real_ids().get(p["id"], p["id"]), "marks": p["marks"], "n_marks": len(p["marks"]),
            # owner 2026-10-27: a proposed split carries its confidence everywhere
            "confidence": p.get("confidence") or n.confidence_now}


def _on_river(p: dict, items: list[str], blks: set[str]) -> bool:
    n = p["split"]
    return bool(n.on_item in items or (n.blk and str(n.blk) in blks))


def proposed_on(items: list[str]) -> list[dict]:
    """The proposed splits (deduped) that cut this river — its item(s), or a blk of their line
    sections — whichever location or entry on it is open."""
    blks = {str(n.blk) for _nid, n, _g in _line_sections(items)} if items else set()
    return [_proposal_view(p) for p in _proposals() if _on_river(p, items, blks)]


@lru_cache(maxsize=2)
def _suggestions_at(path: str, _mtime: float) -> dict:
    try:
        return json.loads(Path(path).read_text()).get("suggestions") or {}
    except (OSError, ValueError):
        return {}


def suggestion(location_id: str) -> dict | None:
    """The SUGGESTED EXTENT for this location (`suggest.suggest_one`'s row: op, points, new splits,
    confidence, reason), or None; a row the split hunt searched and could not place
    (`hunt.status == not_found`) is returned too, with no suggestion, so the screen says why.
    Read only; the mark stays the only write."""
    p = SUGGESTIONS_PATH
    try:
        mt = p.stat().st_mtime
    except OSError:
        return None
    row = _suggestions_at(str(p), mt).get(location_id)
    return row if row and (row.get("suggestion") or row.get("hunt")) else None


#: FIX PROPOSALS (owner 2026-11-02): Claude's proposal per unsure item, from the mark's note — shown on
#: the item, never applied. Machine-written, so under `GENERATED.review` (T324: was beside the marks in curated);
#: the owner's save is the decision. `CURATION_DFO_FIXES` points it at a copy (tests).
def _fixes_path() -> Path:
    if os.environ.get("CURATION_DFO_FIXES"):
        return Path(os.environ["CURATION_DFO_FIXES"])
    from pipeline.common.curated import GENERATED
    return Path(GENERATED.review) / "dfo_fix_proposals.json"


def fix_proposal(location_id: str) -> dict | None:
    """The FIX PROPOSED on this item (`dfo_fix_proposals.json`), or None. Read only."""
    p = _fixes_path()
    try:
        mt = p.stat().st_mtime
    except OSError:
        return None
    return (_fixes_at(str(p), mt).get(location_id) or None)


@lru_cache(maxsize=2)
def _fixes_at(path: str, _mtime: float) -> dict:
    try:
        return json.loads(Path(path).read_text()).get("proposals") or {}
    except (OSError, ValueError):
        return {}


def _suggested(location_id: str) -> str | None:
    """The queue's "suggested · <confidence>" tag: only when there IS a suggested extent."""
    row = suggestion(location_id)
    return row.get("confidence") if row and row.get("suggestion") else None


def _now(loc, water):
    from pipeline.regs.review.marks import Prints
    return Prints.model_validate(D.prints(loc, water, DFO_TYPED))


def _verify(m, loc, water) -> dict:
    from pipeline.regs.review import marks as M
    if m is None:
        return {"status": "unmarked", "verdict": None, "reasons": []}
    # owner 2026-10-28: a mark whose own extent / new splits have LANDED holds over the binding
    # move landing made (`reuse.applied_keys`) — applying the owner's work never undoes it
    return M.status(m, _now(loc, water), applied=reuse.applied_keys())


#: words of a SUB-AREA of a lake: a line, a radius, a bay, a compass side, signs, a portion
AREA_WORDS = re.compile(r"\b(radius|line|bay|portion|part of|known as|signs?|buoys?|north|south|east|west|"
                        r"(?:north|south|east|west)(?:erly|ern)|within \d+\s*m|arm|outlet|inlet|mouth of)\b", re.I)


def lake_area(loc: dict, water: dict | None) -> bool:
    """LAKE SPLIT LATER (owner 2026-10-28): the location's water is a lake (every item) and the notice
    names a SUB-AREA of it (a line, a radius, a bay…, not just "including tributaries"). Such an
    extent needs a lake split, and lake splits are deferred."""
    text = (loc.get("source_text") or {}).get("specific_area") or ""
    its = [reuse._registry().get(i) for i in (water or {}).get("item_ids") or []]
    return bool(its) and all(i is not None and i.kind == "lake" for i in its) \
        and bool(AREA_WORDS.search(text))


def _groups() -> dict:
    """location id -> its HISTORIC GROUP (`review.dfo.groups`, owner 2026-10-28)."""
    return D.groups(_locations())


def _live_waters() -> set:
    """The water ids with a location on the live page."""
    return {l.get("water_id") for _s, l, _w in _locations().values()
            if (l.get("status") or "active") == "active" and l.get("water_id")}


def historic_only(loc: dict, groups: dict | None, live_waters: set) -> bool:
    """A HISTORIC-ONLY PLACE (owner 2026-11-02, the North Alouette): a dormant location that no live
    one rewords — in no group, or the head of a group of dormant rewordings — on a water still on the
    live page. Its own place, so the live queue shows it (the "active" filter used to drop it)."""
    if (loc.get("status") or "active") == "active" or loc.get("water_id") not in live_waters:
        return False
    g = (groups or {}).get(loc["location_id"])
    return g is None or g["canonical"] == loc["location_id"]


#: T449: THE TIDAL CONTEXT (tidal-limit rows by blk, cut labels) once per tidal_limits.json / cuts.json stamp
_TIDAL_MEMO = reuse._Memo()


def _tidal_ctx() -> tuple[dict, dict]:
    from pipeline.common.curated import CURATED
    return _TIDAL_MEMO.get((reuse._stamp((CURATED.waters.tidal, CURATED.waters.cuts)), D.tidal_context),
                           D.tidal_context)


def tidal_of(loc: dict, water: dict | None, m) -> dict | None:
    """T449: THE PLACE'S TIDAL-WATER CALL — {status, why, label?}, or None. `accepted`: the curated place says
    `tidal_water` or the DFO mark in force accepts it (call "tidal water", before its write-through lands);
    `rejected`: the mark in force says "not tidal"; `proposed`: `review.dfo.tidal_proposal` proposes it and no mark
    decided it — shown to the owner, never acted on."""
    call = m.call if m is not None else None
    if loc.get("tidal_water") or call == "tidal water":
        return {"status": "accepted", "why": "accepted: the place lies wholly in tidal water"}
    limits, labels = _tidal_ctx()
    got = D.tidal_proposal(loc, water, D.tidal_limits_on(water, reuse._registry(), limits), labels)
    if got is None:
        return None
    return {**got, "status": "rejected" if call == "not tidal" else "proposed"}


def _extent_key(ex: dict) -> str:
    """An extent as compared (owner 2026-11-09): every field it has, the waters it holds on as a set."""
    items = sorted(ex.get("item_ids") or ([ex["item_id"]] if ex.get("item_id") else []))
    rest = {k: v for k, v in ex.items() if k not in ("item_id", "item_ids")}
    return json.dumps({**rest, "items": items}, sort_keys=True)


def same_extents(a: list[dict], b: list[dict]) -> bool:
    """Do two lists of extents bind the same? A place binds the UNION of its pieces, so they are compared as a set
    (T524 review: the order they are listed in is no difference)."""
    return sorted(map(_extent_key, a)) == sorted(map(_extent_key, b))


def tributaries_now(loc: dict, water: dict | None) -> bool | None:
    """THE TRIBUTARY CALL A PLACE BINDS WITH: its own flag, else its water's (the DFO title, "Morice River (including
    tributaries)") — the builder's own inheritance (`entries.to_reach_input`)."""
    own = (loc.get("binding") or {}).get("tributaries")
    return own if own is not None else (water or {}).get("tributaries")


def fix_extents(fixes) -> tuple[list[dict], list[str]]:
    """THE EXTENTS A MARK'S FIX PIECES LAND AS — `proposals.extent_of` with each point resolved as the proposals
    resolve it: a curated split by its id, a NEW split by the real id its proposal lands as (the mark read through
    `rekeyed`), a T191 point (confluence / lake end / map point) by the id its anchor lands as (`real_ids`, the same
    `place_groups`). -> (extents, problems): a point that names no split is a problem, never a guess."""
    from pipeline.regs.review import proposals as P

    def split_of(f, i, p):
        if p.kind in ("split", "new"):
            return p.split_id
        ref = f"#{i}"
        return real_ids(((ref, P._point_split(p, f.items[0] if len(f.items) == 1 else None, ref)),)).get(ref)

    out, problems = [], []
    for f in fixes:
        ex, probs = P.extent_of(f, lambda i, p, _f=f: split_of(_f, i, p), rest_at_build=True)
        out.append(ex)
        problems += probs
    return out, problems


def fix_vs_current(m, loc: dict, water: dict | None) -> dict | None:
    """DOES THE MARK'S FIX REPLACE THE PLACE'S EXTENT? (owner 2026-11-09: "make it more obvious when fix extent
    overrides existing extent"). None without a fix; else `{state, current, fix, tributaries}` — `current` the curated
    extents, `fix` the extents the mark's pieces land as (`fix_extents`, on the mark as shown: `rekeyed`):

      replaces  the fix differs from the curated extent: saving it / applying it REPLACES the extent
      same      it equals it, on a verified mark naming only existing cuts (T210: "the cuts are right")
      applied   it equals it on a wrong / unsure mark: the fix has LANDED — this is now the curated extent
    Pieces are compared as a set (`same_extents`); a tributary call is compared as it BINDS (`tributaries_now`), so a
    fix calling what the place already inherits from its DFO title replaces nothing."""
    from pipeline.regs.review import marks as M
    if m is None or not m.extent_fix:
        return None
    fixes = [M.ExtentFix.model_validate(f) for f in rekeyed(json.loads(m.model_dump_json()))["extent_fix"]
             if f["scope"] == "entry"]
    b = loc.get("binding") or {}
    current = list(b.get("extents") or [])
    mine, problems = fix_extents(fixes)
    call = next((f.tributaries for f in fixes if f.tributaries is not None), None)
    now = tributaries_now(loc, water)
    same = not problems and same_extents(mine, current) and (call is None or call == now)
    state = "replaces" if not same else ("same" if m.verdict == "verified" else "applied")
    return {"state": state, "current": current, "fix": mine, "tributaries": {"current": now, "fix": call}}


def _built_cuts() -> set[str]:
    """every cut the served atlas has built: the resolved cuts and the registry's boundaries (with their aliases)"""
    return set(reuse._splits_meta()) | set(reuse._boundary_index())


def saved_status(m, fvc: dict | None, built: set[str] | None = None) -> dict | None:
    """T525 round 3 (owner 2026-11-09: "a save doesn't visibly update"): WHAT THE PLACE'S LAST SAVE DID, for the one
    status line the screen shows. None without a mark; else `{state, date, verdict}` plus `cuts` for `waits_cut`:

      landed      the extent is what the save asked (no fix, or its fix is the curated extent now)
      waits_cut   its fix differs from the extent now because it names a cut the atlas has not built yet
                  (`cuts`: their ids) — it lands after the next rebuild builds them
      not_landed  its fix differs from the extent now and every cut it names is built: the write-through refused it
                  or has not run (the screen adds the refusal's reason when it has one)
      deferred    a defer: decides nothing
    `fvc` is `fix_vs_current` for the same mark. Read only."""
    if m is None:
        return None
    base = {"date": m.date, "verdict": getattr(m.verdict, "value", m.verdict)}
    if base["verdict"] == "deferred":
        return {**base, "state": "deferred"}
    if not fvc or fvc["state"] != "replaces":
        return {**base, "state": "landed"}
    have = _built_cuts() if built is None else built
    shown = rekeyed(json.loads(m.model_dump_json()))
    cuts = sorted({p["split_id"] for f in shown.get("extent_fix") or [] for p in f.get("points") or []
                   if p.get("kind") == "new" and p.get("split_id") and p["split_id"] not in have})
    return {**base, "state": "waits_cut", "cuts": cuts} if cuts else {**base, "state": "not_landed"}


def extent_now(loc: dict, water: dict | None) -> dict:
    """T525: THE PLACE'S CURATED EXTENT AS THE REVIEW EDITOR OPENS ON IT — `pieces` (`proposals.fixes_of_extents`: a
    change starts FROM the extent the place has, never from an empty editor or an old mark), `cannot_change` (why no
    fix can say it again: a field the editor has no word for — the change is offered nowhere, never made by dropping
    it), and `seen`: what the owner is shown, sent back with the mark (`extent_before`) so a save made on an extent
    that has moved since is refused (`mark_for`)."""
    from pipeline.regs.review import proposals as P
    b = loc.get("binding") or {}
    pieces, why = P.fixes_of_extents(b.get("extents") or [], b.get("tributaries"))
    return {"pieces": [f.model_dump(mode="json") for f in pieces], "cannot_change": why,
            "tributaries": tributaries_now(loc, water), "seen": seen_extent(loc),
            # (T532 r10) what "tributaries as the place has them" MEANS here: does the builder walk them for this place
            "walks_tributaries": walks_now(loc, water)}


def walks_now(loc: dict, water: dict | None) -> bool | None:
    """Does the builder walk tributaries for this place as it stands (`classify.wants_tributaries` on its own reach
    input; a place with no extent is asked as the whole water)? None when the question cannot be asked."""
    try:
        b = loc.get("binding") or {}
        return bool(_walks_tributaries(*_reach_input(loc, water, None if b.get("extents") else [{"op": "whole"}])))
    except Exception:  # noqa: BLE001 — a place the reach input refuses has no answer here; the screen says "unknown"
        return None


def _strings(x) -> set[str]:
    """every string anywhere in a JSON-shaped value"""
    if isinstance(x, str):
        return {x}
    if isinstance(x, dict):
        return set().union(*map(_strings, x.values())) if x else set()
    if isinstance(x, (list, tuple)):
        return set().union(*map(_strings, x)) if x else set()
    return set()


def cut_labels(view) -> dict[str, str]:
    """T525 (owner 2026-11-09: the owner reviews by WORDS): the label of every cut a place's view names anywhere — its
    extent, the mark's fix, a fix proposal's or a suggestion's pieces, a group member's extent — from the curated cuts
    (the resolved label, else the cuts file's, else the registry boundary's), so no cut reads as a bare id. A cut with
    no label is left out: the screen shows it as an error."""
    meta, records, bounds = reuse._splits_meta(), reuse._split_records(), reuse._boundary_index()
    out = {}
    for sid in _strings(view) & (set(meta) | set(records) | set(bounds)):
        b = bounds.get(sid, (None, None))[1]
        label = ((meta.get(sid) or {}).get("label") or records.get(sid, ("", {}))[1].get("label")
                 or (getattr(b, "label", None) if b is not None else None))
        if label:
            out[sid] = label
    return out


def item_names(view) -> dict[str, str]:
    """T525: the name of every registry water a place's view names (a fix piece on another water, the Forks' Bulkley),
    so a piece reads "on Bulkley River", never "on gnis:2195"."""
    reg = reuse._registry()
    return {i: reg[i].name for i in _strings(view) if i in reg and reg[i].name}


def seen_extent(loc: dict) -> dict:
    """The extent a mark is made on: the place's curated extents and its own tributary flag, as the file has them."""
    b = loc.get("binding") or {}
    return {"extents": list(b.get("extents") or []), "tributaries": b.get("tributaries")}


def binds_same(a: dict, a_water: dict | None, b: dict, b_water: dict | None) -> bool:
    """Do two places bind the same extent? Their curated pieces as a set and the tributary call each binds with."""
    ea =list((a.get("binding") or {}).get("extents") or [])
    eb = list((b.get("binding") or {}).get("extents") or [])
    return same_extents(ea, eb) and tributaries_now(a, a_water) == tributaries_now(b, b_water)


def fix_taking(src: dict, src_water: dict | None, member: dict, member_water: dict | None) -> tuple[list, list[str]]:
    """THE EXTENT FIX THAT GIVES AN OLDER WORDING THE SHOWN PLACE'S EXTENT (owner 2026-11-09: "it should save extent to
    all variants of location text"): the shown place's curated pieces (`proposals.fixes_of_extents`) and the tributary
    call it binds with — its own flag, or, when the wording would bind another call, the shown place's effective one.
    -> (fixes, why not): the shown place has no extent, holds a field a fix cannot say, or its call cannot be made."""
    from pipeline.regs.review import proposals as P
    b = src.get("binding") or {}
    exts = list(b.get("extents") or [])
    if not exts:
        return [], ["the shown place has no extent to give it"]
    call = b.get("tributaries")
    want = tributaries_now(src, src_water)
    after = call if call is not None else tributaries_now(member, member_water)
    if after != want:
        if want is None:
            return [], ["its tributary call cannot be made the shown place's (that one inherits none)"]
        call = want
    return P.fixes_of_extents(exts, call)


def wording_check(src: dict, src_water: dict | None, member: dict, member_water: dict | None) -> dict:
    """AN OLDER WORDING BESIDE THE SHOWN PLACE (owner 2026-11-09): does it bind the same extent; the fields it holds
    that no extent fix can say (R4: a save that changes its extent would drop them, so it is refused by name); and,
    when it differs, why it cannot take the shown place's extent. Read only."""
    from pipeline.regs.review import proposals as P
    same = binds_same(src, src_water, member, member_water)
    take = [] if same else fix_taking(src, src_water, member, member_water)[1]
    return {"same_extent": same,
            "cannot_change": P.unexpressed((member.get("binding") or {}).get("extents") or []),
            "cannot_take": take}


def _row(slug: str, loc: dict, water: dict | None, m, groups: dict | None = None,
         live_waters: set | None = None) -> dict:
    st = loc.get("source_text") or {}
    need = D.need(loc)
    rows, rules = D.rule_rows(loc, DFO_TYPED)
    v = _verify(m, loc, water)
    tidal = tidal_of(loc, water, m)
    if tidal is not None and tidal["status"] == "accepted":
        state = "tidal"                  # T449: binds nothing by decision, missing nothing
    elif not water or not water.get("item_ids"):
        state = "no_water"
    elif need["bound"]:
        state = "bound"
    elif need["missing"]:
        state = "needs_cut"
    else:
        state = "unbound"
    return {"id": loc["location_id"], "region": slug, "kind": loc.get("kind"),
            "water": loc.get("water") or "", "water_id": loc.get("water_id"),
            "items": list((water or {}).get("item_ids") or []),
            "section": loc.get("section"), "status": loc.get("status") or "active",
            "text": st.get("specific_area") or "", "op": st.get("op"),
            "anchors": list(st.get("anchor_types") or []),
            "n_rows": len(rows), "n_rules": len(rules), "need": need, "state": state,
            "duplicate_of": loc.get("duplicate_of"),
            "verify": v["status"], "reasons": v["reasons"],
            "suggested": _suggested(loc["location_id"]),
            "pv": preverify.dfo(loc["location_id"]),
            "fix_proposed": fix_proposal(loc["location_id"]) is not None,
            "fix_vs_current": (fvc["state"] if (fvc := fix_vs_current(m, loc, water)) else None),
            "lake_area": lake_area(loc, water),
            "tidal": tidal,
            "new_splits": len(m.new_splits) if m is not None else 0,
            "historic_only": historic_only(loc, groups, live_waters if live_waters is not None
                                           else _live_waters()),
            # owner 2026-10-28: historic rewordings grouped with this place (one decision labels all)
            "group": ({"id": g["id"], "size": len(g["members"]), "canonical": g["canonical"]}
                      if (g := (groups or {}).get(loc["location_id"])) else None)}


#: THE DFO ROWS, COMPUTED ONCE PER INPUT (owner 2026-10-28): the queue, its progress and the tab
#: badge each recomputed every location's prints (~2-5 s). Inputs: the marks, suggestions,
#: pre-verify and proposal-override files' stamps; the DFO region files and typed feed (cached per
#: their stamps), the registry and the landed marks; and the readers (a test may patch one).
_ROWS_MEMO = reuse._Memo()


def _rows_inputs() -> tuple:
    from pipeline.common.curated import CURATED
    return (reuse._stamp((reuse.MARKS_PATH, SUGGESTIONS_PATH, preverify.FILES["dfo"], _fixes_path(),
                          CURATED.review.proposal_overrides, CURATED.waters.tidal, CURATED.waters.cuts)),
            D.regions(reuse._dfo_dir()), D.typed(DFO_TYPED), reuse._registry(), reuse.applied_keys(),
            _locations, _states, _groups, _row)


def _rows() -> list[dict]:
    """Every DFO row in review order — SHARED: callers filter, never mutate."""
    return _ROWS_MEMO.get(_rows_inputs(), _compute_rows)


def _compute_rows() -> list[dict]:
    states, groups, live = _states(), _groups(), _live_waters()
    # T349 (owner 2026-11-02): a SCOPE entry (region / section / tidal-area / page default — no water) is not a water
    # place; its coverage is its scope, resolved in Python, and it is shown in the Cascade tab only
    out = [_row(slug, loc, water, states.get(lid), groups, live)
           for lid, (slug, loc, water) in _locations().items() if loc.get("water_id")]
    # the review order: the live page first, then what is missing a cut, then region and water
    # (T449: a place accepted as tidal water is missing nothing — last, with the bound ones)
    rank = {"needs_cut": 0, "unbound": 1, "no_water": 2, "bound": 3, "tidal": 4}
    out.sort(key=lambda r: (r["status"] != "active", rank[r["state"]], reuse._region_key(r["region"]),
                            r["water"], r["id"]))
    return out


def queue(region: str | None = None, status: str | None = None, need: str | None = None,
          verify: str | None = None, q: str | None = None, lake: str | None = None) -> list[dict]:
    """THE DFO QUEUE: every scraped location, filtered. `status` active | dormant; `need` one of
    `NEEDS`; `verify` one of `VERIFIES` (todo = unmarked or stale); `q` a substring of the water or
    the notice's words. `lake`: "later" = only the lake-area rows (lake split later); "all" = every
    row; default hides the lake-area rows."""
    rows = _rows()
    if lake == "later":
        rows = [r for r in rows if r["lake_area"]]
    elif lake != "all":
        rows = [r for r in rows if not r["lake_area"]]
    if region:
        rows = [r for r in rows if r["region"] == region]
    if status == "active":       # the live page, and the historic-only places on its waters
        rows = [r for r in rows if r["status"] == "active" or r["historic_only"]]
    elif status:
        rows = [r for r in rows if r["status"] == status]
    if need:
        rows = [r for r in rows if r["state"] == need]
    if verify:
        rows = [r for r in rows if (r["verify"] in ("unmarked", "stale") if verify == "todo"
                                    else r["verify"] == verify)]
    if q and q.strip():
        want = q.strip().casefold()
        rows = [r for r in rows if want in r["water"].casefold() or want in r["text"].casefold()
                or want in r["id"].casefold()]
    return rows


def progress() -> dict:
    """Marked / total overall and per region, with the counts by mark, by extent state and live /
    dormant — the DFO queue's progress bar."""
    rows = [r for r in _rows() if not r["lake_area"]]    # lake split later is not counted

    def count(rs):
        done = sum(1 for r in rs if r["verify"] not in ("unmarked", "stale"))
        return {"total": len(rs), "done": done, "by_verify": dict(Counter(r["verify"] for r in rs)),
                "by_state": dict(Counter(r["state"] for r in rs)),
                "new_splits": sum(r["new_splits"] for r in rs)}   # carried by marks (overlaps)
    regions = sorted({r["region"] for r in rows}, key=lambda s: (int(re.sub(r"\D", "", s) or 0), s))
    allc = {**count(rows), "new_splits": len(_proposals())}      # T210: distinct cuts, not copies
    return {"all": allc, "active": count([r for r in rows if r["status"] == "active"]),
            "regions": {s: count([r for r in rows if r["region"] == s]) for s in regions},
            "vintage": vintage()}


def vintage() -> list[dict]:
    """(T366 item 2) ONE VINTAGE: every region whose typed rows are not from the saved DFO page, and every saved page
    not fetched for a week in season — [{region, kind, text}] for the queue's banner (`refresh.vintage`). A check that
    cannot run is said as one row, never as "all current"."""
    from pipeline.regs.dfo_salmon import refresh as R
    try:
        return R.vintage(DFO_TYPED or D.default_typed_dir())
    except Exception as ex:  # noqa: BLE001 — shown (the DFO-mode banner), and logged with its traceback
        import logging
        logging.getLogger("curation").exception("the DFO vintage check failed")
        return [{"region": None, "kind": "error", "text": f"the DFO vintage check failed: {type(ex).__name__}: {ex}"}]


# --------------------------------------------------------------------------- the detail

def _item_ref(iid: str) -> dict:
    it = reuse._registry().get(iid)
    return {"id": iid, "name": it.name if it else iid, "kind": it.kind if it else None,
            "mus": list(it.mus)[:4] if it else [], "n_sections": len(it.section_ids) if it else 0}


def _split_feature(sid: str, *, rules: list[str], to_verify: bool, item_id: str | None) -> dict | None:
    """One labelled split point (the `EntrySplits` feature shape the map draws)."""
    meta, records, bounds = reuse._splits_meta(), reuse._split_records(), reuse._boundary_index()
    water, sp = records.get(sid, ("", {}))
    on_item, b = bounds.get(sid, (item_id, None))
    if not water and on_item in reuse._registry():
        water = reuse._registry()[on_item].name
    label = ((meta.get(sid) or {}).get("label") or sp.get("label")
             or (getattr(b, "label", None) if b is not None else None) or sid)
    geom = None
    if sid in meta:
        f = reuse._geo(reuse._split_point(sid), "named_split", {})
        geom = f["geometry"] if f else None
    elif b is not None and getattr(b, "wbk", None):
        lake = reuse._features("lakes", [str(b.wbk)], "split")
        pt = reuse._rep_point(lake[0].get("geometry")) if lake else None
        geom = {"type": "Point", "coordinates": pt} if pt else None
    props = {"split_id": sid, "label": label, "water": water,
             "name": f"{water}: {label}" if water else label, "rules": rules, "parts": [],
             "to_verify": to_verify, "grade": None, "item_id": on_item}
    if b is not None and getattr(b, "wbk", None) and on_item:
        # (T532 r10) a LAKE cut's map point is the lake itself; where the water's own line meets the lake (its inlet
        # and outlet) is what a stretch is drawn from — the Thompson's first stretch starts at Kamloops Lake's outlet
        ends = _lake_ends(str(b.wbk), on_item)
        if ends:
            props["ends"] = ends
    if geom is None:
        return {"unplaced": {**props, "why": "the served atlas has no point for it"}}
    return {"type": "Feature", "geometry": geom, "properties": props}


def _splits_fc(loc: dict, water: dict | None) -> dict:
    """Every cut on the location's water (the items' boundaries) and every split its extent names,
    labelled; the extent's own are `rules: ["extent"]`."""
    named = {s for e in (loc.get("binding") or {}).get("extents") or [] for s in e.get("splits") or []}
    named |= {s for e in (loc.get("binding") or {}).get("tributary_excludes") or []
              for s in e.get("splits") or []}
    on_water: dict[str, str] = {}
    for iid in (water or {}).get("item_ids") or []:
        it = reuse._registry().get(iid)
        for b in (it.boundaries if it else ()):
            on_water.setdefault(b.id, iid)
    feats, unplaced = [], []
    for sid in list(dict.fromkeys([*named, *on_water])):
        f = _split_feature(sid, rules=["extent"] if sid in named else [], to_verify=False,
                           item_id=on_water.get(sid))
        if f is None:
            continue
        if "unplaced" in f:
            unplaced.append(f["unplaced"])
        else:
            feats.append(f)
    return {"type": "FeatureCollection", "features": feats, "unplaced": unplaced}


def _lake_ends(wbk: str, item_id: str) -> list[list[float]]:
    """[lon, lat] of each end of the item's line sections that lies on the lake's outline (`LAKE_TOUCH_M`)"""
    from shapely.geometry import Point, shape
    from shapely.ops import transform
    import pyproj
    lake = reuse._features("lakes", [wbk], "split")
    it = reuse._registry().get(item_id)
    if not lake or it is None or not lake[0].get("geometry"):
        return []
    poly = transform(pyproj.Transformer.from_crs(4326, 3005, always_xy=True).transform, shape(lake[0]["geometry"]))
    geoms, to_ll, out = reuse._geoms(), reuse._to_lonlat(), []
    for s_ in it.section_ids:
        g = geoms.get(s_)
        if g is None or g.geom_type != "LineString":
            continue
        for c in (g.coords[0], g.coords[-1]):
            if poly.boundary.distance(Point(c)) <= LAKE_TOUCH_M:
                x, y = to_ll(c[0], c[1])
                pt = [round(x, 6), round(y, 6)]
                if pt not in out:
                    out.append(pt)
    return out


def _hints(loc: dict, water: dict | None) -> list[dict]:
    """ANCHOR HINTS from the notice's words: each named river / creek / lake, with the registry
    items of that name (a tributary of this water first). A hint, never a binding: the reviewer
    picks."""
    from pipeline.common.names import key as name_key
    text = (loc.get("source_text") or {}).get("specific_area") or ""
    own = set((water or {}).get("item_ids") or [])
    out: list[dict] = []
    seen: set[str] = set()
    for rx, kind, anchor in ((TRIB_RE, "stream", "confluence"), (LAKE_RE, "lake", "lake_outlet")):
        for mm in rx.finditer(text):
            name = mm.group(1)
            if name in seen or name_key(name) == name_key(loc.get("water") or ""):
                continue
            seen.add(name)
            hits = [i for i in reuse.search_items(name, kind=kind, limit=8)
                    if name_key(i["name"]) == name_key(name) and i["id"] not in own]
            out.append({"name": name, "anchor": anchor, "items": hits[:4]})
    for a in (loc.get("source_text") or {}).get("anchor_types") or []:
        word = {"bridge": "bridge", "dam_or_hatchery": "dam", "boundary_sign": "sign",
                "road": "bridge", "point": "coord", "place_name": "coord"}.get(a)
        if word and not any(h["anchor"] == word for h in out):
            out.append({"name": a.replace("_", " "), "anchor": word, "items": []})
    return out


def detail(location_id: str) -> dict | None:
    got = _locations().get(location_id)
    if got is None:
        return None
    slug, loc, water = got
    st = loc.get("source_text") or {}
    rows, rules = D.rule_rows(loc, DFO_TYPED)
    labels = []
    try:
        from pipeline.regs.parsing.catalogue import CatalogueRule, label
        for r in rules:
            try:
                labels.append({"rule_id": r.get("rule_id"), "label": label(CatalogueRule(**r)),
                               "verbatim": r.get("verbatim") or ""})
            except Exception:  # noqa: BLE001 — a rule the catalogue model refuses still shows
                labels.append({"rule_id": r.get("rule_id"), "label": r.get("verbatim") or "",
                               "verbatim": r.get("verbatim") or ""})
    except Exception:  # noqa: BLE001
        labels = [{"rule_id": r.get("rule_id"), "label": r.get("verbatim") or "",
                   "verbatim": r.get("verbatim") or ""} for r in rules]
    m = _states().get(location_id)
    v = _verify(m, loc, water)
    # the other locations on this water: the notice's other reaches, to read the river as a whole
    siblings = [{"id": lid, "text": (l.get("source_text") or {}).get("specific_area") or "",
                 "status": l.get("status") or "active", "section": l.get("section")}
                for lid, (_s, l, _w) in _locations().items()
                if l.get("water_id") and l.get("water_id") == loc.get("water_id") and lid != location_id]
    groups = _groups()
    g = groups.get(location_id)
    group = None
    if g is not None:
        states, locs = _states(), _locations()
        group = {"id": g["id"], "canonical": g["canonical"], "members": [
            {"id": x["id"], "why": x["why"], "this": x["id"] == location_id,
             "water": locs[x["id"]][1].get("water") or "",
             "text": (locs[x["id"]][1].get("source_text") or {}).get("specific_area") or "",
             "status": locs[x["id"]][1].get("status") or "active",
             "verify": _verify(states.get(x["id"]), locs[x["id"]][1], locs[x["id"]][2])["status"],
             # owner 2026-11-09: an older wording is never assumed to match — each is listed with its own words and
             # extent and starts UNTICKED; a ticked one takes the check, and this place's extent when it differs
             # (`marks_for_group`); `cannot_change` / `cannot_take` say why one cannot be ticked
             **wording_check(loc, water, locs[x["id"]][1], locs[x["id"]][2]),
             "extent": seen_extent(locs[x["id"]][1]),
             **({} if x["id"] == location_id else _member_check(x["id"], states.get(x["id"])))}
            for x in g["members"]]}
    view = {
        "row": _row(slug, loc, water, m, groups), "location": loc, "water": water, "group": group,
        "places": water_places(loc.get("water_id"), location_id) if loc.get("water_id") else [],
        "wordings": D.location_wordings(loc),
        "items": [_item_ref(i) for i in (water or {}).get("item_ids") or []],
        # the heading with its "(including tributaries)" qualifier put back (`checklist.dfo_heading`)
        "page_url": D.page_url(slug), "verbatim": {"waters": _heading(loc, water),
                                                   "specific_area": st.get("specific_area") or "",
                                                   # …and the words WHOLE: the excluded items put back in the sentence
                                                   "full": _full_words(st)},
        "rows": rows, "rules": labels, "need": D.need(loc),
        "splits": _splits_fc(loc, water),
        "proposed": proposed_on(list((water or {}).get("item_ids") or [])), "hints": _hints(loc, water), "siblings": siblings,
        "withdrawn": withdrawn_on(list((water or {}).get("item_ids") or [])),
        "suggestion": suggestion(location_id), "fix_proposal": fix_proposal(location_id),
        "closure": closure_view(location_id),
        "fix_vs_current": (fvc := fix_vs_current(m, loc, water)), "saved": saved_status(m, fvc),
        "extent_now": extent_now(loc, water),
        "mark": ({**rekeyed(json.loads(m.model_dump_json())), **v} if m is not None else None),
    }
    return reuse._json_safe({**view, "cut_labels": cut_labels(view), "item_names": item_names(view)})


#: ENTRY B PER PLACE (T573): the closure entries derived from the stored rows (`dfo_salmon.closures`), proposals only.
#: `CURATION_DFO_CLOSURES` points it at a copy (tests).
def _closures_path() -> Path:
    if os.environ.get("CURATION_DFO_CLOSURES"):
        return Path(os.environ["CURATION_DFO_CLOSURES"])
    return Path(GENERATED.review) / "dfo_closure_entries.json"


def _closed_geometry_path() -> Path:
    if os.environ.get("CURATION_DFO_CLOSED_GEOMETRY"):
        return Path(os.environ["CURATION_DFO_CLOSED_GEOMETRY"])
    return Path(GENERATED.review) / "dfo_closed_area_geometry.json"


#: owner 2026-11-10: "if one is near one that already existed use the already existing one" — a NEW cut this close
#: (metres along the water) to an existing cut on the same line is flagged, and the existing one offered instead
NEAR_M = 200.0


def nearest_cut(blk, measure) -> dict | None:
    """the existing curated cut nearest a point of a water's line: {split_id, label, metres, near} (`near`: within
    `NEAR_M`), or None when the line has no cut"""
    if blk is None or measure is None:
        return None
    best = None
    for sid, m in reuse._splits_meta().items():
        if str(m.get("blk")) != str(blk) or m.get("route_measure") is None:
            continue
        d = abs(m["route_measure"] - measure)
        if best is None or d < best[0]:
            best = (d, sid, m)
    if best is None:
        return None
    return {"split_id": best[1], "label": best[2].get("label") or best[1], "metres": round(best[0]),
            "near": best[0] <= NEAR_M}


def _whole_piece(g: dict, new: dict) -> dict:
    """the piece standing alone: a stretch's NEW ends carry their definitions (a proposal file keeps them beside the
    pieces; the mark that confirms one holds everything it confirms)"""
    refs = [pt["split_id"] for pt in g.get("points") or [] if pt.get("kind") == "new"]
    if g.get("kind") != "stretch" or g.get("new_splits") is not None or not refs:
        return g
    return {**g, "new_splits": [new[r] for r in refs if r in new]}


def piece_shape(location_id: str, g: dict, new: dict | None = None) -> dict:
    """WHAT ONE PLACEMENT DRAWS (`closures.PieceGeometry` as a dict): a stretch's two ends on the water (the server's
    own anchor check for a new cut; `why` when one does not land) and its length, a circle's ring at true ground
    radius, a line's two ends. Read only."""
    from pipeline.regs.dfo_salmon import closures as K
    from pipeline.regs.review import marks as M
    new = {**(new or {}), **{n["ref"]: n for n in g.get("new_splits") or []}}

    def end(pt: dict) -> dict:
        sid = pt.get("split_id")
        if pt.get("kind") == "split":
            bounds = reuse._boundary_index()         # only a stretch's end reads the build (a circle needs none)
            q = reuse._split_point(sid)
            ll = reuse._to_lonlat()(q.x, q.y) if q is not None else None
            return {"label": getattr(bounds.get(sid, (None, None))[1], "label", None) or sid, "new": False,
                    "coord": [round(ll[0], 6), round(ll[1], 6)] if ll else None,
                    "measure": (reuse._splits_meta().get(sid) or {}).get("route_measure"),
                    **({} if q is not None else {"why": f"no cut {sid} in the curated splits"})}
        n = new.get(sid)
        chk = check_anchor(location_id, M.NewSplit.model_validate(n)) if n else {"ok": False, "why": "no such new cut"}
        near = chk.get("nearest") if chk.get("ok") else None          # `check_anchor` says it for every new cut
        return {"label": (n or {}).get("name") or "a new cut placed by hand", "new": True, "coord": chk.get("coord"),
                "measure": chk.get("measure"), **({} if chk.get("ok") else {"why": chk.get("why")}),
                **({"nearest": near} if near else {})}
    out: dict = {"ends": [], "metres": None}
    if g["kind"] == "stretch":
        out["ends"] = [end(pt) for pt in g["points"]]
        ms = [e.get("measure") for e in out["ends"]]
        out["metres"] = round(abs(ms[0] - ms[1])) if len(ms) == 2 and None not in ms else None
    elif g["kind"] == "circle":
        out["centre"], out["radius_m"] = g["centre"], g["radius_m"]
        out["ring"] = K.circle_polygon(tuple(g["centre"]), g["radius_m"])
    elif g["kind"] == "line":
        out["line"] = g["line"]
    return out


def piece_preview(location_id: str, piece: dict) -> dict:
    """A PLACEMENT THE OWNER IS EDITING, drawn before he saves it (T573): validated as the schema (ValueError says
    what is wrong), then `piece_shape`. `ok` is false, with why, when an end does not land on the water. KeyError: no
    such location. Read only."""
    from pipeline.regs.dfo_salmon import closures as K
    if _locations().get(location_id) is None:
        raise KeyError(location_id)
    g = K.PieceGeometry.model_validate(piece).model_dump(mode="json", exclude_none=True)
    shape = piece_shape(location_id, g)
    bad = [e["why"] for e in shape["ends"] if e.get("why")]
    return reuse._json_safe({"ok": not bad, "why": "; ".join(bad) or None, **shape})


def stamp_piece(target: str, piece: dict | None, location_id: str | None = None) -> dict | None:
    """a closed-area mark's placement as it is STORED: each new cut placed by hand resolved on the water and its point
    stamped (`check_anchor`; ValueError when it does not land), as `mark_for` does for a DFO mark's new cuts"""
    from pipeline.regs.review import closed_areas as CA
    from pipeline.regs.review import marks as M
    if not piece or not piece.get("new_splits"):
        return piece
    loc = location_id or next(iter((CA.items().get(target) or {}).get("rows") or []), None)
    if loc is None or _locations().get(loc) is None:
        raise ValueError("no DFO row names this closed area: its new cut cannot be placed on a water")
    out = []
    for n in piece["new_splits"]:
        ns = M.NewSplit.model_validate(n)
        res = check_anchor(loc, ns)
        if not res.get("ok"):
            raise ValueError(f"the new cut does not land on the water: {res.get('why')}")
        at = res.get("ref") or res
        out.append({**n, "blk": at["blk"], "measure": at["measure"], "on_item": res.get("on_item"),
                    "coord": list(at["coord"])})
    return {**piece, "new_splits": out}


def closure_view(location_id: str) -> dict | None:
    """THE CLOSED-AREAS ENTRY OF A PLACE (T573, owner 2026-11-10: "it should become 2 entries"): entry B derived from
    this place's row, each of its pieces joined to its placement BY THE LIVE ROW (`closures.attach`: a piece whose item
    DFO dropped is listed "not live" and carries nothing to draw). A placed stretch has its two ends on the water (the
    server's own anchor check) and its length; a placed circle its centre, its radius and its ring at true ground
    radius (`closures.circle_polygon`). Read only; proposals: nothing in the places files or the feed holds them. A
    file that cannot be read, or a record that is not the schema, is said (`error`)."""
    from pipeline.regs.dfo_salmon import closures as K
    from pipeline.regs.review import marks as M
    p = _closures_path()
    if not p.exists():
        return None
    try:
        entries = json.loads(p.read_text(encoding="utf-8")).get("entries") or {}
        entry = entries.get(location_id + "#closed")
    except (OSError, ValueError) as ex:
        return {"error": f"the closure entries file cannot be read: {ex}"}
    if entry is None:
        return None
    geo, new, looked, alts = [], {}, {}, {}
    gp = _closed_geometry_path()
    if gp.exists():
        try:
            doc = json.loads(gp.read_text(encoding="utf-8"))
            geo, new = list(doc.get("pieces") or []), {n["ref"]: n for n in doc.get("new_splits") or []}
            looked, alts = doc.get("looked") or {}, doc.get("alternatives") or {}
        except (OSError, ValueError) as ex:
            return {"error": f"the closed-area placements file cannot be read: {ex}"}
    got = _locations().get(location_id)
    live = bool(got) and (got[1].get("status") or "active") == "active"
    # WHAT THE OWNER CONFIRMED (the curated file his closed-area marks write) comes before any proposal of ours, per item
    from pipeline.regs.review import closed_areas as CA
    from pipeline.regs.review import marks as MK
    try:
        mine = CA.confirmed()
    except (OSError, ValueError) as ex:
        return {"error": f"the confirmed closed areas file cannot be read: {ex}"}
    done = {(g.get("water_id"), g.get("key")) for g in mine}
    geo = mine + [g for g in geo if (g.get("water_id"), g.get("key")) not in done]
    for g in mine:
        for n in g.get("new_splits") or []:
            new.setdefault(n["ref"], n)
    inforce = {m.target: m for m in MK.latest(MK.load(reuse.MARKS_PATH)).values() if m.scope == "closed_area"}
    try:
        # the owner's rule: a piece exists only while DFO's LIVE page names its item (per item, across this water's rows)
        pieces = K.attach(entry, geo, live=K.live_keys(entries, entry.get("water_id")),
                          others=K.water_areas(entries, entry.get("water_id")))
    except ValueError as ex:
        return {"error": f"a closed-area placement is not the schema: {ex}"}
    areas = []
    for a in pieces:
        g = a.pop("geometry")
        shown = g is not None      # a not-live piece is drawn ONLY here, muted, to be checked (`live`: false) — never bound
        target = CA.target_of(entry.get("water_id"), a["key"])
        mk = inforce.get(target)
        out = {**a, "ends": [], "metres": None, "confidence": (g or {}).get("confidence"),
               "placed_note": (g or {}).get("note") or (g or {}).get("centre_from") or "",
               # the mark it takes (`closed_area`), whether HE confirmed this placement, and the piece as stored
               "target": target, "confirmed": g is not None and (g.get("water_id"), g.get("key")) in done,
               "mark": ({"verdict": getattr(mk.verdict, "value", mk.verdict), "date": mk.date, "note": mk.note}
                        if mk is not None else None),
               "piece": _whole_piece(g, new) if shown else None}
        if shown:
            out.update(piece_shape(location_id, g, new))
        elif looked.get(target):
            out["placed_note"] = looked[target]            # what was tried before it was left for him to place
        # THE CANDIDATES for an end DFO describes two ways (the Juniper upper signs): each a whole placement he can take
        cands = []
        for alt in (alts.get(target) or []) if shown and g["kind"] == "stretch" and not out["confirmed"] else []:
            pts = list(g["points"])
            pts[alt["end"]] = alt["point"]
            cand = _whole_piece({**{k: v for k, v in g.items() if k != "new_splits"}, "points": pts}, new)
            shape = piece_shape(location_id, cand, new)
            cands.append({"label": alt["label"], "why": alt.get("why") or "", "end": alt["end"], "piece": cand,
                          "coord": shape["ends"][alt["end"]].get("coord"), "metres": shape.get("metres"),
                          "is_now": pts == list(g["points"])})
        if cands:
            out["candidates"] = cands
        areas.append(out)
    items = list(((got[2] if got else None) or {}).get("item_ids") or [])
    return reuse._json_safe({**entry, "areas": areas, "row_live": live, "on_item": items[0] if items else None,
                             "near_m": NEAR_M,
                             "proposed": any(a["state"] in ("placed", "changed", "radius") for a in areas),
                             "new_cuts": [n.get("name") or n["ref"] for n in new.values()
                                          if any(pt.get("split_id") == n["ref"] for g in geo
                                                 if g.get("water_id") == entry.get("water_id")
                                                 for pt in g.get("points") or [])]})


def _full_words(st: dict) -> str:
    from pipeline.regs.review import checklist as CL
    return CL.full_words(st.get("specific_area") or "", st.get("excludes") or ())


def _heading(loc: dict, water: dict | None) -> str:
    from pipeline.regs.review import checklist as CL
    return CL.dfo_heading(loc, water)


def dead_why(fixes, dead: list[int], water: dict | None) -> list[str]:
    """WHY A PIECE BINDS NOTHING, in plain words (owner 2026-11-09, the Forks: a piece on Bulkley River between a cut
    of the MORICE and a cut of the Bulkley): a `between` / upstream / downstream piece holds on its own waters (`items`,
    else the place's), so a point that is a cut on ANOTHER water gives it nothing to run between. -> one line per dead
    piece naming the stray cut, the water it is on, and the cuts that ARE on the piece's water."""
    reg, bounds = reuse._registry(), reuse._boundary_index()
    label = lambda s: getattr(bounds[s][1], "label", None) or s            # noqa: E731
    name = lambda i: getattr(reg.get(i), "name", None) or i                # noqa: E731
    out = []
    for i in dead:
        f = fixes[i]
        items = list(f.items) or list((water or {}).get("item_ids") or [])
        on = ", ".join(name(x) for x in items) or "its water"
        stray = [(p.split_id, bounds[p.split_id][0]) for p in f.points
                 if p.kind == "split" and p.split_id in bounds and bounds[p.split_id][0] not in items]
        here = [s for s, (it, _b) in bounds.items() if it in items]

        def at(sid):
            try:
                return reuse._split_point(sid)
            except Exception:  # noqa: BLE001 — a cut the atlas cannot place is simply not offered as the nearest
                return None

        def nearest(sid) -> str | None:
            p0 = at(sid)
            cand = [(p0.distance(q), c) for c in here if p0 is not None and (q := at(c)) is not None]
            return label(min(cand)[1]) if cand else None
        if stray:
            tips = [n for s, _it in stray if (n := nearest(s))]
            out.append(f"piece {i + 1} is on {on}, but " + " and ".join(
                f"“{label(s)}” is a cut on {name(it)}" for s, it in stray)
                + f": a piece runs between cuts on its own water. Pick the cut on {on} instead"
                + (f" — the nearest one there is “{tips[0]}”" if tips else
                   f" (cuts there: {'; '.join(sorted(map(label, here))[:8])})" if here else f" ({on} has no cut yet: add a new cut)") + ".")
        else:
            out.append(f"piece {i + 1} binds nothing on {on}: its cuts are there but no stretch lies between them as "
                       "given (the same cut twice, or the op runs away from the other point). Check the op and the points.")
    return out


def _member_check(mid: str, m) -> dict:
    """an older wording's own checklist digest (sent back with a group check, refused when it moved), whether a save of
    its own still waits to land (`held`: a check would replace it) and its check state"""
    import checks
    got = checks.dfo_check(mid) or {}
    return {"digest": got.get("digest"), "held": got.get("held"), "check": got.get("check")}


def rekeyed(mark: dict) -> dict:
    """T355: A MARK AS SHOWN — each new split's local ref ("n1-2", a hunt's, a tidal one) read as the REAL id
    of the proposed cut it is (by its anchor, `real_ids`), its extent fix's points with it. Read only: the
    store is never edited; the same anchor always reads as the same id."""
    from pipeline.regs.review import marks as M
    props, ids = _proposals(), real_ids()
    ren: dict[str, str] = {}
    for n in mark.get("new_splits") or []:
        ns = M.NewSplit.model_validate(n)
        hit = next((p for p in props if any(M.same_anchor(ns, q) for q in p["members"])), None)
        if hit is not None and hit["id"] in ids:
            ren[n["ref"]] = ids[hit["id"]]
    if not ren:
        return mark
    return {**mark, "new_splits": [{**n, "ref": ren.get(n["ref"], n["ref"])} for n in mark.get("new_splits") or []],
            "extent_fix": [{**f, "points": [{**pt, "split_id": ren.get(pt.get("split_id"), pt.get("split_id"))}
                                            if pt.get("kind") == "new" else pt for pt in f.get("points") or []]}
                           for f in mark.get("extent_fix") or []]}


def water_places(water_id: str, this: str | None = None) -> list[dict]:
    """EVERY PLACE ON ONE WATER (owner 2026-11-02), for one view of the river: per place (`D.places`)
    its row (verify, state, status…), and every wording of every member with the page dates, so the
    same extent written differently is one item. Read only."""
    locs, rows = _locations(), {r["id"]: r for r in _rows()}
    out = []
    for p in D.places(locs, water_id, _groups()):
        c = p["id"]
        r = rows.get(c) or {}
        wordings = []
        for lid in p["members"]:
            l = locs[lid][1]
            for w in D.location_wordings(l):
                wordings.append({**w, "location_id": lid, "status": l.get("status") or "active",
                                 "variant": bool(l.get("duplicate_of")) and l.get("duplicate_confirmed") is not False,
                                 "verify": (rows.get(lid) or {}).get("verify")})
        out.append({"id": c, "this": this in p["members"], "members": p["members"],
                    "text": r.get("text") or "", "status": "active" if any(
                        (locs[x][1].get("status") or "active") == "active" for x in p["members"]) else "dormant",
                    "verify": r.get("verify"), "state": r.get("state"), "historic_only": r.get("historic_only"),
                    "wordings": wordings})
    return out


# --------------------------------------------------------------------------- reach (read)

def _reach_input(loc: dict, water: dict | None, extents: list[dict] | None = None,
                 tributaries: bool | None = None):
    """`entries.to_reach_input` on the location (optionally with other extents): the builder's
    own shape."""
    import copy
    from dataclasses import fields
    from pipeline.regs.dfo_salmon import entries as E
    loc = copy.deepcopy(loc)
    if extents is not None:
        loc.setdefault("binding", {})["extents"] = extents
    if tributaries is not None:
        loc.setdefault("binding", {})["tributaries"] = tributaries
    el = E.EntryLocation.from_dict(loc)
    wb = None
    if water:
        names = {f.name for f in fields(E.WaterBinding)}
        wb = E.WaterBinding(**{k: v for k, v in water.items() if k in names})
    # a "rest" is the water minus every other PLACED extent on it, resolved now (`to_reach_input`)
    sibs = ({lid: E.EntryLocation.from_dict(l) for lid, (_s, l, _w) in _locations().items()
             if l.get("water_id") and l.get("water_id") == el.water_id}
            if any(e.op == "rest" for e in el.binding.extents) else {})
    entry, rules = E.to_reach_input(el, [{}], wb, siblings=sibs)
    return entry, rules[0]


def _walks_tributaries(entry: dict, rule: dict) -> bool:
    from pipeline.atlas.reach.classify import wants_tributaries
    return bool(wants_tributaries(rule, entry))


def _bind(entry: dict, rule: dict, limit: int, walks: bool = False) -> dict:
    from pipeline.atlas.reach.build import build_reach
    if not rule["extents"]:
        return {"resolvable": False, "why": "no extent curated yet: nothing binds", "n_sections": 0}
    binding, _d = build_reach(entry, rule, reuse._registry(), reuse._graph())
    ids = sorted(binding.sections)
    return reuse._json_safe({
        "resolvable": True, "why": None, "n_sections": len(ids), "walks_tributaries": walks,
        "outcomes": {rule["rule_id"]: binding.outcome.value}, "truncated": len(ids) > limit,
        "geojson": {"type": "FeatureCollection",
                    "features": reuse._sections_geojson(ids[:limit], "preview")}})


def reach(location_id: str, limit: int = 6000, tributaries: bool = False) -> dict | None:
    """WHAT THE LOCATION'S EXTENT BINDS NOW — `build_reach` on the curated binding. The tributary
    walk is the slow, huge part (Bella Coola `whole`: 13 MB), so by default it is skipped and the
    water's own reach comes back; `tributaries=True` walks them as the binding says.
    `walks_tributaries` says whether the binding would walk them at all (the toggle's meaning)."""
    got = _locations().get(location_id)
    if got is None:
        return None
    _slug, loc, water = got
    entry, rule = _reach_input(loc, water, tributaries=None if tributaries else False)
    walks = _walks_tributaries(*_reach_input(loc, water)) if not tributaries else _walks_tributaries(entry, rule)
    return _bind(entry, rule, limit, walks)


def extent_preview(location_id: str, fix, limit: int = 6000, tributaries: bool = False) -> dict | None:
    """A PROPOSED EXTENT (`marks.ExtentFix`, scope "entry") resolved on a copy, read only. A NEW
    split (`new`, or a T191 new point) is not a split yet: no preview, and the answer says so.
    T524: `fix` may be a LIST of pieces — the place binds their UNION, previewed as one; `dead_pieces` names (1-based)
    each piece that binds nothing alone, never hidden in the union — the build's own check
    (`reach.dfo_salmon.dead_pieces`), which refuses it too."""
    got = _locations().get(location_id)
    if got is None:
        return None
    _slug, loc, water = got
    fixes = list(fix) if isinstance(fix, (list, tuple)) else [fix]
    new = [p for f in fixes for p in f.points if p.kind != "split"]
    if new:
        return {"resolvable": False, "n_sections": None, "why": "a NEW split is not a split yet: "
                "its point is checked on the map (check anchor); the extent resolves once T185 "
                "builds it"}
    for f in fixes:
        if f.op in ("steelhead_waters", "within"):
            return {"resolvable": False, "n_sections": None,
                    "why": f"op {f.op} is not previewed on a DFO location"}
    exs, problems = fix_extents(fixes)
    if problems:
        return {"resolvable": False, "n_sections": None, "why": "; ".join(problems)}
    # a rest previews the remainder after every other PLACED extent on the water, as the build does
    call = next((f.tributaries for f in fixes if f.tributaries is not None), None)
    try:
        entry, rule = _reach_input(loc, water, exs, call if tributaries else False)
        walks = _walks_tributaries(*_reach_input(loc, water, exs, call)) if not tributaries \
            else _walks_tributaries(entry, rule)
    except Exception as e:  # noqa: BLE001 — the Extent model refuses a malformed one
        return {"resolvable": False, "n_sections": None, "why": f"not an extent: {e}"}
    out = _bind(entry, rule, limit, walks)
    dead = DS.dead_pieces(entry, rule, reuse._registry(), reuse._graph())
    # a place of ONE piece that binds nothing is dead too (seen on screen 2026-11-10: "binds 0 section(s) · unresolved"
    # with no reason — `dead_pieces` speaks only of a piece among several): it gets the same plain-words why
    if not dead and len(fixes) == 1 and out.get("resolvable") and out.get("n_sections") == 0:
        dead = [0]
    if dead:
        out["dead_pieces"] = [i + 1 for i in dead]
        out["dead_why"] = dead_why(fixes, list(dead), water)
    return out


# --------------------------------------------------------------------------- anchors (read)

@lru_cache(maxsize=1)
def _to_albers():
    from pyproj import Transformer
    return Transformer.from_crs(4326, 3005, always_xy=True).transform


def _lonlat(pt) -> list[float]:
    x, y = reuse._to_lonlat()(pt.x, pt.y)
    return [round(x, 6), round(y, 6)]


def _line_sections(item_ids) -> list[tuple[str, object, object]]:
    """(node_id, graph node, geometry in EPSG:3005) for every LINE section of these items."""
    reg, graph, geoms = reuse._registry(), reuse._graph(), reuse._geoms()
    out = []
    for iid in item_ids:
        it = reg.get(iid)
        for nid in (it.section_ids if it else ()):
            if str(nid).startswith("lake:"):
                continue
            n, g = graph.nodes.get(nid), geoms.get(nid)
            if n is None or g is None or g.is_empty or getattr(n, "blk", None) is None:
                continue
            out.append((nid, n, g))
    return out


def _snap(pt, item_ids) -> dict | None:
    """The nearest point of these items' line sections to `pt` (EPSG:3005): node, blk, measure,
    distance, the snapped point."""
    best = None
    for nid, n, g in _line_sections(item_ids):
        d = g.distance(pt)
        if best is None or d < best[0]:
            best = (d, nid, n, g)
    if best is None:
        return None
    d, nid, n, g = best
    along = g.project(pt)
    down = getattr(n, "down_m", 0.0) or 0.0
    return {"node": nid, "blk": str(n.blk), "measure": round(down + along, 1),
            "distance_m": round(d, 1), "point": g.interpolate(along)}


def _point_at(blk: str, measure: float):
    for down, up, nid in reuse._blk_index().get(str(blk), []):
        if down <= measure <= up:
            g = reuse._geoms().get(nid)
            if g is None or g.is_empty:
                return None
            return g.interpolate(min(max(measure - down, 0.0), g.length))
    return None


def _mouth(item_id: str):
    """A stream's mouth: the downstream end of its lowest section (EPSG:3005), or None."""
    secs = _line_sections([item_id])
    if not secs:
        return None
    nid, n, g = min(secs, key=lambda s: (getattr(s[1], "down_m", 0.0) or 0.0))
    from shapely.geometry import Point
    return Point(g.coords[0]) if g.geom_type == "LineString" else g.interpolate(0)


def _lake_poly(item_id: str):
    if not item_id.startswith("wbk:"):
        it = reuse._registry().get(item_id)
        wbk = next((r.split(":", 1)[1] for r in (it.ref_ids if it else ()) if r.startswith("wbk:")), None)
    else:
        wbk = item_id.split(":", 1)[1]
    if not wbk:
        return None
    return reuse._fwa_lake_poly(wbk) or reuse._wbk_polys().get(wbk) or reuse._geoms().get(f"lake:{wbk}")


def _find_item(name: str | None, item_id: str | None, kind: str) -> str | None:
    if item_id and item_id in reuse._registry():
        return item_id
    if not (name or "").strip():
        return None
    from pipeline.common.names import key as name_key
    hits = [h for h in reuse.search_items(name, kind=kind, limit=10)
            if name_key(h["name"]) == name_key(name)]
    return hits[0]["id"] if len(hits) == 1 else None


def check_anchor(location_id: str, ns) -> dict:
    """A PROPOSED NEW SPLIT (`marks.NewSplit`) resolved, READ ONLY — validated server-side and
    cheap: {ok, why, coord (lon, lat), blk, measure, on_item, distance_m, words}. `ok: false` with
    why when the anchor does not land on the water (a click on another water, a stream that is not
    its tributary, a lake it does not touch, an unknown split). KeyError: no such location."""
    from shapely.geometry import Point
    got = _locations().get(location_id)
    if got is None:
        raise KeyError(location_id)
    _slug, loc, water = got
    items = [ns.on_item] if ns.on_item else list((water or {}).get("item_ids") or [])
    if not items:
        return {"ok": False, "why": "this location's water is not matched to the registry yet"}
    if any(i not in reuse._registry() for i in items):
        return {"ok": False, "why": f"no registry item {items}"}
    wname = ", ".join(_item_ref(i)["name"] for i in items)

    def done(pt, words: str, extra: dict | None = None) -> dict:
        s = _snap(pt, items)
        if s is None:
            return {"ok": False, "why": f"{wname} has no line section to cut"}
        out = {"ok": True, "why": None, "blk": s["blk"], "measure": s["measure"],
               "on_item": items[0] if len(items) == 1 else _item_of(s["node"], items),
               "distance_m": s["distance_m"], "words": words, **(extra or {})}
        p = s["point"]
        # THE REFERENCE (owner 2026-11-02): an offset cut is its anchor + `offset_m` along the water;
        # the mark stamps the reference, never the offset point (the split adds the offset itself)
        out["ref"] = {"blk": s["blk"], "measure": s["measure"], "coord": _lonlat(p)}
        if ns.offset_m:
            q = _point_at(s["blk"], s["measure"] + ns.offset_m)
            if q is None:
                return {"ok": False, "why": f"{abs(ns.offset_m):.0f} m "
                        f"{'above' if ns.offset_m > 0 else 'below'} it runs off the water's line"}
            out["measure"] = round(s["measure"] + ns.offset_m, 1)
            out["words"] += f", {abs(ns.offset_m):.0f} m {'above' if ns.offset_m > 0 else 'below'}"
            p = q
        out["coord"] = _lonlat(p)
        # owner 2026-11-10 (T590): "if one is near one that already existed use the already existing one" — said on
        # EVERY new cut, wherever it is placed: the nearest existing cut on the same line, and whether it is near
        near = nearest_cut(out["blk"], out["measure"])
        if near:
            out["nearest"] = near
        return out

    if ns.anchor in ("bridge", "dam", "sign", "coord"):
        pt = Point(*_to_albers()(*ns.coord))
        s = _snap(pt, items)
        if s is None:
            return {"ok": False, "why": f"{wname} has no line section to cut"}
        if s["distance_m"] > SNAP_MAX_M:
            return {"ok": False, "why": f"the point is {s['distance_m']:.0f} m from {wname}: click on "
                    "the water (or pick the water it is on)", "distance_m": s["distance_m"]}
        return done(pt, f"{ns.anchor} on {wname}, snapped {s['distance_m']:.0f} m")
    if ns.anchor == "confluence":
        trib = _find_item(ns.name, ns.item_id, "stream")
        if trib is None:
            return {"ok": False, "why": f"no single stream named {ns.name or ns.item_id!r}: pick it "
                    "from the list"}
        mouth = _mouth(trib)
        if mouth is None:
            return {"ok": False, "why": f"{trib} has no line section"}
        s = _snap(mouth, items)
        if s is None or s["distance_m"] > MOUTH_MAX_M:
            return {"ok": False, "why": f"{_item_ref(trib)['name']}'s mouth is "
                    f"{(s or {}).get('distance_m', 0):.0f} m from {wname}: not its tributary"}
        return done(mouth, f"confluence of {_item_ref(trib)['name']} ({trib})", {"item_id": trib})
    if ns.anchor in ("lake_inlet", "lake_outlet"):
        lake = _find_item(ns.name, ns.item_id, "lake")
        if lake is None:
            return {"ok": False, "why": f"no single lake named {ns.name or ns.item_id!r}: pick it "
                    "from the list"}
        poly = _lake_poly(lake)
        if poly is None:
            return {"ok": False, "why": f"no outline for {lake}"}
        ends = []
        for nid, n, g in _line_sections(items):
            if g.distance(poly) > LAKE_TOUCH_M:
                continue
            down, up = getattr(n, "down_m", 0.0) or 0.0, getattr(n, "up_m", 0.0) or 0.0
            for m_, p in ((down, g.interpolate(0)), (up, g.interpolate(g.length))):
                if p.distance(poly) <= LAKE_TOUCH_M:
                    ends.append((m_, p, str(n.blk)))
        if not ends:
            return {"ok": False, "why": f"{wname} does not touch {_item_ref(lake)['name']}"}
        m_, p, blk = (max if ns.anchor == "lake_inlet" else min)(ends, key=lambda e: e[0])
        words = f"{'inlet' if ns.anchor == 'lake_inlet' else 'outlet'} of {_item_ref(lake)['name']}"
        if len({round(e[0]) for e in ends}) < 2:
            words += " (the water touches the lake once: inlet and outlet are the same point)"
        return done(p, words, {"item_id": lake})
    if ns.anchor in ("area_boundary", "border", "lake"):
        # T355: an area's edge, the border, a lake's two ends — resolved by the BUILD'S OWN resolver
        # (`split_preview.resolve`: `resolve_split_defs` on the build's chains), never by geometry here
        import split_preview
        got = split_preview.resolve(ns if ns.on_item else ns.model_copy(update={"on_item": items[0]}))
        if not got.get("ok"):
            return {"ok": False, "why": got.get("why")}
        if ns.anchor == "area_boundary":       # an area's edge: the build cuts it, nothing to stamp
            end = {"upstream": "enters", "downstream": "leaves"}.get(ns.area_end or "", "crosses")
            return {"ok": True, "why": None, "area": True, "on_item": ns.on_item or items[0],
                    "coord": got["cuts"][0]["coord"], "blk": got["cuts"][0]["blk"],
                    "measure": got["cuts"][0]["measure"], "cuts": got["cuts"], "outline": got.get("outline"),
                    "words": f"where {wname} {end} {ns.area} ({ns.area_layer or 'parks_bc'}): {got['words']}"}
        cut = got["cuts"][0]
        at = (got.get("reference") or got["cuts"])[0]       # the reference, not the offset point
        return {"ok": True, "why": None, "blk": cut["blk"], "measure": cut["measure"], "coord": cut["coord"],
                "on_item": ns.on_item or items[0], "words": got["words"], "cuts": got["cuts"],
                "ref": {"blk": at["blk"], "measure": at["measure"], "coord": at["coord"]},
                **({"item_id": ns.item_id} if ns.item_id else {})}
    if ns.anchor == "split":
        sid = ns.split_id
        pt = reuse._split_point(sid) if sid in reuse._splits_meta() else None
        if pt is None:
            return {"ok": False, "why": f"no split {sid!r} placed in the served atlas"}
        return done(pt, f"the existing split {sid}", {"split_id": sid})
    return {"ok": False, "why": f"unknown anchor {ns.anchor}"}


def water_items(location_id: str) -> list[str]:
    """The registry items of a location's water (its new splits cut the first). KeyError: no such location."""
    got = _locations().get(location_id)
    if got is None:
        raise KeyError(location_id)
    return list((got[2] or {}).get("item_ids") or [])


def preview_split(location_id: str, ns, others: tuple = ()) -> dict:
    """T355: where the BUILD would cut a proposed split (`split_preview.resolve`) — on the location's
    water when the split names none — and `split_id`, the REAL id it lands under (the proposal's own when it
    is one, an edit's when it edits one; `others`: the mark's other new splits, so a clash is numbered).
    KeyError: no such location."""
    import split_preview
    if not ns.on_item:
        items = water_items(location_id)
        if not items:
            return {"ok": False, "why": "this location's water is not matched to the registry yet"}
        ns = ns.model_copy(update={"on_item": items[0]})
    got = split_preview.resolve(ns)
    try:
        mine = [o.model_dump(mode="json") for o in others] + [ns.model_dump(mode="json")]
        out, ren, _r = _dedupe_against_store(mine)
        got["split_id"] = ren.get(ns.ref) or out[-1]["ref"]
    except ValueError as ex:
        got["split_id"] = None
        got.setdefault("notes", []).append(str(ex))
    return got


def _item_of(nid: str, items) -> str | None:
    reg = reuse._registry()
    return next((i for i in items if nid in set(reg[i].section_ids)), items[0] if items else None)


# --------------------------------------------------------------------------- the mark

def mark_for(body: dict, *, date: str, pdf_sha256: str):
    """THE `Mark` A DFO PAYLOAD MAKES: prints of the location NOW, each new split's anchor checked
    (`check_anchor`; refused, ValueError, when it does not land) and its resolved point stamped.
    T210: a new split that names a cut already proposed (any mark, `marks.proposals`) REUSES that
    proposal's id (the mark's own ref and its extent fix's points are renamed to it); one whose ref
    is taken by another cut gets a free ref. Returns (mark, prints now, reused: [{ref, id, name}]).
    KeyError: no such location. Writes nothing — `reuse.post_mark` commits the mark."""
    from pipeline.regs.review import marks as M
    got = _locations().get(str(body.get("target") or ""))
    if got is None:
        raise KeyError(body.get("target"))
    _slug, loc, water = got
    # T525: a mark is made on the extent the owner was shown (`extent_now.seen`, sent back as `extent_before` and kept
    # in the mark); one made on an extent that has moved since — another mark landed, a hand edit, a re-anchor — is
    # refused, so a save never confirms or replaces an extent nobody looked at (the apply checks it again)
    if "extent_before" not in body:
        raise ValueError("a DFO mark names the extent it was made on (extent_before): reload the place")
    if body["extent_before"] != seen_extent(loc):
        raise ValueError(f"{loc['location_id']}: its extent changed since you opened it (now "
                         f"{json.dumps(seen_extent(loc))}): reload the place and look again — nothing was saved")
    now = D.prints(loc, water, DFO_TYPED)
    stamped: list[dict] = []
    for raw in body.get("new_splits") or []:
        ns = M.NewSplit.model_validate(raw)
        res = check_anchor(loc["location_id"], ns)
        if not res.get("ok"):
            raise ValueError(f"new split {ns.ref}: {res.get('why')}")
        if res.get("area"):                        # a park boundary: nothing to stamp
            stamped.append(ns.model_dump(mode="json"))
            continue
        at = res.get("ref") or res                 # the reference, not the offset point
        upd = {"blk": at["blk"], "measure": at["measure"], "on_item": res.get("on_item"),
               "coord": tuple(at["coord"])}
        if res.get("item_id") and not ns.item_id:
            upd["item_id"] = res["item_id"]
        stamped.append(ns.model_copy(update=upd).model_dump(mode="json"))
    stamped, rename, reused = _dedupe_against_store(stamped)
    fixes = [{**f, "points": [{**p, "split_id": rename.get(p.get("split_id"), p.get("split_id"))}
                              if p.get("kind") == "new" else p for p in f.get("points") or []]}
             for f in body.get("extent_fix") or []]
    data = {**body, "rule_id": None, "new_splits": stamped, "date": date,
            **({"extent_fix": fixes} if fixes else {}),
            "pdf_sha256": pdf_sha256, "pages": [], "prints": now}
    mark = M.Mark.model_validate(data)
    return mark, M.Prints.model_validate(now), reused


def marks_for_group(body: dict, *, date: str, pdf_sha256: str) -> list:
    """ONE DECISION ON THE WORDINGS OF A PLACE (owner 2026-10-28; 2026-11-09: "it should save extent to all variants of
    location text" — and "we cant blindly assume variants … they must match and that requires human check too"): the
    mark `mark_for` makes for `target`, and one for every OTHER wording the owner ticked "same place" — each named in
    `targets` AND listed in `members` with the extent the owner saw it hold (`extent_before`, R3 per member) and, on a
    checklist, its own checklist digest (refused by the app when it moved). The server never adds a wording on its own.

      the save carries an extent fix   every ticked wording takes it (it holds the new extent)
      no fix, a wording binds the same  it takes the same answers / check
      no fix, a wording DIFFERS         it takes the shown place's extent (`fix_taking`) plus the check — never a
                                        "correct" stamped on a wording that holds another extent (a defer changes
                                        nothing, so it carries none)
    REFUSED (ValueError), nothing saved, naming the wording: a wording not in target's group, one not listed, one whose
    extent would change while it holds a field no fix can say (R4), one that cannot take the shown extent. The checklist
    a wording carries is the rows the owner checked, its digest the wording's own as it will bind. -> marks, target's
    first."""
    import checks
    target = str(body.get("target") or "")
    g = _groups().get(target)
    if g is None:
        raise ValueError(f"{target} is in no historic group")
    others = [t for t in dict.fromkeys(body.get("targets") or []) if t != target]
    members = {m["id"] for m in g["members"]}
    stray = [t for t in others if t not in members]
    if stray:
        raise ValueError(f"not in {target}'s group: {', '.join(stray)}")
    listed = body.get("members") or {}
    locs = _locations()
    words = lambda t: f"“{(locs[t][1].get('source_text') or {}).get('specific_area') or t}”"  # noqa: E731
    missing = [t for t in others if not isinstance(listed.get(t), dict) or "extent_before" not in listed[t]]
    extra = [t for t in listed if t not in others]
    if missing or extra:
        raise ValueError("a save on older wordings names each one it writes, with the extent you saw it hold: "
                         + "; ".join([f"{words(t)} is not listed" for t in missing if t in locs]
                                     + [f"{t} is listed but not ticked" for t in extra])
                         + " — reload the place, nothing was saved")
    _s, tloc, twater = locs[target]
    base = {k: v for k, v in body.items() if k not in ("targets", "members")}
    fix = base.get("extent_fix")
    out, refused = [], []
    for t in others:
        _s, loc, water = locs[t]
        mb = {**base, "target": t, "group": g["id"], "extent_before": listed[t]["extent_before"]}
        w = wording_check(tloc, twater, loc, water)
        takes = bool(fix) or not w["same_extent"]
        if fix:
            if w["cannot_change"]:
                refused.append(f"{words(t)} holds what an extent fix cannot say ({'; '.join(w['cannot_change'])})")
                continue
        elif not w["same_extent"] and base.get("verdict") != "deferred":
            why = w["cannot_change"] + w["cannot_take"]
            if why:
                refused.append(f"{words(t)} cannot take this extent ({'; '.join(why)})")
                continue
            mb["extent_fix"] = [f.model_dump(mode="json") for f in fix_taking(tloc, twater, loc, water)[0]]
        else:
            takes = False
        if base.get("checklist"):
            mb["checklist"] = {**base["checklist"],
                               "digest": checks.digest_as(loc, water, tloc if takes and not fix else None, twater)}
        out.append(mb)
    if refused:
        raise ValueError("not saved — untick these older wordings and leave them for a hand edit: "
                         + "; ".join(refused))
    first = mark_for({**base, "target": target, "group": g["id"]}, date=date, pdf_sha256=pdf_sha256)[0]
    return [first] + [mark_for(mb, date=date, pdf_sha256=pdf_sha256)[0] for mb in out]


def real_ids(extra: tuple = (), *, tidal: bool = False, tidal_water: str | None = None) -> dict:
    """T355: EVERY PROPOSED CUT'S REAL ID — {group id: the splits.json id it lands as} — by the proposals'
    own `place_groups` (the same slug, the same clash numbering, from the anchor). `extra`: (key, NewSplit)
    cuts not proposed yet (a draft, a mark being saved), placed beside the proposals."""
    from pipeline.regs.review import proposals as P
    import split_preview
    rows = [(p["id"], p["split"], False, None) for p in _proposals()]
    if not extra:
        from pipeline.common.curated import CURATED
        return _REAL_IDS_MEMO.get((reuse._stamp((reuse.MARKS_PATH, reuse.SPLITS_JSON_PATH, reuse.REGISTRY_PATH,
                                                 CURATED.review.proposal_overrides)),),
                                  lambda: {g: v["id"] for g, v in P.place_groups(rows, split_preview._sources()).items()})
    rows += [(k, n, tidal, tidal_water) for k, n in extra]
    return {g: v["id"] for g, v in P.place_groups(rows, split_preview._sources()).items()}


_REAL_IDS_MEMO = reuse._Memo()


def _dedupe_against_store(stamped: list[dict], *, tidal: bool = False,
                          tidal_water: str | None = None) -> tuple[list[dict], dict, list[dict]]:
    """The mark's stamped new splits with duplicates folded (T210), each under its REAL id (T355: the
    splits.json id it lands as, `real_ids` — never a local ref): a split at an anchor already proposed — by
    an earlier mark, or twice in this one — takes that proposal's id and is said to be reused; an EDIT
    (`amends`) takes the id of the cut it edits. -> (splits, {old ref: id}, reused)."""
    from pipeline.regs.review import marks as M
    props = _proposals()
    ids = real_ids()
    out: list[dict] = []
    rename: dict[str, str] = {}
    reused: list[dict] = []
    for raw in stamped:
        ns = M.NewSplit.model_validate(raw)
        if ns.amends is not None:
            # T355: an EDIT of a proposed cut keeps that cut's id (`marks.proposal_groups` folds it in)
            hit = next((p for p in props if any(M.same_anchor(ns.amends, q) for q in p["members"])), None)
            if hit is None:
                raise ValueError(f"new split {ns.ref}: no proposed split at that anchor to edit")
            rid = ids[hit["id"]]
            rename[ns.ref] = rid
            out.append({**raw, "ref": rid, "amends": {**raw["amends"], "ref": rid}})
            continue
        hit = next((p for p in props if any(M.same_anchor(ns, q) for q in p["members"])), None)
        if hit is not None:
            rid = ids[hit["id"]]
            rename[ns.ref] = rid
            reused.append({"ref": ns.ref, "id": rid, "name": hit["split"].name or hit["split"].anchor})
            if any(o["ref"] == rid for o in out):
                continue
            out.append({**raw, "ref": rid})
            continue
        mine = next((o for o in out if M.same_anchor(ns, M.NewSplit.model_validate(o))), None)
        if mine is not None:                       # twice in this mark: one cut
            rename[ns.ref] = mine["ref"]
            continue
        # a cut nobody proposed yet: the id it will land under, placed beside every proposal and this
        # mark's others (a clash with either is numbered)
        rid = real_ids(tuple((o["ref"], M.NewSplit.model_validate(o)) for o in out) + (("__this__", ns),),
                       tidal=tidal, tidal_water=tidal_water)["__this__"]
        rename[ns.ref] = rid
        out.append({**raw, "ref": rid})
    return out, rename, reused
