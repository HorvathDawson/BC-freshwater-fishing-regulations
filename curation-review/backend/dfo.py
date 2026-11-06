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
             "verify": _verify(states.get(x["id"]), locs[x["id"]][1], locs[x["id"]][2])["status"]}
            for x in g["members"]]}
    return reuse._json_safe({
        "row": _row(slug, loc, water, m, groups), "location": loc, "water": water, "group": group,
        "places": water_places(loc.get("water_id"), location_id) if loc.get("water_id") else [],
        "wordings": D.location_wordings(loc),
        "items": [_item_ref(i) for i in (water or {}).get("item_ids") or []],
        "page_url": D.page_url(slug), "verbatim": {"waters": st.get("waters") or loc.get("water"),
                                                   "specific_area": st.get("specific_area") or ""},
        "rows": rows, "rules": labels, "need": D.need(loc),
        "splits": _splits_fc(loc, water),
        "proposed": proposed_on(list((water or {}).get("item_ids") or [])), "hints": _hints(loc, water), "siblings": siblings,
        "suggestion": suggestion(location_id), "fix_proposal": fix_proposal(location_id),
        "mark": ({**rekeyed(json.loads(m.model_dump_json())), **v} if m is not None else None),
    })


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
    split (`new`, or a T191 new point) is not a split yet: no preview, and the answer says so."""
    got = _locations().get(location_id)
    if got is None:
        return None
    _slug, loc, water = got
    new = [p for p in fix.points if p.kind != "split"]
    if new:
        return {"resolvable": False, "n_sections": None, "why": "a NEW split is not a split yet: "
                "its point is checked on the map (check anchor); the extent resolves once T185 "
                "builds it"}
    if fix.op in ("steelhead_waters", "within"):
        return {"resolvable": False, "n_sections": None,
                "why": f"op {fix.op} is not previewed on a DFO location"}
    # a rest previews the remainder after every other PLACED extent on the water, as the build does
    ex: dict = {"op": fix.op} if fix.op == "rest" else {"op": fix.op, "splits": [p.split_id for p in fix.points]}
    if len(fix.items) == 1:
        ex["item_id"] = fix.items[0]
    elif fix.items:
        ex["item_ids"] = list(fix.items)
    try:
        entry, rule = _reach_input(loc, water, [ex], fix.tributaries if tributaries else False)
        walks = _walks_tributaries(*_reach_input(loc, water, [ex], fix.tributaries)) if not tributaries \
            else _walks_tributaries(entry, rule)
    except Exception as e:  # noqa: BLE001 — the Extent model refuses a malformed one
        return {"resolvable": False, "n_sections": None, "why": f"not an extent: {e}"}
    return _bind(entry, rule, limit, walks)


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
    """ONE EXTENT DECISION FOR A HISTORIC GROUP (owner 2026-10-28): the mark `mark_for` makes for
    `target`, and the same answers / extent fix / new splits for every other id in `targets` — each
    a member of target's group (`review.dfo.groups`), each with its own prints NOW and the group's
    id in `group`. ValueError when a target is not in the group. -> marks, target's first."""
    target = str(body.get("target") or "")
    want = [t for t in dict.fromkeys([target, *(body.get("targets") or [])])]
    g = _groups().get(target)
    if g is None:
        raise ValueError(f"{target} is in no historic group")
    members = {m["id"] for m in g["members"]}
    stray = [t for t in want if t not in members]
    if stray:
        raise ValueError(f"not in {target}'s group: {', '.join(stray)}")
    base = {k: v for k, v in body.items() if k != "targets"}
    return [mark_for({**base, "target": t, "group": g["id"]}, date=date, pdf_sha256=pdf_sha256)[0]
            for t in want]


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
