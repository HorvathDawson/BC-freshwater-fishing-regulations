"""T230: THE IN-SEASON TAB — the provincial in-season feed's rows and WHERE each applies.

The feed is the T225 worker's `inseason-feed/1` (`GENERATED.inseason / "feed" / "index.json"`, or
`CURATION_INSEASON_FEED`); the matches are `data/curated/inseason/notice_waters.json` (`CURATED.inseason.notice_waters`),
read and edited only through `pipeline.regs.inseason.matches`.

    queue()                    every feed row (and removed row, and a match on a row the feed no longer holds:
                               "stale") with its match state, in-effect flag and history; progress
    item_splits(item_id)       the splits.json splits on one water, as labelled map points (the picker)
    extent_preview(extents)    what the extents bind, by the builder's own extent resolver, as geometry
    save_match(...)            THE ONE NON-MARK WRITE (owner 2026-10-28, AGENTS 66): set or clear one row's match
                               and append its history note, via `writes.commit` (backup + atomic) under `writes.LOCK`

INFORMATIONAL ONLY: a match puts a banner on the water in the app; it is never a mark and changes no answer.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from pydantic import ValidationError

import reuse
import writes
from pipeline.common.curated import GENERATED
from pipeline.regs.inseason import matches as M


class Refused(ValueError):
    """A save the backend refuses (400), with the reason in words."""


def feed_path() -> Path:
    env = os.environ.get("CURATION_INSEASON_FEED")
    return Path(env) if env else Path(GENERATED.inseason) / "feed" / "index.json"


def matches_path() -> Path:
    # T331: the curated file, never env-overridable (tests point `matches.default_path` at a temp copy)
    return M.default_path()


def load_feed() -> dict | None:
    """The feed, or None when the file is not there (the queue says so; nothing crashes)."""
    p = feed_path()
    if not p.exists():
        return None
    doc = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(doc.get("rows"), list):
        raise ValueError(f"{p}: not an in-season feed (no rows list)")
    return doc


def _feed_rows(feed: dict) -> dict[str, dict]:
    """id -> row, live rows first, then removed ones (`removed: True`, status "removed")."""
    out: dict[str, dict] = {}
    for r in feed.get("rows") or []:
        out[r["id"]] = {**r, "removed": False}
    for r in feed.get("removed") or []:
        out.setdefault(r["id"], {**r, "removed": True, "status": "removed"})
    return out


def split_ids() -> set[str]:
    """Every split id in the cuts file the app reads (`reuse.SPLITS_JSON_PATH`)."""
    return set(reuse._split_records())


def _load_matches() -> M.Matches:
    """The curated matches, parsed (a missing file RAISES — AGENTS 37)."""
    return M.parse(matches_path().read_text(encoding="utf-8"))


def _in_effect(frm: str | None, to: str | None, today: str) -> bool | None:
    """In force on `today` for the dates `M.span` gives (core's `inSeasonSpan`, the one rule)."""
    if not frm and not to:
        return None
    return (not frm or frm <= today) and (not to or today <= to)


def _extent_view(e: M.InSeasonExtent, labels: dict[str, str]) -> dict:
    it = reuse._registry().get(e.item_id)
    return {"op": e.op, "item_id": e.item_id, "splits": list(e.splits), "tributaries": e.tributaries,
            "item_name": it.name if it else None,
            "split_labels": [labels.get(s) or _derived_label(e.item_id, s) for s in e.splits]}


def _derived_label(item_id: str, pid: str) -> str:
    """A pick-point's label (lake inlet, confluence, crossing …); the id itself when the atlas is not loaded."""
    try:
        return next((p.label for p in _points(item_id) if p.id == pid), pid)
    except Exception:            # the queue must never fail on a label
        return pid


def _labels() -> dict[str, str]:
    return {sid: str(sp.get("label") or sid) for sid, (_w, sp) in reuse._split_records().items()}


def _row_view(rid: str, row: dict | None, m: M.Match | None, history: list[dict], today: str,
              labels: dict[str, str], doc_words: dict[str, str] | None = None) -> dict:
    doc_words = doc_words or {}
    eff = m.effective if m else None
    frm, to = M.span(row or {}, eff)
    base = row or {"id": rid, "region": m.region if m else "", "water": m.water if m else "",
                   "status": "gone", "removed": True}
    state = "stale" if row is None else ("matched" if m else "unmatched")
    return {
        "id": rid, "region": base.get("region") or "", "section": base.get("section") or "",
        "water": base.get("water") or "", "effective": base.get("effective") or "",
        "from": (row or {}).get("from"), "to": (row or {}).get("to"),
        "change": base.get("change") or "", "parsed": base.get("parsed"),
        "status": base.get("status") or "", "removed": bool(base.get("removed")),
        "firstSeen": base.get("firstSeen"), "changedAt": base.get("changedAt"),
        "removedAt": base.get("removedAt"),
        "state": state,
        "match": None if m is None else {
            "extents": [_extent_view(e, labels) for e in m.extents],
            "effective": None if eff is None else {"from": eff.from_, "to": eff.to},
            "note": m.note, "by": m.by, "at": m.at,
            # owner add-on 2026-10-28: an AUTO match (the matcher removed in T520) says so, with its evidence
            "source": m.source, "confidence": m.confidence, "evidence": m.evidence,
            # (T520) where it lands on the file's release: per water, whole or its ends; or why it does not
            "waters": {k: ({"all": True} if w.all else {"ends": [list(p) for p in w.ends], "runs": len(w.runs),
                                                          "lakes": w.lakes})
                       for k, w in sorted(m.waters.items())},
            "words": {t: doc_words.get(t, t) for w in m.waters.values() for p in w.ends for t in p},
            "unresolved": m.unresolved},
        "dates": {"from": frm, "to": to, "source": "curated" if eff else "feed"},
        "in_effect": _in_effect(frm, to, today),
        "history": history,
    }


def queue() -> dict:
    """THE QUEUE: every feed row with its match state; a match on a row the feed no longer holds is listed as
    `stale`. Progress counts the LIVE rows (removed ones are listed, not counted)."""
    feed = load_feed()
    doc = _load_matches()
    today = date.today().isoformat()
    labels = _labels() if doc.matches else {}
    hist: dict[str, list[dict]] = {}
    for h in doc.history:
        hist.setdefault(h.id, []).append(h.model_dump(exclude_none=True))
    if feed is None:
        return {"available": False, "message": f"no in-season feed at {feed_path()} — the T225 worker's "
                "feeds/inseason/index.json has not been copied here", "source": None, "fetchedAt": None,
                "rows": [_row_view(rid, None, m, hist.get(rid, []), today, labels, doc.words)
                         for rid, m in sorted(doc.matches.items())],
                "progress": {"matched": 0, "unmatched": 0, "total": 0, "removed": 0,
                             "stale": len(doc.matches)}}
    rows = _feed_rows(feed)
    out = [_row_view(rid, r, doc.matches.get(rid), hist.get(rid, []), today, labels, doc.words)
           for rid, r in rows.items()]
    out += [_row_view(rid, None, m, hist.get(rid, []), today, labels, doc.words)
            for rid, m in sorted(doc.matches.items()) if rid not in rows]
    live = [r for r in out if not r["removed"]]
    return {"available": True, "message": None, "source": feed.get("source"), "fetchedAt": feed.get("fetchedAt"),
            "rows": out,
            "progress": {"matched": sum(r["state"] == "matched" for r in live),
                         "auto": sum(r["state"] == "matched" and r["match"]["source"] == "auto" for r in live),
                         "unmatched": sum(r["state"] == "unmatched" for r in live), "total": len(live),
                         "removed": sum(r["removed"] and r["state"] != "stale" for r in out),
                         "stale": sum(r["state"] == "stale" for r in out)}}


def row(rid: str) -> dict | None:
    return next((r for r in queue()["rows"] if r["id"] == rid), None)


# --------------------------------------------------------------------------- the picker and the preview (reads)

def _points(item_id: str):
    """The water's pick-points (`pipeline.regs.inseason.points`): splits, lake inlets/outlets, named confluences,
    region / MU crossings, the mouth."""
    from pipeline.regs.inseason import points as P
    return P.points(reuse._registry(), reuse._graph(), item_id)


def _point_coord(node_id: str | None, end: str) -> list[float] | None:
    """A stem node's end, from the served atlas's geometry (FWA lines run mouth -> source)."""
    if not node_id:
        return None
    feats = reuse._features("streams", [node_id], "point")
    if not feats:
        return None
    gm = feats[0].get("geometry") or {}
    lines = gm.get("coordinates") or []
    if gm.get("type") == "LineString":
        lines = [lines]
    if not lines or not lines[0]:
        return None
    return list(lines[0][0] if end == "down" else lines[-1][-1])


def item_splits(item_id: str) -> dict | None:
    """EVERY PICK-POINT on one water (owner 2026-10-29), as labelled map points: its splits.json and atlas cuts,
    lake inlets / outlets, named tributaries' confluences, region / MU boundary crossings and its mouth — each with
    `kind`. None: no such item."""
    import dfo
    it = reuse._registry().get(item_id)
    if it is None:
        return None
    known = split_ids()
    feats, unplaced = [], []
    for p in _points(item_id):
        f = dfo._split_feature(p.id, rules=[], to_verify=False, item_id=item_id) if p.id in known else None
        if f is not None and "unplaced" not in f:
            f["properties"].update({"kind": p.kind, "label": p.label, "name": p.label})
            feats.append(f)
            continue
        c = _point_coord(p.node, p.end)
        props = {"split_id": p.id, "label": p.label, "name": p.label, "water": it.name, "rules": [], "parts": [],
                 "to_verify": False, "grade": None, "item_id": item_id, "kind": p.kind}
        if c is None:
            unplaced.append({**props, "why": "no point on the served atlas"})
        else:
            feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": c}, "properties": props})
    return {"item": {"id": item_id, "name": it.name, "kind": it.kind, "mus": list(it.mus)[:4]},
            "splits": {"type": "FeatureCollection", "features": feats, "unplaced": unplaced}}


def parse_extents(raw) -> list[M.InSeasonExtent]:
    """The models' own validation (op / split counts, registry-id shape); Refused with the words."""
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        raise Refused("extents is a list of {op, item_id, splits}")
    try:
        return [M.InSeasonExtent.model_validate(e) for e in raw]
    except ValidationError as ex:
        raise Refused("; ".join(e["msg"] for e in ex.errors()))
    except ValueError as ex:
        raise Refused(str(ex))


def _check(extents: list[M.InSeasonExtent]) -> None:
    """Every item in this build's registry; every place a splits.json split or one of the water's pick-points."""
    reg, known = reuse._registry(), split_ids()
    for e in extents:
        if e.item_id not in reg:
            raise Refused(f"{e.item_id}: no such water in this build's registry")
        if not e.splits:
            continue
        on = {p.id for p in _points(e.item_id)}
        bad = [s for s in e.splits if s not in known and s not in on]
        if bad:
            raise Refused(f"unknown place(s) {', '.join(bad)}: not in splits.json and not a place on "
                          f"{reg[e.item_id].name}")


def extent_preview(extents: list[M.InSeasonExtent], limit: int = 6000) -> dict:
    """WHAT THE EXTENTS BIND (their union), each resolved on its own water by the shared resolver the builder
    runs (`pipeline.atlas.reach.extent.resolve_extent`). Read only. `per_extent[i]` is that extent's count and
    why it binds nothing, if it does not."""
    _check(extents)
    reg, graph = reuse._registry(), reuse._graph()
    total: set[str] = set()
    per: list[dict] = []
    from pipeline.regs.inseason import points as P
    for e in extents:
        try:
            got = P.resolve(reg, graph, e.model_dump(exclude_none=True))
            secs, why, ntrib = set(got["sections"]), None, got["tributaries"]
        except P.Unresolved as ex:
            secs, why, ntrib = set(), str(ex) or "the extent binds nothing", 0
        total |= secs
        per.append({"n_sections": len(secs), "n_tributaries": ntrib, "why": why})
    ids = sorted(total)
    why = next((f"extent {i + 1}: {p['why']}" for i, p in enumerate(per) if p["why"]), None)
    return reuse._json_safe({
        "resolvable": bool(ids) and why is None, "why": why, "n_sections": len(ids), "per_extent": per,
        "truncated": len(ids) > limit,
        "geojson": {"type": "FeatureCollection", "features": reuse._sections_geojson(ids[:limit], "preview")}})


# --------------------------------------------------------------------------- THE WRITE

def _atlas():
    from pipeline.regs.inseason import resolve as R
    return R.load_atlas(reuse._registry(), reuse._graph(), reuse._BUILD)


def resolve_all(by: str = "") -> dict:
    """(T520) RE-RESOLVE every match on the shipped release (after a release promote: the file names an older one),
    through `writes.commit`. Matches that no longer resolve keep their extents and say why (`unresolved`); publish
    refuses them until re-matched. -> {release, unresolved: [...]}."""
    from pipeline.regs.inseason import resolve as R
    rel = R.read_release(R.release_dir(None))
    with writes.LOCK:
        doc = _load_matches()
        new, bad = R.resolve_doc(doc, _atlas(), rel)
        if M.dumps(new) != M.dumps(doc):
            writes.commit([(matches_path(), M.dumps(new))], "inseason-resolve")
    return {"release": rel.name, "unresolved": bad}


def save_match(rid: str, extents, effective: dict | None, note: str, by: str = "", source: str = "owner") -> dict:
    """SET (extents given) or CLEAR (None / []) one row's match, with a history row carrying `note`. Refused:
    no note, an id the feed does not hold (rows or removed), a malformed extent, an unknown water or split, an
    extent that binds nothing. Written by `writes.commit` (backup + atomic) under `writes.LOCK`."""
    note = (note or "").strip()
    if not note:
        raise Refused("a note is required: it is this edit's history row")
    feed = load_feed()
    if feed is None:
        raise Refused(f"no in-season feed at {feed_path()}: nothing to match")
    rows = _feed_rows(feed)
    if rid not in rows:
        raise Refused(f"{rid}: no such in-season row in the feed")
    at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = matches_path()
    with writes.LOCK:
        doc = _load_matches()
        if not extents:
            if rid not in doc.matches:
                raise Refused(f"{rid} is not matched: nothing to clear")
            new = M.clear_match(doc, rid, at=at, by=by, note=note)
        else:
            exts = parse_extents(extents)
            _check(exts)
            # (T520) RESOLVED ONCE, HERE, where the atlas is: FWA ranges + end tokens on the shipped release
            # (`resolve_match` refuses a release not built from this atlas); publish only formats
            from pipeline.regs.inseason import resolve as R
            rel = R.read_release(R.release_dir(None))
            if doc.release and doc.release != rel.name:
                raise Refused(f"the matches are resolved on {doc.release}, the shipped release is {rel.name}: "
                              "re-resolve them first (Re-resolve on the In-season tab), then commit")
            try:
                # (T623 F7, F20) the row's region: a notice covers only fresh water of its own region
                waters, words = R.resolve_match(_atlas(), rel, M.Match(region=str(rows[rid].get("region") or ""),
                                                                       extents=exts), M.split_labels())
            except R.Unresolved as ex:
                raise Refused(f"the extent binds no section on {rel.name}: {ex}")
            eff = None
            if effective and (effective.get("from") or effective.get("to")):
                try:
                    eff = M.Effective.model_validate({k: v for k, v in effective.items() if v})
                except (ValidationError, ValueError) as ex:
                    raise Refused(f"effective: {ex}")
            r = rows[rid]
            try:
                match = M.stamped(M.Match(region=str(r.get("region") or ""), water=str(r.get("water") or ""),
                                          extents=exts, effective=eff, note=note, by=by, at=at, waters=waters,
                                          source=source),
                                  rel.name)
            except (ValidationError, ValueError) as ex:
                raise Refused(str(ex))
            new = M.set_match(doc, rid, match, at=at, by=by, note=note)
            new = new.model_copy(update={"release": rel.name, "words": {**new.words, **words}})
        text = M.dumps(new)
        errs = [p for p in M.problems(M.parse(text), split_ids=split_ids()) if p.startswith(rid)]
        if errs:
            raise Refused("; ".join(errs))
        writes.commit([(path, text)], "inseason-match")
    return row(rid)
