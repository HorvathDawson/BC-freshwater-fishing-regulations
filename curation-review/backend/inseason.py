"""T230: THE IN-SEASON TAB — the provincial in-season feed's rows and WHERE each applies.

The feed is the T225 worker's `inseason-feed/1` (`GENERATED.inseason / "feed" / "index.json"`, or
`CURATION_INSEASON_FEED`); the matches are `data/curated/inseason/matches.json` (`CURATION_INSEASON_MATCHES`),
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
    env = os.environ.get("CURATION_INSEASON_MATCHES")
    return Path(env) if env else M.default_path()


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
    """Every split id in the splits.json the app reads (`CURATION_SPLITS_JSON` honoured)."""
    return set(reuse._split_records())


def _load_matches() -> M.Matches:
    """The curated matches, parsed (a missing file RAISES — AGENTS 37)."""
    return M.parse(matches_path().read_text(encoding="utf-8"))


def _in_effect(frm: str | None, to: str | None, today: str) -> bool | None:
    if not frm and not to:
        return None
    return (not frm or frm <= today) and (not to or today <= to)


def _extent_view(e: M.InSeasonExtent, labels: dict[str, str]) -> dict:
    it = reuse._registry().get(e.item_id)
    return {"op": e.op, "item_id": e.item_id, "splits": list(e.splits),
            "item_name": it.name if it else None,
            "split_labels": [labels.get(s) or s for s in e.splits]}


def _labels() -> dict[str, str]:
    return {sid: str(sp.get("label") or sid) for sid, (_w, sp) in reuse._split_records().items()}


def _guess_view(g: dict | None, labels: dict[str, str]) -> dict | None:
    """auto's BEST GUESS for an unmatched row (`Matches.guesses`): pre-fills the editor, never applied."""
    if not g:
        return None
    exts = []
    for e in g.get("extents") or []:
        try:
            exts.append(_extent_view(M.InSeasonExtent.model_validate(e), labels))
        except (ValidationError, ValueError):
            continue
    return {"why": g.get("why") or "", "evidence": g.get("evidence"), "extents": exts}


def _row_view(rid: str, row: dict | None, m: M.Match | None, history: list[dict], today: str,
              labels: dict[str, str], guess: dict | None = None) -> dict:
    eff = m.effective if m else None
    frm = (eff.from_ if eff and eff.from_ else None) or (row or {}).get("from")
    to = (eff.to if eff and eff.to else None) or (row or {}).get("to")
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
            # owner add-on 2026-10-28: an AUTO match (`pipeline.regs.inseason.auto`) says so, with its evidence
            "source": m.source, "confidence": m.confidence, "evidence": m.evidence},
        "guess": guess,
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
    labels = _labels() if (doc.matches or doc.guesses) else {}
    hist: dict[str, list[dict]] = {}
    for h in doc.history:
        hist.setdefault(h.id, []).append(h.model_dump(exclude_none=True))
    if feed is None:
        return {"available": False, "message": f"no in-season feed at {feed_path()} — the T225 worker's "
                "feeds/inseason/index.json has not been copied here", "source": None, "fetchedAt": None,
                "rows": [_row_view(rid, None, m, hist.get(rid, []), today, labels)
                         for rid, m in sorted(doc.matches.items())],
                "progress": {"matched": 0, "unmatched": 0, "total": 0, "removed": 0,
                             "stale": len(doc.matches)}}
    rows = _feed_rows(feed)
    out = [_row_view(rid, r, doc.matches.get(rid), hist.get(rid, []), today, labels,
                     None if rid in doc.matches else _guess_view(doc.guesses.get(rid), labels))
           for rid, r in rows.items()]
    out += [_row_view(rid, None, m, hist.get(rid, []), today, labels)
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

def item_splits(item_id: str) -> dict | None:
    """The splits.json splits on one water (`reuse._item_split_source`, the review app's own answer) plus
    the built boundaries on it that splits.json holds, as labelled points. None: no such item."""
    import dfo
    it = reuse._registry().get(item_id)
    if it is None:
        return None
    known = split_ids()
    ids = [sp["id"] for sp in reuse._item_split_source(it) if sp.get("id") in known]
    ids += [b.id for b in (it.boundaries or ()) if b.id in known]
    feats, unplaced = [], []
    for sid in dict.fromkeys(ids):
        f = dfo._split_feature(sid, rules=[], to_verify=False, item_id=item_id)
        if f is None:
            continue
        (unplaced.append(f["unplaced"]) if "unplaced" in f else feats.append(f))
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
    """Every item in this build's registry, every split in splits.json."""
    reg, known = reuse._registry(), split_ids()
    for e in extents:
        if e.item_id not in reg:
            raise Refused(f"{e.item_id}: no such water in this build's registry")
        bad = [s for s in e.splits if s not in known]
        if bad:
            raise Refused(f"unknown split(s) {', '.join(bad)}: not in splits.json")


def extent_preview(extents: list[M.InSeasonExtent], limit: int = 6000) -> dict:
    """WHAT THE EXTENTS BIND (their union), each resolved on its own water by the shared resolver the builder
    runs (`pipeline.atlas.reach.extent.resolve_extent`). Read only. `per_extent[i]` is that extent's count and
    why it binds nothing, if it does not."""
    _check(extents)
    reg, graph = reuse._registry(), reuse._graph()
    total: set[str] = set()
    per: list[dict] = []
    for e in extents:
        reasons: list = []
        got = reuse._resolve.resolve_extent(reg, graph, [e.item_id], e.model_dump(), reasons=reasons)
        secs = set((got or {}).get("sections") or ())
        total |= secs
        why = None if secs else ("; ".join(f"{c}{': ' + d if d else ''}" for c, d in reasons)
                                 or "the extent binds nothing")
        per.append({"n_sections": len(secs), "why": why})
    ids = sorted(total)
    why = next((f"extent {i + 1}: {p['why']}" for i, p in enumerate(per) if p["why"]), None)
    return reuse._json_safe({
        "resolvable": bool(ids) and why is None, "why": why, "n_sections": len(ids), "per_extent": per,
        "truncated": len(ids) > limit,
        "geojson": {"type": "FeatureCollection", "features": reuse._sections_geojson(ids[:limit], "preview")}})


# --------------------------------------------------------------------------- THE WRITE

def save_match(rid: str, extents, effective: dict | None, note: str, by: str = "") -> dict:
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
            prev = extent_preview(exts, limit=0)
            if not prev["resolvable"]:
                raise Refused(f"the extent binds no section: {prev['why']}")
            eff = None
            if effective and (effective.get("from") or effective.get("to")):
                try:
                    eff = M.Effective.model_validate({k: v for k, v in effective.items() if v})
                except (ValidationError, ValueError) as ex:
                    raise Refused(f"effective: {ex}")
            r = rows[rid]
            try:
                match = M.Match(region=str(r.get("region") or ""), water=str(r.get("water") or ""),
                                extents=exts, effective=eff, note=note, by=by, at=at)
            except (ValidationError, ValueError) as ex:
                raise Refused(str(ex))
            new = M.set_match(doc, rid, match, at=at, by=by, note=note)
        text = M.dumps(new)
        errs = [p for p in M.problems(M.parse(text), split_ids=split_ids()) if p.startswith(rid)]
        if errs:
            raise Refused("; ".join(errs))
        writes.commit([(path, text)], "inseason-match")
    return row(rid)
