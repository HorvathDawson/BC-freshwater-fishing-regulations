"""THE CHECKLIST (T532): every DFO place and every regulation extent as generated rows (`review.checklist`), the
place's check state, and the queue grouped BY WATER (owner 2026-11-09: "review a whole river or waterbody at one time,
never jump between waters").

    context()             what the build knows, for the row generator: cut labels, built / pending cuts, water names,
                          pins (a cut's point, a water's middle) — read from the served atlas and the curated cuts
    dfo_check(id)         one DFO place: rows, digest, summary, its check state
    rule_check(e, r)      one regulation rule: the same
    queue(source, ...)    every place of a source (dfo | regs | all), grouped by water, with its summary and check state
    progress(...)         done / total over the whole list, by state, per water
    water(key, source)    every place on ONE water — older wordings too — each with its extent, grouped by same place
    water_reach(key)      what each of the water's DFO extents binds, as geometry (the faint lines under the shown one)

READ ONLY. The rows resolve nothing (AGENTS 16): the cuts built are the served atlas's (`dfo._built_cuts`), the
tributary call the builder's own (`classify.wants_tributaries`, `dfo.tributaries_now`). A check is a mark
(`marks.Checklist`), posted through the one write.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping

import reuse
from pipeline.regs.review import checklist as C

#: the most sections a regulation's reach draws (a zone table binds tens of thousands)
REACH_LIMIT = 3000
#: a place's check state, in the order the filters offer them
CHECK_STATES = ("unchecked", "stale", "checked", "earlier", "saved", "unapplied", "wrong")
#: the states that count as DONE (owner 2026-11-09: "the review app doesnt show how many I already did"): a check, a
#: wrong check, and `earlier` — a verified mark made before the checklist that the marks' own staleness still holds
#: `saved` (owner 2026-11-09: "if you save extent with a new cut it should show as complete … just needing cut added"):
#: THE OWNER'S OWN EXTENT SAVE IS A REVIEW (`saved_kind`; owner: "if you save extent you assume it is correct", "i
#: dont want to have to review the items 2 times"): while the place holds the extent he saved — waiting for its new
#: cut, or built as saved — it is done and never asked again. The mark and its verdict are untouched. `unapplied` (a
#: save every cut of which is built but that never reached the place) is NOT done: shown, reported, never re-asked.
DONE_STATES = ("checked", "earlier", "saved", "wrong")
#: the DFO extent states (`dfo._row`) with nothing to check yet: the place leads with its fix, not with ✗ rows
NEEDS_EXTENT = ("unbound", "needs_cut")


class _Lazy(Mapping):
    """a mapping computed key by key (a pin's point is a geometry read; only the rows shown need it)"""

    def __init__(self, fn, keys=None):
        self._fn, self._keys, self._got = fn, keys, {}

    def __getitem__(self, k):
        if k not in self._got:
            self._got[k] = self._fn(k)
        if self._got[k] is None:
            raise KeyError(k)
        return self._got[k]

    def __contains__(self, k):
        try:
            self[k]
            return True
        except KeyError:
            return False

    def __iter__(self):
        return iter(self._keys or ())

    def __len__(self):
        return len(self._keys or ())


def _cut_point(sid: str):
    try:
        p = reuse._split_point(sid)
    except Exception:  # noqa: BLE001 — a cut the atlas cannot place has no pin; its row still shows
        return None
    if p is None:
        return None
    x, y = reuse._to_lonlat()(p.x, p.y)
    return (round(x, 6), round(y, 6))


def _item_point(iid: str):
    it = reuse._registry().get(iid)
    if it is None:
        return None
    lines = [s for s in it.section_ids if not str(s).startswith("lake:")] or list(it.section_ids)
    if not lines:
        return None
    # a pin in the middle of the water: the section nearest the centre of all its sections (ids are in no order)
    geoms = reuse._geoms()
    gs = [g for g in (geoms.get(s_) for s_ in lines) if g is not None and not g.is_empty]
    if not gs:
        return None
    mids = [g.interpolate(0.5, normalized=True) if g.geom_type in ("LineString", "MultiLineString")
            else g.representative_point() for g in gs]
    w = [max(g.length, 1.0) for g in gs]          # weighted by length: a braid's many short pieces do not pull it
    cx = sum(m.x * k for m, k in zip(mids, w)) / sum(w)
    cy = sum(m.y * k for m, k in zip(mids, w)) / sum(w)
    g = min(zip(gs, mids), key=lambda gm: (gm[1].x - cx) ** 2 + (gm[1].y - cy) ** 2)[0]
    p = g.interpolate(0.5, normalized=True) if g.geom_type in ("LineString", "MultiLineString") \
        else g.representative_point()
    x, y = reuse._to_lonlat()(p.x, p.y)
    return (round(x, 6), round(y, 6))


def _end_point(end: str, items: list[str]):
    """a water's natural end: its MOUTH (of several waters, the mouth farthest from the others' lines — where the run
    leaves them) or its SOURCE (the line end farthest from that mouth)"""
    import dfo
    from shapely.geometry import Point
    mouths = [(i, dfo._mouth(i)) for i in items]
    mouths = [(i, m) for i, m in mouths if m is not None]
    if not mouths:
        return None
    lines = {i: [g for _n, _nd, g in dfo._line_sections([i])] for i in items}

    def off(i, m):
        others = [g for j, gs in lines.items() if j != i for g in gs]
        return min((g.distance(m) for g in others), default=0.0)
    mouth = max(mouths, key=lambda im: off(*im))[1]
    p = mouth
    if end == "source":
        ends = [Point(c) for gs in lines.values() for g in gs if g.geom_type == "LineString"
                for c in (g.coords[0], g.coords[-1])]
        if not ends:
            return None
        p = max(ends, key=lambda q: q.distance(mouth))
    x, y = reuse._to_lonlat()(p.x, p.y)
    return (round(x, 6), round(y, 6))


def _point(k: str):
    end, _, rest = k.partition(":")
    if end in ("mouth", "source") and rest:
        return _end_point(end, rest.split("|"))
    return _item_point(k) if ":" in k and not k.startswith("area:") else _cut_point(k)


def _ctx_now() -> C.Context:
    import dfo
    meta, records, bounds = reuse._splits_meta(), reuse._split_records(), reuse._boundary_index()
    labels = {sid: sp.get("label") for sid, (_w, sp) in records.items() if sp.get("label")}
    labels.update({sid: getattr(b, "label", None) for sid, (_i, b) in bounds.items() if getattr(b, "label", None)})
    labels.update({sid: m["label"] for sid, m in meta.items() if m.get("label")})
    built = frozenset(dfo._built_cuts())
    proposed = {}
    for p in dfo._proposals():
        rid = dfo.real_ids().get(p["id"], p["id"])
        n = p["split"]
        proposed[rid] = n.name or n.anchor
    labels.update({k: v for k, v in proposed.items() if k not in labels and v})
    pending = frozenset((set(records) | set(proposed)) - built)
    reg = reuse._registry()
    items = _Lazy(lambda i: getattr(reg.get(i), "name", None) or None)
    return C.Context(labels=labels, items=items, built=built, pending=pending, points=_Lazy(_point))


_CTX = reuse._Memo()


def context() -> C.Context:
    from pipeline.common.curated import CURATED
    return _CTX.get((reuse._stamp((reuse.MARKS_PATH, reuse.SPLITS_JSON_PATH, reuse.REGISTRY_PATH,
                                   CURATED.review.proposal_overrides)), reuse._registry()), _ctx_now)


# --------------------------------------------------------------------------- the check state

def check_state(mark, digest: str, earlier: str | None = None) -> str:
    """unchecked (no mark in force, a defer, or an earlier mark that is not a verified one still holding) · stale (its
    digest is not the place's now: the words or the extent changed) · checked (all correct) · wrong (rows picked
    wrong) · earlier (owner 2026-11-09: a VERIFIED mark made before the checklist — it carries none — that the marks'
    own staleness still calls verified: `earlier` is that status, `marks.status`; it is done, never re-asked).
    No second staleness rule: a mark `marks.status` calls stale is unchecked here."""
    if mark is None or mark.verdict not in ("verified", "wrong"):
        return "unchecked"
    if getattr(mark, "not_variant_of", None):
        return "unchecked"          # taken out of its group: it stands alone and needs its own extent — never done by that
    if getattr(mark, "checklist", None) is None:
        return "earlier" if mark.verdict == "verified" and earlier == "verified" else "unchecked"
    if mark.checklist.digest != digest:
        return "stale"
    return "checked" if mark.verdict == "verified" else "wrong"


def _with_check(rs: list[dict], digest: str, mark, earlier: str | None = None) -> dict:
    """`earlier`: the status the marks' own machinery gives a mark with no checklist (verified / wrong / unsure /
    deferred / stale) — said on the place whatever it is, done only when verified"""
    out = {"rows": rs, "digest": digest, "summary": C.summary(rs), "check": check_state(mark, digest, earlier)}
    if mark is not None and getattr(mark, "checklist", None) is not None:
        out["checked"] = {"date": mark.date, "verdict": mark.verdict, "note": mark.note,
                          "rows": [r.model_dump() for r in mark.checklist.rows]}
    elif mark is not None:
        out["earlier"] = {"date": mark.date, "status": "taken out of its group: it stands alone and needs its own extent"
                          if getattr(mark, "not_variant_of", None) else earlier or getattr(mark.verdict, "value", mark.verdict),
                          "note": mark.note}
    return out


# --------------------------------------------------------------------------- DFO

#: THE WRITE-THROUGH'S WORD ON A DFO PLACE'S LAST SAVE THIS SESSION ("saving" | "saved" | "refused" | "waiting", or
#: None when it holds none): set by the app (`app.py`) — a save is applied in the BACKGROUND, seconds after the mark,
#: and a place read in between must say "being applied", never "not applied" (the owner's Alouette save, 2026-11-09)
apply_outcome = lambda target: None            # noqa: E731
#: every outcome it holds, as a stamp: the places list is recomputed when one changes
apply_stamp = lambda: ()                       # noqa: E731


def saved_kind(mark, fvc_state: str | None, saved_state: str | None, verify: str | None,
               outcome: dict | None = None) -> str | None:
    """WHAT THE OWNER'S EXTENT SAVE MAKES OF THE PLACE, derived from the save itself — the mark in force carries an
    extent fix (no checklist, not a defer, neither T nor R answered n) — against the place now (`dfo.fix_vs_current`,
    `dfo.saved_status`) and the marks' own staleness (`verify`, `marks.status`):

      saved      the place holds the extent he saved (fix == the curated extent: `same` / `applied`), or the save waits
                 only for its new cut (`waits_cut`) — DONE, never asked again
      stale      the marks' own staleness says the place moved since (its words, its rules, or — after his save had
                 landed — its extent is no longer the one he saved): it comes back as changed
      applying   the extent is not yet the one he saved, every cut it names is built, and the write-through is
                 still applying this save (`outcome` saving / waiting / just saved) — DONE, said as being applied
      unapplied  the same, but the write-through REFUSED it, or holds no record of it (a save of an earlier
                 session that never reached the place) — a fault, shown and reported, not his to review again
    None: no such save (the ordinary rules judge the place). Pure."""
    if mark is None or not getattr(mark, "extent_fix", None) or getattr(mark, "checklist", None) is not None:
        return None
    if getattr(mark.verdict, "value", mark.verdict) == "deferred":
        return None
    answers = getattr(mark, "answers", None) or {}
    if answers.get("T") == "n" or answers.get("R") == "n":
        return None
    if fvc_state in ("same", "applied"):
        return "stale" if verify == "stale" else "saved"
    if saved_state == "waits_cut":
        return "saved"
    if saved_state == "not_landed":
        if verify == "stale":
            return "stale"
        return "applying" if (outcome or {}).get("state") in ("saving", "waiting", "saved") else "unapplied"
    return None


def dfo_inputs(loc: dict, water: dict | None, m, verify: str | None = None) -> dict:
    """the place's extent as the rows read it: the curated pieces — or, while the owner's save waits for its cut or
    the write-through (`dfo.saved_status`), the saved pieces, each marked landed or not; and what his save makes of
    the place (`kind`, `saved_kind`)"""
    import dfo
    b = loc.get("binding") or {}
    extents = list(b.get("extents") or [])
    trib = dfo.tributaries_now(loc, water)
    landed = [True] * len(extents)
    fvc = dfo.fix_vs_current(m, loc, water) if m is not None and m.extent_fix else None
    st = dfo.saved_status(m, fvc) if fvc else None
    outcome = apply_outcome(loc.get("location_id")) if st and st["state"] == "not_landed" else None
    kind = saved_kind(m, fvc["state"] if fvc else None, st["state"] if st else None, verify, outcome)
    # a save the place moved away from (`stale`) no longer holds the place: its rows are the extent NOW, to check again
    waiting = bool(st and st["state"] in ("waits_cut", "not_landed") and kind != "stale")
    if waiting:
        now = {dfo._extent_key(x) for x in extents}
        extents = list(fvc["fix"])
        landed = [dfo._extent_key(x) in now for x in extents]
        if fvc["tributaries"]["fix"] is not None:
            trib = fvc["tributaries"]["fix"]
    held = None
    if waiting:
        held = (f"your save of {st['date']} has not landed yet"
                + (f" (it waits for the new cut{'s' if len(st.get('cuts') or []) > 1 else ''} to be built)"
                   if st["state"] == "waits_cut" else " (the write-through has not applied it)")
                + ": a check now would replace it — check the place once it lands, or change it again")
    return {"extents": extents, "landed": landed, "tributaries": trib, "held": held,
            "kind": "saved" if kind == "applying" else kind, "saved": _saved_view(kind, fvc, st, m, outcome),
            "differs": ({"saved": fvc["fix"], "now": fvc["current"]}
                        if kind == "stale" and fvc and fvc["state"] == "replaces" else None),
            "curated": list(b.get("extents") or []), "curated_tributaries": dfo.tributaries_now(loc, water),
            "only": bool(b.get("tributaries_only")), "excludes": list(b.get("tributary_excludes") or [])}


def _saved_view(kind: str | None, fvc: dict | None, st: dict | None, m, outcome: dict | None = None) -> dict | None:
    """`saved`: waits_cut (a cut it names is not built yet), built (built as saved) or applying (the write-through is
    at it); `unapplied`: not_applied, with the write-through's reason when it refused"""
    import dfo
    if kind not in ("saved", "applying", "unapplied"):
        return None
    if kind == "applying":
        return {"state": "applying", "date": m.date, "cuts": []}
    if kind == "unapplied":
        return {"state": "not_applied", "date": m.date, "cuts": [],
                "why": (outcome or {}).get("why") or "the write-through holds no record of this save (an earlier session)"}
    if st and st["state"] == "waits_cut":
        return {"state": "waits_cut", "date": m.date, "cuts": list(st.get("cuts") or [])}
    unbuilt = sorted({s for e in (fvc or {}).get("fix") or [] for s in e.get("splits") or []} - set(dfo._built_cuts()))
    return {"state": "waits_cut" if unbuilt else "built", "date": m.date, "cuts": unbuilt}


def dfo_rows(loc: dict, water: dict | None, m, ctx: C.Context | None = None, verify: str | None = None) -> dict:
    """`verify`: the DFO queue's word for the mark in force (`dfo._verify`, the rows' `verify`); read here when not
    handed in and the mark carries no checklist (an earlier mark)"""
    if verify is None and m is not None and getattr(m, "checklist", None) is None:
        import dfo
        verify = dfo._verify(m, loc, water)["status"]
    src = C.dfo_source(loc, water)
    x = dfo_inputs(loc, water, m, verify)
    rs = C.rows(src, x["extents"], own_items=(water or {}).get("item_ids") or [], ctx=ctx or context(),
                tributaries=x["tributaries"], tributaries_only=x["only"], tributary_excludes=x["excludes"],
                landed=x["landed"])
    # the digest is of the CURATED extent (a save waiting to land is not what a check confirms)
    dg = C.digest(src, x["curated"], tributaries=x["curated_tributaries"], tributaries_only=x["only"],
                  tributary_excludes=x["excludes"])
    out = _with_check(rs, dg, m, verify)
    if x["held"]:
        out["held"] = x["held"]
    if x.get("kind") in ("saved", "unapplied"):
        # the owner's own extent save: done while the place holds it (or waits only for its cut); `unapplied` is a
        # fault of ours — neither is "your earlier mark was: not sure"
        out["check"], out["saved"] = x["kind"], x["saved"]
        out.pop("earlier", None)
    elif x.get("kind") == "stale":
        out["check"] = "stale"
        out.pop("earlier", None)
        if x.get("differs"):
            out["differs"] = x["differs"]
    return out


def digest_as(loc: dict, water: dict | None, src: dict | None = None, src_water: dict | None = None) -> str:
    """A DFO wording's checklist digest — as it binds now, or (`src`) once it holds `src`'s extent and the tributary
    call `src` binds with (`dfo.fix_taking`: an older wording taking the shown place's extent). Its own words, its own
    tributaries-only / carve-outs (no fix changes those)."""
    import dfo
    b = loc.get("binding") or {}
    held = src if src is not None else loc
    return C.digest(C.dfo_source(loc, water), list((held.get("binding") or {}).get("extents") or []),
                    tributaries=dfo.tributaries_now(held, src_water if src is not None else water),
                    tributaries_only=bool(b.get("tributaries_only")),
                    tributary_excludes=list(b.get("tributary_excludes") or []))


def dfo_check(location_id: str) -> dict | None:
    import dfo
    got = dfo._locations().get(location_id)
    if got is None:
        return None
    _s, loc, water = got
    return reuse._json_safe(dfo_rows(loc, water, dfo._states().get(location_id)))


# --------------------------------------------------------------------------- regulations

def rule_rows(entry: dict, rule: dict, mark, ctx: C.Context | None = None) -> dict:
    from pipeline.atlas.reach.classify import wants_tributaries
    src = C.rule_source(entry, rule)
    only = bool(rule.get("tributaries_only"))
    trib = bool(wants_tributaries(rule, entry)) and not only
    exts = [x for x in rule.get("extents") or [] if isinstance(x, dict)]
    tex = [x for x in rule.get("tributary_excludes") or [] if isinstance(x, dict)]
    rs = C.rows(src, exts, own_items=entry.get("matched") or [], ctx=ctx or context(), tributaries=trib,
                tributaries_only=only, tributary_excludes=tex)
    dg = C.digest(src, exts, tributaries=trib, tributaries_only=only, tributary_excludes=tex)
    return _with_check(rs, dg, mark, _rule_earlier(entry, mark))


def _rule_earlier(entry: dict, mark) -> str | None:
    """An EARLIER rule mark's status (one with no checklist), by the rule marks' own staleness (`marks.status` on the
    entry's prints now, a landed fix holding — what the Entries queue shows). A verified one counts only with its
    place check answered yes (P = y): the checklist is the place's check."""
    if mark is None or getattr(mark, "checklist", None) is not None:
        return None
    from pipeline.regs.review import marks as M
    st = M.status(mark, reuse._prints(entry), applied=reuse.applied_keys())["status"]
    st = getattr(st, "value", st)
    if st == "verified" and (mark.answers or {}).get("P") != "y":
        return "verified without the place check"
    return st


def _rule_marks() -> dict:
    from pipeline.regs.review import marks as M
    return {(m.target, m.rule_id): m for m in M.latest(M.load(reuse.MARKS_PATH)).values() if m.scope == "rule"}


def rule_check(entry_id: str, rule_id: str) -> dict | None:
    found = reuse._find(entry_id)
    if found is None:
        return None
    e = found[1]
    r = next((x for x in e.get("rules") or [] if isinstance(x, dict) and x.get("rule_id") == rule_id), None)
    if r is None:
        return None
    out = rule_rows(e, r, _rule_marks().get((entry_id, rule_id)))
    # what the rule binds, drawn in the extent's one colour: the builder's own reach (`rule_resolved_reach`)
    got = reuse.rule_resolved_reach(entry_id, rule_id, limit=REACH_LIMIT)
    ids = list(got.get("sections") or [])[:REACH_LIMIT]
    out["reach"] = {"n_sections": got.get("n_total"), "truncated": len(got.get("sections") or []) > REACH_LIMIT,
                    "geojson": {"type": "FeatureCollection", "features": reuse._sections_geojson(ids, "preview")}}
    out["source"] = {"entry_id": entry_id, "rule_id": rule_id, "name": e.get("display_name") or e.get("name"),
                     "verbatim": r.get("verbatim") or "", "region": e.get("region"),
                     "pages": [p for p in e.get("source_pages") or [] if isinstance(p, int)]}
    return reuse._json_safe(out)


# --------------------------------------------------------------------------- the queue, by water

def _dfo_places() -> list[dict]:
    """one row per DFO place (a historic rewording rides its place, as the DFO queue lists them; `older_unconfirmed`:
    how many of its older wordings have no check in force)"""
    import dfo
    states, locs, ctx, groups = dfo._states(), dfo._locations(), context(), dfo._groups()
    verify = {r["id"]: r["verify"] for r in dfo._rows()}
    out = []
    for r in dfo._rows():
        if r["lake_area"] or (r["status"] != "active" and not r["historic_only"]):
            continue
        g = r.get("group")
        if g and g["canonical"] != r["id"] and g["canonical"] in locs:
            continue
        _s, loc, water = locs[r["id"]]
        got = dfo_rows(loc, water, states.get(r["id"]), ctx, r["verify"])
        items = list((water or {}).get("item_ids") or [])
        # owner 2026-11-09: an older wording is checked only when the owner ticks it "same place" — one not checked
        # (or whose check went stale) keeps its place in the queue, flagged on the place it rides
        older = [m["id"] for m in ((groups.get(r["id"]) or {}).get("members") or [])
                 if m["id"] != r["id"] and m["id"] in locs]
        unconfirmed = sum(1 for o in older
                          if dfo_rows(locs[o][1], locs[o][2], states.get(o), ctx, verify.get(o))["check"]
                          not in DONE_STATES)
        out.append({"source": "dfo", "id": r["id"], "water": r["water"], "water_key": _water_key(items, r["water"]),
                    "items": items,
                    "region": r["region"], "text": r["text"] or "(the whole water)", "summary": got["summary"],
                    "check": got["check"], "new": r["verify"] == "unmarked",
                    # what the place needs before any row can be checked: no extent (unbound), or a cut (needs_cut)
                    "state": r["state"], "proposal": bool(r["suggested"] or r["fix_proposed"]),
                    **({"earlier": got["earlier"]} if "earlier" in got else {}),
                    **({"older": len(older), "older_unconfirmed": unconfirmed} if older else {})})
    return out


def _regs_places() -> list[dict]:
    marks, ctx = _rule_marks(), context()
    out = []
    for region, e in reuse._all_entries():
        for r in e.get("rules") or []:
            if not isinstance(r, dict) or not r.get("extents"):
                continue
            got = rule_rows(e, r, marks.get((e["entry_id"], r.get("rule_id"))), ctx)
            items = list(e.get("matched") or [])
            name = e.get("display_name") or e.get("name") or e["entry_id"]
            out.append({"source": "regs", "id": f"{e['entry_id']}#{r.get('rule_id')}", "entry_id": e["entry_id"],
                        "rule_id": r.get("rule_id"), "water": name, "water_key": _water_key(items, name),
                        "items": items,
                        "region": str(e.get("region") or region), "text": r.get("verbatim") or "",
                        "summary": got["summary"], "check": got["check"],
                        **({"earlier": got["earlier"]} if "earlier" in got else {})})
    return out


def _water_key(items: list[str], name: str) -> str:
    """ONE WATER: its registry waters (a DFO place and a regulation row on the same river sit together), else its
    name"""
    return "|".join(sorted(items)) if items else f"name:{name.strip().lower()}"


_PLACES = reuse._Memo()


def _places() -> list[dict]:
    import dfo
    return _PLACES.get((reuse._stamp((reuse.MARKS_PATH,)), dfo._rows(), context(), reuse.regions(), tuple(apply_stamp())),
                       lambda: by_water(one_water(_dfo_places() + _regs_places())))


def one_water(places: list[dict]) -> list[dict]:
    """ONE WATER ACROSS BOTH SOURCES: places sharing a registry water are one water (the DFO "Chilliwack/Vedder River
    (including Sumas River)" and the book's "CHILLIWACK / VEDDER RIVERS" rows), keyed by the first place's key, named
    by its DFO place when it has one. Places with no water keep their own key. Pure."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for p in places:
        its = p.get("items") or []
        for i in its[1:]:
            parent[find(i)] = find(its[0])
    key: dict[str, str] = {}
    out = []
    for p in places:
        its = p.get("items") or []
        root = find(its[0]) if its else p["water_key"]
        out.append({**p, "water_key": key.setdefault(root, p["water_key"])})
    name: dict[str, str] = {}
    for p in sorted(out, key=lambda x: x["source"] != "dfo"):      # a DFO place names its water, else the first row
        name.setdefault(p["water_key"], p["water"])
    return [{**p, "water": name[p["water_key"]], "place_water": p["water"]} for p in out]


def by_water(places: list[dict]) -> list[dict]:
    """THE QUEUE BY WATER (owner 2026-11-09): every place on one water before the next water — a water's DFO places
    and its regulation extents together, DFO first — waters in the order their first place comes. Pure."""
    order: list[str] = []
    by: dict[str, list[dict]] = {}
    for p in places:
        if p["water_key"] not in by:
            by[p["water_key"]] = []
            order.append(p["water_key"])
        by[p["water_key"]].append(p)
    return [p for k in order for p in sorted(by[k], key=lambda x: x["source"] != "dfo")]


def _matches(p: dict, want: str | None) -> bool:
    if not want:
        return True
    if want in CHECK_STATES:
        return p["check"] == want
    if want == "todo":
        return p["check"] in ("unchecked", "stale") or p.get("older_unconfirmed", 0) > 0
    if want == "bad":
        return p["summary"]["bad"] > 0
    if want == "needs":                      # a DFO place with no extent yet, or waiting on a cut
        return p.get("state") in NEEDS_EXTENT
    raise ValueError(f"filter is one of todo, bad, needs, {', '.join(CHECK_STATES)}")


def queue(source: str = "dfo", filter: str | None = None, q: str | None = None) -> list[dict]:
    """every place of `source` (dfo | regs | all), grouped by water; `filter` keeps a WHOLE WATER when any of its
    places matches (a water is reviewed at one time), `q` a substring of the water or the words"""
    if source not in ("dfo", "regs", "all"):
        raise ValueError("source is dfo, regs or all")
    rows = [p for p in _places() if source == "all" or p["source"] == source]
    keep = {p["water_key"] for p in rows if _matches(p, filter)}
    if q and q.strip():
        want = q.strip().casefold()
        keep &= {p["water_key"] for p in rows if want in p["water"].casefold() or want in p["text"].casefold()}
    return [p for p in rows if p["water_key"] in keep]


def progress(source: str = "dfo") -> dict:
    """done / total over the WHOLE list (filters never change it), the counts by check state and by worst row
    state, and per water. DONE (`DONE_STATES`): checked, checked wrong, or verified earlier and still holding."""
    return progress_of([p for p in _places() if source == "all" or p["source"] == source])


def progress_of(rows: list[dict]) -> dict:
    """`progress` over the given places. Pure."""
    waters: dict[str, dict] = {}
    for p in rows:
        w = waters.setdefault(p["water_key"], {"water": p["water"], "total": 0, "done": 0})
        w["total"] += 1
        w["done"] += p["check"] in DONE_STATES
    done = sum(1 for p in rows if p["check"] in DONE_STATES)
    return {"total": len(rows), "done": done, "left": len(rows) - done,
            "needs": sum(1 for p in rows if p.get("state") in NEEDS_EXTENT),
            "by_check": dict(Counter(p["check"] for p in rows)),
            "by_worst": dict(Counter(p["summary"]["worst"] for p in rows)),
            "rows": dict(sum((Counter({s: p["summary"][s] for s in C.STATES}) for p in rows), Counter())),
            "older_unconfirmed": sum(p.get("older_unconfirmed", 0) for p in rows),
            "waters": waters}


# --------------------------------------------------------------------------- one water, all in sight

def same_place(places: list[dict]) -> dict[str, dict]:
    """WHICH OF A WATER'S PLACES ARE THE SAME PLACE (owner 2026-11-09: "have them grouped then you confirm them"), from
    the data that exists — never a new matcher: an older wording rides its place (`older_of`, `dfo._groups`), DFO lists
    it as a rewording (`duplicate_of`), or the two hold the IDENTICAL extent (`extent_key`, same source; a place with no
    extent matches nothing by it). -> id -> {"same": the group's first id in list order, "why": [...]}. A grouping is
    a prior shown to the owner; it confirms nothing. Pure."""
    ids = [p["id"] for p in places]
    parent = {i: i for i in ids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    why: dict[str, list[str]] = {i: [] for i in ids}

    def join(a: str, b: str, word: str) -> None:
        if a not in parent or b not in parent or a == b:
            return
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb, key=ids.index)] = min(ra, rb, key=ids.index)
        for x in (a, b):
            if word not in why[x]:
                why[x].append(word)
    by_extent: dict[tuple, str] = {}
    for p in places:
        if p.get("older_of") and not p.get("apart"):
            join(p["id"], p["older_of"], "your decision: linked as the same place" if p.get("confirmed")
                 else "our proposal: an older wording, its words alike")
        if p.get("duplicate_of"):
            # WHOSE CLAIM: `duplicate_confirmed` true is the owner's decision; unconfirmed is our matcher's proposal
            join(p["id"], p["duplicate_of"], "your decision: linked as the same place" if p.get("confirmed")
                 else "our proposal: matched as a rewording")
        if p.get("extent_key") and not p.get("apart"):       # a wording he took out is never put back by its extent
            first = by_extent.setdefault((p["source"], p["extent_key"]), p["id"])
            join(p["id"], first, "the identical extent")
    return {i: {"same": find(i), "why": why[i]} for i in ids}


def _extent_key(extents: list[dict], tributaries, only: bool = False) -> str:
    import dfo
    return "" if not extents else "|".join(sorted(map(dfo._extent_key, extents))) + f"#{tributaries}#{only}"


def water_key_of(place_id: str) -> str | None:
    """the water a place is on: its own queue row's, or — an older wording — the place it rides"""
    import dfo
    by = {p["id"]: p["water_key"] for p in _places()}
    if place_id in by:
        return by[place_id]
    g = dfo._groups().get(place_id)
    return by.get(g["canonical"]) if g else None


def water(water_key: str, source: str = "dfo") -> dict | None:
    """EVERY PLACE ON ONE WATER, ALL IN SIGHT (owner 2026-11-09: "for a river show all historical boundaries too"): the
    queue's places on it (DFO first, then its regulation extents when `source` takes them) and, under each DFO place,
    its older wordings — each with its words, its extent now (the frontend says it in words: `cut_labels`,
    `item_names`), whether it is on the live page, its check state, and the group it sits in (`same_place`). Read
    only."""
    import dfo
    mine = [p for p in _places() if p["water_key"] == water_key and (source == "all" or p["source"] == source)]
    if not mine:
        return None
    from pipeline.regs.review import dfo as D
    locs, states, ctx, groups = dfo._locations(), dfo._states(), context(), dfo._groups()
    rows = {r["id"]: r for r in dfo._rows()}
    rules = None
    out: list[dict] = []

    def dfo_place(lid: str, queued: dict | None, older_of: str | None) -> dict:
        _s, loc, w = locs[lid]
        r = rows.get(lid) or {}
        b = loc.get("binding") or {}
        exts = list(b.get("extents") or [])
        got = queued or dfo_rows(loc, w, states.get(lid), ctx, r.get("verify"))
        return {"source": "dfo", "id": lid, "text": (loc.get("source_text") or {}).get("specific_area") or "",
                "live": (loc.get("status") or "active") == "active", "check": got["check"],
                "bad": got["summary"]["bad"], "state": r.get("state"), "extents": exts,
                "tributaries": dfo.tributaries_now(loc, w),
                "extent_key": _extent_key(exts, dfo.tributaries_now(loc, w), bool(b.get("tributaries_only"))),
                "older_of": older_of,
                # the entry model's own link: confirmed true = the owner's decision, None = proposed by our matcher,
                # false = CONTESTED (taken out: it stands alone)
                "duplicate_of": loc.get("duplicate_of") if loc.get("duplicate_confirmed") is not False else None,
                "confirmed": loc.get("duplicate_confirmed") is True, "apart": loc.get("duplicate_confirmed") is False,
                # what a mark on it sends back as `extent_before` (T525 R3: refused when the file moved since)
                "seen": dfo.seen_extent(loc),
                # the last date DFO's page carried this wording (`review.dfo.location_wordings`)
                "last_seen": max((d_ for w_ in D.location_wordings(loc) for d_ in w_.get("dates") or []), default=None),
                "proposal": bool(r.get("suggested") or r.get("fix_proposed")),
                **({"earlier": got["earlier"]} if "earlier" in got else {})}
    for p in mine:
        if p["source"] == "dfo":
            out.append(dfo_place(p["id"], p, None))
            for m in (groups.get(p["id"]) or {}).get("members") or []:
                if m["id"] != p["id"] and m["id"] in locs:
                    out.append(dfo_place(m["id"], None, p["id"]))
        else:
            if rules is None:
                rules = {(e["entry_id"], r.get("rule_id")): (e, r) for _rg, e in reuse._all_entries()
                         for r in e.get("rules") or [] if isinstance(r, dict)}
            e, r = rules[(p["entry_id"], p["rule_id"])]
            exts = [x for x in r.get("extents") or [] if isinstance(x, dict)]
            out.append({"source": "regs", "id": p["id"], "text": p["text"], "live": True, "check": p["check"],
                        "bad": p["summary"]["bad"], "state": None, "extents": exts, "tributaries": None,
                        "extent_key": f"{_extent_key(exts, None, bool(r.get('tributaries_only')))}", "older_of": None,
                        "duplicate_of": None, "confirmed": False, "apart": False, "proposal": False, "name": p.get("place_water") or p["water"],
                        **({"earlier": p["earlier"]} if "earlier" in p else {})})
    same = same_place(out)
    out = [{**p, **same[p["id"]]} for p in out]
    view = {"water_key": water_key, "water": mine[0]["water"], "places": out}
    return reuse._json_safe({**view, "cut_labels": dfo.cut_labels(view), "item_names": dfo.item_names(view)})


#: the most sections one place's extent draws faintly (the water's own line, never its tributaries)
WATER_REACH_LIMIT = 1500
_WATER_REACH = reuse._Memo()


def water_reach(water_key: str) -> dict | None:
    """WHAT EACH OF THE WATER'S DFO EXTENTS BINDS, as geometry (the builder's own `dfo.reach`, tributaries not walked):
    one reach per distinct extent, each feature naming the places that hold it (`places`) — drawn faintly under the
    shown place's, so every boundary DFO has used on the river is in sight. Regulation extents are not drawn here (a
    zone table binds tens of thousands of sections). `failed`: the places whose extent could not be drawn, said."""
    import dfo
    got = water(water_key, "dfo")
    if got is None:
        return None
    # kept per water while the marks / DFO rows / served atlas hold
    held = _WATER_REACH.get((reuse._stamp((reuse.MARKS_PATH,)), dfo._rows(), context()), dict)
    if water_key in held:
        return held[water_key]
    by: dict[str, list[str]] = {}
    for p in got["places"]:
        if p["extent_key"]:
            by.setdefault(p["extent_key"], []).append(p["id"])
    feats, failed = [], []
    for ids in by.values():
        try:
            r = dfo.reach(ids[0], limit=WATER_REACH_LIMIT)
        except Exception as ex:  # noqa: BLE001 — said on the screen (`failed`), the other extents still draw
            failed.append({"id": ids[0], "why": f"{type(ex).__name__}: {ex}"})
            continue
        if not r or not r.get("resolvable"):
            failed.append({"id": ids[0], "why": (r or {}).get("why") or "no reach"})
            continue
        for f in (r["geojson"] or {}).get("features") or []:
            feats.append({**f, "properties": {**(f.get("properties") or {}), "places": ids}})
    held[water_key] = reuse._json_safe({"type": "FeatureCollection", "features": feats, "failed": failed})
    return held[water_key]
