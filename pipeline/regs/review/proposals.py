"""T185: REVIEW MARKS -> PROPOSED EDITS the owner accepts, never a tool writing curated files.

    marks.jsonl (read only)  ->  proposals (one per mark, one per new split)  ->  owner decides in
    the review app (a `proposal` mark: P y accept / P n reject)  ->  `apply_proposals` (the owner
    runs it) lands each ACCEPTED one as its own commit

What a mark proposes, structured (`marks.Mark`):

    extent_fix        (T191)  an entry's / a rule's / a DFO location's extent -> `extents` (and
                              `includes_tributaries` / `binding.tributaries` when it says so)
    tributaries       (T189)  include / exclude -> `includes_tributaries` on the entry or the rule
    new_splits        (T164)  a cut splits.json lacks -> a split record, ONE proposal per distinct
                              cut across every mark (`marks.proposals`, T210)
    lake_split_later  (T189)  listed (the deferred lake splits, RULINGS K5) — no curated edit

THE ONE MARK -> ONE PATCH rule: every mark in force that carries an extent fix or a tributary call
is one proposal (all its edits, one file); each new split is one proposal on splits.json, and a
mark's proposal that names it DEPENDS on it. A proposal holds STRUCTURED edits (file, entry / rule /
location, field, before, after) and the unified diff they make against the files NOW; its `digest`
covers the edits (before and after), so a decision taken on one proposal never applies another.

WRITES ONLY under `handoff/PLAN/curation-review/proposals/` (index.json + one .patch per proposal),
and — `side` — patched COPIES in a side directory the caller names (never under data/curated).
`apply_proposals` is the one writer of the curated files, and only the owner runs it.

    python -m pipeline.regs.review.proposals build [--check]      # the proposals (+ cheap check)
    python -m pipeline.regs.review.proposals side <id> --out DIR  # patched copies for a side rebuild

`--check` runs the in-memory `build_reach` path the review app's extent preview uses (T191) on the
entry before and after, per rule — sections gained / lost, km — and prints, per proposal, the full
side-rebuild + `binding_diff` chain (the owner's step; nothing here runs it).
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import difflib
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Optional

REPO = Path(__file__).resolve().parents[3]
#: where the proposals are written (index.json + <id>.patch) — handoff, never curated data
OUT_DIR = REPO / "handoff" / "PLAN" / "curation-review" / "proposals"
INDEX = "index.json"
#: the review app's backend: the in-memory binding path (`reuse.binding_change`) lives there
BACKEND = REPO / "curation-review" / "backend"
#: the proposal kinds
KINDS = ("new_split", "extent", "lake_split_later")


def _slug(s: str, n: int = 60) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")[:n].strip("_")


def _h(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def _rel(p: Path) -> str:
    try:
        return str(Path(p).resolve().relative_to(REPO.resolve()))
    except ValueError:
        return str(p)


# --------------------------------------------------------------------------- the sources

@dataclass
class Sources:
    """The curated files a proposal is made against (read only here) and the build whose registry
    names a new split's anchor (a tributary's WSC, a lake's wbk). `build` None or missing: a
    tributary / lake anchor falls back to the point the mark stamped, and says so."""
    entries_dir: Path
    dfo_dir: Path
    splits_path: Path
    build: Optional[Path] = None
    _reg: Optional[dict] = field(default=None, repr=False)
    _texts: dict = field(default_factory=dict, repr=False)

    @classmethod
    def live(cls, build: Optional[Path] = None) -> "Sources":
        from pipeline.common.curated import CURATED, GENERATED
        return cls(entries_dir=Path(CURATED.regulations.entries.catalogue),
                   dfo_dir=Path(CURATED.regulations.entries.dfo_salmon),
                   splits_path=Path(CURATED.waters.splits),
                   build=Path(build) if build else GENERATED.build())

    def text(self, path: Path) -> str:
        p = Path(path)
        if p not in self._texts:
            self._texts[p] = p.read_text(encoding="utf-8")
        return self._texts[p]

    def doc(self, path: Path) -> dict:
        return json.loads(self.text(path))

    def item(self, iid: str) -> Optional[dict]:
        """A registry item as registry.json holds it (raw), or None."""
        if self._reg is None:
            p = Path(self.build or "") / "registry.json"
            self._reg = ({i["id"]: i for i in json.loads(p.read_text(encoding="utf-8"))["items"]}
                         if self.build and p.exists() else {})
        return self._reg.get(iid)


def _region_files(d: Path) -> list[Path]:
    return sorted(Path(d).glob("region-*.json"))


def _find_entry(src: Sources, entry_id: str) -> Optional[tuple[Path, dict]]:
    for p in _region_files(src.entries_dir):
        for e in src.doc(p).get("entries") or []:
            if e.get("entry_id") == entry_id:
                return p, e
    return None


def _find_location(src: Sources, location_id: str) -> Optional[tuple[Path, dict]]:
    for p in _region_files(src.dfo_dir):
        for loc in src.doc(p).get("locations") or []:
            if loc.get("location_id") == location_id:
                return p, loc
    return None


# --------------------------------------------------------------------------- text

def _indent(text: str) -> int:
    lines = text.splitlines()
    return (len(lines[1]) - len(lines[1].lstrip(" "))) or 1 if len(lines) > 1 else 1


def _dump(doc, like: str) -> str:
    return json.dumps(doc, indent=_indent(like), ensure_ascii=False) + "\n"


def _splice(text: str, before: str, after: str) -> str:
    """`text` with the change `before` -> `after` made in it, where `before` is `text` as the
    serialiser writes it. A file whose hand-kept spelling differs (splits.json writes a few anchors
    on one line) keeps it: only the changed lines move. Falls back to `after` whole when a change
    lands on a line the serialiser spells differently."""
    if text == before:
        return after
    T, A, B = (s.splitlines(keepends=True) for s in (text, before, after))
    at: dict[int, int] = {}
    for blk in difflib.SequenceMatcher(None, A, T, autojunk=False).get_matching_blocks():
        for k in range(blk.size):
            at[blk.a + k] = blk.b + k
    out: list[str] = []
    pos = 0
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, A, B, autojunk=False).get_opcodes():
        if tag == "equal":
            continue
        if i2 > i1:
            ts = [at.get(i) for i in range(i1, i2)]
            if None in ts or ts != list(range(ts[0], ts[0] + len(ts))):
                return after
            t1, t2 = ts[0], ts[-1] + 1
        elif i1 in at:
            t1 = t2 = at[i1]
        elif i1 - 1 in at:
            t1 = t2 = at[i1 - 1] + 1
        else:
            return after
        if t1 < pos:
            return after
        out += T[pos:t1] + B[j1:j2]
        pos = t2
    return "".join(out + T[pos:])


def unified(rel: str, before: str, after: str) -> str:
    return "".join(difflib.unified_diff(before.splitlines(keepends=True),
                                        after.splitlines(keepends=True),
                                        f"a/{rel}", f"b/{rel}", n=3))


# --------------------------------------------------------------------------- edits

_MISSING = "__absent__"


def _target_of(doc: dict, ed: Mapping) -> Optional[dict]:
    """The dict an edit sets a field on, inside one file's document."""
    if ed["kind"] == "catalogue":
        e = next((x for x in doc.get("entries") or [] if x.get("entry_id") == ed["entry_id"]), None)
        if e is None or ed["scope"] == "entry":
            return e
        return next((r for r in e.get("rules") or [] if r.get("rule_id") == ed["scope"]), None)
    if ed["kind"] == "dfo":
        loc = next((x for x in doc.get("locations") or []
                    if x.get("location_id") == ed["location_id"]), None)
        return None if loc is None else loc.setdefault("binding", {})
    return None


def apply_edit(doc: dict, ed: Mapping, *, strict: bool = True) -> None:
    """Make one structured edit in a file's document. `strict`: the field must hold `before`
    (a proposal made on another state of the file is refused, never applied over it)."""
    if ed["kind"] == "split":
        wbs = doc.setdefault("waterbodies", [])
        wb = next((w for w in wbs if w.get("applies_to") == ed["waterbody"]["applies_to"]), None)
        if wb is None:
            wb = {"name": ed["waterbody"]["name"], "applies_to": ed["waterbody"]["applies_to"],
                  "splits": []}
            wbs.append(wb)
        if any(sp.get("id") == ed["split"]["id"] for w in wbs for sp in w.get("splits") or []):
            raise ValueError(f"splits.json already has a split {ed['split']['id']}")
        wb.setdefault("splits", []).append(copy.deepcopy(ed["split"]))
        return
    t = _target_of(doc, ed)
    if t is None:
        raise ValueError(f"no {ed.get('entry_id') or ed.get('location_id')} / {ed.get('scope')} "
                         "in the file")
    now = t.get(ed["field"], _MISSING)
    if strict and now != ed["before"]:
        raise ValueError(f"{ed.get('entry_id') or ed.get('location_id')} {ed.get('scope', '')} "
                         f"{ed['field']} is {now!r} now, the proposal was made on {ed['before']!r}")
    if ed["after"] == _MISSING:
        t.pop(ed["field"], None)
    else:
        t[ed["field"]] = copy.deepcopy(ed["after"])


def materialize(props: Iterable[Mapping], src: Sources, *, strict: bool = True) -> dict[Path, str]:
    """THE FILES' NEW TEXT with every edit of `props` made, in order: {path: text} for each file
    they touch (a file's untouched lines keep their spelling, `_splice`). Writes nothing."""
    docs: dict[Path, dict] = {}
    for p in props:
        for ed in p["edits"]:
            path = _resolve_file(ed, src)
            if path not in docs:
                docs[path] = src.doc(path)
            apply_edit(docs[path], ed, strict=strict)
    out: dict[Path, str] = {}
    for path, doc in docs.items():
        text = src.text(path)
        out[path] = _splice(text, _dump(json.loads(text), text), _dump(doc, text))
    return out


def _resolve_file(ed: Mapping, src: Sources) -> Path:
    """An edit's file in `src` (its name inside the source dir, so a side copy resolves too)."""
    name = Path(ed["file"]).name
    if ed["kind"] == "catalogue":
        return Path(src.entries_dir) / name
    if ed["kind"] == "dfo":
        return Path(src.dfo_dir) / name
    return Path(src.splits_path)


# --------------------------------------------------------------------------- new splits

_ANCHOR_WORDS = {"confluence": "confluence", "lake_inlet": "lake inlet", "lake_outlet": "lake outlet",
                 "bridge": "bridge", "dam": "dam", "sign": "boundary sign", "coord": "map point"}


def _applies_to(on_item: str) -> Optional[dict]:
    kind, _, v = (on_item or "").partition(":")
    return {"gnis": {"gnis_id": v}, "blk": {"blk": v}, "wsc": {"wsc": v}}.get(kind)


def _ref(item: Optional[dict], prefix: str) -> list[str]:
    return sorted({r.split(":", 1)[1] for r in (item or {}).get("ref_ids") or ()
                   if r.startswith(prefix + ":")})


def split_record(n, src: Sources, *, sid: str, provenance: str) -> tuple[dict, list[str]]:
    """A `marks.NewSplit` as a splits.json split (the curated shape, `SplitAnchor`): a confluence
    by the tributary's WSC (its mouth), a lake inlet / outlet by the lake's wbk and edge, a bridge /
    dam / sign / map point by its coordinate. Returns (split, notes): a note says where the
    registry could not name the anchor and the stamped point stood in for it."""
    notes: list[str] = []
    anchor: Optional[dict] = None
    item = src.item(n.item_id) if n.item_id else None
    if n.anchor == "confluence":
        wsc, blk = _ref(item, "wsc"), _ref(item, "blk")
        if len(wsc) == 1:
            anchor = {"type": "confluence", "tributary_wsc": wsc[0], "_name": n.name or item["name"]}
        elif len(blk) == 1:
            anchor = {"type": "confluence", "tributary_blk": blk[0], "_name": n.name or item["name"]}
        else:
            notes.append(f"the registry names no single WSC / blk for {n.item_id or n.name}: "
                         "placed at the point the mark stamped")
    elif n.anchor in ("lake_inlet", "lake_outlet"):
        wbk = _ref(item, "wbk") or ([n.item_id.split(":", 1)[1]] if (n.item_id or "").startswith(
            "wbk:") else [])
        if len(wbk) == 1:
            anchor = {"type": "lake", "wbk": wbk[0]}
            if not n.offset_m:
                anchor["edge"] = "inlet" if n.anchor == "lake_inlet" else "outlet"
        else:
            notes.append(f"the registry names no single lake for {n.item_id or n.name}: placed at "
                         "the point the mark stamped")
    if anchor is None:
        if n.coord is None:
            raise ValueError(f"new split {n.ref} ({n.anchor}) has no point and no registry anchor")
        anchor = {"type": "point", "coord": [round(n.coord[0], 6), round(n.coord[1], 6)],
                  "is_lonlat": True}
    if n.offset_m:
        anchor["offset_m"] = abs(float(n.offset_m))
        anchor["offset_dir"] = "upstream" if n.offset_m > 0 else "downstream"
    from pipeline.common.models.splits import SplitAnchor
    SplitAnchor.from_dict({k: v for k, v in anchor.items() if not k.startswith("_")})
    label = n.name or _ANCHOR_WORDS.get(n.anchor, n.anchor)
    if n.anchor == "confluence" and n.name:
        label = f"{n.name} confluence"
    rec = {"id": sid, "label": label, "kind": anchor["type"], "anchor": anchor, "note": "",
           "_note": (f"[T185 proposal {dt.date.today().isoformat()}] {provenance}"
                     + (f' — "{n.quote}"' if n.quote else "")
                     + ("".join(f" [{x}]" for x in notes)))}
    if n.coord is not None:
        rec["_coord"] = [round(n.coord[0], 6), round(n.coord[1], 6)]
    return rec, notes


# --------------------------------------------------------------------------- extents

def extent_of(fix, split_of) -> tuple[Optional[dict], list[str]]:
    """A `marks.ExtentFix` as the catalogue's extent (the preview's own shape, T191): op, the
    splits its points name (`split_of(i, point)` -> split id or None), item(s), area. Returns
    (extent, problems)."""
    problems: list[str] = []
    ex: dict = {"op": fix.op}
    if fix.op == "rest":
        problems.append("op rest names its siblings (the entry's other rules): a hand edit")
    sids = []
    for i, p in enumerate(fix.points):
        sid = split_of(i, p)
        if sid is None:
            problems.append(f"point {i + 1} ({p.kind}) names no split")
        sids.append(sid)
    if sids:
        ex["splits"] = sids
    if len(fix.items) == 1:
        ex["item_id"] = fix.items[0]
    elif fix.items:
        ex["item_ids"] = list(fix.items)
    if fix.op == "within":
        area = str(fix.area or "")
        if area.startswith("area:"):
            ex["area_id"] = area
        else:
            problems.append(f"within names an area by id (area:…), not {area!r}")
    return ex, problems


def _validate_catalogue(doc: dict) -> list[str]:
    from pydantic import ValidationError
    from pipeline.regs.parsing.catalogue import CatalogueFile
    try:
        CatalogueFile.model_validate(json.loads(json.dumps(doc)))
        return []
    except ValidationError as ex:
        return [f"{'.'.join(str(x) for x in e['loc'])}: {e['msg']}" for e in ex.errors()[:5]]


def _validate_dfo(loc: dict) -> list[str]:
    try:
        from pipeline.regs.dfo_salmon import entries as E
        E.EntryLocation.from_dict(loc)
        return []
    except Exception as ex:  # noqa: BLE001 — reported as the proposal's problem
        return [f"not a DFO location: {ex}"]


# --------------------------------------------------------------------------- the builder

def _provenance(m) -> dict:
    return {"scope": m.scope, "target": m.target, "rule_id": m.rule_id, "verdict": m.verdict,
            "answers": dict(m.answers), "reviewer": m.reviewer, "date": m.date, "note": m.note}


def _proposes(m) -> bool:
    return m.scope in ("entry", "rule", "dfo") and bool(
        m.extent_fix or m.tributaries or m.new_splits or m.lake_split_later)


def _point_split(fix_point, on_item: Optional[str], ref: str):
    """A T191 NEW point (confluence / lake inlet / outlet / map point) as a `NewSplit`."""
    from pipeline.regs.review.marks import NewSplit
    return NewSplit(ref=ref, anchor=fix_point.kind, name=fix_point.name,
                    item_id=fix_point.item_id, coord=fix_point.coord, on_item=on_item)


def build(marks: Iterable, src: Sources) -> list[dict]:
    """EVERY PROPOSAL the marks make against the files NOW, new splits first. Pure: reads `src`."""
    from pipeline.regs.review import marks as M
    marks = list(marks)
    inforce = [m for m in M.latest(marks).values() if _proposes(m)]
    shared = M.proposals(marks)                         # T210: one per distinct cut, every mark
    splits_doc = src.doc(src.splits_path)
    have = {sp["id"]: sp for wb in splits_doc.get("waterbodies") or []
            for sp in wb.get("splits") or [] if sp.get("id")}
    taken = set(have)

    # the cuts the marks in force name: their new splits (by the shared group) and their NEW points
    groups: list[dict] = []                              # {gid, split (NewSplit), marks}

    def group_for(n, who: dict) -> dict:
        for g in groups:
            if M.same_anchor(n, g["split"]):
                if who not in g["marks"]:
                    g["marks"].append(who)
                return g
        sh = next((p for p in shared if M.same_anchor(n, p["split"])), None)
        g = {"gid": sh["id"] if sh else n.ref, "split": sh["split"] if sh else n,
             "marks": [who]}
        groups.append(g)
        return g

    ref_group: dict[tuple, dict] = {}                    # (mark key, ref | #i) -> group
    for m in inforce:
        who = {"target": m.target, "rule_id": m.rule_id, "reviewer": m.reviewer, "date": m.date}
        for n in m.new_splits:
            if n.anchor != "split":
                ref_group[(m.key, n.ref)] = group_for(n, who)
        for fi, f in enumerate(m.extent_fix):
            for pi, p in enumerate(f.points):
                if p.kind in ("confluence", "lake_inlet", "lake_outlet", "coord"):
                    on = f.items[0] if len(f.items) == 1 else None
                    if on is None and m.scope != "dfo":
                        got = _find_entry(src, m.target)
                        matched = (got[1].get("matched") or []) if got else []
                        on = matched[0] if matched else None
                    n = _point_split(p, on, f"{_slug(m.target, 30)}_{fi}_{pi}")
                    ref_group[(m.key, f"#{fi}.{pi}")] = group_for(n, who)

    out: list[dict] = []
    # ---- new splits
    gid_split: dict[str, str] = {}                       # group id -> its split id (new or existing)
    for g in groups:
        n = g["split"]
        pid = f"split-{_slug(g['gid'], 40)}"
        targets = ", ".join(sorted({w["target"] for w in g["marks"]}))
        prov = f"proposed in review marks on {targets}"
        problems: list[str] = []
        at = _applies_to(n.on_item) if n.on_item else None
        if at is None:
            problems.append(f"the cut names no stream it cuts (on_item {n.on_item!r})")
        wb = next((w for w in splits_doc.get("waterbodies") or [] if w.get("applies_to") == at),
                  None) if at else None
        water = (wb or {}).get("name") or ((src.item(n.on_item) or {}).get("name")
                                           if n.on_item else None) or (n.on_item or "water")
        base = f"{_slug(water.split('(')[0], 40)}__{_slug(n.name or n.anchor, 32)}"
        rec, notes = None, []
        try:
            probe, _ = split_record(n, src, sid="probe", provenance=prov)
            dup = next((sid for sid, sp in have.items() if sp.get("anchor") == probe["anchor"]
                        and probe["anchor"]["type"] != "lake"), None)
        except ValueError as ex:
            problems.append(str(ex))
            dup = None
        if dup is not None:                              # already in splits.json: applied
            gid_split[g["gid"]] = dup
            continue
        sid, k = base, 1
        while sid in taken:
            k += 1
            sid = f"{base}_{k}"
        taken.add(sid)
        gid_split[g["gid"]] = sid
        edits = []
        if not problems:
            rec, notes = split_record(n, src, sid=sid, provenance=prov)
            edits = [{"file": _rel(src.splits_path), "kind": "split",
                      "waterbody": {"name": water.upper() if wb is None else wb["name"],
                                    "applies_to": at},
                      "split": rec}]
        out.append({"id": pid, "kind": "new_split", "target": n.on_item, "scope": "split",
                    "rule_id": None, "title": f"new split {sid} on {water}: "
                    + _ANCHOR_WORDS.get(n.anchor, n.anchor) + (f" {n.name}" if n.name else ""),
                    "marks": g["marks"], "edits": edits, "depends_on": [], "problems": problems,
                    "notes": notes, "split_id": sid})

    # ---- one proposal per mark
    for m in inforce:
        who = _provenance(m)
        pid = "mark-" + _slug(m.scope, 6) + "-" + _slug(m.target, 70) + (
            "-" + _slug(m.rule_id, 40) if m.rule_id else "")
        if m.lake_split_later:
            out.append({"id": pid + "-lake-split", "kind": "lake_split_later", "target": m.target,
                        "scope": m.scope, "rule_id": m.rule_id,
                        "title": f"{m.target}: needs a lake split later (deferred, RULINGS K5)",
                        "marks": [who], "edits": [], "depends_on": [], "problems": [],
                        "notes": ["listed by `python -m pipeline.regs.review.tickets "
                                  "--lake-splits`; no curated edit until lake splits are taken up"],
                        "split_id": None})
        if not (m.extent_fix or m.tributaries):
            continue
        problems: list[str] = []
        depends: list[str] = []
        new_ref = {n.ref: n for n in m.new_splits}

        def split_of(fi: int, pi: int, p, _m=m, _new=new_ref, _deps=depends) -> Optional[str]:
            if p.kind == "split":
                return p.split_id
            if p.kind == "new":
                n = _new.get(p.split_id)
                if n is not None and n.anchor == "split":
                    return n.split_id
                g = ref_group.get((_m.key, p.split_id))
            else:
                g = ref_group.get((_m.key, f"#{fi}.{pi}"))
            if g is None:
                return None
            sid = gid_split.get(g["gid"])
            dep = f"split-{_slug(g['gid'], 40)}"
            if sid is not None and sid not in have and dep not in _deps:
                _deps.append(dep)
            return sid

        # what to set, per scope: {scope: {field: value}}
        sets: dict[str, dict] = {}
        tri_field = "tributaries" if m.scope == "dfo" else "includes_tributaries"
        for fi, f in enumerate(m.extent_fix):
            ex, probs = extent_of(f, lambda i, p, _fi=fi: split_of(_fi, i, p))
            problems += [f"{f.scope}: {x}" for x in probs]
            sets.setdefault(f.scope, {})["extents"] = [ex]
            if f.tributaries is not None:
                sets[f.scope][tri_field] = f.tributaries
        for scope, call in m.tributaries.items():
            want = call == "include"
            got = sets.setdefault(scope, {})
            if tri_field in got and got[tri_field] != want:
                problems.append(f"{scope}: the extent fix says tributaries "
                                f"{'in' if got[tri_field] else 'out'}, the call {call}")
            got.setdefault(tri_field, want)

        edits: list[dict] = []
        if m.scope == "dfo":
            found = _find_location(src, m.target)
            if found is None:
                problems.append(f"no DFO location {m.target}")
            else:
                path, loc = found
                b = loc.get("binding") or {}
                for scope, fields in sets.items():
                    if scope != "entry":
                        problems.append(f"a DFO location has no rule {scope}")
                        continue
                    for fld, val in fields.items():
                        before = b.get(fld, _MISSING)
                        if before != val:
                            edits.append({"file": _rel(path), "kind": "dfo",
                                          "location_id": m.target, "scope": "entry",
                                          "field": fld, "before": before, "after": val})
        else:
            found = _find_entry(src, m.target)
            if found is None:
                problems.append(f"no catalogue entry {m.target}")
            else:
                path, e = found
                for scope, fields in sets.items():
                    t = e if scope == "entry" else next(
                        (r for r in e.get("rules") or [] if r.get("rule_id") == scope), None)
                    if t is None:
                        problems.append(f"no rule {scope} on {m.target}")
                        continue
                    for fld, val in fields.items():
                        before = t.get(fld, _MISSING)
                        if before != val:
                            edits.append({"file": _rel(path), "kind": "catalogue",
                                          "entry_id": m.target, "scope": scope, "field": fld,
                                          "before": before, "after": val})
        if not edits and not problems:
            continue                                     # the files already say it: applied
        words = []
        from pipeline.regs.review.tickets import fix_words
        words += [fix_words(f) for f in m.extent_fix]
        words += [f"{'the entry' if k == 'entry' else 'rule ' + k}: tributaries {v}d"
                  for k, v in m.tributaries.items()]
        out.append({"id": pid, "kind": "extent", "target": m.target, "scope": m.scope,
                    "rule_id": m.rule_id, "title": f"{m.target}: " + "; ".join(words),
                    "marks": [who], "edits": edits, "depends_on": depends, "problems": problems,
                    "notes": [], "split_id": None})

    # ---- check every proposal's file still validates with its edits (and its new splits')
    by_id = {p["id"]: p for p in out}
    for p in out:
        p["applicable"] = bool(p["edits"]) and not p["problems"]
        if not p["applicable"]:
            continue
        chain = [by_id[d] for d in p["depends_on"] if d in by_id] + [p]
        try:
            texts = materialize(chain, src)
        except ValueError as ex:
            p["problems"].append(str(ex))
            p["applicable"] = False
            continue
        for path, text in texts.items():
            if path.parent == Path(src.entries_dir):
                bad = _validate_catalogue(json.loads(text))
            elif path.parent == Path(src.dfo_dir):
                bad = [x for ed in p["edits"] if ed["kind"] == "dfo"
                       for x in _validate_dfo(next(
                           loc for loc in json.loads(text)["locations"]
                           if loc["location_id"] == ed["location_id"]))]
            else:
                bad = []
            if bad:
                p["problems"] += bad
                p["applicable"] = False
    for p in out:
        p["digest"] = _h({"kind": p["kind"], "target": p["target"], "edits": [
            {k: v for k, v in ed.items() if k != "file"}
            | ({"split": {k: v for k, v in ed["split"].items() if k != "_note"}}
               if ed["kind"] == "split" else {}) for ed in p["edits"]]})
    order = {"new_split": 0, "extent": 1, "lake_split_later": 2}
    out.sort(key=lambda p: order[p["kind"]])
    return out


def chain_of(p: Mapping, props: Iterable[Mapping]) -> list[Mapping]:
    """A proposal with the new splits it depends on, dependencies first."""
    by_id = {q["id"]: q for q in props}
    return [by_id[d] for d in p.get("depends_on") or () if d in by_id] + [p]


# --------------------------------------------------------------------------- the side rebuild

def side_commands(p: Mapping, props: Iterable[Mapping]) -> list[str]:
    """THE OWNER'S STEP for a proposal (printed, never run): patched copies in a side directory,
    the side atlas when a split changes, a reach run on them, and the per-rule binding diff
    against the live reach run (AGENTS 63)."""
    from pipeline.common.curated import GENERATED
    live_build = _rel(GENERATED.build())
    live_reach = _rel(GENERATED.reaches / GENERATED.build().name)
    side = f"data/generated/side/t185-{_slug(p['id'], 40)}-{p['digest'][:6]}"
    chain = chain_of(p, props)
    splits = any(ed["kind"] == "split" for q in chain for ed in q["edits"])
    dfo = any(ed["kind"] == "dfo" for ed in p["edits"])
    cmds = [f"python -m pipeline.regs.review.proposals side {p['id']} --out {side}"]
    build = live_build
    if splits:
        cmds.append(f"scripts/heavy.sh atlas 30 -- python -m pipeline.atlas.build --full "
                    f"--splits {side}/splits.json --out {side}/atlas")
        build = f"{side}/atlas"
    if dfo:
        if not splits:
            return ["# a DFO location is not in the reach run: the in-memory check above IS its "
                    "binding (no side rebuild)"]
        cmds.append(f"CURATION_BUILD={side}/atlas CURATION_SPLITS_JSON={side}/splits.json "
                    f"CURATION_DFO_DIR={side}/dfo_salmon bash curation-review/backend/run.sh"
                    "   # the DFO location on the side atlas (stop the live app first, or set "
                    "its port)")
        return cmds
    cmds += [f"scripts/heavy.sh reach 30 -- python -m pipeline.atlas.reach.cli --build {build} "
             f"--entries {side}/catalogue --out {side}/reach",
             f"python -m pipeline.atlas.reach.binding_diff {live_reach} {side}/reach"
             "   # per-rule bindings, live vs side — read it before accepting (AGENTS 63)"]
    return cmds


def write_side(p: Mapping, props: Iterable[Mapping], src: Sources, out: Path) -> list[Path]:
    """PATCHED COPIES for a side rebuild: the catalogue, splits.json and the DFO files, copied into
    `out` with the proposal (and the new splits it names) applied. Refuses a directory under
    data/curated: the curated files are only ever written by `apply_proposals`."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.parsing.io import atomic_write
    out = Path(out).resolve()
    cur = Path(CURATED.waters.splits).resolve().parents[1]
    if out == cur or cur in out.parents:
        raise ValueError(f"{out} is under the curated data ({cur}): a side copy goes elsewhere")
    texts = materialize(chain_of(p, props), src)
    written = []
    for name, d in (("catalogue", src.entries_dir), ("dfo_salmon", src.dfo_dir)):
        for f in _region_files(d):
            dst = out / name / f.name
            atomic_write(dst, texts.get(f, src.text(f)))
            written.append(dst)
    dst = out / "splits.json"
    atomic_write(dst, texts.get(Path(src.splits_path), src.text(src.splits_path)))
    written.append(dst)
    return written


# --------------------------------------------------------------------------- the cheap check

def _backend():
    if str(BACKEND) not in sys.path:
        sys.path.insert(0, str(BACKEND))
    import reuse                                         # noqa: E402 — the review app's reads
    return reuse


def check(props: list[dict], src: Sources) -> None:
    """THE CHEAP CHECK (fills `check` and `severity` on each proposal): the review app's in-memory
    `build_reach` on the entry / DFO location before and after its edits (`reuse.binding_change`,
    the path T191's preview uses) — each rule's sections gained / lost and km. A proposal that
    names a NEW split (or is one) cannot be bound in memory: the side rebuild answers it."""
    reuse = _backend()
    regions = set()
    for p in props:
        if p["kind"] == "extent" and p["scope"] != "dfo":
            got = _find_entry(src, p["target"])
            if got is not None and got[1].get("region"):
                regions.add(str(got[1]["region"]))
    sev: dict[str, dict] = {}
    for r in sorted(regions):
        try:
            for row in reuse.queue(region=str(r)):
                sev[row["entry_id"]] = {"level": row.get("severity"),
                                        "reasons": row.get("severity_reasons") or []}
        except Exception as ex:  # noqa: BLE001 — severity is a label, never a gate
            print(f"  (severity of region {r}: {type(ex).__name__}: {ex})", file=sys.stderr)
    for p in props:
        p["severity"] = sev.get(p["target"]) if p["scope"] in ("entry", "rule") else None
        if not p["applicable"]:
            p["check"] = {"cheap": False, "why": "not applicable: " + "; ".join(
                p["problems"] or ["no edit"])}
            continue
        if p["kind"] == "new_split":
            p["check"] = {"cheap": False, "why": "a new split changes the atlas: the side rebuild "
                          "and its binding diff answer it"}
            continue
        if p["depends_on"]:
            p["check"] = {"cheap": False, "why": "names a new split (" + ", ".join(
                p["depends_on"]) + "): bound only on the side atlas"}
            continue
        changes = [{"scope": ed["scope"], "field": ed["field"], "after": ed["after"]}
                   for ed in p["edits"]]
        try:
            got = (reuse.dfo_binding_change(p["target"], changes) if p["scope"] == "dfo"
                   else reuse.binding_change(p["target"], changes))
        except Exception as ex:  # noqa: BLE001 — reported on the proposal
            got = {"cheap": False, "why": f"{type(ex).__name__}: {ex}"}
        p["check"] = got if got is not None else {"cheap": False, "why": "target not found"}


# --------------------------------------------------------------------------- the files

def write(props: list[dict], out: Path, src: Sources, *, marks_path: Path) -> Path:
    """index.json + one .patch per applicable proposal into `out` (replacing the last run's)."""
    from pipeline.regs.parsing.io import atomic_write
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.patch"):
        old.unlink()
    rows = []
    for p in props:
        row = dict(p)
        row["side_commands"] = side_commands(p, props) if p["applicable"] else []
        if p["applicable"]:
            texts = materialize([p], src) if not p["depends_on"] else {
                k: v for k, v in materialize(chain_of(p, props), src).items()
                if any(_resolve_file(ed, src) == k for ed in p["edits"])}
            diff = "".join(unified(_rel(path), src.text(path), text)
                           for path, text in sorted(texts.items()))
            name = f"{_slug(p['id'], 100)}.patch"
            atomic_write(out / name, diff)
            row["patch"] = name
        else:
            row["patch"] = None
        rows.append(row)
    doc = {"_about": "T185 proposed edits from the review marks — written by `python -m "
                     "pipeline.regs.review.proposals build`; decisions are `proposal` marks; "
                     "`python -m pipeline.regs.review.apply_proposals` (the owner) applies the "
                     "accepted ones",
           "made": dt.datetime.now().isoformat(timespec="seconds"),
           "marks": _rel(marks_path), "proposals": rows}
    atomic_write(out / INDEX, json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    return out / INDEX


def read_index(out: Path = OUT_DIR) -> dict:
    p = Path(out) / INDEX
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {"proposals": []}


def prints_of(proposal_id: str, out: Path = OUT_DIR) -> Optional[dict]:
    """THE PRINTS a decision on a proposal is held against (`marks.Prints` shape): the proposal's
    digest as last written. None: no such proposal."""
    p = next((q for q in read_index(out)["proposals"] if q["id"] == proposal_id), None)
    if p is None:
        return None
    return {"text": p["digest"],
            "records": [{"key": f"proposal:{proposal_id}", "meaning": p["digest"],
                         "binding": p["digest"]}]}


def _summary(props: list[dict], decided: Mapping[str, dict]) -> str:
    lines = []
    for p in props:
        d = decided.get(p["id"])
        state = ("—" if d is None else
                 f"{d['decision']}{'' if d['digest'] == p['digest'] else ' (STALE: changed since)'}")
        lines.append(f"[{p['kind']}] {p['id']}  {'ok' if p['applicable'] else 'n/a'}  "
                     f"decision {state}\n    {p['title']}")
        for x in p["problems"]:
            lines.append(f"    problem: {x}")
        c = p.get("check")
        if c:
            if c.get("cheap"):
                for rid, r in (c.get("rules") or {}).items():
                    lines.append(f"    {rid}: {r['n_before']} -> {r['n_after']} sections "
                                 f"(+{r['gained']} -{r['lost']}), {r['km_before']} -> "
                                 f"{r['km_after']} km, {r['outcome_before']} -> {r['outcome_after']}")
            else:
                lines.append(f"    check: {c.get('why')}")
        for cmd in side_commands(p, props) if p["applicable"] else []:
            lines.append(f"    $ {cmd}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import marks as M
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.proposals",
                                 description="review marks -> proposed edits (T185); writes only "
                                             "the proposals dir (and side copies)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="the proposals, written to the proposals dir")
    b.add_argument("--check", action="store_true",
                   help="bind each proposal in memory before/after (loads the graph)")
    s = sub.add_parser("side", help="patched copies of the curated files for a side rebuild")
    s.add_argument("id")
    s.add_argument("--out", type=Path, required=True)
    for x in (b, s):
        x.add_argument("--marks", type=Path, default=CURATED.regulations.review.marks)
        x.add_argument("--build", type=Path, help="the atlas whose registry names new anchors")
        x.add_argument("--dir", type=Path, default=OUT_DIR, help="the proposals dir")
    a = ap.parse_args(argv)
    src = Sources.live(a.build)
    marks = M.load(a.marks)
    props = build(marks, src)
    if a.cmd == "side":
        p = next((q for q in props if q["id"] == a.id), None)
        if p is None or not p["applicable"]:
            print(f"no applicable proposal {a.id}", file=sys.stderr)
            return 2
        for w in write_side(p, props, src, a.out):
            print(f"  {w}")
        return 0
    if a.check:
        check(props, src)
    idx = write(props, a.dir, src, marks_path=a.marks)
    print(_summary(props, M.decisions(marks)))
    print(f"{len(props)} proposal(s) -> {_rel(idx)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
