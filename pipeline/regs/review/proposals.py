"""T185: REVIEW MARKS -> PROPOSED EDITS -> applied by Claude, never by the review tool.

    marks.jsonl (read only)  ->  proposals (one per mark, one per new split)  ->  the review app's
    read-only "waiting to apply" list  ->  `apply_proposals` (Claude runs it) lands each as its own
    commit, after the binding check (and the side rebuild where a cut changes)

THE OWNER'S MARKS ARE THE DECISIONS (owner 2026-10-27): there is no accept / reject step. To change
or stop a proposal, the owner changes the mark.

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
covers the edits (before and after) and names the proposal's side-rebuild directory.

CONFIDENCE (owner 2026-10-27): every proposed split carries one — the mark's own (the new-split
picker sets it), an owner override (`proposal_overrides.json`, since the marks are never edited),
else the anchor's default (a hand-placed point medium, a registry anchor high). It rides into the
split record (`confidence`, absent = high, as splits.json spells it) and the proposal row.

WRITES ONLY under `handoff/PLAN/curation-review/proposals/` (index.json + one .patch per proposal),
and — `side` — patched COPIES in a side directory the caller names (never under data/curated).
`apply_proposals` is the one writer of the curated files (Claude runs it, never the review app).

    python -m pipeline.regs.review.proposals build [--check]      # the proposals (+ cheap check)
    python -m pipeline.regs.review.proposals side <id> --out DIR  # patched copies for a side rebuild

`--check` runs the in-memory `build_reach` path the review app's extent preview uses (T191) on the
entry before and after, per rule — sections gained / lost, km — and prints, per proposal, the full
side-rebuild + `binding_diff` chain (Claude's step before applying a cut change; nothing here runs it).
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
#: the proposal kinds — T166: `tidal_flag` flags a cut already in splits.json a TIDAL BOUNDARY;
#: `tidal_mouth` lists a tidal boundary the owner put at the stream's mouth (nothing is tidal, no edit)
KINDS = ("new_split", "tidal_flag", "extent", "lake_split_later", "tidal_mouth", "tidal_followup")
#: T166: a tidal boundary within this many metres of the FWA mouth IS the mouth — no cut is made
#: (a cut on a line's end breaks both halves: memory end-cut-halves-the-water) and nothing is tidal
TIDAL_MOUTH_M = 1.0
#: T212 owner calls made before the Tidal tab could record a mark (Capilano, Cowichan, 2026-10-27):
#: each is the candidate's proposed boundary, accepted; a later tidal mark on the same candidate wins
TIDAL_DECISIONS = REPO / "handoff" / "PLAN" / "curation-review" / "tidal" / "owner-decisions.json"


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
    #: the owner's confidence calls on proposed splits (`marks.ConfidenceOverride`)
    overrides: tuple = ()
    #: T166: the owner's tidal calls recorded before the Tidal tab (`TIDAL_DECISIONS`); None = none
    tidal_decisions: Optional[Path] = None
    _reg: Optional[dict] = field(default=None, repr=False)
    _texts: dict = field(default_factory=dict, repr=False)

    @classmethod
    def live(cls, build: Optional[Path] = None) -> "Sources":
        from pipeline.common.curated import CURATED, GENERATED
        from pipeline.regs.review.marks import load_overrides
        return cls(entries_dir=Path(CURATED.regulations.entries.catalogue),
                   dfo_dir=Path(CURATED.regulations.entries.dfo_salmon),
                   splits_path=Path(CURATED.waters.splits),
                   build=Path(build) if build else GENERATED.build(),
                   overrides=tuple(load_overrides(CURATED.regulations.review.proposal_overrides)),
                   tidal_decisions=TIDAL_DECISIONS if TIDAL_DECISIONS.exists() else None)

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
    if ed["kind"] == "split_flag":                       # T166: one field on an existing split
        sp = next((sp for w in doc.get("waterbodies") or [] for sp in w.get("splits") or []
                   if sp.get("id") == ed["split_id"]), None)
        if sp is None:
            raise ValueError(f"splits.json has no split {ed['split_id']}")
        now = sp.get(ed["field"], _MISSING)
        if strict and now != ed["before"]:
            raise ValueError(f"split {ed['split_id']} {ed['field']} is {now!r} now, the proposal "
                             f"was made on {ed['before']!r}")
        sp[ed["field"]] = copy.deepcopy(ed["after"])
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


def split_record(n, src: Sources, *, sid: str, provenance: str,
                 confidence: Optional[str] = None) -> tuple[dict, list[str]]:
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
    conf = confidence or n.confidence_now
    rec = {"id": sid, "label": label, "kind": anchor["type"], "anchor": anchor, "note": "",
           **({} if conf == "high" else {"confidence": conf}),
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


# --------------------------------------------------------------------------- tidal (T166)

#: OWNER 2026-10-28: a tidal split comes only from WRITTEN WORDS or the DFO default (the mouth) — the DFO polygon
#: decides no cut (it is still drawn, as the `tidal_water` area). A tidal mark whose chosen point is the polygon's
#: crossing (the Tidal tab stamps "chosen point: polygon ..." on its new split) is listed, never applied.
POLYGON_POINT_WORDS = ("chosen point: polygon", "polygon crossing")


def polygon_point(n) -> bool:
    """Was this tidal cut placed by the DFO polygon (not by words, the mouth or the owner's own point)?"""
    text = f"{n.note or ''} {n.quote or ''}".lower()
    return any(w in text for w in POLYGON_POINT_WORDS) and "chosen point: words" not in text \
        and "chosen point: mouth" not in text and "chosen point: custom" not in text


#: T166 (owner 2026-10-28): a tidal cut's point must be WITHIN THIS of a located words point of its candidate
#: (`words_point`, a `words` alternate, a book / DFO source), else of the mouth — never anywhere else
TIDAL_POINT_TOL_M = 25.0


def words_measures(cand: Mapping) -> list[float]:
    """The located WORDS points of a tidal candidate (above-mouth measures): its `words_point`, each `words`
    alternate, each written source (synopsis, DFO notice / area page, a curated split). Never the polygon."""
    out = []
    wp = (cand.get("words_point") or {}).get("measure")
    if wp is not None:
        out.append(float(wp))
    out += [float(a["measure"]) for a in cand.get("alternates") or ()
            if a.get("kind") == "words" and a.get("measure") is not None]
    out += [float(x["measure"]) for x in cand.get("sources") or ()
            if x.get("measure") is not None and x.get("kind") not in ("polygon", "dem", "tidal_gauge")]
    return out


def tidal_point(n, cand: Mapping, line_start: Optional[float]) -> Optional[str]:
    """WHAT A TIDAL MARK'S POINT IS (owner 2026-10-28: words or the DFO mouth default, never the polygon):
    `mouth` (<= `TIDAL_MOUTH_M`), `line_end` (within `TIDAL_POINT_TOL_M` of the atlas line's own lower end when
    it starts above the FWA mouth — a stream entering a lake, or one FWA carries only from the border: a cut
    there breaks both halves, memory end-cut-halves-the-water), `words` (within
    the tolerance of a located words point), `place` (a registry anchor the owner named: a lake inlet, a
    confluence), or None — anything else, which is never cut."""
    if n.anchor not in ("coord", "bridge", "dam", "sign"):
        return "place"
    if n.measure is None:
        return None
    d = float(n.measure)
    if d <= TIDAL_MOUTH_M:
        return "mouth"
    if line_start is not None and line_start > TIDAL_MOUTH_M and abs(d - line_start) <= TIDAL_POINT_TOL_M:
        return "line_end"
    if any(abs(d - w) <= TIDAL_POINT_TOL_M for w in words_measures(cand)):
        return "words"
    return None


def _line_starts(build: Optional[Path], blks: set) -> dict[str, float]:
    """{blk: the lowest section measure the atlas has on it} for `blks` (from `section_handles.txt`)."""
    blks = {b for b in blks if b}
    p = Path(build or "") / "section_handles.txt"
    if not blks or not build or not p.exists():
        return {}
    out: dict[str, float] = {}
    with open(p, encoding="utf-8") as f:
        for ln in f:
            b, _, m = ln.strip().rpartition(":")
            if b in blks:
                v = float(m)
                out[b] = min(out.get(b, v), v)
    return out


def _tidal_off_point(m, n, water: str) -> dict:
    """A Place y tidal mark whose point is neither the words nor the mouth (`tidal_point` None): listed."""
    return {"id": f"tidal-open-{_slug(m.target, 40)}", "kind": "tidal_followup", "target": m.target,
            "scope": "tidal", "rule_id": None,
            "title": (f"{water}: the point ({n.measure} m above the mouth) is neither a located words point nor the "
                      f"mouth (within {TIDAL_POINT_TOL_M:g} m) — no cut (owner 2026-10-28); mark it again"),
            "marks": [{"target": m.target, "rule_id": None, "reviewer": m.reviewer, "date": m.date,
                       "note": m.note}],
            "edits": [], "depends_on": [], "problems": [], "notes": [n.note] if n.note else [],
            "split_id": None, "confidence": None}


def _tidal_decides(m) -> bool:
    """Does a tidal mark in force DECIDE a tidal boundary? Only Place y with its one cut (the point
    the owner chose: the proposal, a polygon crossing, the words' landmark or his own). A wrong or
    unsure mark decides nothing yet — one carrying a point is LISTED for follow-up (Bear River:
    "most likely the split is at the bridge, we should look more online"), never applied."""
    return (m.scope == "tidal" and len(m.new_splits) == 1 and m.verdict == "verified"
            and not polygon_point(m.new_splits[0]))


def _tidal_followup(m) -> bool:
    """A tidal mark in force that is not a decision but says something: wrong / unsure, or a Place y
    on a polygon point (`POLYGON_POINT_WORDS`)."""
    return m.scope == "tidal" and (m.verdict in ("wrong", "unsure") or (
        m.verdict == "verified" and len(m.new_splits) == 1 and polygon_point(m.new_splits[0])))


def tidal_decision_marks(path: Optional[Path], candidates: Optional[Path] = None) -> list:
    """THE OWNER'S EARLIER TIDAL CALLS (`TIDAL_DECISIONS`) as tidal marks: each candidate's proposed
    boundary as its one new split, Place y, the call's date and note. They come BEFORE the marks
    store, so a later mark on the same candidate supersedes them (`marks.latest`). The file is
    handoff data the owner dictated (decisions.md 2026-10-27); a call naming a candidate the
    candidates file no longer has is refused, never skipped."""
    if path is None or not Path(path).exists():
        return []
    from pipeline.regs.review import marks as M
    from pipeline.regs.review import tidal as T
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    out = []
    for d in doc.get("decisions") or []:
        row = T.candidate(d["id"], candidates)
        if row is None:
            raise ValueError(f"tidal decision {d['id']} ({d.get('water')}): no such candidate in "
                             f"{candidates or T.path()}")
        pr = row.get("proposed") or {}
        reuse = pr.get("reuse") or {}
        n = {"ref": d["id"].replace(":", "-")[:40], "on_item": (row.get("items") or [None])[0],
             "name": pr.get("split_name") or f"{row.get('water')}: tidal boundary",
             "quote": d.get("where") or "", "note": d.get("note") or ""}
        if d.get("anchor"):                          # the owner named the place himself (Nitinat: the lake inlet)
            a = dict(d["anchor"])
            n.update({k: v for k, v in a.items() if k in ("anchor", "item_id", "split_id", "coord", "blk",
                                                         "measure", "offset_m")})
            n["name"] = a.get("name") or n["name"]
            n["on_item"] = a.get("on_item") or n["on_item"]
        elif reuse.get("split_id"):
            n.update(anchor="split", split_id=reuse["split_id"])
        else:
            n.update(anchor="coord", coord=tuple(pr["coord"]), blk=row.get("blk"),
                     measure=pr.get("measure"))
        out.append(M.Mark.model_validate({
            "scope": "tidal", "target": d["id"], "verdict": "verified", "answers": {"P": "y"},
            "reviewer": "dawson", "date": d.get("decided") or "", "pdf_sha256": "0" * 64,
            "note": f"owner call ({_rel(Path(path))}): {d.get('note') or ''}".strip(),
            "prints": T.prints(row), "new_splits": [n]}))
    return out


def _tidal_label(n, water: str = "") -> str:
    """A tidal cut's label: its name without the water's own name ("Nanaimo River: tidal boundary"
    -> "tidal boundary"), as every other split of the water is labelled; it always says tidal."""
    name = (n.name or "").strip() or "tidal boundary"
    head, sep, tail = name.partition(": ")
    if sep and tail.strip():                     # T212 names every cut "<water>: <place>"
        name = tail.strip()
    # a cut ABOVE the mouth is no "mouth": T212 labelled every polygon crossing of a stream it found
    # no words for "mouth (tidal boundary)", Amos Creek's 500 m up included
    if name.lower().startswith("mouth") and (n.measure or 0) > TIDAL_MOUTH_M:
        name = "tidal boundary"
    return name if "tidal" in name.lower() else f"{name} (tidal boundary)"


def _point_split(fix_point, on_item: Optional[str], ref: str):
    """A T191 NEW point (confluence / lake inlet / outlet / map point) as a `NewSplit`."""
    from pipeline.regs.review.marks import NewSplit
    return NewSplit(ref=ref, anchor=fix_point.kind, name=fix_point.name,
                    item_id=fix_point.item_id, coord=fix_point.coord, on_item=on_item)


def _tidal_flag(sid: str, have: Mapping, src: Sources, marks: list, water: str) -> list[dict]:
    """T166: flag an EXISTING split a tidal boundary (a reused cut: the Fraser's CPR bridge at
    Mission, the Campbell's cement block sign) — one `tidal_flag` proposal, or none when it is
    flagged already. The cut does not move; the piece below it becomes tidal water."""
    sp = have.get(sid)
    if sp is not None and sp.get("tidal") is True:
        return []
    problems = [] if sp is not None else [f"splits.json has no split {sid}"]
    edits = [] if problems else [{"file": _rel(src.splits_path), "kind": "split_flag",
                                  "split_id": sid, "field": "tidal", "before": _MISSING,
                                  "after": True}]
    return [{"id": f"tidal-flag-{_slug(sid, 60)}", "kind": "tidal_flag", "target": sid,
             "scope": "split", "rule_id": None,
             "title": f"TIDAL BOUNDARY: flag {sid} on {water} (the cut stays; the piece below it is "
                      "tidal water)",
             "marks": marks, "edits": edits, "depends_on": [], "problems": problems,
             "notes": ["an existing cut: the atlas sidecar carries the flag "
                       "(`python -m pipeline.atlas.sidecars`), no new cut"],
             "split_id": sid, "confidence": None}]


def build(marks: Iterable, src: Sources, *, applied: Optional[set] = None) -> list[dict]:
    """EVERY PROPOSAL the marks make against the files NOW, new splits first. Pure: reads `src`.
    `applied`, when given, collects the key of every mark whose extent / tributary fix the files
    already say (landed: no proposal is made for it) — `applied_keys`."""
    from pipeline.regs.review import marks as M
    # T166: the owner's earlier tidal calls first, so a mark made since supersedes one
    marks = tidal_decision_marks(src.tidal_decisions) + list(marks)
    latest = M.latest(marks).values()
    inforce = [m for m in latest if _proposes(m)]
    tidal_in = [m for m in latest if _tidal_decides(m)]
    # T166: where each tidal stream's atlas line starts (its lowest section), from the build's handles
    line_starts = _line_starts(src.build, {str(x.new_splits[0].blk or "") for x in tidal_in})
    tidal_open = [m for m in latest if _tidal_followup(m)]
    shared = M.proposals(marks, src.overrides)          # T210: one per distinct cut, every mark
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
                g["members"].append(n)
                return g
        sh = next((p for p in shared if M.same_anchor(n, p["split"])), None)
        g = {"gid": sh["id"] if sh else n.ref, "split": sh["split"] if sh else n,
             "marks": [who], "members": [n]}
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

    # T166: EACH TIDAL BOUNDARY THE OWNER DECIDED. A cut above the mouth is a new split (shared with
    # any other mark naming the same cut) flagged `tidal`; on an existing split, that split is flagged;
    # at the mouth there is no cut and nothing is tidal (listed).
    out: list[dict] = []
    for m in sorted(tidal_in, key=lambda m: m.target):
        who = {"target": m.target, "rule_id": None, "reviewer": m.reviewer, "date": m.date}
        n = m.new_splits[0]
        water = ((src.item(n.on_item) or {}).get("name") if n.on_item else None) or (
            n.name or "").partition(":")[0] or (n.on_item or "water")
        if n.anchor == "split":
            out.extend(_tidal_flag(n.split_id, have, src, [who], water))
            continue
        from pipeline.regs.review import tidal as T
        cand = T.candidate(m.target) or {}
        point = tidal_point(n, cand, line_starts.get(str(n.blk or cand.get("blk") or "")))
        if point is None:
            out.append(_tidal_off_point(m, n, water))
            continue
        if point in ("mouth", "line_end"):
            out.append({"id": f"tidal-mouth-{_slug(m.target, 40)}", "kind": "tidal_mouth",
                        "target": n.on_item, "scope": "tidal", "rule_id": None,
                        "title": f"{water}: the tidal boundary is the mouth — no cut, nothing tidal",
                        "marks": [who], "edits": [], "depends_on": [], "problems": [],
                        "notes": ["the mouth is where the stream meets tidal water already: the "
                                  "whole stream stays freshwater (T166)"] + (
                            [f"the point ({n.measure} m) is the atlas line's own lower end (it starts "
                             f"{line_starts.get(str(n.blk or cand.get('blk') or ''))} m: where the stream enters a "
                             "lake, or crosses the border): a cut there would break both halves — no cut"]
                            if point == "line_end" else []),
                        "split_id": None, "confidence": None})
            continue
        g = group_for(n, who)
        g["tidal"] = True
        if cand.get("water"):
            g.setdefault("tidal_water", cand["water"])
        if cand.get("confidence") and n.confidence is None:
            g.setdefault("tidal_conf", cand["confidence"])  # the candidate's grade, unless set
    for m in sorted(tidal_open, key=lambda m: m.target):
        n = m.new_splits[0] if m.new_splits else None
        out.append({"id": f"tidal-open-{_slug(m.target, 40)}", "kind": "tidal_followup",
                    "target": m.target, "scope": "tidal", "rule_id": None,
                    "title": (f"{m.target}: a polygon point — the DFO polygon decides no tidal cut "
                              "(owner 2026-10-28: words or the mouth); mark it again"
                              if m.verdict == "verified" else
                              f"{m.target}: the owner's tidal mark says {m.verdict} — no cut until a "
                              "Place y mark") + (f" (point: {n.name}, {n.measure} m above the mouth)"
                                                 if n is not None else ""),
                    "marks": [{"target": m.target, "rule_id": None, "reviewer": m.reviewer,
                               "date": m.date, "note": m.note}],
                    "edits": [], "depends_on": [], "problems": [], "notes": [m.note] if m.note else [],
                    "split_id": None, "confidence": None})

    # ---- new splits
    gid_split: dict[str, str] = {}                       # group id -> its split id (new or existing)
    for g in groups:
        n = g["split"]
        sh = next((p for p in shared if p["id"] == g["gid"]), None)
        conf = sh["confidence"] if sh else M.confidence_of(g["members"], src.overrides)
        if g.get("tidal_conf") and not any(x.confidence for x in g["members"]) and not any(
                M.same_anchor(o.match, n) for o in src.overrides):
            conf = g["tidal_conf"]                       # T166: the tidal candidate's own grade
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
                                           if n.on_item else None) or g.get("tidal_water") or (
                                               n.on_item or "water")
        if g.get("tidal"):                               # T166: labelled as its water's cuts are
            n = n.model_copy(update={"name": _tidal_label(n, water)})
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
            if g.get("tidal"):                           # T166: ... but flagged tidal?
                out.extend(_tidal_flag(dup, have, src, g["marks"], water))
            continue
        sid, k = base, 1
        while sid in taken:
            k += 1
            sid = f"{base}_{k}"
        taken.add(sid)
        gid_split[g["gid"]] = sid
        edits = []
        if not problems:
            rec, notes = split_record(n, src, sid=sid, provenance=prov, confidence=conf)
            if g.get("tidal"):                           # T166: the piece below it is tidal water
                rec["tidal"] = True
            edits = [{"file": _rel(src.splits_path), "kind": "split",
                      "waterbody": {"name": water.upper() if wb is None else wb["name"],
                                    "applies_to": at},
                      "split": rec}]
        out.append({"id": pid, "kind": "new_split", "target": n.on_item, "scope": "split",
                    "rule_id": None, "title": ("TIDAL BOUNDARY " if g.get("tidal") else "")
                    + f"new split {sid} on {water}: "
                    + _ANCHOR_WORDS.get(n.anchor, n.anchor) + (f" {n.name}" if n.name else "")
                    + (f" ({n.measure:.0f} m above the mouth; the piece below is tidal water)"
                       if g.get("tidal") and n.measure is not None else ""),
                    "marks": g["marks"], "edits": edits, "depends_on": [], "problems": problems,
                    "notes": notes, "split_id": sid, "confidence": conf,
                    **({"tidal": {"blk": n.blk, "measure": n.measure}} if g.get("tidal") else {})})

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
                        "split_id": None, "confidence": None})
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
            if applied is not None:
                applied.add(m.key)
            continue                                     # the files already say it: applied
        words = []
        from pipeline.regs.review.tickets import fix_words
        words += [fix_words(f) for f in m.extent_fix]
        words += [f"{'the entry' if k == 'entry' else 'rule ' + k}: tributaries {v}d"
                  for k, v in m.tributaries.items()]
        out.append({"id": pid, "kind": "extent", "target": m.target, "scope": m.scope,
                    "rule_id": m.rule_id, "title": f"{m.target}: " + "; ".join(words),
                    "marks": [who], "edits": edits, "depends_on": depends, "problems": problems,
                    "notes": [], "split_id": None, "confidence": None})

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
    order = {"new_split": 0, "tidal_flag": 0, "extent": 1, "lake_split_later": 2, "tidal_mouth": 3,
             "tidal_followup": 3}
    out.sort(key=lambda p: order[p["kind"]])
    return out


def applied_keys(marks: Iterable, src: Sources) -> set:
    """THE MARKS WHOSE OWN FIX HAS LANDED (owner 2026-10-28): the key of every mark in force whose
    extent fix / tributary call the curated files now say exactly (`build` makes no proposal for it
    — new points resolved to the split that landed for them, by anchor, never by proposal content).
    Landing a mark moves its target's binding fingerprint; `marks.status(applied=...)` holds such a
    mark instead of calling it stale, so applying the owner's work never undoes it. Cheap: only the
    scopes that propose extents are built, without the tidal decisions."""
    import dataclasses
    keep = [m for m in marks if m.scope in ("entry", "rule", "dfo")]
    out: set = set()
    build(keep, dataclasses.replace(src, tidal_decisions=None), applied=out)
    return out


def chain_of(p: Mapping, props: Iterable[Mapping]) -> list[Mapping]:
    """A proposal with the new splits it depends on, dependencies first."""
    by_id = {q["id"]: q for q in props}
    return [by_id[d] for d in p.get("depends_on") or () if d in by_id] + [p]


# --------------------------------------------------------------------------- the side rebuild

def side_dir(p: Mapping) -> str:
    """A proposal's side-rebuild directory (repo-relative): named by its id AND digest, so a side
    rebuild made for one state of the proposal never vouches for another."""
    return f"data/generated/side/t185-{_slug(p['id'], 40)}-{p['digest'][:6]}"


def changes_cuts(p: Mapping, props: Iterable[Mapping]) -> bool:
    """Does landing `p` add or move a cut — a split edit in it or in a new split it waits on? Such
    a proposal is bound only on a side atlas: it needs the side rebuild first."""
    # T166: flagging a cut tidal moves no cut, but it moves what is freshwater — the side atlas
    # (its sidecar carries the flag) and its binding diff answer it the same way
    return any(ed["kind"] in ("split", "split_flag") for q in chain_of(p, props) for ed in q["edits"])


def side_ready(p: Mapping) -> bool:
    """Has this proposal's side rebuild been made (its side atlas, `side_commands`)?"""
    return (REPO / side_dir(p) / "atlas").is_dir()


def side_commands(p: Mapping, props: Iterable[Mapping]) -> list[str]:
    """THE SIDE CHAIN for a proposal (printed; Claude runs it before applying a cut change):
    patched copies in a side directory, the side atlas when a split changes, a reach run on them,
    and the per-rule binding diff against the live reach run (AGENTS 63)."""
    from pipeline.common.curated import GENERATED
    live_build = _rel(GENERATED.build())
    live_reach = _rel(GENERATED.reaches / GENERATED.build().name)
    side = side_dir(p)
    splits = changes_cuts(p, props)
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
             "   # per-rule bindings, live vs side — read it before applying (AGENTS 63)"]
    return cmds


def tidal_props(props: Iterable[Mapping]) -> list[Mapping]:
    """T166: every APPLICABLE tidal proposal — a new split flagged `tidal`, or a `tidal_flag`."""
    return [p for p in props if p["applicable"] and (p["kind"] == "tidal_flag" or any(
        ed["kind"] == "split" and ed["split"].get("tidal") for ed in p["edits"]))]


def write_side_many(chain: list[Mapping], src: Sources, out: Path) -> list[Path]:
    """PATCHED COPIES with every proposal of `chain` applied, in order (T166: all the tidal
    boundaries on one side atlas). The same refusal as `write_side`."""
    return write_side(chain[-1], chain, src, out, chain=chain)


def write_side(p: Mapping, props: Iterable[Mapping], src: Sources, out: Path,
               chain: Optional[list] = None) -> list[Path]:
    """PATCHED COPIES for a side rebuild: the catalogue, splits.json and the DFO files, copied into
    `out` with the proposal (and the new splits it names) applied. Refuses a directory under
    data/curated: the curated files are only ever written by `apply_proposals`."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.parsing.io import atomic_write
    out = Path(out).resolve()
    cur = Path(CURATED.waters.splits).resolve().parents[1]
    if out == cur or cur in out.parents:
        raise ValueError(f"{out} is under the curated data ({cur}): a side copy goes elsewhere")
    texts = materialize(chain if chain is not None else chain_of(p, props), src)
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
        if p["kind"] in ("new_split", "tidal_flag"):
            p["check"] = {"cheap": False, "why": "a new split changes the atlas: the side rebuild "
                          "and its binding diff answer it" if p["kind"] == "new_split" else
                          "a tidal flag moves what is freshwater: the side atlas and its binding "
                          "diff answer it (T166)"}
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
    doc = {"_about": "T185 proposed edits from the review marks, waiting to apply — written by "
                     "`python -m pipeline.regs.review.proposals build`; the marks are the "
                     "decisions; `python -m pipeline.regs.review.apply_proposals` (Claude) lands "
                     "each, one commit",
           "made": dt.datetime.now().isoformat(timespec="seconds"),
           "marks": _rel(marks_path), "proposals": rows}
    atomic_write(out / INDEX, json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    return out / INDEX


def read_index(out: Path = OUT_DIR) -> dict:
    p = Path(out) / INDEX
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {"proposals": []}


def _summary(props: list[dict]) -> str:
    lines = []
    for p in props:
        conf = f"  confidence {p['confidence']}" if p.get("confidence") else ""
        lines.append(f"[{p['kind']}] {p['id']}  {'ok' if p['applicable'] else 'n/a'}{conf}"
                     f"\n    {p['title']}")
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
    s.add_argument("id", help="a proposal id, or `tidal` for EVERY applicable tidal proposal at once "
                              "(T166: one side atlas for all the tidal boundaries)")
    s.add_argument("--out", type=Path, required=True)
    for x in (b, s):
        x.add_argument("--marks", type=Path, default=CURATED.regulations.review.marks)
        x.add_argument("--build", type=Path, help="the atlas whose registry names new anchors")
        x.add_argument("--dir", type=Path, default=OUT_DIR, help="the proposals dir")
    a = ap.parse_args(argv)
    src = Sources.live(a.build)
    marks = M.load(a.marks)
    props = build(marks, src)
    if a.cmd == "side" and a.id == "tidal":
        tid = tidal_props(props)
        if not tid:
            print("no applicable tidal proposal", file=sys.stderr)
            return 2
        for w in write_side_many(tid, src, a.out):
            print(f"  {w}")
        print(f"{len(tid)} tidal proposal(s) patched: " + " ".join(p["id"] for p in tid),
              file=sys.stderr)
        return 0
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
    print(_summary(props))
    print(f"{len(props)} proposal(s) -> {_rel(idx)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
