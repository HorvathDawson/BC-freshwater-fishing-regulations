"""T185: REVIEW MARKS -> PROPOSED EDITS -> applied by Claude, never by the review tool.

    marks.jsonl -> `review.export.load` (in memory, T408)  ->  proposals (one per mark, one per new split)  ->  the review app's
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

WRITES ONLY under `data/generated/review/proposals/` (index.json + one .patch per proposal),
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

from pipeline.common import split_ids as _ids
from pipeline.common.curated import GENERATED

REPO = Path(__file__).resolve().parents[3]
#: where the proposals are written (index.json + <id>.patch) — handoff, never curated data
OUT_DIR = GENERATED.review / "proposals"
INDEX = "index.json"
#: the review app's backend: the in-memory binding path (`reuse.binding_change`) lives there
BACKEND = REPO / "curation-review" / "backend"
#: the proposal kinds — T166: `tidal_flag` flags a cut already in splits.json a TIDAL BOUNDARY;
#: `tidal_mouth` lists a tidal boundary the owner put at the stream's mouth (nothing is tidal, no edit)
KINDS = ("new_split", "tidal_flag", "extent", "lake_split_later", "tidal_mouth", "tidal_followup")
#: T166: a tidal boundary within this many metres of the FWA mouth IS the mouth — no cut is made
#: (a cut on a line's end breaks both halves: memory end-cut-halves-the-water) and nothing is tidal
TIDAL_MOUTH_M = 1.0


def _slug(s: str, n: int = 60) -> str:
    return _ids.slug(s, n)


def _cut_slug(s: str, n: int) -> str:
    """`_slug`, kept unique when the cut at `n` drops something: a slug that was truncated ends in a short hash of the
    whole string (T545: two DFO location ids alike in their first 70 characters — `…boun`, `…boun~2`, `…boun~3` —
    made ONE proposal id, and only one of their marks landed per apply). A slug that fits is unchanged, so every id
    that was already its own stays the same."""
    cut = _slug(s, n)
    if cut == _slug(s, 10 ** 6):
        return cut
    return f"{cut}-{hashlib.sha256(str(s).encode()).hexdigest()[:8]}"


def mark_pid(scope: str, target: str, rule_id: Optional[str] = None) -> str:
    """THE ID of a mark's proposal: `mark-<scope>-<target>[-<rule>]`, one per (scope, target, rule) — see `_cut_slug`."""
    return "mark-" + _slug(scope, 6) + "-" + _cut_slug(target, 70) + ("-" + _cut_slug(rule_id, 40) if rule_id else "")


def duplicate_ids(props: Iterable[Mapping]) -> dict[str, list[Mapping]]:
    """The proposal ids more than one proposal of `props` carries (T545) -> those proposals. An apply keys everything
    by id, so two proposals under one id would land as one and the other would vanish unsaid: `apply_proposals`
    refuses every one of them, by name."""
    by: dict[str, list[Mapping]] = {}
    for p in props:
        by.setdefault(p["id"], []).append(p)
    return {i: ps for i, ps in by.items() if len(ps) > 1}


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
    #: T166: the owner's tidal calls recorded before the Tidal tab (a file); None = none (T326)
    tidal_decisions: Optional[Path] = None
    _reg: Optional[dict] = field(default=None, repr=False)
    _texts: dict = field(default_factory=dict, repr=False)
    #: (T421) whether registry.json was there (None = not read yet), and whether an item was asked for without one:
    #: `applied_keys` refuses that
    _has_reg: Optional[bool] = field(default=None, repr=False)
    _asked_without_registry: bool = field(default=False, repr=False)

    @classmethod
    def live(cls, build: Optional[Path] = None) -> "Sources":
        from pipeline.common.curated import CURATED, GENERATED
        from pipeline.regs.review.marks import load_overrides
        return cls(entries_dir=Path(CURATED.regulations.synopsis),
                   dfo_dir=Path(CURATED.dfo_salmon.places),
                   splits_path=Path(CURATED.waters.cuts),
                   build=Path(build) if build else GENERATED.build(),
                   overrides=tuple(load_overrides(CURATED.review.proposal_overrides)),
                   tidal_decisions=None)

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
            self._has_reg = bool(self.build and p.exists())
        if not self._has_reg:
            self._asked_without_registry = True
        return self._reg.get(iid)

    def registry_path(self) -> Path:
        return Path(self.build or "") / "registry.json"


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
    if ed["kind"] in ("dfo", "dfo_loc"):
        loc = next((x for x in doc.get("locations") or []
                    if x.get("location_id") == ed["location_id"]), None)
        if loc is None or ed["kind"] == "dfo_loc":       # a field of the location itself
            return loc
        return loc.setdefault("binding", {})
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


def materialize(props: Iterable[Mapping], src: Sources, *, strict: bool = True, splice: bool = True) -> dict[Path, str]:
    """THE FILES' NEW TEXT with every edit of `props` made, in order: {path: text} for each file
    they touch (a file's untouched lines keep their spelling, `_splice`). Writes nothing.
    `splice=False`: the text as the serialiser writes it — the same JSON, for a caller that only PARSES it (`build`'s
    validation). The line diff that keeps hand spelling cost ~0.65 s per proposal on the cuts file: every save in the
    review app waited 8–16 s on it through `applied_keys` (T532 r9)."""
    docs: dict[Path, dict] = {}
    for p in props:
        for ed in p["edits"]:
            path = _resolve_file(ed, src)
            if path not in docs:
                docs[path] = src.doc(path)
            apply_edit(docs[path], ed, strict=strict)
    _only_real_split_ids(props, docs.get(Path(src.splits_path)) or src.doc(src.splits_path))
    out: dict[Path, str] = {}
    for path, doc in docs.items():
        text = src.text(path)
        out[path] = _splice(text, _dump(json.loads(text), text), _dump(doc, text)) if splice else _dump(doc, text)
    return out


def is_local_ref(sid) -> bool:
    """A PROPOSAL'S LOCAL ID (T355), never a split id: the review app's "n1" / "n1-2", a hunt's
    "stamp_lagoon_outlet", a tidal "tidal-354091024", a T191 point's "<entry>_0_1". Every real id has
    a "__" (a curated `water__label` slug, `gauge__08HD034`) or a ":" (a minted `border:<blk>:<n>`,
    `area:…`, `lake:<wbk>`)."""
    return not sid or ("__" not in str(sid) and ":" not in str(sid))


def _only_real_split_ids(props: Iterable[Mapping], splits_doc: Mapping) -> None:
    """THE REAL-ID GUARD (T355): every split id an edit writes — an extent's `splits`, a split flagged,
    a new split's own id — is a splits.json id once the edits are made. A mark names a new cut by its
    local ref ("n1-2"); `build` turns each into the slug the new split lands under, and a ref that
    slipped through (a ref no group took, an `anchor: split` naming a proposal) must never reach a
    curated file. ValueError naming it."""
    real = {sp.get("id") for wb in splits_doc.get("waterbodies") or [] for sp in wb.get("splits") or []}
    for p in props:
        for ed in p.get("edits") or ():
            if ed.get("kind") == "split":
                sid = (ed.get("split") or {}).get("id") or ""
                if is_local_ref(sid):
                    raise ValueError(f"{p.get('id')}: a new split lands under its proposal's local id {sid!r}")
                continue
            if ed.get("kind") == "split_flag":
                named = [ed.get("split_id")]
            elif ed.get("field") == "extents" and isinstance(ed.get("after"), list):
                named = [x for ex in ed["after"] if isinstance(ex, Mapping) for x in ex.get("splits") or ()]
            else:
                continue
            bad = [x for x in named if x not in real and is_local_ref(x)]
            if bad:
                raise ValueError(f"{p.get('id')}: names split(s) {', '.join(map(str, bad))} not in splits.json — "
                                 "a proposal's local id never lands in a curated file")


def _resolve_file(ed: Mapping, src: Sources) -> Path:
    """An edit's file in `src` (its name inside the source dir, so a side copy resolves too)."""
    name = Path(ed["file"]).name
    if ed["kind"] == "catalogue":
        return Path(src.entries_dir) / name
    if ed["kind"] in ("dfo", "dfo_loc"):
        return Path(src.dfo_dir) / name
    return Path(src.splits_path)


# --------------------------------------------------------------------------- new splits

_ANCHOR_WORDS = {"confluence": "confluence", "lake_inlet": "lake inlet", "lake_outlet": "lake outlet",
                 "lake": "lake", "bridge": "bridge", "dam": "dam", "sign": "boundary sign",
                 "coord": "map point", "area_boundary": "park boundary", "split": "offset from split",
                 "border": "border"}


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
    elif n.anchor in ("lake_inlet", "lake_outlet", "lake"):
        wbk = _ref(item, "wbk") or ([n.item_id.split(":", 1)[1]] if (n.item_id or "").startswith(
            "wbk:") else [])
        if len(wbk) == 1:
            anchor = {"type": "lake", "wbk": wbk[0]}
            # T355: `lake` is both edges; an offset runs from the end it leaves (the build's rule)
            if not n.offset_m and n.anchor != "lake":
                anchor["edge"] = "inlet" if n.anchor == "lake_inlet" else "outlet"
        else:
            notes.append(f"the registry names no single lake for {n.item_id or n.name}: placed at "
                         "the point the mark stamped")
    elif n.anchor == "area_boundary":
        # owner 2026-11-02 (Little Qualicum Falls park): the park's own boundary, never a hand point
        # T355: any polygon layer (`marks.AREA_LAYERS`), the WSC subtree and the waters it leaves alone
        anchor = {"type": "area_boundary", "area_layer": n.area_layer or "parks_bc", "area_name": n.area,
                  "area_name_field": n.area_name_field, **({"side": n.area_end} if n.area_end else {}),
                  **({"wsc_descendants": True} if n.wsc_descendants else {}),
                  **({"exclude_gnis": list(n.exclude_gnis)} if n.exclude_gnis else {})}
    elif n.anchor == "border":
        # T355: lands ON the border pass's cut; the point only picks the crossing when there are several
        anchor = {"type": "border", **({"coord": [round(n.coord[0], 6), round(n.coord[1], 6)],
                                        "is_lonlat": True} if n.coord else {})}
    elif n.anchor == "split" and n.offset_m:
        # an existing cut + an offset: that cut's own anchor, with the offset as its attribute
        ref = next((sp for wb in src.doc(src.splits_path).get("waterbodies") or []
                    for sp in wb.get("splits") or [] if sp.get("id") == n.split_id), None)
        base = dict((ref or {}).get("anchor") or {})
        if base.get("type") in ("point", "confluence", "lake") and not base.get("offset_m"):
            anchor = {k: v for k, v in base.items() if k != "edge"}
        else:
            notes.append(f"the split {n.split_id} is not a point / confluence / lake without an offset: "
                         "placed at the point the mark stamped")
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

def extent_of(fix, split_of, rest_at_build: bool = False) -> tuple[Optional[dict], list[str]]:
    """A `marks.ExtentFix` as the catalogue's extent (the preview's own shape, T191): op, the
    splits its points name (`split_of(i, point)` -> split id or None), item(s), area. Returns
    (extent, problems). `rest_at_build`: a DFO place's op rest names no siblings — the build takes
    every other placed extent on its water (`entries.to_reach_input`); a catalogue rule's are a hand edit."""
    problems: list[str] = []
    ex: dict = {"op": fix.op}
    if fix.op in ("rest", "other_places"):
        if not rest_at_build:
            problems.append(f"op {fix.op} names its siblings (the entry's other rules): a hand edit")
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


#: T525: the fields of a curated extent an extent fix can say (`extent_of` writes exactly these)
EXPRESSIBLE = frozenset({"op", "splits", "item_id", "item_ids", "area_id"})


def unexpressed(extents: Iterable[Mapping]) -> list[str]:
    """T525: why each curated extent CANNOT be said as an extent fix, one line per extent that cannot: the fields an
    extent fix has no word for. A fix replacing such an extent would drop them, so it is refused (`build`), and the
    review app offers no change of it (`fixes_of_extents`)."""
    out = []
    for i, ex in enumerate(extents):
        extra = sorted(set(ex) - EXPRESSIBLE)
        if extra:
            out.append(f"extent {i + 1} ({ex.get('op')}) holds {', '.join(extra)}, which an extent fix cannot express: "
                       "a hand edit")
    return out


def fixes_of_extents(extents: Iterable[Mapping], tributaries: Optional[bool]) -> tuple[list, list[str]]:
    """T525: A PLACE'S CURATED EXTENTS AS EXTENT FIXES — the pieces the review app's editor opens on, so a change
    starts FROM the extent the place has (never from an empty editor or an old mark). The inverse of `extent_of`:
    each extent's cuts as `split` points, its item(s), its area, and the place's own tributary flag as it is (None
    stays "no call": the place keeps inheriting its water's). -> (fixes, why not): every extent `unexpressed` names,
    or that the `ExtentFix` model refuses, is a line in `why not`, and no fixes are returned (a part of the extent
    is never dropped)."""
    from pipeline.regs.review.marks import ExtentFix
    extents = list(extents)
    why = unexpressed(extents)
    fixes = []
    for i, ex in enumerate(extents):
        items = list(ex.get("item_ids") or ([ex["item_id"]] if ex.get("item_id") else []))
        try:
            fixes.append(ExtentFix(scope="entry", op=ex["op"], items=items, tributaries=tributaries,
                                   points=[{"kind": "split", "split_id": s} for s in ex.get("splits") or []],
                                   area=ex.get("area_id")))
        except ValueError as e:
            why.append(f"extent {i + 1} ({ex.get('op')}): {e}")
    return ([], why) if why else (fixes, [])


def grouping_note(m) -> str:
    """the note a "same place" mark leaves on the place: his own words and the date, else that he confirmed it, when,
    and which mark (12 hex of the mark's stored line, `recheck.mark_digest`)"""
    from pipeline.regs.review import recheck as RC
    said = " ".join((m.note or "").split())
    if said:
        return f"{said} — owner confirmed same place {m.date}"
    return f"owner confirmed same place, {m.date}, mark {RC.mark_digest(m)}"


def _variant_proposal(m, pid: str, who: dict, src: "Sources") -> dict:
    """A DFO wording ATTACHED AS A VARIANT (owner 2026-11-02): the target's `duplicate_of` set to the
    place it rewords, confirmed — the entry model's own variant link (`EntryLocation.is_variant`)."""
    problems: list[str] = []
    edits: list[dict] = []
    found, there = _find_location(src, m.target), _find_location(src, m.variant_of)
    if found is None:
        problems.append(f"no DFO location {m.target}")
    if there is None:
        problems.append(f"no DFO location {m.variant_of} to be a variant of")
    elif found is not None:
        path, loc = found
        if there[1].get("water_id") != loc.get("water_id"):
            problems.append(f"{m.variant_of} is on another water")
        if (there[1].get("duplicate_of") or "") == m.target:
            problems.append(f"{m.variant_of} is already a variant of {m.target}: no loops")
        for fld, val in (("duplicate_of", m.variant_of), ("duplicate_confirmed", True)):
            before = loc.get(fld, _MISSING)
            if before != val:
                edits.append({"file": _rel(path), "kind": "dfo_loc", "location_id": m.target,
                              "scope": "entry", "field": fld, "before": before, "after": val})
        # WHY IT IS THE SAME PLACE, ON THE PLACE (T610): a confirmed grouping carries its reasoning in the binding's
        # notes ("an exemption that carries no reasoning is indistinguishable from a mistake",
        # `test_a_curator_confirmed_grouping_records_why_it_crossed_an_op`) — the grouping apply wrote the link and
        # no note (the owner's Serpentine link, 2026-11-10). The note is the owner's own, from his mark; with none,
        # that he confirmed it, when, and on which mark. Written once: a place that has a note keeps it.
        b = loc.get("binding") or {}
        if not problems and not (b.get("notes") or []):
            edits.append({"file": _rel(path), "kind": "dfo", "location_id": m.target, "scope": "entry",
                          "field": "notes", "before": b.get("notes", _MISSING), "after": [grouping_note(m)]})
    return {"id": pid + "-variant", "kind": "extent", "target": m.target, "scope": m.scope,
            "rule_id": None, "title": f"{m.target}: a wording of {m.variant_of} (variant)",
            "marks": [who], "edits": edits, "depends_on": [], "problems": problems,
            "notes": [], "split_id": None, "confidence": None}


def _apart_proposal(m, pid: str, who: dict, src: "Sources") -> dict:
    """A DFO wording TAKEN OUT OF ITS GROUP (T532, owner 2026-11-09): the target's `duplicate_confirmed` false and its
    `duplicate_of` cleared — the entry model's CONTESTED state (`EntryLocation.duplicate_confirmed`): it is bound on
    its own (it no longer rides the other place's extent, so it needs one) and no grouping re-absorbs it
    (`review.dfo.group_why`). A later `variant_of` mark puts it back."""
    problems: list[str] = []
    edits: list[dict] = []
    found = _find_location(src, m.target)
    if found is None:
        problems.append(f"no DFO location {m.target}")
    else:
        path, loc = found
        for fld, val in (("duplicate_of", None), ("duplicate_confirmed", False)):
            before = loc.get(fld, _MISSING)
            if before != val and not (val is None and before is _MISSING):
                edits.append({"file": _rel(path), "kind": "dfo_loc", "location_id": m.target,
                              "scope": "entry", "field": fld, "before": before, "after": val})
    return {"id": pid + "-apart", "kind": "extent", "target": m.target, "scope": m.scope,
            "rule_id": None, "title": f"{m.target}: not the same place as {m.not_variant_of} (stands alone)",
            "marks": [who], "edits": edits, "depends_on": [], "problems": problems,
            "notes": [], "split_id": None, "confidence": None}


def _tidal_water_proposal(m, pid: str, who: dict, src: "Sources") -> dict:
    """T449 (owner 2026-11-06): A DFO PLACE WHOLLY IN TIDAL WATER — the owner's call on the proposal
    (`review.dfo.tidal_proposal`). "tidal water" sets the place's `tidal_water` (it binds nothing and is not missing);
    "not tidal" takes it back if a former acceptance set it. A place with an extent is refused: it binds something."""
    problems: list[str] = []
    edits: list[dict] = []
    found = _find_location(src, m.target)
    if found is None:
        problems.append(f"no DFO location {m.target}")
    else:
        path, loc = found
        before = loc.get("tidal_water", _MISSING)
        if m.call == "tidal water":
            if (loc.get("binding") or {}).get("extents"):
                problems.append(f"{m.target} has an extent: a place in tidal water binds nothing — clear the extent "
                                "first, or reject the tidal proposal")
            if before is not True:
                edits.append({"file": _rel(path), "kind": "dfo_loc", "location_id": m.target, "scope": "entry",
                              "field": "tidal_water", "before": before, "after": True})
        elif before is not _MISSING:
            edits.append({"file": _rel(path), "kind": "dfo_loc", "location_id": m.target, "scope": "entry",
                          "field": "tidal_water", "before": before, "after": _MISSING})
    title = (f"{m.target}: wholly in tidal water (binds nothing)" if m.call == "tidal water"
             else f"{m.target}: not in tidal water")
    return {"id": pid + "-tidal-water", "kind": "extent", "target": m.target, "scope": m.scope,
            "rule_id": None, "title": title, "marks": [who], "edits": edits, "depends_on": [],
            "problems": problems, "notes": [], "split_id": None, "confidence": None}


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
        m.extent_fix or m.tributaries or m.new_splits or m.lake_split_later
        or getattr(m, "variant_of", None) or getattr(m, "not_variant_of", None) or _dfo_tidal_call(m))


def _dfo_tidal_call(m) -> bool:
    """T449: a DFO mark that calls a place tidal water (or takes the call back)."""
    return m.scope == "dfo" and getattr(m, "call", None) in ("tidal water", "not tidal")


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
                       "note": m.note, "scope": m.scope}],
            "edits": [], "depends_on": [], "problems": [], "notes": [n.note] if n.note else [],
            "split_id": None, "confidence": None}


def _tidal_decides(m) -> bool:
    """Does a tidal mark in force DECIDE a tidal boundary? Only Place y with its one cut (the point
    the owner chose: the proposal, a polygon crossing, the words' landmark or his own). A wrong or
    unsure mark decides nothing yet — one carrying a point is LISTED for follow-up (Bear River:
    "most likely the split is at the bridge, we should look more online"), never applied."""
    return (m.scope == "tidal" and len(m.new_splits) == 1 and m.verdict == "verified"
            and not polygon_point(m.new_splits[0]))


def _tidal_whole(m) -> bool:
    """T359: does a tidal mark in force say THE WHOLE WATER IS TIDAL (Place y, call "whole", no cut)?"""
    return m.scope == "tidal" and m.verdict == "verified" and m.call == "whole" and not m.new_splits


def _tidal_followup(m) -> bool:
    """A tidal mark in force that is not a decision but says something: wrong / unsure, or a Place y
    on a polygon point (`POLYGON_POINT_WORDS`)."""
    return m.scope == "tidal" and (m.verdict in ("wrong", "unsure") or (
        m.verdict == "verified" and len(m.new_splits) == 1 and polygon_point(m.new_splits[0])))


def tidal_decision_marks(path: Optional[Path], candidates: Optional[Path] = None) -> list:
    """THE OWNER'S EARLIER TIDAL CALLS (a calls file) as tidal marks: each candidate's proposed
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


#: a tidal point this near (route metres, same blue line) an existing cut IS that cut — flag it, never a second
#: cut beside it (the atlas refuses two of one water's cuts within 5 m: Nanaimo's tidal boundary on the Cedar Road
#: bridge, 0.06 m; Ksi Gingolx's highway bridge on the Kincolith River bridge, 4 m). T212's reuse distance.
TIDAL_REUSE_M = 25.0


def _cut_already_there(n, have: Mapping, src: "Sources") -> Optional[str]:
    """The splits.json cut the built atlas resolved within `TIDAL_REUSE_M` of the tidal point `n` on its blue line."""
    blk, m = getattr(n, "blk", None), getattr(n, "measure", None)
    if blk is None or m is None or not src.build:
        return None
    p = Path(src.build) / "splits.resolved.json"
    if not p.exists():
        return None
    rows = [r for r in json.loads(src.text(p)) if str(r.get("blk")) == str(blk) and r.get("split_id") in have
            and r.get("route_measure") is not None and abs(float(r["route_measure"]) - float(m)) <= TIDAL_REUSE_M]
    return min(rows, key=lambda r: abs(float(r["route_measure"]) - float(m)))["split_id"] if rows else None


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
    # a DFO AREA DESCRIPTION is no place name ("That portion of Toquart Bay in Subarea 23-10 bounded on the …",
    # cut to 60 characters by the mark): the cut is the tidal boundary, named by the subarea it quotes
    if _DFO_AREA_WORDS.match(name):
        sub = re.search(r"\bSubarea\s+(\d+-\d+)", name, re.IGNORECASE)
        name = f"tidal boundary (DFO Subarea {sub.group(1)})" if sub else "tidal boundary"
    return name if "tidal" in name.lower() else f"{name} (tidal boundary)"


#: the openings of a DFO area / subarea description quoted as a tidal point's words (T212 pass 2)
_DFO_AREA_WORDS = re.compile(r"^\s*(that portion|those waters|the waters|subarea\s+\d)", re.IGNORECASE)


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


#: the fields that make a proposed cut THIS cut (its id's tie-break): the anchor, never the order it came in
_ID_FIELDS = ("anchor", "on_item", "item_id", "split_id", "coord", "blk", "measure", "offset_m", "area",
              "area_end", "area_layer", "wsc_descendants", "exclude_gnis")


def proposal_digest(n) -> str:
    """A proposed cut's anchor as a short digest — the same anchor always gives the same digest."""
    d = n.model_dump(mode="json")
    return _h({k: d.get(k) for k in _ID_FIELDS})


def split_base(n, water: str) -> str:
    """THE SLUG a new split lands under, before a clash suffix: `<water>__<name or anchor>`, an offset cut ending
    `_u100m` / `_d100m` (`common.split_ids`, T523)."""
    return _ids.base(water, n.name or n.anchor, n.offset_m)


#: the one id function (`common.split_ids.assign_ids`): the atlas mints tidal ids with it too (T326)
assign_ids = _ids.assign_ids


def place_groups(rows: Iterable[tuple], src: Sources, splits_doc: Optional[Mapping] = None,
                 have: Optional[Mapping] = None) -> dict:
    """EACH PROPOSED CUT'S PLACE IN splits.json — (gid, NewSplit, tidal, tidal_water) rows ->
    {gid: {id, dup, water, split, at, wb, problems}}: the water it cuts, the split as it lands (a tidal
    cut relabelled), the existing split it already is (`dup`), else its REAL id (`assign_ids`). The one
    function the proposals AND the review app's display take ids from, so a proposal shows the id it lands as."""
    if splits_doc is None:
        splits_doc = src.doc(src.splits_path)
    if have is None:
        have = {sp["id"]: sp for wb in splits_doc.get("waterbodies") or []
                for sp in wb.get("splits") or [] if sp.get("id")}
    out: dict = {}
    for gid, n, tidal, tidal_water in rows:
        problems: list[str] = []
        at = _applies_to(n.on_item) if n.on_item else None
        if at is None:
            problems.append(f"the cut names no stream it cuts (on_item {n.on_item!r})")
        wb = next((w for w in splits_doc.get("waterbodies") or [] if w.get("applies_to") == at),
                  None) if at else None
        water = (wb or {}).get("name") or ((src.item(n.on_item) or {}).get("name")
                                           if n.on_item else None) or tidal_water or (n.on_item or "water")
        if tidal:                                        # T166: labelled as its water's cuts are
            n = n.model_copy(update={"name": _tidal_label(n, water)})
        dup = None
        try:
            probe, _ = split_record(n, src, sid="probe", provenance="probe")
            dup = next((sid for sid, sp in have.items() if sp.get("anchor") == probe["anchor"]
                        and probe["anchor"]["type"] != "lake"), None)
        except ValueError as ex:
            problems.append(str(ex))
        if dup is None and tidal:                        # T166: a tidal point ON a cut already there
            dup = _cut_already_there(n, have, src)
        out[gid] = {"dup": dup, "water": water, "split": n, "at": at, "wb": wb, "problems": problems,
                    "base": split_base(n, water), "digest": proposal_digest(n)}
    ids = assign_ids([(g, v["base"], v["digest"]) for g, v in out.items() if v["dup"] is None], have)
    for g, v in out.items():
        v["id"] = v["dup"] or ids[g]
    return out


def build(decided, src: Sources, *, applied: Optional[set] = None) -> list[dict]:
    """EVERY PROPOSAL the decided data makes against the files NOW, new splits first. Pure: reads `src`.
    `decided`: the marks' decisions (`review.export.load` — what apply paths read), or an
    iterable of marks, decided first (`review.export.decide`, the owner's earlier tidal
    calls `src.tidal_decisions` folded in before them, so a mark made since supersedes one).
    `applied`, when given, collects the key of every mark whose extent / tributary fix the files
    already say (landed: no proposal is made for it) — `applied_keys`."""
    from pipeline.regs.review import marks as M
    from pipeline.regs.review import export as E
    if not isinstance(decided, E.Decided):
        decided = E.decide(decided, tidal_decisions=src.tidal_decisions)
    latest = decided.inforce
    inforce = [m for m in latest if _proposes(m)]
    shared = decided.shared(src.overrides)             # T210: one per distinct cut, every mark
    splits_doc = src.doc(src.splits_path)
    have = {sp["id"]: sp for wb in splits_doc.get("waterbodies") or []
            for sp in wb.get("splits") or [] if sp.get("id")}

    # the cuts the marks in force name: their new splits (by the shared group) and their NEW points
    groups: list[dict] = []                              # {gid, split (NewSplit), marks}

    def _join(g: dict, n, who: dict) -> dict:
        if who not in g["marks"]:
            g["marks"].append(who)
        g["members"].append(n)
        return g

    def group_for(n, who: dict) -> Optional[dict]:
        # T639: a cut the owner TOOK BACK (a `withdrawn` override, T589) is not built — `marks.amend` left it out
        # of `shared`, and this must not make it again from the mark that still names it. A fix that points at it
        # finds no split and says so (`split_of` -> None), never a quiet rebuild.
        if any(o.withdrawn and o.matches([n]) for o in src.overrides):
            return None
        # the shared row(s) this proposal is a member of — an owner override may have moved it or
        # made it a reference with one cut per offset (`marks.amend`): every one is a cut
        shs = [p for p in shared if any(M.same_anchor(n, q) for q in p.get("members") or [p["split"]])]
        if shs:
            got = []
            for sh in shs:
                g = next((g for g in groups if g["gid"] == sh["id"]), None)
                if g is None:
                    g = {"gid": sh["id"], "split": sh["split"], "marks": [], "members": []}
                    groups.append(g)
                got.append(_join(g, n, who))
            return got[0]
        for g in groups:
            if any(M.same_anchor(n, q) for q in g["members"]):
                return _join(g, n, who)
        g = {"gid": n.ref, "split": n, "marks": [who], "members": [n]}
        groups.append(g)
        return g

    ref_group: dict[tuple, dict] = {}                    # (mark key, ref | #i) -> group
    for m in inforce:
        who = {"target": m.target, "rule_id": m.rule_id, "reviewer": m.reviewer, "date": m.date, "scope": m.scope}
        for n in m.new_splits:
            if n.anchor != "split" or n.offset_m:     # an existing cut + an offset is a new cut
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

    # T326: a tidal decision is no split proposal any more — it lands in waters/tidal_limits.json, the one tidal truth
    # (`pipeline.tools.tidal_limits`, the tidal write-through), and a new tidal cut is minted by the atlas from it.
    out: list[dict] = []
    # ---- new splits
    gid_split: dict[str, str] = {}                       # group id -> its split id (new or existing)
    # every pending cut is placed — the ones no mark in force names too — so a clash is numbered the same
    # here as in the review app (which places every proposal, `place_groups`)
    mine = {g["gid"] for g in groups}
    placed = place_groups([(g["gid"], g["split"], bool(g.get("tidal")), g.get("tidal_water")) for g in groups]
                          + [(sh["id"], sh["split"], False, None) for sh in shared if sh["id"] not in mine],
                          src, splits_doc, have)
    for g in groups:
        pl = placed[g["gid"]]
        n, water, at, wb, problems = pl["split"], pl["water"], pl["at"], pl["wb"], list(pl["problems"])
        sh = next((p for p in shared if p["id"] == g["gid"]), None)
        conf = sh["confidence"] if sh else M.confidence_of(g["members"], src.overrides)
        if g.get("tidal_conf") and not any(x.confidence for x in g["members"]) and not any(
                M.same_anchor(o.match, g["split"]) for o in src.overrides):
            conf = g["tidal_conf"]                       # T166: the tidal candidate's own grade
        pid = f"split-{_slug(g['gid'], 40)}"
        targets = ", ".join(sorted({w["target"] for w in g["marks"]}))
        prov = f"proposed in review marks on {targets}"
        dup = pl["dup"]
        if dup is not None:                              # already in splits.json: applied
            gid_split[g["gid"]] = dup
            if g.get("tidal"):                           # T166: ... but flagged tidal?
                out.extend(_tidal_flag(dup, have, src, g["marks"], water))
            continue
        sid = pl["id"]                                   # THE REAL ID, from the anchor (`place_groups`)
        gid_split[g["gid"]] = sid
        rec, notes, edits = None, [], []
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
        pid = mark_pid(m.scope, m.target, m.rule_id)
        if m.lake_split_later:
            out.append({"id": pid + "-lake-split", "kind": "lake_split_later", "target": m.target,
                        "scope": m.scope, "rule_id": m.rule_id,
                        "title": f"{m.target}: needs a lake split later (deferred, RULINGS K5)",
                        "marks": [who], "edits": [], "depends_on": [], "problems": [],
                        "notes": ["listed by `python -m pipeline.regs.review.tickets "
                                  "--lake-splits`; no curated edit until lake splits are taken up"],
                        "split_id": None, "confidence": None})
        if m.scope == "dfo" and getattr(m, "variant_of", None):
            vp = _variant_proposal(m, pid, who, src)
            if vp["edits"] or vp["problems"]:
                out.append(vp)
            elif applied is not None and not (m.extent_fix or m.tributaries):
                # the place file already says it (the write-through wrote the link): APPLIED — an edit-less proposal
                # used to be re-offered and read "refused: not applicable: no edit" (owner's Serpentine link, 11-10)
                applied.add(m.key)
        if m.scope == "dfo" and getattr(m, "not_variant_of", None):
            ap = _apart_proposal(m, pid, who, src)
            if ap["edits"] or ap["problems"]:
                out.append(ap)
            elif applied is not None and not (m.extent_fix or m.tributaries):
                applied.add(m.key)                       # the place already says it: applied
        if _dfo_tidal_call(m):
            tp = _tidal_water_proposal(m, pid, who, src)
            if tp["edits"] or tp["problems"]:
                out.append(tp)
            elif applied is not None and not (m.extent_fix or m.tributaries):
                applied.add(m.key)                       # the place already says it: applied
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
                if n is not None and n.anchor == "split" and not n.offset_m:
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
            ex, probs = extent_of(f, lambda i, p, _fi=fi: split_of(_fi, i, p), rest_at_build=m.scope == "dfo")
            problems += [f"{f.scope}: {x}" for x in probs]
            # T524: a DFO mark's fixes are the PIECES of its place — every one lands, in order (the union); any
            # other mark holds one fix per scope (`Mark`), so this is the one-element list it always was
            got = sets.setdefault(f.scope, {})
            got.setdefault("extents", []).append(ex)
            if f.tributaries is not None:                # one call for every piece: `marks.pieces_ok`
                got[tri_field] = f.tributaries
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
                    # T525: the extents an extent fix replaces must be ones it can say again — a field the review
                    # editor cannot express (a `within_area`, `outside_areas`, ...) would be dropped by the write
                    if "extents" in fields and fields["extents"] != b.get("extents"):
                        problems += [f"entry: {x}" for x in unexpressed(b.get("extents") or [])]
                    for fld, val in fields.items():
                        before = b.get(fld, _MISSING)
                        if before != val:
                            edits.append({"file": _rel(path), "kind": "dfo",
                                          "location_id": m.target, "scope": "entry",
                                          "field": fld, "before": before, "after": val})
                    # T525: a mark made on another extent than the file has now (`Mark.extent_before`) never lands over
                    # it — the place moved after the owner looked (another mark landed, a hand edit); a mark whose own
                    # fix has landed changes nothing, so it is not held
                    seen = m.extent_before
                    if seen is not None and edits and (seen.extents != (b.get("extents") or [])
                                                       or seen.tributaries != b.get("tributaries")):
                        problems.append("entry: the extent changed since this mark was made (it saw "
                                        f"{json.dumps(seen.model_dump())}): mark the place again")
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
            texts = materialize(chain, src, splice=False)      # parsed below, never written: no spelling to keep
        except ValueError as ex:
            p["problems"].append(str(ex))
            p["applicable"] = False
            continue
        for path, text in texts.items():
            if path.parent == Path(src.entries_dir):
                bad = _validate_catalogue(json.loads(text))
            elif path.parent == Path(src.dfo_dir):
                bad = [x for ed in p["edits"] if ed["kind"] in ("dfo", "dfo_loc")
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
    cheap = dataclasses.replace(src, tidal_decisions=None)
    build(keep, cheap, applied=out)
    # (T421) a new point's anchor comes from the registry; without one it falls back to the stamped point, which never
    # equals the split that landed — every such mark would read "binding moved". Name the missing build instead.
    if cheap._asked_without_registry:
        raise FileNotFoundError(f"applied_keys: marks need the build's registry to tell a landed fix from a moved "
                                f"binding, and there is none at {cheap.registry_path()} (build the atlas, or point "
                                f"CURATION_BUILD at one)")
    return out


def chain_of(p: Mapping, props: Iterable[Mapping]) -> list[Mapping]:
    """A proposal with the new splits it depends on, dependencies first."""
    by_id = {q["id"]: q for q in props}
    return [by_id[d] for d in p.get("depends_on") or () if d in by_id] + [p]


# --------------------------------------------------------------------------- the side rebuild

#: a side directory's curated root (`write_side`): `<side>/curated`, the layout of data/curated
SIDE_ROOT = "curated"


def side_dir(p: Mapping) -> str:
    """A proposal's side-rebuild directory (repo-relative): named by its id AND digest, so a side
    rebuild made for one state of the proposal never vouches for another."""
    # T562: `_cut_slug`, not a bare cut — two proposal ids alike in their first 40 characters AND their digest's first
    # six never share a directory (an id that fits in 40 keeps the directory it had)
    return f"data/generated/side/t185-{_cut_slug(p['id'], 40)}-{p['digest'][:6]}"


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
    root = f"{side}/{SIDE_ROOT}"
    from pipeline.common.curated import load_repo
    live = load_repo()
    cuts = f"{root}/{Path(live.waters.cuts).relative_to(live.base)}"
    corpus = f"{root}/{Path(live.regulations.synopsis).relative_to(live.base)}"
    splits = changes_cuts(p, props)
    dfo = any(ed["kind"] in ("dfo", "dfo_loc") for ed in p["edits"])
    cmds = [f"python -m pipeline.regs.review.proposals side {p['id']} --out {side}"]
    build = live_build
    if splits:
        cmds.append(f"scripts/heavy.sh atlas 30 -- python -m pipeline.atlas.build --full "
                    f"--splits {cuts} --out {side}/atlas")
        build = f"{side}/atlas"
    if dfo:
        if not splits:
            return ["# a DFO location is not in the reach run: the in-memory check above IS its "
                    "binding (no side rebuild)"]
        cmds.append(f"CURATION_BUILD={side}/atlas CURATED_ROOT={root} "
                    "bash curation-review/backend/run.sh"
                    "   # the DFO location on the side atlas (stop the live app first, or set "
                    "its port)")
        return cmds
    cmds += [f"scripts/heavy.sh reach 30 -- python -m pipeline.atlas.reach.cli --build {build} "
             f"--entries {corpus} --out {side}/reach",
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
    """PATCHED COPIES for a side rebuild: a whole curated root at `out/curated` (`curated.make_root`, the root a
    side app runs on with `CURATED_ROOT`, T422) with the proposal (and the new splits it names) applied to its
    catalogue, cuts and DFO files. Returns the patched files. Refuses a directory under data/curated: the curated
    files are only ever written by `apply_proposals`."""
    from pipeline.common import curated as C
    from pipeline.regs.parsing.io import atomic_write
    out = Path(out).resolve()
    live = C.load_repo()
    cur = Path(live.base).resolve()
    if out == cur or cur in out.parents:
        raise ValueError(f"{out} is under the curated data ({cur}): a side copy goes elsewhere")
    texts = materialize(chain if chain is not None else chain_of(p, props), src)
    root = C.make_root(out / SIDE_ROOT)

    def at(key: Path) -> Path:
        return root / Path(key).resolve().relative_to(cur)
    written = []
    for d, key in ((src.entries_dir, live.regulations.synopsis), (src.dfo_dir, live.dfo_salmon.places)):
        for f in _region_files(d):
            dst = at(key) / f.name
            atomic_write(dst, texts.get(f, src.text(f)))
            written.append(dst)
    dst = at(live.waters.cuts)
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
        if p["edits"] and all(ed["kind"] == "dfo_loc" for ed in p["edits"]):
            # a wording attached as a variant (owner 2026-11-02): no extent of its own changes — it
            # rides the place it rewords (`resolve_all` binds a variant through its primary)
            p["check"] = {"cheap": True, "why": None, "rules": {}, "changed": []}
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
            name = f"{_slug(p['id'], 200)}.patch"             # the whole id (T545: a cut one could clash)
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
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.proposals",
                                 description="review marks -> proposed edits (T185); writes only "
                                             "the proposals dir (and side copies)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="the proposals, written to the proposals dir")
    b.add_argument("--check", action="store_true",
                   help="bind each proposal in memory before/after (loads the graph)")
    s = sub.add_parser("side", help="patched copies of the curated files for a side rebuild")
    s.add_argument("id", help="a proposal id, `a,b,c` for several in ONE side atlas (each with the new splits it "
                              "waits on), or `tidal` for EVERY applicable tidal proposal at once "
                              "(T166: one side atlas for all the tidal boundaries)")
    s.add_argument("--out", type=Path, required=True)
    for x in (b, s):
        x.add_argument("--marks", type=Path, default=None,
                       help="the marks store whose decisions are proposed (default CURATED's)")
        x.add_argument("--build", type=Path, help="the atlas whose registry names new anchors")
        x.add_argument("--dir", type=Path, default=OUT_DIR, help="the proposals dir")
    a = ap.parse_args(argv)
    from pipeline.regs.review import export as E
    src = Sources.live(a.build)
    from pipeline.common.curated import CURATED
    marks_path = Path(a.marks or CURATED.review.marks)
    props = build(E.load(marks_path), src)
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
    if a.cmd == "side" and "," in a.id:
        want = [i for i in a.id.split(",") if i]
        got = {q["id"]: q for q in props if q["id"] in want}
        bad = [i for i in want if i not in got or not got[i]["applicable"]]
        if bad:
            print(f"no applicable proposal {', '.join(bad)}", file=sys.stderr)
            return 2
        chain = list({q["id"]: q for i in want for q in chain_of(got[i], props)}.values())
        for w in write_side_many(chain, src, a.out):
            print(f"  {w}")
        print(f"{len(chain)} proposal(s) patched: " + " ".join(q["id"] for q in chain), file=sys.stderr)
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
    idx = write(props, a.dir, src, marks_path=marks_path)
    print(_summary(props))
    print(f"{len(props)} proposal(s) -> {_rel(idx)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
