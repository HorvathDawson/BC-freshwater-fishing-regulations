"""T326: THE TIDAL LIMITS — `data/curated/waters/tidal.json`, the one tidal truth, read by the atlas only.

One row per ocean-entering water, keyed by its blue line (`blk`, the Tidal tab's candidate `tidal:<blk>`), saying where
its tidal limit is:

  mouth   the water meets tidal water at its mouth: NOTHING is tidal and NOTHING is cut. The mouth boundary gains
          the name "tidal boundary" (owner 2026-11-02): the id `<water>__tidal_boundary` becomes an ALIAS of the
          boundary standing at the line's lower end (`sectionizer._coincident`, the mechanism region and border
          cuts use), or — at a natural end, where no boundary object stands — of a registry boundary
          `<item>__mouth` (kind `outlet`, ref `mouth:<blk>`). 0 new sections.
  split   an existing splits.json cut (`split`) is the tidal limit: the piece below it is tidal water.
  cut     a new cut at `at` {coord, measure, label}: the atlas adds it as a curated cut whose id follows from the
          anchor (`common.split_ids`), so no id is stored. The piece below it is tidal water.
  whole   the registry item `item` is tidal along its whole length (a Fraser delta channel).

GUARD (memory end-cut-halves-the-water): a tidal row NEVER cuts a line's end. A `mouth` row never becomes a
SplitPoint, and a `cut` within `SLIVER_M` of its line's lower end is refused (`refuse_end_cuts`).

The atlas writes what it made of every row to `tidal.resolved.json` beside registry.json; the reach run, the tiles and
the review app read that sidecar (`reach.outside.tidal_splits` / `tidal_waters`), never this file.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Optional

from pipeline.common import split_ids as _ids
from pipeline.common import tolerances as _tol

FORMAT = "tidal/1"
LIMITS = ("mouth", "split", "cut", "whole")
#: the name every tidal limit carries
LABEL = "tidal boundary"
#: the atlas sidecar (`write_resolved`)
RESOLVED = "tidal.resolved.json"
#: a `cut` this close to its line's lower end is ON the end: refused (`refuse_end_cuts`)
END_M = _tol.SLIVER_M
ABOUT = ("T326: THE TIDAL LIMIT of every ocean-entering water, one row per blue line: `mouth` (an alias at the mouth, "
         "no cut), `split` (an existing splits.json cut), `cut` (a new cut, its id derived from the point), or `whole` "
         "(the item is tidal along its whole length). Rows with a `mark` are written from the Tidal-tab marks by "
         "`python -m pipeline.tools.tidal_limits` (review write-through) — never edit them by hand; rows without one "
         "are the owner's own calls and are kept. Read by the atlas only (`pipeline.atlas.splits.tidal`).")


@dataclass(frozen=True)
class Limit:
    blk: str
    limit: str
    name: str = ""
    split: Optional[str] = None
    at: Optional[Mapping] = None
    item: Optional[str] = None
    decided: str = ""
    mark: Optional[str] = None
    why: str = ""
    extra: Mapping = field(default_factory=dict, compare=False)

    def to_dict(self) -> dict:
        d = {"blk": self.blk, "name": self.name, "limit": self.limit}
        if self.split:
            d["split"] = self.split
        if self.at:
            d["at"] = dict(self.at)
        if self.item:
            d["item"] = self.item
        d["decided"] = self.decided
        if self.mark:
            d["mark"] = self.mark
        if self.why:
            d["why"] = self.why
        return d


def parse(doc: Mapping, where: str = "tidal.json") -> list[Limit]:
    """The rows of a tidal document, validated: a known format and limit, the field each limit needs, and ONE row
    per blue line. Anything else is refused, never skipped."""
    if doc.get("format") != FORMAT:
        raise ValueError(f"{where}: format {doc.get('format')!r}, expected {FORMAT}")
    out, seen = [], set()
    for r in doc.get("waters") or ():
        blk, lim = str(r.get("blk") or ""), r.get("limit")
        if not blk.isdigit():
            raise ValueError(f"{where}: a row with no blue line: {r}")
        if lim not in LIMITS:
            raise ValueError(f"{where}: {blk}: limit {lim!r} is not one of {', '.join(LIMITS)}")
        if blk in seen:
            raise ValueError(f"{where}: {blk} has two rows — one tidal limit per water")
        seen.add(blk)
        if lim == "split" and not r.get("split"):
            raise ValueError(f"{where}: {blk}: limit split names no split")
        if lim == "whole" and not r.get("item"):
            raise ValueError(f"{where}: {blk}: limit whole names no item")
        if lim == "cut":
            at = r.get("at") or {}
            if not (isinstance(at.get("coord"), (list, tuple)) and len(at["coord"]) == 2):
                raise ValueError(f"{where}: {blk}: limit cut has no point (at.coord)")
        stray = sorted(k for k in ("split", "at", "item") if r.get(k) and k != {"split": "split", "cut": "at",
                                                                                   "whole": "item"}.get(lim))
        if stray:
            raise ValueError(f"{where}: {blk}: limit {lim} carries {stray}")
        out.append(Limit(blk=blk, limit=lim, name=str(r.get("name") or ""), split=r.get("split"),
                         at=r.get("at"), item=r.get("item"), decided=str(r.get("decided") or ""),
                         mark=r.get("mark"), why=str(r.get("why") or "")))
    return out


def load(path: Optional[Path] = None) -> list[Limit]:
    """The curated tidal limits. A missing file is refused (AGENTS 37: never a silent "nothing is tidal")."""
    if path is None:
        from pipeline.common.curated import CURATED
        path = CURATED.waters.tidal
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"tidal: {p} is missing — make it: python -m pipeline.tools.tidal_limits")
    return parse(json.loads(p.read_text(encoding="utf-8")), str(p))


def document(rows: Iterable[Limit]) -> str:
    """The file's text: one row per line, by blue line."""
    rows = sorted(rows, key=lambda r: int(r.blk))
    head = json.dumps({"_about": ABOUT, "format": FORMAT}, ensure_ascii=False)[:-1]
    body = ",\n".join(json.dumps(r.to_dict(), ensure_ascii=False) for r in rows)
    return f"{head}, \"waters\": [\n{body}\n]}}\n" if rows else f"{head}, \"waters\": []}}\n"


# --------------------------------------------------------------------------- ids


def _label(r: Limit) -> str:
    if r.limit == "cut":
        name = str((r.at or {}).get("label") or "").strip() or LABEL
        return name if "tidal" in name.lower() else f"{name} ({LABEL})"
    return LABEL


def ids(limits: Iterable[Limit], taken: Iterable[str]) -> dict[str, str]:
    """{blk: id} for every `mouth` and `cut` row — `<water>__tidal_boundary` (a cut: `<water>__<its label>`), past
    every id in `taken` (splits.json) and numbered among themselves by blue line, so the id follows from the rows,
    never from their order. The ONE id function (`common.split_ids`, d9b7c7c40)."""
    rows = []
    for r in limits:
        if r.limit in ("mouth", "cut"):
            water = r.name or f"blk {r.blk}"
            dig = r.blk if r.limit == "mouth" else f"{r.blk}@{(r.at or {}).get('measure')}"
            rows.append((r.blk, _ids.base(water, _label(r)), dig))
    return _ids.assign_ids(rows, taken)


def cut_defs(limits: Iterable[Limit], id_of: Mapping[str, str]) -> list:
    """Each `cut` row as a curated SplitDef on its own blue line (point anchor at `at.coord`)."""
    from pipeline.common.models.splits import SplitDef
    out = []
    for r in limits:
        if r.limit != "cut":
            continue
        lon, lat = r.at["coord"]
        out.append(SplitDef.from_dict({
            "id": id_of[r.blk], "label": _label(r), "blk": r.blk, "entry": f"tidal:{r.name or r.blk}",
            "anchor": {"type": "point", "coord": [float(lon), float(lat)], "is_lonlat": True}}))
    return out


# --------------------------------------------------------------------------- the atlas


def _lowest(graph, blk: str, by_blk: Optional[Mapping] = None):
    """The lowest stream piece of `blk` in `graph` (its mouth end), or None."""
    from pipeline.common.models import NodeKind
    nids = by_blk.get(blk, ()) if by_blk is not None else list(graph.nodes)
    pieces = [graph.nodes[n] for n in nids if graph.nodes.get(n) is not None
              and graph.nodes[n].kind == NodeKind.stream and graph.nodes[n].blk == blk]
    return min(pieces, key=lambda n: n.down_m) if pieces else None


def refuse_end_cuts(points: Iterable, graph, cut_ids: Iterable[str]) -> None:
    """THE GUARD: a tidal cut on (or within `END_M` of) its line's lower end would break both halves (memory
    end-cut-halves-the-water) — such a row is a `mouth`, so the build stops and says so."""
    want, bad = set(cut_ids), []
    for p in points:
        if p.split_id not in want:
            continue
        low = _lowest(graph, p.blk)
        if low is not None and p.route_measure - low.down_m <= END_M:
            bad.append(f"{p.split_id} on {p.blk} at {p.route_measure:.1f} m (line starts {low.down_m:.1f} m)")
    if bad:
        raise SystemExit("tidal: a tidal cut stands on its line's lower end — make it limit `mouth` in "
                         "waters/tidal.json: " + "; ".join(bad))


def alias_mouths(graph, limits: Iterable[Limit], id_of: Mapping[str, str]) -> dict[str, dict]:
    """Put each `mouth` row's tidal id on the boundary standing at its line's lower end (`sectionizer._coincident`
    — an alias, never a cut). Returns {blk: {id, nid, measure, on}}: `on` the boundary aliased, or "" where the mouth
    is a natural end (no boundary object; `add_mouth_boundaries` names it on the registry), or {..., absent} where
    the atlas has no stream piece on that line."""
    from pipeline.atlas.splits.sectionizer import _coincident, _stream_nids_by_blk
    from pipeline.common.models import AnchorType, SplitPoint
    mouths = [r for r in limits if r.limit == "mouth"]
    by_blk = _stream_nids_by_blk(graph, {r.blk for r in mouths})
    out: dict[str, dict] = {}
    for r in mouths:
        sid = id_of[r.blk]
        low = _lowest(graph, r.blk, by_blk)
        if low is None:
            out[r.blk] = {"id": sid, "absent": "no stream piece on this blue line in the atlas"}
            continue
        on = ""
        if low.lower_bound is not None:
            on = str(low.lower_bound.boundary_id)
            sp = SplitPoint(split_id=sid, blk=r.blk, route_measure=low.lower_bound.route_measure, fid="",
                            label=LABEL, anchor_type=AnchorType.point, source="curated")
            if not _coincident(graph, r.blk, sp, by_blk):
                raise SystemExit(f"tidal: the mouth boundary of {r.blk} ({on}) did not take the alias {sid}")
        out[r.blk] = {"id": sid, "nid": low.node_id, "measure": round(low.down_m, 2), "on": on}
    return out


def add_mouth_boundaries(registry: dict, placed: Mapping[str, dict]) -> dict:
    """At a NATURAL mouth (no boundary object stands there) the item whose lowest piece it is gains the boundary
    `<item>__mouth` (kind `outlet`, ref `mouth:<blk>`), whose alias is the tidal id. Returns the registry and fills
    each placed row's `items`."""
    from dataclasses import replace
    from pipeline.common.models.registry import RegistryBoundary
    want = {v["nid"]: blk for blk, v in placed.items() if v.get("nid")}
    for blk in placed:
        placed[blk].setdefault("items", [])
    out = dict(registry)
    for iid, it in registry.items():
        if it.kind == "area":                  # a basin / region / park holds the section, it is not the water
            continue
        hits = [want[s] for s in it.section_ids if s in want]
        for blk in hits:
            v = placed[blk]
            v["items"].append(iid)
            if v["on"]:
                continue                       # aliased onto a standing boundary: the registry carries it
            ref = f"mouth:{blk}"
            if any(b.ref == ref for b in out[iid].boundaries):
                continue
            slug = _ids.slug(it.name) or iid.replace(":", "_")
            b = RegistryBoundary(id=f"{slug}__mouth", label="mouth", kind="outlet", ref=ref,
                                 aliases=(f"split:{v['id']}",), names=(LABEL,))
            out[iid] = replace(out[iid], boundaries=tuple(out[iid].boundaries) + (b,))
    for v in placed.values():
        v["items"] = sorted(v["items"])
    return out


def resolved_rows(limits: Iterable[Limit], id_of: Mapping[str, str], placed: Mapping[str, dict]) -> list[dict]:
    """The sidecar's rows: what the atlas made of each tidal row."""
    rows = []
    for r in sorted(limits, key=lambda r: int(r.blk)):
        row = {"blk": r.blk, "name": r.name, "limit": r.limit}
        if r.limit == "split":
            row["split_id"] = r.split
        elif r.limit == "cut":
            row["split_id"] = id_of[r.blk]
        elif r.limit == "whole":
            row["item"] = r.item
        else:
            v = placed.get(r.blk) or {}
            row["alias"] = id_of[r.blk]
            for k in ("on", "measure", "items", "absent"):
                if k in v:
                    row[k] = v[k]
        rows.append(row)
    return rows


def write_resolved(out_dir: Path, rows: list[dict]) -> Path:
    p = Path(out_dir) / RESOLVED
    p.write_text("[\n" + ",\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n]\n", encoding="utf-8")
    return p


def read_resolved(build_dir) -> list[dict]:
    """The atlas's tidal sidecar. Missing is refused: an atlas without it is not "nothing is tidal"."""
    p = Path(build_dir) / RESOLVED
    if not p.exists():
        raise SystemExit(f"tidal: {p} is missing — the atlas writes it (T326); rebuild the atlas "
                         f"(T678: only the atlas build writes it; there is no post-build tidal rewrite)")
    return json.loads(p.read_text(encoding="utf-8"))
