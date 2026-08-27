"""Classify each municipal line against FWA streams — fuzzy + FWA-favouring.

A municipal line is usually an offset re-draw of a real stream, so we compare by coverage within a
generous tolerance, not vertex-exactly:

  - duplicate/subset : most of the municipal line hugs one FWA stream -> DROP it (favour FWA), even if
                       it is only a small part of that FWA stream.
  - extension        : it hugs an FWA stream but a single contiguous tail runs PAST the FWA line's end
                       -> keep only that novel tail, on the FWA stream's own blk/wsc.
  - novel            : it doesn't match any FWA stream -> keep whole; its receiver is resolved later.

Also emits name-variant candidates: when a matched municipal name differs from the FWA name the FWA
name stays boss and the municipal name becomes a searchable alias (display only if FWA is unnamed).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pyproj import Transformer
from shapely.geometry import LineString, Point
from shapely.strtree import STRtree

from pipeline.models import BlkChain

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
_TOL_M = 40.0          # base tol: municipal geometry drifts from FWA
_WIDE_M = 50.0         # wide band: a line offset the WHOLE way from an FWA centerline is still a re-draw
                       # (kept modest so genuine tribs running NEAR an offset FWA aren't called duplicate)
_DUP = 0.80           # covered >= this -> duplicate (drop, favour FWA)
_EXT = 0.30           # covered >= this -> overlaps FWA (partial) -> duplicate unless a real tail extends
_MIN_TAIL_FRAC = 0.15  # the novel tail must be at least this fraction of the municipal line


@dataclass
class Match:
    feature: dict
    line3005: LineString
    klass: str                              # 'duplicate' | 'extension' | 'novel'
    covered: float = 0.0
    fwa_blk: str = ""
    fwa_wsc: str = ""
    fwa_name: str = ""
    clip3005: Optional[LineString] = None   # extension: the novel tail only
    divergence: Optional[tuple] = None      # extension: (x, y) where the tail meets the FWA line


@dataclass
class NameCandidate:
    target_gnis: str
    target_blk: str
    name: str                               # the municipal (non-FWA) name
    display: bool                           # True only when the FWA stream is unnamed
    conflict: bool                          # True when both are named and differ (warn)


def line_to_albers(coords) -> LineString:
    return LineString([_TO_ALBERS.transform(x, y) for x, y in coords])


class _FwaIndex:
    """STRtree over FWA chain geometries (EPSG:3005) with parallel blk/wsc/name arrays."""
    def __init__(self, chains: list[BlkChain]):
        self.geoms = [c.geometry for c in chains]
        self.blk = [str(c.blk) for c in chains]
        self.wsc = [c.fwa_watershed_code for c in chains]
        self.name = [c.gnis_name or "" for c in chains]
        self.gnis = [c.gnis_id or "" for c in chains]
        self.tree = STRtree(self.geoms) if self.geoms else None

    def candidates(self, line: LineString, tol: float) -> list[int]:
        if self.tree is None:
            return []
        return [int(i) for i in self.tree.query(line.buffer(tol))]


def _tail_past_end(line: LineString, fwa_geom: LineString, tol: float) -> Optional[LineString]:
    """The single contiguous uncovered tail of ``line`` if it runs past ``fwa_geom`` — else None."""
    rem = line.difference(fwa_geom.buffer(tol))
    pieces = [rem] if rem.geom_type == "LineString" else \
        [g for g in getattr(rem, "geoms", []) if g.geom_type == "LineString"]
    pieces = [g for g in pieces if g.length > tol]
    if not pieces:
        return None
    tail = max(pieces, key=lambda g: g.length)
    if tail.length < _MIN_TAIL_FRAC * line.length:
        return None
    ends = [Point(line.coords[0]), Point(line.coords[-1])]
    t0, t1 = Point(tail.coords[0]), Point(tail.coords[-1])
    touches_muni_end = min(t0.distance(e) for e in ends) <= tol or \
        min(t1.distance(e) for e in ends) <= tol
    # one tail end is the divergence (at the FWA buffer edge, ~tol away) and near an FWA END — i.e. the
    # FWA line STOPS there; the other tail end runs clearly past it. This rejects mid-river offsets.
    fwa_ends = [Point(fwa_geom.coords[0]), Point(fwa_geom.coords[-1])]
    near = min((t0, t1), key=lambda p: p.distance(fwa_geom))
    far = t1 if near is t0 else t0
    near_is_divergence = (near.distance(fwa_geom) <= tol * 1.6
                          and min(near.distance(e) for e in fwa_ends) <= tol * 3.0)
    if not (touches_muni_end and near_is_divergence and far.distance(fwa_geom) > tol * 1.6):
        return None
    return tail


def _longest_unique(line: LineString, fwa_geom: LineString, tol: float) -> Optional[LineString]:
    """The longest contiguous piece of ``line`` that lies OUTSIDE the FWA's ``tol`` buffer — the part that is
    genuinely this creek's own, not a re-draw of the FWA it runs beside (Brackendale Creek's 467 m unique reach
    where the whole line is 766 m, ~40% on Dryden Creek)."""
    rem = line.difference(fwa_geom.buffer(tol))
    parts = ([rem] if rem.geom_type == "LineString"
             else [g for g in getattr(rem, "geoms", []) if g.geom_type == "LineString"])
    parts = [p for p in parts if p.length > tol]
    return max(parts, key=lambda p: p.length) if parts else None


_NAME_TOL_M = 400.0    # a name-matched FWA stream this close is the same river even if offset (wide rivers)


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _looks_like_trib(name: str) -> bool:
    """True when a municipal name marks the line a tributary ("… Trib 4", "… Trib.2", "… Tributary").
    Such a line is a DISTINCT small stream — it must mint its own sub-code, never be classified as a
    duplicate/extension of the (often unnamed) FWA blue line it merely hugs near the confluence."""
    import re
    return bool(re.search(r"\btrib(?:utary)?\b", (name or "").lower()))


def _dup_match(m: Match, index: _FwaIndex, i: int) -> Match:
    m.klass = "duplicate"
    m.fwa_blk, m.fwa_wsc, m.fwa_name = index.blk[i], index.wsc[i], index.name[i]
    return m


def classify(line3005: LineString, index: _FwaIndex, tol: float = _TOL_M, wide: float = _WIDE_M,
             dup: float = _DUP, ext: float = _EXT, muni_name: str = "") -> Match:
    """Classify one municipal line (already EPSG:3005) against the FWA index."""
    best_i, best_covb, best_covw = -1, 0.0, 0.0
    L = line3005.length
    for i in index.candidates(line3005, wide):
        g = index.geoms[i]
        covw = line3005.intersection(g.buffer(wide)).length / L if L else 0.0
        if covw > best_covw:
            covb = line3005.intersection(g.buffer(tol)).length / L if L else 0.0
            best_i, best_covb, best_covw = i, covb, covw
    m = Match(feature={}, line3005=line3005, klass="novel", covered=best_covb)
    # name-aware: a same-named FWA stream nearby IS this river even if its centerline is far
    # (wide rivers like the Fraser). Favour FWA -> duplicate.
    if muni_name and best_covb < dup:
        nm = _norm(muni_name)
        for i in index.candidates(line3005, _NAME_TOL_M):
            if _norm(index.name[i]) == nm and line3005.distance(index.geoms[i]) <= _NAME_TOL_M:
                return _dup_match(m, index, i)
    if best_i < 0:
        return m                                              # novel (nothing even in the wide band)
    if muni_name and _looks_like_trib(muni_name) and not _norm(index.name[best_i]):
        return m                    # a tributary-named line hugging an UNNAMED FWA line is a DISTINCT
                                    # tributary -> novel (mints a sub-code below the mainstem that shares
                                    # that unnamed FWA), NOT a dup/ext that would inherit the mainstem code.
                                    # A trib-named line matching a NAMED FWA falls through: high overlap =>
                                    # it IS that named stream (a name variant) -> duplicate, favour FWA.
    # a real novel tail past the FWA end => extension (keep the tail) EVEN if mostly covered, so a
    # stream that is 85% on FWA but continues past it doesn't lose its connecting/upstream piece.
    tail = _tail_past_end(line3005, index.geoms[best_i], tol)
    if tail is not None:
        m.klass = "extension"
        m.fwa_blk, m.fwa_wsc, m.fwa_name = index.blk[best_i], index.wsc[best_i], index.name[best_i]
        m.clip3005 = tail
        near = min((Point(tail.coords[0]), Point(tail.coords[-1])),
                   key=lambda p: p.distance(index.geoms[best_i]))
        m.divergence = (near.x, near.y)
        return m
    if best_covw >= dup:                                      # offset re-draw the whole way -> drop, favour FWA
        return _dup_match(m, index, best_i)
    if best_covb >= ext:                                      # partial overlap with the FWA line...
        fwa_nm = _norm(index.name[best_i])
        if not (muni_name and fwa_nm and _norm(muni_name) != fwa_nm):
            return _dup_match(m, index, best_i)              # ...a re-draw (same/unnamed) -> drop, favour FWA
        # a DIFFERENTLY-named creek hugging this FWA for part of its length (Brackendale Creek runs along
        # Dryden Creek ~40%, 59% unique) is its OWN stream -> keep only its UNIQUE reach as a novel, dropping
        # the overlapping part (which IS the FWA there); the clip connects to the FWA at the divergence.
        uniq = _longest_unique(line3005, index.geoms[best_i], tol)
        if uniq is not None and uniq.length >= _MIN_TAIL_FRAC * L:
            m.clip3005 = uniq
    return m                                                  # genuinely new (little overlap even wide)


def _name_candidate(m: Match) -> Optional[NameCandidate]:
    muni = (m.feature.get("properties", {}).get("name") or "").strip()
    if not muni:
        return None
    fwa = (m.fwa_name or "").strip()
    if fwa and fwa.lower() == muni.lower():
        return None                                          # same name, nothing to add
    return NameCandidate(target_gnis=_fwa_gnis(m), target_blk=m.fwa_blk, name=muni,
                         display=not fwa, conflict=bool(fwa))


def _fwa_gnis(m: Match) -> str:
    return getattr(m, "_fwa_gnis", "")


def match_features(features: list[dict], fwa_chains: list[BlkChain],
                   tol: float = _TOL_M) -> tuple[list[Match], list[NameCandidate], dict]:
    """Classify every cleaned municipal feature. Returns (matches, name_candidates, report)."""
    index = _FwaIndex(fwa_chains)
    gnis_by_blk = {str(c.blk): (c.gnis_id or "") for c in fwa_chains}
    matches: list[Match] = []
    candidates: list[NameCandidate] = []
    counts = {"duplicate": 0, "extension": 0, "novel": 0}
    for ft in features:
        line = line_to_albers(ft["geometry"]["coordinates"])
        if line.length <= 0:
            continue
        m = classify(line, index, tol=tol, muni_name=ft.get("properties", {}).get("name", ""))
        m.feature = ft
        setattr(m, "_fwa_gnis", gnis_by_blk.get(m.fwa_blk, ""))
        counts[m.klass] += 1
        matches.append(m)
        if m.klass in ("duplicate", "extension"):
            cand = _name_candidate(m)
            if cand:
                candidates.append(cand)
    report = {"counts": counts,
              "name_conflicts": [c for c in candidates if c.conflict]}
    return matches, candidates, report
