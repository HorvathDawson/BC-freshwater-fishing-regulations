"""Normalize the raw municipal GeoJSON layers into one uniform, cruft-free schema.

Each source (Port Moody / Burnaby / Squamish / Abbotsford — see SOURCES.md) has its own ArcGIS
property soup. `clean_source` flattens MultiLineStrings, drops the cruft (we only carry mapped
fields), and maps every feature to:

    {source, src_id, name, connect_to_hint, ftype, fish, species, trib_parent}

Nothing is filtered by type — `ftype`/`fish` are carried as tags so a later batch can filter. Output
is a lon/lat FeatureCollection written to `data/cleaned/<source>.geojson`.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Callable, Optional

_DATA = Path(__file__).resolve().parent / "data"
_CLEANED = _DATA / "cleaned"

_JUNK_NAMES = {"", "hide", "na", "n/a", "none", "null", "unnamed"}


def _title(name: Optional[str]) -> str:
    """Title-case an ALL-CAPS/whitespace name for matching (`STONEY CREEK` -> `Stoney Creek`);
    leave already-mixed-case names as-is. Empty/junk -> ''. """
    s = (name or "").strip()
    if s.lower() in _JUNK_NAMES:
        return ""
    if s.isupper():
        s = s.title()
    return s


def _norm_fish(v: Optional[str]) -> str:
    """Collapse the many fish-bearing spellings across sources to a small set."""
    s = (v or "").strip()
    if not s:
        return ""
    u = s.upper()
    if u.startswith("UNOFFICIAL:"):                       # abbotsford: 'UNOFFICIAL: CLASS A (RED)'
        m = re.search(r"CLASS\s+[A-Z0-9]+", u)
        return m.group(0).title() if m else "Unclassified"
    if u in ("Y", "YES"):
        return "yes"
    if u in ("U", "UNCONFIRMED", "UNCONFIRMED - PROBABLE"):
        return "unknown"
    if u in ("N", "NO"):
        return "no"
    if u == "POTENTIAL":
        return "potential"
    return s                                              # e.g. port_moody feature codes, kept verbatim


_TRIB_RE = re.compile(r"^(.*?)\s+Trib\.?\s*\d", re.IGNORECASE)   # 'Beaver Trib.1' -> 'Beaver'


def _trib_parent(name: str) -> str:
    """Base creek from a `X Trib.N` style name, normalised toward a full creek name."""
    m = _TRIB_RE.match(name or "")
    if not m:
        return ""
    base = m.group(1).strip()
    if base and not re.search(r"\b(creek|brook|river|slough)\b", base, re.IGNORECASE):
        base += " Creek"                                  # 'Beaver' -> 'Beaver Creek'
    return base


def _pm_connect(description: Optional[str]) -> str:
    """Port Moody `description` free text -> a receiver name when it encodes topology."""
    s = (description or "").strip()
    m = re.search(r"(?:TRIBUTARY TO|DRAINS TO)\s+(.+)", s, re.IGNORECASE)
    return _title(m.group(1)) if m else ""


# source -> (id_field, mapper(props) -> uniform-props dict without 'source')
def _map_port_moody(p: dict) -> dict:
    name = _title(p.get("local_stream_name") or p.get("stream_names"))
    return {"src_id": p.get("OBJECTID"), "name": name,
            "connect_to_hint": _pm_connect(p.get("description")),
            "ftype": (p.get("theme") or "").strip().lower(),
            "fish": _norm_fish(p.get("stream_feature_code")), "species": "",
            "trib_parent": _trib_parent(name)}


def _map_burnaby(p: dict) -> dict:
    name = _title(p.get("WATERWAYNAME"))
    parent = _trib_parent(name)
    return {"src_id": p.get("OBJECTID"), "name": name,
            "connect_to_hint": parent, "ftype": "waterway", "fish": "", "species": "",
            "trib_parent": parent}


def _map_squamish(p: dict) -> dict:
    name = _title(p.get("StreamName") or p.get("StreamDetail"))
    spp = (p.get("Spp_Pres") or "").strip()
    return {"src_id": p.get("OBJECTID"), "name": name,
            "connect_to_hint": _title(p.get("Contributing_to_DS")),
            "ftype": (p.get("Status") or "").strip().lower(),
            "fish": _norm_fish(p.get("Fish_Beari")),
            "species": "" if spp.upper() == "NA" else spp,
            "trib_parent": _trib_parent(_title(p.get("StreamDetail")))}


def _map_abbotsford(p: dict) -> dict:
    raw = (p.get("STREAM_NAME") or "").strip()
    is_ditch = raw.upper().startswith("DITCH-")
    return {"src_id": p.get("OBJECTID"), "name": "" if is_ditch else _title(raw),
            "connect_to_hint": "", "ftype": "ditch" if is_ditch else "stream",
            "fish": _norm_fish(p.get("FISH_CLASS")), "species": "", "trib_parent": ""}


_SOURCES: dict[str, Callable[[dict], dict]] = {
    "port_moody": _map_port_moody, "burnaby": _map_burnaby,
    "squamish": _map_squamish, "abbotsford": _map_abbotsford,
}
# raw file basename per source
_FILES = {"port_moody": "port_moody_esa_streams", "burnaby": "burnaby_waterways",
          "squamish": "squamish_watercourses", "abbotsford": "abbotsford_streams"}

# Municipal features to DROP per source, by exact (case-insensitive) name — bad/duplicated data that
# should never be minted (e.g. a neighbouring municipality's creek that leaked into this layer).
_EXCLUDE_NAMES_BY_SOURCE: dict[str, set] = {
    "port_moody": {"stoney creek"},        # a Burnaby creek that does not belong in the Port Moody layer
}

# Municipal features to DROP per source by exact src_id — for pruning a SINGLE bad piece (a spurious
# connector / a fragment that mis-routes flow) without dropping every other feature that shares its name.
_EXCLUDE_SRC_IDS_BY_SOURCE: dict[str, set] = {
    "burnaby": {"344", "356.1", "279", "305", "274", "268"},  # Ancient Grove Trib.1 stub; Rudolph 356.1 part;
    #                                             Fraser River; 305 Lost<->Holmes connector; 268/274 Thomas Trib.5
    "squamish": {"654", "654.1", "654.2", "608.5", "597.1", "597.2", "664", "658.1"},  # spurious pieces
}


def _linestrings(geom: dict) -> list[list]:
    """Flatten a geometry to a list of >=2-point coordinate rings (LineString parts)."""
    t = geom.get("type")
    if t == "LineString":
        return [geom["coordinates"]] if len(geom["coordinates"]) >= 2 else []
    if t == "MultiLineString":
        return [c for c in geom["coordinates"] if len(c) >= 2]
    return []


def clean_features(features: list[dict], source: str) -> list[dict]:
    """Map raw features of one source to uniform-schema LineString features (MLS flattened)."""
    mapper = _SOURCES[source]
    exclude = _EXCLUDE_NAMES_BY_SOURCE.get(source, set())
    exclude_ids = _EXCLUDE_SRC_IDS_BY_SOURCE.get(source, set())
    out: list[dict] = []
    for ft in features:
        props = mapper(ft.get("properties", {}))
        props["source"] = source
        if (props.get("name") or "").strip().lower() in exclude:
            continue                                      # bad/duplicated data (see _EXCLUDE_NAMES_BY_SOURCE)
        parts = _linestrings(ft.get("geometry") or {})
        for i, coords in enumerate(parts):
            p = dict(props)
            if len(parts) > 1:                            # keep part identity distinct after a flatten
                p = {**p, "src_id": f"{p['src_id']}.{i}"}
            if str(p.get("src_id")) in exclude_ids:       # a single pruned piece (see _EXCLUDE_SRC_IDS_BY_SOURCE)
                continue                                  # checked AFTER the .N suffix so one MLS part can go
            out.append({"type": "Feature", "properties": p,
                        "geometry": {"type": "LineString", "coordinates": coords}})
    return out


def clean_source(source: str, data_dir: Path = _DATA) -> list[dict]:
    raw = json.loads((data_dir / f"{_FILES[source]}.geojson").read_text(encoding="utf-8"))
    return clean_features(raw.get("features", []), source)


def write_cleaned(source: str, data_dir: Path = _DATA) -> Path:
    feats = clean_source(source, data_dir)
    out_dir = data_dir / "cleaned"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{source}.geojson"
    out.write_text(json.dumps({"type": "FeatureCollection", "features": feats}, indent=1),
                   encoding="utf-8")
    return out


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Clean raw municipal stream layers -> uniform schema.")
    ap.add_argument("sources", nargs="*", default=list(_SOURCES), help="which sources (default: all)")
    args = ap.parse_args()
    for src in (args.sources or list(_SOURCES)):
        feats = clean_source(src)
        out = write_cleaned(src)
        named = sum(1 for f in feats if f["properties"]["name"])
        print(f"  {src:12} -> {len(feats):6} features ({named} named)  {out}")


if __name__ == "__main__":
    main()
