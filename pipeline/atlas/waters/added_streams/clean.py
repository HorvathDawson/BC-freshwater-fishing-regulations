"""Normalize the raw municipal GeoJSON layers into one uniform, cruft-free schema.

Each source (Port Moody / Burnaby / Squamish / Abbotsford — see SOURCES.md) has its own ArcGIS
property soup. `clean_source` flattens MultiLineStrings, drops the cruft (we only carry mapped
fields), and maps every feature to:

    {source, src_id, name, connect_to_hint, ftype, fish, species, trib_parent, drainage}

Nothing is filtered by type — `ftype`/`fish` are carried as tags so a later batch can filter. Output
is a lon/lat FeatureCollection written to `data/generated/municipal/cleaned/<source>.geojson`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional
from pipeline.common.curated import SOURCE

#: The raw municipal exports — FETCHED from four city open-data portals, so they live with
#: every other fetched thing. They are TRACKED in git despite being source: they are small
#: (20,064 features across four files), the portals rewrite their URLs, and the minted ids
#: bound to them are an ABI — a re-download that returned something slightly different
#: would remint 361 streams and strand every split pointing at the old ones.
_DATA = SOURCE / "municipal"
#: `clean.py`'s output is a pure function of the raw, so it is generated, not curated. (T777) Nothing here names its
#: folder any more: an unused `_CLEANED = generated(...)` made `data/generated/municipal` on every import.

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
#: (owner 2026-10-29) Port Moody's own type fields that mark a piece as DRAINAGE, not a stream: a culvert or ditch
#: (`theme`), hidden (piped) drainage, a ditch or an unverified connection line (`stream_type`). Such a piece is never
#: added. NOT the `description` "CONNECTED TO PORT MOODY DRAINAGE SYSTEM": it sits on open STREAM/CREEK pieces of real
#: salmon creeks (Goulet, Slaughterhouse, Dallas), so it says where the creek ends up, not what the piece is.
_PM_DRAINAGE_THEME = {"culvert", "ditch"}
_PM_DRAINAGE_TYPE = {"hidden drainage", "ditch", "un-verified connection line"}
_PM_DRAINAGE_DESC = ("culverted", "ditch")


def _pm_drainage(p: dict) -> str:
    theme = (p.get("theme") or "").strip().lower()
    stype = (p.get("stream_type") or "").strip().lower()
    desc = (p.get("description") or "").strip().lower()
    if theme in _PM_DRAINAGE_THEME:
        return f"theme {theme}"
    if stype in _PM_DRAINAGE_TYPE:
        return f"stream_type {stype}"
    if any(desc.startswith(d) for d in _PM_DRAINAGE_DESC):
        return f"description {desc}"
    return ""


def _map_port_moody(p: dict) -> dict:
    name = _title(p.get("local_stream_name") or p.get("stream_names"))
    return {"src_id": p.get("OBJECTID"), "name": name,
            "connect_to_hint": _pm_connect(p.get("description")),
            "ftype": (p.get("theme") or "").strip().lower(),
            "fish": _norm_fish(p.get("stream_feature_code")), "species": "",
            "trib_parent": _trib_parent(name), "drainage": _pm_drainage(p)}


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
            "fish": _norm_fish(p.get("FISH_CLASS")), "species": "", "trib_parent": "",
            "drainage": "STREAM_NAME ditch" if is_ditch else ""}


def _map_osm(p: dict) -> dict:
    """T222: hand-picked OpenStreetMap waterways (`osm_streams.geojson`), each feature already one named line."""
    name = _title(p.get("name"))
    return {"src_id": p.get("id"), "name": name, "connect_to_hint": "", "ftype": "stream",
            "fish": "", "species": "", "trib_parent": _trib_parent(name)}


def _map_manual(p: dict) -> dict:
    """T806 (owner 2026-11-16): a channel drawn BY HAND (`manual_streams.geojson`) for a recorded owner decision, each
    feature one named line, its vertex order the flow (head first)."""
    name = _title(p.get("name"))
    return {"src_id": p.get("id"), "name": name, "connect_to_hint": "", "ftype": "stream",
            "fish": "", "species": "", "trib_parent": _trib_parent(name)}


_SOURCES: dict[str, Callable[[dict], dict]] = {
    "port_moody": _map_port_moody, "burnaby": _map_burnaby,
    "squamish": _map_squamish, "abbotsford": _map_abbotsford, "osm": _map_osm, "manual": _map_manual,
}
# raw file basename per source
_FILES = {"port_moody": "port_moody_esa_streams", "burnaby": "burnaby_waterways",
          "squamish": "squamish_watercourses", "abbotsford": "abbotsford_streams",
          "osm": "osm_streams", "manual": "manual_streams"}

# (T679 #13) THE HAND FIXES are curated data, not code: `waters/added_stream_calls.json` (`calls()`) — a feature dropped
# by name, a misspelt name mapped onto its creek, a single bad piece dropped by src_id, a piece forced to drain into a
# named creek (`build_dataset._apply_receiver_overrides`). Read by this generator only; never by the atlas build.
CALLS_FORMAT = "added_stream_calls/1"


@dataclass(frozen=True)
class Calls:
    """The added-streams hand fixes, per source (`calls()`)."""
    #: features dropped by exact name, lower case (bad/duplicated data that should never be minted)
    exclude_names: dict
    #: a misspelling mapped onto the canonical name, so the creek's pieces merge as one channel
    name_aliases: dict
    #: single pieces dropped by src_id (checked AFTER the `.N` multi-part suffix, so one part can go)
    exclude_src_ids: dict
    #: {src_id: receiver creek name}: the piece drains into the NEAREST channel of that name
    receiver_override: dict


def calls(path: Optional[Path] = None) -> Calls:
    """The curated hand fixes (`CURATED.waters.added_stream_calls`). A missing or malformed file RAISES (AGENTS 37),
    as does a source this module does not read, a src_id listed twice for one source, or a row without its `why`."""
    from pipeline.common.curated import CURATED
    p = Path(path) if path is not None else Path(CURATED.waters.added_stream_calls)
    doc = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or doc.get("format") != CALLS_FORMAT:
        raise ValueError(f"{p}: not a {CALLS_FORMAT} file")
    names: dict[str, set] = {}
    aliases: dict[str, dict] = {}
    ids: dict[str, set] = {}
    recv: dict[str, dict] = {}

    def src(kind: str, i: int, r: dict) -> str:
        s = (r or {}).get("source")
        if s not in _SOURCES:
            raise ValueError(f"{p}: {kind}[{i}] names source {s!r}, not one of {sorted(_SOURCES)}")
        if not str((r or {}).get("why") or "").strip():
            raise ValueError(f"{p}: {kind}[{i}] has no `why`")
        return s
    for i, r in enumerate(doc.get("exclude_names") or ()):
        names.setdefault(src("exclude_names", i, r), set()).update(str(n).strip().lower() for n in r["names"])
    for i, r in enumerate(doc.get("name_aliases") or ()):
        aliases.setdefault(src("name_aliases", i, r), {})[str(r["from"])] = str(r["to"])
    for i, r in enumerate(doc.get("exclude_src_ids") or ()):
        have = ids.setdefault(src("exclude_src_ids", i, r), set())
        twice = sorted(have & {str(x) for x in r["src_ids"]})
        if twice or len(set(map(str, r["src_ids"]))) != len(r["src_ids"]):
            raise ValueError(f"{p}: exclude_src_ids[{i}] lists src_id(s) twice for {r['source']}: {twice}")
        have.update(str(x) for x in r["src_ids"])
    for i, r in enumerate(doc.get("receiver_override") or ()):
        recv.setdefault(src("receiver_override", i, r), {})[str(r["src_id"])] = str(r["receiver"])
    return Calls(names, aliases, ids, recv)


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
    hand = calls()                                        # (T679 #13) the curated hand fixes
    exclude = hand.exclude_names.get(source, set())
    exclude_ids = hand.exclude_src_ids.get(source, set())
    aliases = hand.name_aliases.get(source, {})
    out: list[dict] = []
    for ft in features:
        props = mapper(ft.get("properties", {}))
        props.setdefault("drainage", "")                  # the layer's own drainage marking (owner 2026-10-29)
        props["source"] = source
        if props.get("name") in aliases:                  # fix a misspelling so the creek's pieces merge as one
            props["name"] = aliases[props["name"]]
        if (props.get("name") or "").strip().lower() in exclude:
            continue                                      # bad/duplicated data (`calls().exclude_names`)
        parts = _linestrings(ft.get("geometry") or {})
        for i, coords in enumerate(parts):
            p = dict(props)
            if len(parts) > 1:                            # keep part identity distinct after a flatten
                p = {**p, "src_id": f"{p['src_id']}.{i}"}
            if str(p.get("src_id")) in exclude_ids:       # a single pruned piece (`calls().exclude_src_ids`)
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
