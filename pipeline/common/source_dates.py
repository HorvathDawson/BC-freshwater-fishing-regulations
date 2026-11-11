"""THE UPSTREAM DAY OF EACH FETCHED LAYER (T561 item 1): a small sidecar beside a fetched GeoPackage,
`<stem>.source.json` (`data/source/bc_fisheries_data.source.json`, in git), written by `data/fetch_data.py` as it fetches.

    {"_about": "…", "layers": {"streams": {"date": "2024-03-26", "basis": "…", "rows": 4945123}, …}}

`date` is the day the SOURCE's bytes are from — the FWA publication (the object store's `s3b-last-modified`), the
OpenStreetMap base an Overpass answer states (`osm3s.timestamp_osm_base`), the fetch day for a live WFS service — never
the day the GeoPackage layer was written locally (`gpkg_contents.last_change`, which the release used to show as the
FWA's date). `basis` says which of those it is; `rows` (when recorded) is the layer's feature count as fetched, which
`source_pins pin` holds against the previous pin (`row_drops`: a layer that lost more than 10 % is refused without
`--accept-drop`, T561 item 3). The release's `sources` block (`pipeline.deliver.release.manifest`)
reads it; `pipeline.tools.source_pins pin` copies it into the pin.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Dict, Optional

ABOUT = ("T561: the day each fetched layer's SOURCE is from (not the day it was written into the GeoPackage). "
         "Written by data/fetch_data.py per layer as it fetches; read by pipeline.deliver.release.manifest "
         "sources_block; copied into data/source/pins.json by pipeline.tools.source_pins pin.")

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def sidecar(gpkg) -> Path:
    """`bc_fisheries_data.gpkg` -> `bc_fisheries_data.source.json` beside it."""
    g = Path(gpkg)
    return g.with_name(g.stem + ".source.json")


def read(gpkg) -> Optional[Dict[str, dict]]:
    """The recorded layers (`{layer: {date, basis}}`), or None when no sidecar exists. A sidecar that is not the
    documented shape raises naming the file: a malformed record is a bug, never "no date"."""
    p = sidecar(gpkg)
    if not p.is_file():
        return None
    doc = json.loads(p.read_text(encoding="utf-8"))
    layers = doc.get("layers") if isinstance(doc, dict) else None
    if not isinstance(layers, dict):
        raise SystemExit(f"source dates: {p} has no `layers` object — rewrite it with data/fetch_data.py")
    for name, rec in layers.items():
        if not (isinstance(rec, dict) and _DATE.match(str(rec.get("date", ""))) and rec.get("basis")
                and (rec.get("rows") is None or (isinstance(rec["rows"], int) and rec["rows"] >= 0))):
            raise SystemExit(f"source dates: {p} layer {name!r} is not {{date: YYYY-MM-DD, basis, rows?}}: {rec!r}")
    return layers


def day(gpkg, layer: str) -> Optional[str]:
    """The recorded upstream day of `layer`, or None when the sidecar or the layer is not recorded."""
    layers = read(gpkg)
    rec = (layers or {}).get(layer)
    return rec["date"] if rec else None


def record(gpkg, layer: str, date: str, basis: str, rows: Optional[int] = None) -> None:
    """Record (replace) one layer's upstream day (and its row count); the file is rewritten whole, sorted, atomically."""
    if not _DATE.match(date or ""):
        raise ValueError(f"source dates: {layer}: {date!r} is not YYYY-MM-DD")
    if not basis:
        raise ValueError(f"source dates: {layer}: a date needs its basis")
    layers = dict(read(gpkg) or {})
    layers[layer] = {"date": date, "basis": basis, **({} if rows is None else {"rows": int(rows)})}
    p = sidecar(gpkg)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps({"_about": ABOUT, "layers": dict(sorted(layers.items()))}, indent=2) + "\n",
                   encoding="utf-8")
    os.replace(tmp, p)


#: (T561 item 3) a layer may lose at most this share of its rows between two pins without `--accept-drop`
DROP_LIMIT = 0.10


def row_drops(old: Optional[Dict[str, dict]], new: Optional[Dict[str, dict]], limit: float = DROP_LIMIT) -> list:
    """The layers that shrank by more than `limit` from the previous pin's `layer_dates` (`old`) to this fetch's (`new`),
    as readable lines — a layer the previous pin counted that is now gone, or uncounted, is one too. Empty when there
    is no previous count to hold it to."""
    out = []
    for name, rec in sorted((old or {}).items()):
        was = rec.get("rows")
        if not was:
            continue
        now = ((new or {}).get(name) or {}).get("rows")
        if now is None:
            out.append(f"{name}: {was:,} rows at the previous pin, none recorded now")
        elif now < was * (1 - limit):
            out.append(f"{name}: {was:,} -> {now:,} rows ({(was - now) / was:.0%} fewer)")
    return out
