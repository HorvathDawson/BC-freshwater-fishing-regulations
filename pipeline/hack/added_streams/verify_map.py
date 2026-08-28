"""Verification map for the added-streams PACKAGE, with the downstream FWA context.

Renders the FINAL minted added streams together with the FWA they reconcile against, so the whole package
can be eyeballed before it is wired into the pipeline build:

  - added streams      -> novel (green) / extension (blue), coloured lines with the flow arrow. Popup:
                          name, blk, wsc, receiver, and any name variants (aliases) that point at this blk.
  - KEPT FWA           -> thin grey (the FWA that survives — added streams should connect INTO it).
  - EXCLUDED FWA       -> red dashed (the FWA the municipal layer supersedes; anything on a `fwa_exclude`
                          prefix). You should see an added stream drawn where each red line was.

Gap connectors are folded INTO the mainstem geometry (same blk/wsc), so they are drawn as part of the green
added stream, not as separate features.

Self-contained html (Leaflet + OSM from CDN) -> ``output/verify_<source>.html``. Needs the gpkg only for the
FWA geometry; the stream package itself comes from ``resolve_and_mint`` (identical to the build export).
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from shapely.geometry import LineString

from pipeline.hack.added_streams.build_dataset import (_load_fwa, resolve_and_mint, FWA_EXCLUDE_BY_SOURCE,
                                                  approved_lake_polys, RELIABLE_SOURCES)
from pipeline.hack.added_streams.clean import clean_source
from pipeline.hack.added_streams.dem import ElevationSampler
from pipeline.hack.added_streams.mapcheck import _TO_ALBERS, _TO_LONLAT, _feature, _SOURCES
from pipeline.utils.wsc import trim_wsc


def _excluded(wsc: str, prefixes) -> bool:
    tw = trim_wsc(wsc or "")
    return any(tw.startswith(p) for p in prefixes)


def verify_map(source: str, gpkg: str, out_dir: Path, pad: float = 3000.0) -> Path:
    features = [f for f in clean_source(source)
                if not str(f.get("properties", {}).get("ftype", "")).lower().startswith("unconfirmed")]
    pts = [_TO_ALBERS.transform(x, y) for f in features for x, y in f["geometry"]["coordinates"]]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    bbox = (min(xs) - pad, min(ys) - pad, max(xs) + pad, max(ys) + pad)
    chains, lake_index, tidal, _, _ = _load_fwa(gpkg, bbox)
    approved = approved_lake_polys(gpkg, bbox, source)
    exclude = FWA_EXCLUDE_BY_SOURCE.get(source, ())
    streams, _, report = resolve_and_mint(features, chains, lake_index, tidal, exclude_wsc=exclude,
                                          approved_lakes=approved, orient_sampler=ElevationSampler(),
                                          trust_source=source in RELIABLE_SOURCES)
    # NB: lake decluttering now happens IN the resolver (lake_through_spine): a lake's outlet stream runs through
    # the lake as a central spine and its inlets attach to it at distinct measures — so we just draw the streams.

    # name variants keyed by the blk they alias, so a stream / FWA line can show its aliases in the popup
    variants_by_blk: dict = defaultdict(list)
    for v in report["name_variants"]:
        variants_by_blk[str(v["target_blk"])].append(f"{v['name']} ({v['kind']})")

    def _lonlat(coords3005):
        return [[round(x, 6), round(y, 6)] for x, y in (_TO_LONLAT.transform(px, py) for px, py in coords3005)]

    # added streams (one feature per drawn segment; the primary segment carries the arrow)
    added = []
    for s in streams:
        for i, seg in enumerate(s["segments"]):
            c = seg["coords3005"]
            if len(c) < 2:
                continue
            added.append(_feature(_lonlat(c), {
                "name": s["name"] or "(unnamed)", "blk": s["blk"], "wsc": s["wsc"], "klass": s["klass"],
                "recv": f"{s['receiver_kind']}:{s['receiver_blk']}", "primary": i == 0,
                "aliases": variants_by_blk.get(str(s["blk"]), [])}))
    # FWA near the municipal data, split kept vs excluded
    from shapely.ops import unary_union
    muni_union = unary_union([LineString([_TO_ALBERS.transform(x, y) for x, y in f["geometry"]["coordinates"]])
                              for f in features])
    buf = muni_union.buffer(200.0)
    kept_fwa, excl_fwa = [], []
    for c in chains:
        if c.geometry is None or not c.geometry.intersects(buf):
            continue
        coords = [[round(x, 6), round(y, 6)] for x, y in
                  (_TO_LONLAT.transform(px, py) for px, py in c.geometry.coords)]
        if len(coords) < 2:
            continue
        props = {"blk": str(c.blk), "wsc": c.fwa_watershed_code, "name": c.gnis_name or "",
                 "aliases": variants_by_blk.get(str(c.blk), [])}
        (excl_fwa if _excluded(c.fwa_watershed_code, exclude) else kept_fwa).append(_feature(coords, props))

    out = out_dir / f"verify_{source}.html"
    out.write_text(_html(source, added, kept_fwa, excl_fwa, report, list(exclude)),
                   encoding="utf-8")
    return out


_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>verify — %(title)s</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet-polylinedecorator@1.6.0/dist/leaflet.polylineDecorator.js"></script>
<style>html,body,#map{height:100%%;margin:0}.panel{position:absolute;z-index:1000;top:8px;right:8px;
background:#fff;padding:8px 10px;font:12px sans-serif;border-radius:4px;box-shadow:0 1px 4px #0006;max-width:320px}
.sw{display:inline-block;width:11px;height:11px;margin-right:4px;vertical-align:middle;border:1px solid #0003}
code{font-size:11px}</style></head><body><div id="map"></div>
<div class="panel"><b>verify — %(title)s</b><br>
added streams: <b>%(nadded)d</b> · excluded FWA: <b>%(nexcl)d</b> · name variants: <b>%(nvar)d</b><br>
<div style="margin:6px 0">
<span class=sw style="background:#2ca02c"></span>novel &nbsp;
<span class=sw style="background:#1f77b4"></span>extension<br>
<span class=sw style="background:#7a7a7a"></span>kept FWA &nbsp;
<span class=sw style="background:#d62728"></span>excluded FWA</div>
<b>fwa_exclude</b>: <code>%(excl)s</code></div>
<script>
const ADDED=%(added)s, KEPT=%(kept)s, EXCL=%(excl_fwa)s;
const KL={novel:'#2ca02c',extension:'#1f77b4',duplicate:'#9e9e9e'};
const map=L.map('map');
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);
function pop(p){let s='<b>'+(p.name||'')+'</b><br>blk '+p.blk+'<br>'+(p.wsc||'—');
  if(p.recv)s+='<br>→ '+p.recv; if(p.aliases&&p.aliases.length)s+='<br><i>aliases: '+p.aliases.join(', ')+'</i>';
  return s;}
const keptL=L.geoJSON(KEPT,{style:{color:'#7a7a7a',weight:1.5,opacity:.6},
  onEachFeature:(f,l)=>l.bindPopup('FWA (kept)<br>'+pop(f.properties))}).addTo(map);
const exclL=L.geoJSON(EXCL,{style:{color:'#d62728',weight:3,opacity:.9,dashArray:'6,4'},
  onEachFeature:(f,l)=>l.bindPopup('<b>FWA EXCLUDED</b> (municipal supersedes)<br>'+pop(f.properties))}).addTo(map);
const arrows=[];
const addL=L.geoJSON(ADDED,{style:f=>({color:KL[f.properties.klass]||'#000',weight:3,opacity:.95}),
  onEachFeature:(f,l)=>{l.bindPopup(pop(f.properties));
    if(f.properties.primary){let ll=l.getLatLngs().slice().reverse();   // coords[0]=mouth -> arrow downstream
      const d=L.polylineDecorator(ll,{patterns:[{offset:14,repeat:70,
        symbol:L.Symbol.arrowHead({pixelSize:8,pathOptions:{color:KL[f.properties.klass]||'#000',fillOpacity:.95,weight:0}})}]});
      d.addTo(map);arrows.push(d);}}}).addTo(map);
L.control.layers(null,{'added streams':addL,'FWA kept (grey)':keptL,
  'FWA excluded (red)':exclL}).addTo(map);
const grp=L.featureGroup([addL,keptL,exclL]);
try{map.fitBounds(grp.getBounds(),{padding:[20,20]});}catch(e){map.setView([49.25,-122.95],12);}
</script></body></html>"""


def _html(title, added, kept, excl_fwa, report, exclude) -> str:
    return _HTML % {"title": title, "added": json.dumps(added),
                    "kept": json.dumps(kept), "excl_fwa": json.dumps(excl_fwa),
                    "nadded": report["minted"], "nexcl": len(excl_fwa),
                    "nvar": len(report["name_variants"]), "excl": ", ".join(exclude) or "—"}


def main() -> None:
    import argparse
    from project_config import get_config
    ap = argparse.ArgumentParser(description="Verification map: added streams + FWA (kept/excluded) context.")
    ap.add_argument("sources", nargs="*", default=_SOURCES, help="sources (default: all)")
    ap.add_argument("--gpkg", default=None)
    ap.add_argument("--out", default="output")
    args = ap.parse_args()
    gpkg = args.gpkg or get_config().fwa_data_gpkg
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    for src in (args.sources or _SOURCES):
        out = verify_map(src, gpkg, out_dir)
        print(f"  {src:12} -> {out}")


if __name__ == "__main__":
    main()
