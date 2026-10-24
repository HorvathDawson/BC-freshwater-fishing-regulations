"""THE ONE MAP CHECKER for the added streams (plan 2.21: it and `verify_map` were two checkers for one job).

    python -m pipeline.atlas.waters.added_streams.mapcheck [source ...] [--only batch|package]

For each municipal source it resolves the batch ONCE (`resolve_and_mint`, identical to the build export) and writes
two SELF-CONTAINED html maps (Leaflet + OSM tiles from CDN, no build deps) into `GENERATED.added_streams`:

BATCH — `added_map_<source>.html`, every municipal feature by class:

  - FWA streams near the municipal data  -> thin grey background (what was ORIGINAL FWA)
  - municipal features coloured by class  -> duplicate (grey, dropped/kept-as-FWA), extension (blue),
    novel (green), unresolved (red).  Toggle colour-by klass / blk / ftype in the page.
  - hover a line -> name • class • blk • wsc • type

PACKAGE — `verify_<source>.html`, the FINAL minted streams with the downstream FWA context:
  - added streams -> novel (green) / extension (blue), with the flow arrow; popup: name, blk, wsc, receiver, and
                     any name variants (aliases) that point at this blk
  - KEPT FWA      -> thin grey (the FWA that survives — added streams should connect INTO it)
  - EXCLUDED FWA  -> red dashed (the FWA the municipal layer supersedes; anything on a `fwa_exclude` prefix)
  Gap connectors are folded INTO the mainstem geometry (same blk/wsc), so they are drawn as part of the stream.

Disposable — no polish. Just open the html to eyeball each region.
"""

from __future__ import annotations

import json
from pathlib import Path
from collections import defaultdict
from typing import Optional

from pyproj import Transformer
from shapely.geometry import LineString, Point
from shapely.ops import unary_union

from pipeline.atlas.waters.added_streams.build_dataset import (_albers, _load_fwa, resolve_and_mint,
                                                  FWA_EXCLUDE_BY_SOURCE, approved_lake_polys,
                                                  RELIABLE_SOURCES)
from pipeline.atlas.waters.added_streams.dem import ElevationSampler, dem_flow
from pipeline.atlas.waters.added_streams.clean import clean_source
from pipeline.common.curated import GENERATED
from pipeline.common.utils.wsc import trim_wsc

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
_TO_LONLAT = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)
_SOURCES = ["port_moody", "burnaby", "squamish"]     # abbotsford is huge/slow — run it explicitly


def _feature(coords, props):
    return {"type": "Feature", "properties": props,
            "geometry": {"type": "LineString", "coordinates": coords}}


class Resolved:
    """One source resolved once, for both maps: its cleaned features, the FWA in its bbox, the result."""

    def __init__(self, source: str, gpkg: str, pad: float = 3000.0):
        features = clean_source(source)
        # drop 'Unconfirmed' watercourses (Squamish) from ALL views, matching resolve_and_mint's default
        features = [f for f in features
                    if not str(f.get("properties", {}).get("ftype", "")).lower().startswith("unconfirmed")]
        pts = [_TO_ALBERS.transform(x, y) for f in features for x, y in f["geometry"]["coordinates"]]
        xs, ys = [p[0] for p in pts], [p[1] for p in pts]
        bbox = (min(xs) - pad, min(ys) - pad, max(xs) + pad, max(ys) + pad)
        self.source, self.features = source, features
        self.chains, _lake_index, self.tidal, _, _ = _load_fwa(gpkg, bbox)
        self.approved = approved_lake_polys(gpkg, bbox, source)   # only approved lakes form drainage nodes
        self.exclude = FWA_EXCLUDE_BY_SOURCE.get(source, ())
        self.streams, _, self.report = resolve_and_mint(features, self.chains, _lake_index, self.tidal,
                                                        exclude_wsc=self.exclude,
                                                        approved_lakes=self.approved,
                                                        orient_sampler=ElevationSampler(),
                                                        trust_source=source in RELIABLE_SOURCES)


def batch_map(r: Resolved, out_dir: Path) -> Path:
    """The batch map (`added_map_<source>.html`): every municipal feature by class, raw and DEM views."""
    source, features, chains, tidal, approved, report = (r.source, r.features, r.chains, r.tidal, r.approved,
                                                          r.report)

    diags = report["diagnostics"]
    # Lake decluttering now happens IN the resolver (lake_through_spine): a lake's outlet stream runs through the
    # lake as a central spine and its inlet connectors land on it at distinct points — so the diagnostics already
    # show the tidy lake node; nothing to reroute here.
    muni = [_feature(d["coords"], {"klass": d["klass"], "blk": d["blk"], "wsc": d["wsc"],
                                   "name": d["name"], "ftype": d["ftype"], "fish": d["fish"]})
            for d in diags if len(d["coords"]) >= 2]
    matched = {d["blk"] for d in diags if d["klass"] in ("duplicate", "extension")}  # FWA parents

    # FWA background near the municipal data; the matched parents (extended/duplicated) are highlighted
    muni_union = unary_union([_albers(LineString(f["geometry"]["coordinates"])) for f in features])
    buf = muni_union.buffer(150.0)
    fwa = []
    for c in chains:
        if c.geometry is None or not c.geometry.intersects(buf):
            continue
        coords = [[round(x, 6), round(y, 6)] for x, y in
                  (_TO_LONLAT.transform(px, py) for px, py in c.geometry.coords)]
        if len(coords) >= 2:
            fwa.append(_feature(coords, {"blk": str(c.blk), "wsc": c.fwa_watershed_code,
                                         "name": c.gnis_name or "", "matched": str(c.blk) in matched}))

    # RAW cleaned municipal geojson, exactly as `clean_source` emits it (no FWA, no resolution,
    # no re-orientation) — a toggle to tell apart source artifacts from resolver artifacts.
    def _props(f):
        return {"name": f["properties"].get("name") or "", "src_id": f["properties"].get("src_id"),
                "ftype": f["properties"].get("ftype") or "",
                "hint": f["properties"].get("connect_to_hint") or ""}
    raw = [_feature(f["geometry"]["coordinates"], _props(f))
           for f in features if len(f["geometry"]["coordinates"]) >= 2]

    # DEM-CORRECTED raw: same geometry, but each piece re-oriented by ground elevation (connect touching
    # pieces, flow high -> the lowest node/outlet). Shows the flow direction the terrain implies, ignoring
    # the source's unreliable line direction. Best-effort: if tiles can't be fetched, the layer is empty.
    demraw, dembridges, demmarkers = [], [], []
    from pipeline.common.utils.wsc import trim_wsc
    from pipeline.common.utils import wsc as _wsc
    _ex = FWA_EXCLUDE_BY_SOURCE.get(source, ())
    fwa_out = [c.geometry for c in chains if c.geometry is not None       # FWA outlets a stranded sink can
               and not _wsc.has_prefix(trim_wsc(c.fwa_watershed_code), _ex)]  # reach (minus excluded)
    tidal_out = unary_union([t for t in tidal]) if tidal else None
    try:
        flow, bridges, markers = dem_flow(features, ElevationSampler(),
                                          trust_source=source in RELIABLE_SOURCES,
                                          lakes=approved, fwa=fwa_out, tidal=tidal_out)
        demraw = []                                                # a T-noded feature yields >1 reach (an
        for i, f in enumerate(features):                           # apex-draining loop's two arms) — draw each
            if i not in flow:
                continue
            v = flow[i]
            for coords in ([r["coords"] for r in v["reaches"]] if v.get("reaches") else [v["coords"]]):
                demraw.append(_feature(coords, {**_props(f), "comp": v["comp"]}))
        dembridges = [_feature([b["a"], b["b"]], {"comp": b["comp"]}) for b in bridges]
        demmarkers = markers                                        # {kind, lonlat, comp, elev}
    except Exception as e:  # network/tile failure — leave the DEM layer empty rather than break the map
        print(f"    (dem raw skipped for {source}: {e})")

    out = out_dir / f"added_map_{source}.html"
    out.write_text(_html(source, muni, fwa, raw, demraw, dembridges, demmarkers,
                         report["counts"], len(report["unresolved"])), encoding="utf-8")
    return out


_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>added streams — %(title)s</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet-polylinedecorator@1.6.0/dist/leaflet.polylineDecorator.js"></script>
<style>html,body,#map{height:100%%;margin:0} .panel{position:absolute;z-index:1000;top:8px;right:8px;
background:#fff;padding:8px 10px;font:12px sans-serif;border-radius:4px;box-shadow:0 1px 4px #0006}
.panel button{font:12px sans-serif;margin:2px} .sw{display:inline-block;width:11px;height:11px;
margin-right:4px;vertical-align:middle;border:1px solid #0003}</style></head>
<body><div id="map"></div>
<div class="panel">
<b>%(title)s</b> — dup %(dup)d · ext %(ext)d · novel %(novel)d · unresolved %(unres)d<br>
colour by: <button onclick="setColor('klass')">class</button>
<button onclick="setColor('blk')">blk</button>
<button onclick="setColor('ftype')">type</button><br>
view: <button onclick="showView('resolved')">resolved</button>
<button onclick="showView('raw')">raw geojson</button>
<button onclick="showView('demraw')">dem raw</button><br>
<label style="font:12px sans-serif"><input type="checkbox" id="compchk" onchange="showView('demraw')">
 colour dem by connected component</label>
<div id="legend" style="margin-top:6px"></div>
</div>
<script>
const MUNI=%(muni)s, FWA=%(fwa)s, RAW=%(raw)s, DEMRAW=%(demraw)s, DEMBRIDGES=%(dembridges)s;
const DEMMARKERS=%(demmarkers)s;
const KLASS={duplicate:'#9e9e9e',extension:'#1f77b4',novel:'#2ca02c',unresolved:'#d62728',
  connector:'#ff7f0e'};
let colorBy='klass';
function hashColor(s){let h=0;s=String(s||'');for(let i=0;i<s.length;i++)h=(h*31+s.charCodeAt(i))|0;
  return 'hsl('+((h%%360)+360)%%360+',70%%,45%%)';}
function colorOf(p){return colorBy==='klass'?(KLASS[p.klass]||'#000'):hashColor(p[colorBy]);}
const map=L.map('map');
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,
  attribution:'© OpenStreetMap'}).addTo(map);
// FWA background; the matched parents (a municipal stream extends/duplicates them) are highlighted
const fwaLayer=L.geoJSON(FWA,{style:f=>f.properties.matched
    ?{color:'#e6550d',weight:4,opacity:.85}:{color:'#7a7a7a',weight:1,opacity:.5},
  onEachFeature:(f,l)=>l.bindTooltip((f.properties.matched?'★ ':'')+'FWA blk '+f.properties.blk+
   ' · '+(f.properties.name||'')+'<br>'+f.properties.wsc)}).addTo(map);
function styleMuni(f){const p=f.properties;
  return {color:colorOf(p),weight:p.klass==='connector'?2:3,opacity:.9,
    dashArray:p.klass==='connector'?'4,4':null};}
const arrows=[];   // flow-direction arrowheads; novels/extensions store MOUTH first (coords[0]=mouth),
// connectors store mouth->confluence, so a novel points toward coords[0] and a connector toward the end
function addArrow(f,l){const p=f.properties;if(p.klass==='duplicate'||p.klass==='extension')return;
  let ll=l.getLatLngs();if(p.klass!=='connector')ll=ll.slice().reverse();   // point DOWNSTREAM
  const d=L.polylineDecorator(ll,{patterns:[{offset:14,repeat:70,
    symbol:L.Symbol.arrowHead({pixelSize:8,pathOptions:{color:colorOf(p),fillOpacity:.95,weight:0}})}]});
  d.addTo(map);arrows.push(d);}
const muniLayer=L.geoJSON(MUNI,{style:styleMuni,onEachFeature:(f,l)=>{const p=f.properties;
  l.bindTooltip((p.name||'(unnamed)')+' · '+p.klass+' · blk '+p.blk+'<br>'+(p.wsc||'—')+
   ' · '+(p.ftype||'')+(p.fish?(' · '+p.fish):''));addArrow(f,l);}}).addTo(map);
// RAW geojson (magenta): arrows follow the source's OWN vertex order (not reoriented) — a source-baked
// zigzag shows here. DEM RAW (teal): same geometry re-oriented by ground elevation (flow high->low toward
// each component's lowest node), so arrows show the terrain's implied flow, ignoring the source direction.
const extraArrows=[];
function drawArrows(layer,color,reverse){
  layer.eachLayer(l=>{let ll=l.getLatLngs();if(reverse)ll=ll.slice().reverse();
    const d=L.polylineDecorator(ll,{patterns:[{offset:14,repeat:70,
      symbol:L.Symbol.arrowHead({pixelSize:7,pathOptions:{color:color,fillOpacity:.95,weight:0}})}]});
    d.addTo(map);extraArrows.push(d);});}
function clearExtra(){extraArrows.forEach(a=>map.removeLayer(a));extraArrows.length=0;}
const rawLayer=L.geoJSON(RAW,{style:{color:'#c026d3',weight:2,opacity:.85},
  onEachFeature:(f,l)=>{const p=f.properties;l.bindTooltip('RAW: '+(p.name||'(unnamed)')+' · src_id '+
    p.src_id+'<br>'+(p.ftype||'')+(p.hint?(' → '+p.hint):''));}});
function demStyle(f){const byComp=document.getElementById('compchk').checked;
  return {color:byComp?hashColor('c'+f.properties.comp):'#0d9488',weight:2,opacity:.85};}
const demLayer=L.geoJSON(DEMRAW,{style:demStyle,
  onEachFeature:(f,l)=>{const p=f.properties;l.bindTooltip('DEM: '+(p.name||'(unnamed)')+' · src_id '+
    p.src_id+' · comp '+p.comp+'<br>flow = downhill (arrow points to the lower end)');}});
// gap-fill connectors added to join fragmented pieces of one drainage (bold dashed red, arrow=downhill)
const demBridgeLayer=L.geoJSON(DEMBRIDGES,{style:{color:'#e11d48',weight:4,opacity:1,dashArray:'6,4'},
  onEachFeature:(f,l)=>l.bindTooltip('gap connector · comp '+f.properties.comp)});
// per-component nodes: SINK = square (the single outlet), SOURCE = circle (a headwater leaf)
let demMarkerObjs=[];
function clearDemMarkers(){demMarkerObjs.forEach(m=>map.removeLayer(m));demMarkerObjs=[];}
function showDemMarkers(){clearDemMarkers();
  const byComp=document.getElementById('compchk').checked;
  DEMMARKERS.forEach(m=>{const ll=[m.lonlat[1],m.lonlat[0]];
    const col=byComp?hashColor('c'+m.comp):(m.kind==='sink'?'#111':'#2563eb');let mk;
    if(m.kind==='sink')mk=L.marker(ll,{icon:L.divIcon({className:'',iconSize:[12,12],
      html:'<div style="width:10px;height:10px;background:'+col+';border:2px solid #000"></div>'})});
    else mk=L.circleMarker(ll,{radius:4,color:'#000',weight:1,fillColor:col,fillOpacity:.95});
    mk.bindTooltip((m.kind==='sink'?'■ SINK':'● source')+' · comp '+m.comp+' · '+m.elev+'m');
    mk.addTo(map);demMarkerObjs.push(mk);});}
L.control.layers(null,{'FWA (grey, ★=matched)':fwaLayer,'municipal (resolved)':muniLayer,
  'raw geojson (magenta)':rawLayer,'dem raw (teal)':demLayer,
  'dem gap bridges (red)':demBridgeLayer}).addTo(map);
function showView(mode){
  clearExtra();arrows.forEach(a=>map.removeLayer(a));arrows.length=0;clearDemMarkers();
  [muniLayer,rawLayer,demLayer,demBridgeLayer].forEach(l=>map.removeLayer(l));
  if(mode==='resolved'){muniLayer.addTo(map);muniLayer.eachLayer(l=>addArrow(l.feature,l));}
  else if(mode==='raw'){rawLayer.addTo(map);drawArrows(rawLayer,'#c026d3',false);}   // source order
  else{demLayer.setStyle(demStyle).addTo(map);drawArrows(demLayer,'#0d9488',true);   // low end first
    demBridgeLayer.addTo(map);demBridgeLayer.bringToFront();drawArrows(demBridgeLayer,'#e11d48',false);
    showDemMarkers();
    const n=new Set(DEMRAW.map(f=>f.properties.comp)).size;
    document.getElementById('legend').innerHTML='<i>dem raw — '+n+' components · '+
      DEMBRIDGES.length+' gap bridges · ■ sink / ● source per component</i>';}
}
const grp=L.featureGroup([fwaLayer,muniLayer]);
try{map.fitBounds(grp.getBounds(),{padding:[20,20]});}catch(e){map.setView([49.25,-122.95],12);}
function legend(){const d=document.getElementById('legend');
  if(colorBy==='klass'){d.innerHTML=Object.entries(KLASS).map(([k,c])=>
    '<span class=sw style="background:'+c+'"></span>'+k).join('<br>');}
  else{d.innerHTML='<i>colour = hash of '+colorBy+'</i>';}}
function setColor(c){colorBy=c;muniLayer.setStyle(styleMuni);
  arrows.forEach(a=>map.removeLayer(a));arrows.length=0;
  muniLayer.eachLayer(l=>addArrow(l.feature,l));legend();}
legend();
</script></body></html>"""


def _html(title, muni, fwa, raw, demraw, dembridges, demmarkers, counts, unresolved) -> str:
    return _HTML % {"title": title, "muni": json.dumps(muni), "fwa": json.dumps(fwa),
                    "raw": json.dumps(raw), "demraw": json.dumps(demraw),
                    "dembridges": json.dumps(dembridges), "demmarkers": json.dumps(demmarkers),
                    "dup": counts["duplicate"], "ext": counts["extension"],
                    "novel": counts["novel"], "unres": unresolved}


def _excluded(wsc: str, prefixes) -> bool:
    tw = trim_wsc(wsc or "")
    return any(tw.startswith(p) for p in prefixes)


def package_map(r: Resolved, out_dir: Path) -> Path:
    """The package map (`verify_<source>.html`): the final minted streams with the kept / excluded FWA."""
    source, features, chains, exclude, streams, report = (r.source, r.features, r.chains, r.exclude, r.streams,
                                                          r.report)
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
    out.write_text(_package_html(source, added, kept_fwa, excl_fwa, report, list(exclude)),
                   encoding="utf-8")
    return out


_PACKAGE_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>verify — %(title)s</title>
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


def _package_html(title, added, kept, excl_fwa, report, exclude) -> str:
    return _PACKAGE_HTML % {"title": title, "added": json.dumps(added),
                    "kept": json.dumps(kept), "excl_fwa": json.dumps(excl_fwa),
                    "nadded": report["minted"], "nexcl": len(excl_fwa),
                    "nvar": len(report["name_variants"]), "excl": ", ".join(exclude) or "—"}


MAPS = {"batch": batch_map, "package": package_map}


def main() -> None:
    import argparse
    from project_config import get_config
    ap = argparse.ArgumentParser(description="The added-streams check maps: batch (by class) and package (with FWA).")
    ap.add_argument("sources", nargs="*", default=_SOURCES, help="sources (default: all)")
    ap.add_argument("--only", choices=sorted(MAPS), default=None, help="write one of the two maps")
    ap.add_argument("--gpkg", default=None)
    ap.add_argument("--out", default=None, help="output dir (default: GENERATED.added_streams)")
    args = ap.parse_args()
    gpkg = args.gpkg or get_config().fwa_data_gpkg
    out_dir = Path(args.out) if args.out else GENERATED.added_streams
    out_dir.mkdir(parents=True, exist_ok=True)
    for src in (args.sources or _SOURCES):
        r = Resolved(src, gpkg)
        for name, fn in MAPS.items():
            if args.only in (None, name):
                print(f"  {src:12} {name:8} -> {fn(r, out_dir)}")


if __name__ == "__main__":
    main()
