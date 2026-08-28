"""Throwaway visual check: one Leaflet map per region showing the added-streams batch result.

For each municipal source it runs the batch, then writes a SELF-CONTAINED html (Leaflet + OSM tiles
from CDN, no build deps) to `<output.added_streams>/added_map_<source>.html` (config-driven):

  - FWA streams near the municipal data  -> thin grey background (what was ORIGINAL FWA)
  - municipal features coloured by class  -> duplicate (grey, dropped/kept-as-FWA), extension (blue),
    novel (green), unresolved (red).  Toggle colour-by klass / blk / ftype in the page.
  - hover a line -> name • class • blk • wsc • type

Disposable — no polish. Just open the html to eyeball each region.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from pyproj import Transformer
from shapely.geometry import LineString, Point
from shapely.ops import unary_union

from pipeline.hack.added_streams.build_dataset import (_albers, _load_fwa, resolve_and_mint,
                                                  FWA_EXCLUDE_BY_SOURCE, approved_lake_polys,
                                                  RELIABLE_SOURCES)
from pipeline.hack.added_streams.dem import ElevationSampler, dem_flow
from pipeline.hack.added_streams.clean import clean_source

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
_TO_LONLAT = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)
_SOURCES = ["port_moody", "burnaby", "squamish"]     # abbotsford is huge/slow — run it explicitly


def _feature(coords, props):
    return {"type": "Feature", "properties": props,
            "geometry": {"type": "LineString", "coordinates": coords}}


def region_map(source: str, gpkg: str, out_dir: Path, pad: float = 3000.0) -> Path:
    features = clean_source(source)
    # drop 'Unconfirmed' watercourses (Squamish) from ALL views, matching resolve_and_mint's default
    features = [f for f in features
               if not str(f.get("properties", {}).get("ftype", "")).lower().startswith("unconfirmed")]
    pts = [_TO_ALBERS.transform(x, y) for f in features for x, y in f["geometry"]["coordinates"]]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    bbox = (min(xs) - pad, min(ys) - pad, max(xs) + pad, max(ys) + pad)
    chains, lake_index, tidal, _, _ = _load_fwa(gpkg, bbox)
    approved = approved_lake_polys(gpkg, bbox, source)   # only approved lakes form drainage nodes
    _, _, report = resolve_and_mint(features, chains, lake_index, tidal,
                                    exclude_wsc=FWA_EXCLUDE_BY_SOURCE.get(source, ()),
                                    approved_lakes=approved,
                                    orient_sampler=ElevationSampler(),
                                    trust_source=source in RELIABLE_SOURCES)

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
    from pipeline.utils.wsc import trim_wsc
    _ex = FWA_EXCLUDE_BY_SOURCE.get(source, ())
    fwa_out = [c.geometry for c in chains if c.geometry is not None       # FWA outlets a stranded sink can
               and not any(trim_wsc(c.fwa_watershed_code).startswith(p) for p in _ex)]  # reach (minus excluded)
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


def main() -> None:
    import argparse
    from project_config import get_config
    ap = argparse.ArgumentParser(description="Write a Leaflet check map per municipal source.")
    ap.add_argument("sources", nargs="*", default=_SOURCES, help="sources (default: all)")
    ap.add_argument("--gpkg", default=None)
    ap.add_argument("--out", default=None, help="output dir (default: config output.added_streams)")
    args = ap.parse_args()
    gpkg = args.gpkg or get_config().fwa_data_gpkg
    out_dir = Path(args.out) if args.out else get_config().added_streams_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    for src in (args.sources or _SOURCES):
        out = region_map(src, gpkg, out_dir)
        print(f"  {src:12} -> {out}")


if __name__ == "__main__":
    main()
