"""Plot + map outputs from profile.csv (run after profile.py, from the repo root or this folder)."""
import json
from pathlib import Path
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

OUT = Path(__file__).parent
EST_D, LO, HI = 475.0, 400.0, 750.0
df = pd.read_csv(OUT / "profile.csv")
est = df.loc[(df.d_below_falls - EST_D).abs().idxmin()]

fig, (a, b) = plt.subplots(2, 1, figsize=(10, 6.5), sharex=True, gridspec_kw=dict(height_ratios=[1, 1]))
x = df.d_below_falls / 1000
a.plot(x, df.ch_dem, color="#2b2b2b", lw=1.5, label="channel (DEM min within 30 m)")
a.plot(x, df.fwa_z, color="#3a78c2", lw=1, alpha=.8, label="FWA line z")
a.set_ylabel("elevation (m)")
a.legend(frameon=False, fontsize=8)
b.plot(x, df.rel100.rolling(5, center=True).median(), color="#3a78c2", lw=1.5, label="relief within 100 m (lower side)")
b.plot(x, df.rel200.rolling(5, center=True).median(), color="#9a9a9a", lw=1.2, label="relief within 200 m (lower side)")
b.set_ylabel("side relief above channel (m)")
b.set_xlabel("distance below DFO falls point (km; negative = upstream)")
b.legend(frameon=False, fontsize=8)
for ax in (a, b):
    ax.axvspan(LO / 1000, HI / 1000, color="#f0b400", alpha=.18, lw=0)
    ax.axvline(0, color="#555", lw=.8, ls="--"); ax.axvline(EST_D / 1000, color="#c0392b", lw=1)
    ax.grid(axis="y", color="#e5e5e5", lw=.6); ax.spines[["top", "right"]].set_visible(False)
a.annotate("falls", (0, a.get_ylim()[1]), xytext=(3, -12), textcoords="offset points", fontsize=8)
a.annotate("estimated canyon end", (EST_D / 1000, a.get_ylim()[1]), xytext=(4, -24), textcoords="offset points", fontsize=8, color="#c0392b")
fig.suptitle("Cranberry River below the falls: elevation and side relief", fontsize=11)
fig.tight_layout(); fig.savefig(OUT / "profile.png", dpi=130)

# zoom on the first 2 km
b.set_xlim(-1, 2); a.set_xlim(-1, 2); a.set_ylim(185, 220); b.set_ylim(0, 25); fig.savefig(OUT / "profile_zoom.png", dpi=130)

falls = json.load(open(OUT / "snap.json"))
feats = [
    dict(type="Feature", properties=dict(name="DFO falls point (canyon top, as given)", role="falls"),
         geometry=dict(type="Point", coordinates=[-128.68426, 55.56407])),
    dict(type="Feature", properties=dict(name="Cranberry River Canyon (downstream end, estimated)", role="estimate",
         d_below_falls_m=EST_D, range_m=[LO, HI]), geometry=dict(type="Point", coordinates=[est.lon, est.lat])),
    dict(type="Feature", properties=dict(name="uncertainty range", role="range"),
         geometry=dict(type="LineString", coordinates=df[(df.d_below_falls >= LO) & (df.d_below_falls <= HI)][["lon", "lat"]].values.tolist())),
    dict(type="Feature", properties=dict(name="profile line (2 km above falls to Nass confluence)", role="profile"),
         geometry=dict(type="LineString", coordinates=df[["lon", "lat"]].values.tolist())),
]
gj = dict(type="FeatureCollection", features=feats)
json.dump(gj, open(OUT / "cranberry_canyon.geojson", "w"), indent=1)
html = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Cranberry Canyon Estimate</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>html,body,#m{height:100%;margin:0;background:#fff}</style></head><body><div id="m"></div><script>
const gj=__GJ__;
const m=L.map('m');
L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',{maxZoom:17,attribution:'OpenTopoMap'}).addTo(m);
const col={falls:'#555',estimate:'#c0392b',range:'#f0b400',profile:'#3a78c2'};
const lay=L.geoJSON(gj,{style:f=>({color:col[f.properties.role],weight:f.properties.role==='range'?7:3}),
 pointToLayer:(f,ll)=>L.circleMarker(ll,{radius:7,color:col[f.properties.role],fillOpacity:.9}),
 onEachFeature:(f,l)=>l.bindPopup(f.properties.name)}).addTo(m);
m.fitBounds(L.geoJSON(gj.features[2]).getBounds().pad(3));
</script></body></html>"""
(OUT / "map.html").write_text(html.replace("__GJ__", json.dumps(gj)))
print(est.to_dict())
