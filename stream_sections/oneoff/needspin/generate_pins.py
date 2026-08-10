"""Generate offline_pins.json (FWA-only, no live OSM) for the 28 needs-pin rows.
Schema: {rid: {"method": str, "pins": [[label, [lat,lon] | None], ...]}}
Coords lat,lon. Consumed by build_queue.py -> curation-review-queue.md.

Run from repo root:  PYTHONPATH=stream_sections/oneoff/needspin .venv/bin/python \\
                        stream_sections/oneoff/needspin/generate_pins.py
Writes offline_pins.json next to this script."""
import json, os, sys, numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fwa_helpers as H
_HERE = os.path.dirname(os.path.abspath(__file__))

P = {}
def river(name, mus):
    l, _ = H.mainstem(name, mus)
    return l if l is not None else H.river_any(name, mus)
def LL(pt): return list(H.latlon(pt))
def walk(l, km): return LL(H.walk_from(l, 0, km))
def fsr(l, maxkm=99):
    return [(LL(p), m/1000, nm) for p, m, nm in H.fsr_crossings(l) if m/1000 <= maxkm]

# ---------------- A. STRONG ----------------
# Copper: two FSR bridges above mouth; reg "second bridge 6km above tidal"
l = river('Copper Creek', ['6-12']); fx = fsr(l, 12)
P['copper-creek-538967-b'] = {'method': 'FWA river x FSR: 2 logging bridges above mouth. Reg="second bridge 6 km above tidal" -> the 2nd = South Bay Main. (mouth=tidal ref)',
  'pins': [['South Bay Main bridge (2nd, ~7.7km up) — PICK', [53.11437, -131.80771]],
           ['Copper Bay M/L (1st, ~3.9km up)', [53.134, -131.80497]]]}
# Eve: JOINT SOUTHMAIN FSR = South Main Bridge
P['eve-river-41b1eb'] = {'method': 'FWA river x FSR "JOINT SOUTHMAIN" @1.8km = the South Main Bridge crossing named in reg.',
  'pins': [['South Main Bridge (JOINT SOUTHMAIN FSR)', [50.4247, -126.24777]]]}
# Artlish: bridge ~10km from mouth; 2 FSR bridges bracket it
l = river('Artlish River', ['1-12'])
P['artlish-river-9e60a3'] = {'method': 'Reg="bridge ~10 km from mouth". Two FSR bridges bracket 10km; walk-10km pin between them. Eyeball which is signed.',
  'pins': [['~10 km walk pin (between the two)', walk(l, 10.0)],
           ['FSR (9217) bridge @8.8km', [50.11567, -126.97894]],
           ['FSR LA1000 bridge @11.5km', [50.12006, -126.94515]]]}
# Mamin: third bridge ~10km above tidal -> walk pin (only 2 FSR in data)
l = river('Mamin River', ['6-13'])
P['mamin-river-608d92'] = {'method': 'Reg="third bridge ~10 km above tidal". Only Datlamen(4.6km)/Mamin Main(28km) in FSR data; walk-10km pin is the springboard for the 3rd bridge.',
  'pins': [['~10 km upstream walk pin (third-bridge area)', walk(l, 10.0)]]}
# Mohun: MENZMN.2 mainline bridge -> Morton Lake
l = river('Mohun Creek', ['1-10']); fx = fsr(l, 12)
menz = [pin for pin, km, nm in fx if 'MENZMN' in nm]
P['mohun-creek-1fa4a0-a'] = {'method': 'FWA river x FSR "MENZMN.2" (Menzies Bay mainline) @7.4km = reach start to Morton Lake.',
  'pins': [['Menzies Bay mainline bridge (MENZMN.2)', menz[0] if menz else walk(l, 7.4)]]}
# Thorn: Attichika confluence (Thorn not in FWA -> use Attichika mouth as the confluence anchor)
P['thorn-creek-b2b2c7-a'] = {'method': 'Thorn Ck NOT in FWA. Anchor = Attichika Creek confluence; using Attichika mouth (where it meets Thorn).',
  'pins': [['Attichika Creek confluence (Attichika mouth)', [56.96833, -126.87528]]]}

# ---------------- B. SPRINGBOARD (correct FWA channel) ----------------
l = river('Deena Creek', ['6-12'])
P['deena-creek-af3d2f'] = {'method': 'Reg="second bridge ~5 km above tidal"; no FSR near 5km in data. Walk-5km pin on FWA Deena -> eyeball the bridge.',
  'pins': [['~5 km upstream walk pin', walk(l, 5.0)]]}
P['north-alouette-river-e3810a'] = {'method': 'FWA North Alouette sampled at 216th St longitude (mainstem 216 St curated 49.2409,-122.62242 just S).',
  'pins': [['N. Alouette @ 216th St (Fifth Ave)', [49.25991, -122.62223]]]}
P['elk-river-s-tributaries-see-exceptions-e09244'] = {'method': 'Reg="Coal Ck d/s of old MF&M Railway Br 7 km up from Elk R". FWA Coal Ck walked 7km from mouth -> eyeball the rail bridge.',
  'pins': [['Coal Ck ~7 km up (MF&M rail br area)', [49.48831, -114.99223]],
           ['Coal Ck mouth at Elk R (ref)', [49.49786, -115.07037]]]}
l = river('Chowade River', ['7-43']); fx = fsr(l)
P['chowade-river-42a570'] = {'method': 'FWA river x FSR: exactly one road crossing @21.6km = Horseshoe Road Bridge.',
  'pins': [['Horseshoe Road Bridge (sole crossing)', fx[0][0] if fx else None]]}
l = river('Kitimat River', ['6-3'])
P['kitimat-river-angling-regulations-for-the-kit-b3b21f'] = {'method': 'Reg="tributaries and upstream of Hwy 37 bridge". FWA Kitimat near Kitimat townsite; Hwy 37 crosses lower valley -> eyeball.',
  'pins': [['Kitimat R ~ Hwy37 area (lower valley springboard)', walk(l, 6.0)]]}
P['kitimat-river-angling-regulations-for-the-kit-664c72'] = {'method': 'WEST-HALF partial-width closure near Kitimat Hatchery outfall (NOT clean cross-channel). FWA mouth ref.',
  'pins': [['Kitimat R mouth (hatchery outfall reach ref)', [54.01089, -128.66227]]]}
l = river('Ptarmigan Creek', ['7-5'])
P['ptarmigan-creek-dd88fc-b'] = {'method': 'Quarry Bridge on Ptarmigan Ck (Robson valley). No FSR/OSM hit offline; FWA channel + mouth as springboard.',
  'pins': [['Ptarmigan Ck mouth (Quarry Br upstream) — eyeball', LL(l.interpolate(0))]]}
l = river('Chuckwalla River', ['5-7'])
P['chuckwalla-river-eec2f0'] = {'method': 'Reg="Ten Mile Pool" (named pool ~10 mi up). Walk 10 mi (16 km) on FWA Chuckwalla -> eyeball the pool.',
  'pins': [['~10 mi (16 km) upstream walk pin', walk(l, 16.0)]]}
l = river('Pinkut Creek', ['6-6'])
P['pinkut-creek-03a36e'] = {'method': 'DFO Pinkut Ck fish fence / spawning channel. FWA mouth (Babine) as springboard; fence is near mouth reach.',
  'pins': [['Pinkut Ck mouth @ Babine (fish fence nearby)', LL(l.interpolate(0))]]}
l = river('Swift Creek', ['7-2'])
P['swift-creek-a32fd8-a'] = {'method': 'Weir (u/s side) d/s of CNR Br in Valemount (CNR br curated 52.83822,-119.26786). FWA mouth as springboard.',
  'pins': [['Swift Ck mouth @ Valemount (weir just u/s)', LL(l.interpolate(0))]]}
P['thorn-creek-b2b2c7-b'] = {'method': 'Point 500 m upstream of Attichika confl. Thorn not in FWA -> approximate 500m up from the Attichika-mouth anchor (needs Thorn channel to finalize).',
  'pins': [['~500 m u/s of Attichika confl (approx)', None]]}
l = river('Cranberry River', ['6-15'])
P['cranberry-river-699352-a'] = {'method': 'Signs bracket "Cranberry River Canyon"; canyon not in FWA falls layer. Cranberry-Kiteen jct as locator ref. Needs topo/map canyon pin (u/s sign).',
  'pins': [['Cranberry-Kiteen junction (ref only)', [55.51394, -128.8105]]]}
P['cranberry-river-699352-b'] = {'method': 'Downstream-of-canyon sign; pairs with -a once the canyon is located on topo.',
  'pins': [['(pair with -a canyon)', None]]}
P['thompson-river-downstream-of-signs-at-kamloop-1c9ce3'] = {'method': 'Reg="1 km d/s of Martel" (rail locality on the Thompson, Ashcroft-Spences Bridge). Needs Martel gazetteer/topo -> then 1 km downstream.',
  'pins': [['1 km d/s of Martel (needs Martel locality)', None]]}

# ---------------- C. TOWNSITE (FWA name/MU miss; townsite eyeball) ----------------
P['burton-creek-b25ef3-b'] = {'method': 'Hwy 6 bridge over Burton Ck at Burton (Arrow Lakes). FWA "Burton Creek" collides w/ Adams-Lk creek; correct creek is by Woden confl (49.92206,-117.88365). Eyeball Hwy 6 crossing near there.',
  'pins': [['Burton Ck @ Woden confl (Hwy 6 br just u/s) — eyeball', [49.92206, -117.88365]]]}
P['trepanier-river-a999b0-a'] = {'method': 'Hwy 97C (Okanagan Connector) over Trepanier Ck at Peachland. FWA "Trepanier River" name miss. Eyeball 97C crossing above Okanagan Lk.',
  'pins': [['Trepanier Ck @ Peachland / Hwy 97C — eyeball', [49.7735, -119.7365]]]}
P['capilano-river-496399'] = {'method': 'Footbridge ~100m d/s of hatchery fish fence (below Cleveland Dam). OSM Capilano Pacific Trail footbridge.',
  'pins': [['Capilano Pacific Trail footbridge (OSM way 60608394)', [49.35037, -123.11759]]]}
P['hays-creek-in-prince-rupert-6e14c4'] = {'method': 'Lower culvert near fish cannery, Prince Rupert (urban; incl "Oldfield" Ck). OSM Hays Cove Circle @ 6th Ave as springboard.',
  'pins': [['Hays Ck area, Prince Rupert (OSM Hays Cove Circle)', [54.31886, -130.30971]]]}
P['ksi-x-anmas-river-formerly-kwinamass-river-fe2f0b'] = {'method': "Lower bridge abutments on Ksi X'anmas (Kwinamass) R. Remote coastal river N of Prince Rupert; not in FWA under either name -> needs topo/map.",
  'pins': [['(remote coastal — needs topo)', None]]}

# ---------------- D. NEEDS FISS ----------------
l = river('Ptarmigan Creek', ['7-5'])
P['ptarmigan-creek-dd88fc-a'] = {'method': 'Falls on Ptarmigan Ck (reach start). Not in FWA waterfalls layer -> needs FISS/topo. FWA mouth as area springboard.',
  'pins': [['Ptarmigan Ck mouth (falls upstream) — needs FISS', LL(l.interpolate(0))]]}
P['white-river-da5ee3'] = {'method': 'Salmon viewing pool near Sayward; sibling Sayward Road Bridge curated 50.30805,-125.91741. Viewing pool is a signed reach nearby -> needs FISS/local map.',
  'pins': [['Near Sayward Road Bridge (viewing pool nearby)', [50.30805, -125.91741]]]}

# ---------------- E. LIKELY n/a ----------------
P['skeena-river-mainstem-only-c2e0df'] = {'method': 'Bait-ban applies to WHOLE "Skeena River Section 4" (already defined by sibling boundary rows). No new point split -> PROPOSE n/a.',
  'pins': [['(no split — propose n/a)', None]]}

json.dump(P, open(os.path.join(_HERE, 'offline_pins.json'), 'w'), indent=1)
print('wrote offline_pins.json;', len(P), 'rows')
miss = [k for k,v in P.items() if all(pin[1] is None for pin in v['pins'])]
print('rows with NO coord (need topo/FISS):', miss)
